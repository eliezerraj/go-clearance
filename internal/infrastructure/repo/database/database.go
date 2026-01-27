package database

import (
	"time"
	"fmt"
	"context"
	"database/sql"

	"github.com/rs/zerolog"
	"github.com/jackc/pgx/v5"

	"github.com/go-clearance/shared/erro"
	"github.com/go-clearance/internal/domain/model"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/codes"
	
	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
	go_core_db_pg "github.com/eliezerraj/go-core/v2/database/postgre"
)

type WorkerRepository struct {
	DatabasePG 		*go_core_db_pg.DatabasePGServer
	logger			*zerolog.Logger
	tracerProvider 	*go_core_otel_trace.TracerProvider
}

// Above new worker
func NewWorkerRepository(databasePG *go_core_db_pg.DatabasePGServer,
						appLogger *zerolog.Logger,
						tracerProvider *go_core_otel_trace.TracerProvider) *WorkerRepository{
	logger := appLogger.With().
						Str("package", "repo.database").
						Logger()
	logger.Info().
			Str("func","NewWorkerRepository").Send()

	return &WorkerRepository{
		DatabasePG: databasePG,
		logger: &logger,
		tracerProvider: tracerProvider,
	}
}

// Helper function to convert nullable time to pointer
func (w *WorkerRepository) pointerTime(nt sql.NullTime) *time.Time {
	if nt.Valid {
		return &nt.Time
	}
	return nil
}

// Helper function to scan payment from rows iterator
func (w *WorkerRepository) scanPaymentFromRow(rows pgx.Rows) (*model.Payment, error) {
	payment := model.Payment{Order: &model.Order{}}
	var nullUpdatedAt sql.NullTime
	
	err := rows.Scan(&payment.ID,
					&payment.Order.ID,
					&payment.Transaction,
					&payment.Type,
					&payment.Status,
					&payment.Currency,
					&payment.Amount,
					&payment.CreatedAt,
					&nullUpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("FAILED to scan payment row: %w", err)
	}
	
	payment.UpdatedAt = w.pointerTime(nullUpdatedAt)
	return &payment, nil
}

// Above get stats from database
func (w *WorkerRepository) Stat(ctx context.Context) (go_core_db_pg.PoolStats){
	w.logger.Info().
			Ctx(ctx).
			Str("func","Stat").Send()
	
	stats := w.DatabasePG.Stat()

	resPoolStats := go_core_db_pg.PoolStats{
		AcquireCount:         stats.AcquireCount(),
		AcquiredConns:        stats.AcquiredConns(),
		CanceledAcquireCount: stats.CanceledAcquireCount(),
		ConstructingConns:    stats.ConstructingConns(),
		EmptyAcquireCount:    stats.EmptyAcquireCount(),
		IdleConns:            stats.IdleConns(),
		MaxConns:             stats.MaxConns(),
		TotalConns:           stats.TotalConns(),
	}

	return resPoolStats
}

// About create a clearance
func (w* WorkerRepository) AddPayment(	ctx context.Context, 
										tx pgx.Tx, 
										payment *model.Payment) (*model.Payment, error){
	w.logger.Info().
			Ctx(ctx).
			Str("func","AddPayment").Send()

	// trace
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.AddPayment", trace.SpanKindInternal)
	defer span.End()

	//Prepare
	var id int

	// Query Execute
	query := `INSERT INTO clearance ( 	fk_order_id,
										type,
										transaction_id,
										status,
										currency,
										amount,
										created_at) 
				VALUES($1, $2, $3, $4, $5, $6, $7) RETURNING id`

	row := tx.QueryRow(	ctx, 
						query,
						payment.Order.ID,
						payment.Type,
						payment.Transaction,
						payment.Status,
						payment.Currency,
						payment.Amount,
						payment.CreatedAt)
						
	if err := row.Scan(&id); err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, fmt.Errorf("FAILED to scan clearance ID: %w", err)
	}

	// Set PK
	payment.ID = id
	
	return payment , nil
}

// About get a cart_item
func (w *WorkerRepository) GetPayment(	ctx context.Context,
										payment *model.Payment) (*model.Payment, error) {
	w.logger.Info().
			Ctx(ctx).
			Str("func","GetPayment").Send()

	// trace
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.GetPayment", trace.SpanKindInternal)
	defer span.End()

	// db connection
	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, fmt.Errorf("FAILED to acquire database connection: %w", err)
	}
	defer w.DatabasePG.Release(conn)

	// Query and Execute
	query := `select cl.id,
					cl.fk_order_id,
					cl.transaction_id,
					cl.type,
					cl.status,
					cl.currency,
					cl.amount,										
					cl.created_at,
					cl.updated_at
			from clearance cl
				where cl.id = $1`

	rows, err := conn.Query(ctx, 
						query, 
						payment.ID)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, fmt.Errorf("FAILED to query clearance: %w", err)
	}
	defer rows.Close()
	
    if err := rows.Err(); err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
			Ctx(ctx).
			Err(err).Msg("error iterating payment rows")
        return nil, fmt.Errorf("error iterating payment rows: %w", err)
    }

	var resPayment *model.Payment

	for rows.Next() {
		payment, err := w.scanPaymentFromRow(rows)
		if err != nil {
			span.RecordError(err) 
			span.SetStatus(codes.Error, err.Error())
			w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
			return nil, err
        }
		resPayment = payment
	}

	if resPayment == nil || resPayment.ID == 0 {
		w.logger.Warn().
			Ctx(ctx).
			Err(erro.ErrNotFound).
			Interface("payment.ID",payment.ID).Send()
		return nil, erro.ErrNotFound
	}
		
	return resPayment, nil
}

// About get a payment from an order
func (w *WorkerRepository) GetPaymentFromOrder(	ctx context.Context,
												order *model.Order) (*[]model.Payment, error) {
	w.logger.Info().
			Ctx(ctx).
			Str("func","GetPaymentFromOrder").Send()
			
	// trace
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.GetPaymentFromOrder", trace.SpanKindInternal)
	defer span.End()

	// db connection
	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, fmt.Errorf("FAILED to acquire database connection: %w", err)
	}
	defer w.DatabasePG.Release(conn)

	// Query and Execute
	query := `select cl.id,
					cl.fk_order_id,
					cl.transaction_id,
					cl.type,
					cl.status,
					cl.currency,
					cl.amount,										
					cl.created_at,
					cl.updated_at
			from clearance cl
				where cl.fk_order_id = $1`

	rows, err := conn.Query(ctx, 
						query, 
						order.ID)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, fmt.Errorf("FAILED to query clearance: %w", err)
	}
	defer rows.Close()
	
    if err := rows.Err(); err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
			Ctx(ctx).
			Err(err).Msg("error iterating payment rows")
        return nil, fmt.Errorf("error iterating payment rows: %w", err)
    }

	listPayment := []model.Payment{}

	for rows.Next() {
		payment, err := w.scanPaymentFromRow(rows)
		if err != nil {
			span.RecordError(err) 
			span.SetStatus(codes.Error, err.Error())
			w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
			return nil, err
        }

		listPayment = append(listPayment, *payment)
	}

	return &listPayment, nil
}
