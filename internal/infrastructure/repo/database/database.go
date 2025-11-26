package database

import (
		"context"
		"errors"
		"database/sql"

		"github.com/rs/zerolog"
		"github.com/jackc/pgx/v5"

		"github.com/go-clearance/shared/erro"
		"github.com/go-clearance/internal/domain/model"

		go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
		go_core_db_pg "github.com/eliezerraj/go-core/v2/database/postgre"
)

var tracerProvider go_core_otel_trace.TracerProvider

type WorkerRepository struct {
	DatabasePG *go_core_db_pg.DatabasePGServer
	logger		*zerolog.Logger
}

// Above new worker
func NewWorkerRepository(databasePG *go_core_db_pg.DatabasePGServer,
						appLogger *zerolog.Logger) *WorkerRepository{
	logger := appLogger.With().
						Str("package", "repo.database").
						Logger()
	logger.Info().
			Str("func","NewWorkerRepository").Send()

	return &WorkerRepository{
		DatabasePG: databasePG,
		logger: &logger,
	}
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
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "database.AddPayment")
	defer span.End()

	w.logger.Info().
			Ctx(ctx).
			Str("func","AddPayment").Send()

	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
	}
	defer w.DatabasePG.Release(conn)

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
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
	}

	// Set PK
	payment.ID = id
	
	return payment , nil
}

// About get a cart_item
func (w *WorkerRepository) GetPayment(	ctx context.Context,
										payment *model.Payment) (*model.Payment, error) {
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "database.GetPayment")
	defer span.End()

	w.logger.Info().
			Ctx(ctx).
			Str("func","GetPayment").Send()

	// db connection
	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
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
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
	}
	defer rows.Close()
	
    if err := rows.Err(); err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Msg("fatal error closing rows")
        return nil, errors.New(err.Error())
    }

	resOrder := model.Order{}
	resPayment := model.Payment{Order: &resOrder}

	var nullPaymentUpdatedAt sql.NullTime

	for rows.Next() {
		err := rows.Scan(	&resPayment.ID,
							&resPayment.Order.ID, 
							&resPayment.Transaction,
							&resPayment.Type,
							&resPayment.Status,
							&resPayment.Currency,
							&resPayment.Amount,
							&resPayment.CreatedAt,
							&nullPaymentUpdatedAt,
						)
		if err != nil {
			w.logger.Error().
					Ctx(ctx).
					Err(err).Send()
			return nil, errors.New(err.Error())
        }

		if nullPaymentUpdatedAt.Valid {
        	resPayment.UpdatedAt = &nullPaymentUpdatedAt.Time
    	} else {
			resPayment.UpdatedAt = nil
		}
	}

	if resPayment.ID == 0 {
		w.logger.Warn().
				Ctx(ctx).
				Err(erro.ErrNotFound).
				Interface("payment.ID",payment.ID).Send()
		return nil, erro.ErrNotFound
	}
		
	return &resPayment, nil
}

// About get a payment from an order
func (w *WorkerRepository) GetPaymentFromOrder(	ctx context.Context,
												order *model.Order) (*[]model.Payment, error) {
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "database.GetPaymentFromOrder")
	defer span.End()

	w.logger.Info().
			Ctx(ctx).
			Str("func","GetPaymentFromOrder").Send()

	// db connection
	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
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
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
	}
	defer rows.Close()
	
    if err := rows.Err(); err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Msg("fatal error closing rows")
        return nil, errors.New(err.Error())
    }

	resOrder := model.Order{}
	resPayment := model.Payment{Order: &resOrder}
	listPayment := []model.Payment{}

	var nullPaymentUpdatedAt sql.NullTime

	for rows.Next() {
		err := rows.Scan(	&resPayment.ID,
							&resOrder.ID, 
							&resPayment.Transaction,
							&resPayment.Type,
							&resPayment.Status,
							&resPayment.Currency,
							&resPayment.Amount,
							&resPayment.CreatedAt,
							&nullPaymentUpdatedAt,
						)
		if err != nil {
			w.logger.Error().
					Ctx(ctx).
					Err(err).Send()
			return nil, errors.New(err.Error())
        }

		if nullPaymentUpdatedAt.Valid {
        	resPayment.UpdatedAt = &nullPaymentUpdatedAt.Time
    	} else {
			resPayment.UpdatedAt = nil
		}

		listPayment = append(listPayment, resPayment)
	}

	return &listPayment, nil
}
