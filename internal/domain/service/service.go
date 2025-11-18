package service

import (
	"time"
	"context"

	"github.com/rs/zerolog"

	"github.com/go-clearance/shared/erro"
	"github.com/go-clearance/internal/domain/model"

	database "github.com/go-clearance/internal/infrastructure/repo/database"

	go_core_http "github.com/eliezerraj/go-core/http"
	go_core_db_pg "github.com/eliezerraj/go-core/database/postgre"
	go_core_otel_trace "github.com/eliezerraj/go-core/otel/trace"
)

var tracerProvider go_core_otel_trace.TracerProvider

type WorkerService struct {
	appServer			*model.AppServer
	workerRepository	*database.WorkerRepository
	logger 				*zerolog.Logger
	httpService			*go_core_http.HttpService		 	
}

// About new worker service
func NewWorkerService(appServer	*model.AppServer,
					  workerRepository *database.WorkerRepository, 
					  appLogger *zerolog.Logger) *WorkerService {
	logger := appLogger.With().
						Str("package", "domain.service").
						Logger()
	logger.Info().
			Str("func","NewWorkerService").Send()

	httpService := go_core_http.NewHttpService(&logger)					

	return &WorkerService{
		appServer: appServer,
		workerRepository: workerRepository,
		logger: &logger,
		httpService: httpService,
	}
}

// About database stats
func (s *WorkerService) Stat(ctx context.Context) (go_core_db_pg.PoolStats){
	s.logger.Info().
			Str("func","Stat").Send()

	return s.workerRepository.Stat(ctx)
}

// About check health service
func (s * WorkerService) HealthCheck(ctx context.Context) error {
	s.logger.Info().
			Str("func","HealthCheck").Send()

	// Check database health
	err := s.workerRepository.DatabasePG.Ping()
	if err != nil {
		s.logger.Error().
				Err(err).Msg("*** Database HEALTH FAILED ***")
		return erro.ErrHealthCheck
	}

	s.logger.Info().
			Str("func","HealthCheck").
			Msg("*** Database HEALTH SUCCESSFULL ***")

	return nil
}

// About create a payment
func (s *WorkerService) AddPayment(ctx context.Context, 
									payment *model.Payment) (*model.Payment, error){
	// trace and log
	ctx, span := tracerProvider.SpanCtx(ctx, "service.AddPayment")

	s.logger.Info().
			Ctx(ctx).
			Str("func","AddPayment").Send()

	// prepare database
	tx, conn, err := s.workerRepository.DatabasePG.StartTx(ctx)
	if err != nil {
		return nil, err
	}

	// handle connection
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
		s.workerRepository.DatabasePG.ReleaseTx(conn)
		span.End()
	}()

	// prepare data
	payment.CreatedAt = time.Now()

	// Create payment
	resPayment, err := s.workerRepository.AddPayment(ctx, tx, payment)
	if err != nil {
		return nil, err
	}
	payment.ID = resPayment.ID

	return payment, nil
}

// About get payment
func (s * WorkerService) GetPayment(ctx context.Context, 
									payment *model.Payment) (*model.Payment, error){
	// trace and log
	ctx, span := tracerProvider.SpanCtx(ctx, "service.GetPayment")
	defer span.End()

	s.logger.Info().
			Ctx(ctx).
			Str("func","GetPayment").Send()

	// Call a service
	resCart, err := s.workerRepository.GetPayment(ctx, payment)
	if err != nil {
		return nil, err
	}
								
	return resCart, nil
}

// About get payment
func (s * WorkerService) GetPaymentFromOrder(ctx context.Context, 
											order *model.Order) (*[]model.Payment, error){
	// trace and log
	ctx, span := tracerProvider.SpanCtx(ctx, "service.GetPaymentFromOrder")
	defer span.End()

	s.logger.Info().
			Ctx(ctx).
			Str("func","GetPaymentFromOrder").Send()

	// Call a service
	resPayment, err := s.workerRepository.GetPaymentFromOrder(ctx, order)
	if err != nil {
		return nil, err
	}
								
	return resPayment, nil
}