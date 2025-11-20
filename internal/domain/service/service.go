package service

import (
	"fmt"
	"time"
	"context"
	"sync"
	"encoding/json"

	"github.com/rs/zerolog"

	"github.com/go-clearance/shared/erro"
	"github.com/go-clearance/internal/domain/model"

	"github.com/go-clearance/internal/infrastructure/repo/database"
	"github.com/go-clearance/internal/infrastructure/adapter/event"

	go_core_http 		"github.com/eliezerraj/go-core/v2/http"
	go_core_db_pg 		"github.com/eliezerraj/go-core/v2/database/postgre"
	go_core_otel_trace 	"github.com/eliezerraj/go-core/v2/otel/trace"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var tracerProvider go_core_otel_trace.TracerProvider

type WorkerService struct {
	appServer			*model.AppServer
	workerRepository	*database.WorkerRepository
	logger 				*zerolog.Logger
	workerEvent			*event.WorkerEvent
	httpService			*go_core_http.HttpService
	mutex    				sync.Mutex 	 	
}

// About new worker service
func NewWorkerService(appServer	*model.AppServer,
					  workerRepository *database.WorkerRepository, 
					  workerEvent	*event.WorkerEvent,
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
		workerEvent: workerEvent,
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
				Err(err).Msg("*** Database HEALTH CHECK FAILED ***")
		return erro.ErrHealthCheck
	}

	s.logger.Info().
			Str("func","HealthCheck").
			Msg("*** Database HEALTH CHECK SUCCESSFULL ***")

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
	now := time.Now()
	payment.CreatedAt = now

	// Create payment
	resPayment, err := s.workerRepository.AddPayment(ctx, tx, payment)
	if err != nil {
		return nil, err
	}
	payment.ID = resPayment.ID

	if s.workerEvent != nil {
		event := model.Event{
			ID: "teste",
			Type: "teste-2",
			EventAt: now,
			EventData: payment,
		}
		err = s.ProducerEventKafka(ctx, 
									payment.Transaction,
									&event)
		if err != nil {
			return nil, err
		}
		//res_moviment_transaction.Status = "event sended via kafka"
	} else {
		//res_moviment_transaction.Status = "event not send, kafka unabled"
	}
	
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

// About producer a event in kafka
func(s *WorkerService) ProducerEventKafka(	ctx context.Context,
											key string, 
											event *model.Event) (err error) {
	// trace and log
	ctx, span := tracerProvider.SpanCtx(ctx, "service.ProducerEventKafka")
	defer span.End()

	s.logger.Info().
			Ctx(ctx).
			Str("func","ProducerEventKafka").Send()

	trace_id := fmt.Sprintf("%v",ctx.Value("trace-request-id"))

	// create a mutex to avoid commit over a transaction on air
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Create a transacrion
	err = s.workerEvent.ProducerWorker.BeginTransaction()
	if err != nil {
		s.logger.Error().
				Ctx(ctx).
				Err(err).Msg("failed to kafka BeginTransaction")

		// Create a new producer and start a transaction
		/*err = s.workerEvent.DestroyWorkerEventProducerTx(ctx)
		if err != nil {
			return  err
		}
		s.workerEvent.WorkerKafka.BeginTransaction()
		if err != nil {
			return err
		}
		childLogger.Info().Interface("trace-request-id", trace_id ).Msg("success to recreate a new producer")*/
	}

	// Prepare to event
	//key := strconv.Itoa(key)
	payload_bytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
		
	// prepare header
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, &carrier)
	
	headers_msk := make(map[string]string)
	for k, v := range carrier {
		headers_msk[k] = v
	}

	spanContext := span.SpanContext()
	headers_msk["trace-request-id"] = trace_id
	headers_msk["TraceID"] = spanContext.TraceID().String()
	headers_msk["SpanID"] = spanContext.SpanID().String()

	// publish event
	err = s.workerEvent.ProducerWorker.Producer(s.workerEvent.Topics[0], 
												key, 
												&headers_msk, 
												payload_bytes)
		
	//force a error SIMULARTION
	//if(trace_id == "force-rollback"){
	//	err = erro.ErrForceRollback
	//}
	
	if err != nil {
		s.logger.Error().
				Ctx(ctx).
				Err(err).Msg("KAFKA ROLLBACK !!!")
		err_msk := s.workerEvent.ProducerWorker.AbortTransaction(ctx)
		if err_msk != nil {
			s.logger.Error().
					Ctx(ctx).
					Err(err).Msg("failed to kafka AbortTransaction")
			return err_msk
		}
		return err
	}

	err = s.workerEvent.ProducerWorker.CommitTransaction(ctx)
	if err != nil {
		s.logger.Error().
				Ctx(ctx).
				Err(err).Msg("Failed to Kafka CommitTransaction = KAFKA ROLLBACK COMMIT !!!")

		errMskAbort := s.workerEvent.ProducerWorker.AbortTransaction(ctx)
		if errMskAbort != nil {
			s.logger.Error().
				Ctx(ctx).
				Err(errMskAbort).Msg("failed to kafka AbortTransaction during CommitTransaction")
			return errMskAbort
		}
		return err
	}

	s.logger.Info().
			Ctx(ctx).Msg("KAFKA SUCCESS COMMIT !!!")

    return 
}