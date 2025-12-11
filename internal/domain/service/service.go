package service

import (
	"fmt"
	"time"
	"context"
	"sync"
	"errors"
	"net/http"
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
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

// about do http call 
func (s *WorkerService) doHttpCall(ctx context.Context,
									httpClientParameter go_core_http.HttpClientParameter) (interface{},error) {
		
	resPayload, statusCode, err := s.httpService.DoHttp(ctx, 
														httpClientParameter)
	if err != nil {
		s.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, err
	}

	if statusCode != http.StatusOK {
		if statusCode == http.StatusNotFound {
			s.logger.Error().
					Ctx(ctx).
					Err(erro.ErrNotFound).Send()
			return nil, erro.ErrNotFound
		} else {		
			jsonString, err  := json.Marshal(resPayload)
			if err != nil {
				s.logger.Error().
						Ctx(ctx).
						Err(err).Send()
				return nil, errors.New(err.Error())
			}			
			
			message := model.APIError{}
			json.Unmarshal(jsonString, &message)

			newErr := errors.New(fmt.Sprintf("http call error: status code %d - message: %s", message.StatusCode ,message.Msg))
			s.logger.Error().
					Ctx(ctx).
					Err(newErr).Send()
			return nil, newErr
		}
	}

	return resPayload, nil
}

// register a new step proccess
func registerOrchestrationProcess(nameStepProcess string,
								 listStepProcess *[]model.StepProcess) {

	stepProcess := model.StepProcess{Name: nameStepProcess,
									ProcessedAt: time.Now(),}

	*listStepProcess = append(*listStepProcess, stepProcess)								
}

// About database stats
func (s *WorkerService) Stat(ctx context.Context) (go_core_db_pg.PoolStats){
	s.logger.Info().
			Ctx(ctx).
			Str("func","Stat").Send()

	return s.workerRepository.Stat(ctx)
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

// About check health service
func (s * WorkerService) HealthCheck(ctx context.Context) error {
	s.logger.Info().
			Str("func","HealthCheck").Send()

	ctx, span := tracerProvider.SpanCtx(ctx, "service.HealthCheck")
	defer span.End()

	// Check database health
	_, spanDB := tracerProvider.SpanCtx(ctx, "DatabasePG.Ping")

	err := s.workerRepository.DatabasePG.Ping()
	if err != nil {
		s.logger.Error().
				Err(err).Msg("*** Database HEALTH CHECK FAILED ***")
		return erro.ErrHealthCheck
	}

	spanDB.End()
	
	s.logger.Info().
			Str("func","HealthCheck").
			Msg("*** Database HEALTH CHECK SUCCESSFULL ***")

	return nil
}

// About create a payment
func (s *WorkerService) AddPayment(ctx context.Context, 
									payment *model.Payment) (*model.Payment, error){
	s.logger.Info().
			Ctx(ctx).
			Str("func","AddPayment").Send()

	// trace and log
	ctx, span := tracerProvider.SpanCtx(ctx, "service.AddPayment")

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

	// get order details
	// prepare headers http for calling services
	trace_id := fmt.Sprintf("%v",ctx.Value("trace-request-id"))

	headers := map[string]string{
		"Content-Type":  "application/json;charset=UTF-8",
		"X-Request-Id": trace_id,
	}

	httpClientParameter := go_core_http.HttpClientParameter {
		Url:	fmt.Sprintf("%s%s%v", (*s.appServer.Endpoint)[0].Url , "/order/" , payment.Order.ID ),
		Method:	"GET",
		Timeout: (*s.appServer.Endpoint)[0].HttpTimeout,
		Headers: &headers,
	}

	// call a service via http
	resPayload, err := s.doHttpCall(ctx, 
									httpClientParameter)
	if err != nil {
		return nil, err
	}

	// convert json to struct
	jsonString, err  := json.Marshal(resPayload)
	if err != nil {
		s.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
	}
	order := model.Order{}
	json.Unmarshal(jsonString, &order)

	// prepare data
	now := time.Now()
	payment.CreatedAt = now
	payment.Order = &order

	// Create payment
	resPayment, err := s.workerRepository.AddPayment(ctx, tx, payment)
	if err != nil {
		return nil, err
	}
	payment.ID = resPayment.ID

	// create saga orchestration process
	listStepProcess := []model.StepProcess{}

	// emit a event
	if s.workerEvent != nil {
		event := model.Event{
			ID: fmt.Sprintf("%v",payment.ID),
			Type: "cleareance.order",
			EventAt: now,
			EventData: payment,
		}
		err = s.ProducerEventKafka(ctx, 
									payment.Transaction,
									&event)
		if err != nil {
			return nil, err
		}
		registerOrchestrationProcess("RECONCILIATION MSG KAFKA:OK", &listStepProcess)
	} else {
		registerOrchestrationProcess("RECONCILIATION MSG KAFKA:NOK KAFKA UNABLE *nil)", &listStepProcess)
	}

	payment.StepProcess = &listStepProcess
	return payment, nil
}

// About get payment
func (s * WorkerService) GetPayment(ctx context.Context, 
									payment *model.Payment) (*model.Payment, error){
	s.logger.Info().
			Ctx(ctx).
			Str("func","GetPayment").Send()

	// trace and log
	ctx, span := tracerProvider.SpanCtx(ctx, "service.GetPayment")
	defer span.End()

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
	s.logger.Info().
			Ctx(ctx).
			Str("func","GetPaymentFromOrder").Send()

	// trace and log
	ctx, span := tracerProvider.SpanCtx(ctx, "service.GetPaymentFromOrder")
	defer span.End()

	// Call a service
	resPayment, err := s.workerRepository.GetPaymentFromOrder(ctx, order)
	if err != nil {
		return nil, err
	}
								
	return resPayment, nil
}

// About producer a event in kafka
func(s *WorkerService) ProducerEventKafka(ctx context.Context,
										  key string, 
										  event *model.Event) (err error) {
	s.logger.Info().
			Ctx(ctx).
			Str("func","ProducerEventKafka").Send()
			
	// trace and log
	ctx, span := tracerProvider.SpanCtx(ctx, "service.ProducerEventKafka")
	defer span.End()

	trace_id := fmt.Sprintf("%v",ctx.Value("trace-request-id"))

	// create a mutex to avoid commit over a transaction on air
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.logger.Info().
			Ctx(ctx).
			Str("func","ProducerEventKafka").Msg("BeginTransaction")
	// Create a transaction
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
	payload_bytes, err := json.Marshal(event)
	if err != nil {
		s.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return err
	}

	// prepare header
	kafkaHeaders := []kafka.Header{}
	appCarrier := KafkaHeaderCarrier{Headers: &kafkaHeaders}
	otel.GetTextMapPropagator().Inject(ctx, appCarrier)
	appCarrier.Set("trace-request-id", trace_id)

	s.logger.Printf("=========================================================")
    for _, h := range kafkaHeaders {
        s.logger.Printf("Header: %s = %s\n", h.Key, string(h.Value))
    }
	s.logger.Printf("=========================================================")
	
	//--------------------------------------------------------
	// publish event
	// ------------------------------------------------------
	s.logger.Info().
			Ctx(ctx).
			Str("func","ProducerEventKafka").Msg("Producer MSG KAFKA")
	err = s.workerEvent.ProducerWorker.Producer(s.workerEvent.Topics[0], 
												key, 
												kafkaHeaders,
												payload_bytes)
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

	s.logger.Info().
			Ctx(ctx).
			Str("func","ProducerEventKafka").Msg("CommitTransaction")
	err = s.workerEvent.ProducerWorker.CommitTransaction(ctx)
	if err != nil {
		s.logger.Error().
				Ctx(ctx).
				Err(err).
				Msg("Failed to Kafka CommitTransaction = KAFKA ROLLBACK COMMIT !!!")

		errMskAbort := s.workerEvent.ProducerWorker.AbortTransaction(ctx)
		if errMskAbort != nil {
			s.logger.Error().
				Ctx(ctx).
				Err(errMskAbort).
				Msg("failed to kafka AbortTransaction during CommitTransaction")
			return errMskAbort
		}
		return err
	}

	s.logger.Info().
			Ctx(ctx).
			Msg("KAFKA PRODUCER COMMIT SUCCESS !!!")

    return 
}

// ----------------------------------------------
// Helper kafka header OTEL
// ----------------------------------------------

type KafkaHeaderCarrier struct {
	Headers *[]kafka.Header
}

func (c KafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c KafkaHeaderCarrier) Set(key string, value string) {
	// remove existing key
	newHeaders := make([]kafka.Header, 0)
	for _, h := range *c.Headers {
		if h.Key != key {
			newHeaders = append(newHeaders, h)
		}
	}
	// append new key
	newHeaders = append(newHeaders, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
	*c.Headers = newHeaders
}

func (c KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.Headers))
	for _, h := range *c.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

func (c KafkaHeaderCarrier) MapToKafkaHeaders(m map[string]string) []kafka.Header {
    hdrs := make([]kafka.Header, 0, len(m))
    for k, v := range m {
        hdrs = append(hdrs, kafka.Header{
            Key:   k,
            Value: []byte(v),
        })
    }
    return hdrs
}