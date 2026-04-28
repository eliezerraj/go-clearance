package service

import (
	"fmt"
	"time"
	"context"
	"encoding/json"

	"github.com/go-clearance/internal/domain/model"

	go_core_http "github.com/eliezerraj/go-core/v2/http"
	go_core_middleware "github.com/eliezerraj/go-core/v2/middleware"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/codes"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// register a new step proccess
func registerOrchestrationProcess(nameStepProcess string,
								 listStepProcess *[]model.StepProcess) {

	stepProcess := model.StepProcess{Name: nameStepProcess,
									ProcessedAt: time.Now(),}

	*listStepProcess = append(*listStepProcess, stepProcess)								
}

// Helper: Parse order from HTTP response payload
func (s *WorkerService) parseOrderFromPayload(ctx context.Context, payload interface{}) (*model.Order, error) {
	jsonString, err := json.Marshal(payload)
	if err != nil {
		s.logger.Error().Ctx(ctx).Err(err).Send()
		return nil, fmt.Errorf("FAILED to marshal response payload: %w", err)
	}
	
	order := &model.Order{}
	if err := json.Unmarshal(jsonString, order); err != nil {
		s.logger.Error().Ctx(ctx).Err(err).Send()
		return nil, fmt.Errorf("FAILED to unmarshal order: %w", err)
	}
	return order, nil
}

// About create a payment
func (s *WorkerService) AddPayment(ctx context.Context, 
									payment *model.Payment) (*model.Payment, error){
	s.logger.Info().
		Ctx(ctx).
		Str("func","AddPayment").Send()

	// trace and log
	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.AddPayment", trace.SpanKindServer)

	// prepare database
	tx, conn, err := s.workerRepository.DatabasePG.StartTx(ctx)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error().
			Ctx(ctx).
			Err(err).Send()
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

	// call a service to get order information for given order id
	endpoint, err := s.getServiceEndpoint(0)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	headers := s.buildHeaders(ctx)
	httpClientParameter := go_core_http.HttpClientParameter {
		Url:  fmt.Sprintf("%s%s%v", endpoint.Url, "/order/", payment.Order.ID),
		Method: "GET",
		Timeout: endpoint.HttpTimeout,
		Headers: &headers,
	}

	// call a service via http
	resPayload, err := s.doHttpCall(ctx, httpClientParameter)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	order, err := s.parseOrderFromPayload(ctx, resPayload)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// prepare data
	now := time.Now()
	payment.CreatedAt = now
	payment.Order = order

	if payment.Amount <= 0 {
		err = fmt.Errorf("payment amount must be greater than zero")
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}

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
			span.RecordError(err) 
			span.SetStatus(codes.Error, err.Error())
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
	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.GetPayment", trace.SpanKindServer)
	defer span.End()

	// Call a service
	resCart, err := s.workerRepository.GetPayment(ctx, payment)
	if err != nil {
		return nil, err
	}
								
	return resCart, nil
}

// About get payment from given order
func (s * WorkerService) GetPaymentFromOrder(ctx context.Context, 
											order *model.Order) (*[]model.Payment, error){
	s.logger.Info().
		Ctx(ctx).
		Str("func","GetPaymentFromOrder").Send()

	// trace and log
	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.GetPaymentFromOrder", trace.SpanKindServer)
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
	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.ProducerEventKafka", trace.SpanKindProducer)
	defer span.End()

	// create a mutex to avoid commit over a transaction on air
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.logger.Info().
			Ctx(ctx).
			Str("func","ProducerEventKafka").Msg("BeginTransaction")
	// Create a transaction
	err = s.workerEvent.ProducerWorker.BeginTransaction()
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error().
			Ctx(ctx).
			Err(err).Msg("FAILED to kafka BeginTransaction")

		// Create a new producer and start a transaction
		/*err = s.workerEvent.DestroyWorkerEventProducerTx(ctx)
		if err != nil {
			return  err
		}
		s.workerEvent.WorkerKafka.BeginTransaction()
		if err != nil {
			return err
		}
		childLogger.Info().Interface("request-id", trace_id ).Msg("success to recreate a new producer")*/
	}

	// Prepare to event
	payload_bytes, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return err
	}

	// prepare header
	kafkaHeaders := []kafka.Header{}
	appCarrier := KafkaHeaderCarrier{Headers: &kafkaHeaders}
	otel.GetTextMapPropagator().Inject(ctx, appCarrier)

	requestID := go_core_middleware.GetRequestID(ctx)
	if requestID == "" {
		requestID = "not-found:from-service"
	}

	appCarrier.Set(string(go_core_middleware.RequestIDKey), requestID)

	s.logger.Info().Msg("============== KAFKA HEADER ========================")
    for _, h := range kafkaHeaders {
        s.logger.Info().Msgf("Header: %s = %s\n", h.Key, string(h.Value))
    }
	s.logger.Info().Msg("============== KAFKA HEADER ========================")
	
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
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())

		s.logger.Error().
			Ctx(ctx).
			Err(err).Msg("KAFKA ROLLBACK !!!")

		err_msk := s.workerEvent.ProducerWorker.AbortTransaction(ctx)
		if err_msk != nil {
			span.RecordError(err_msk) 
			span.SetStatus(codes.Error, err_msk.Error())
			s.logger.Error().
				Ctx(ctx).
				Err(err_msk).Msg("FAILED to kafka AbortTransaction")
			return err_msk
		}
		return err
	}

	s.logger.Info().
		Ctx(ctx).
		Str("func","ProducerEventKafka").Msg("CommitTransaction")
	err = s.workerEvent.ProducerWorker.CommitTransaction(ctx)
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error().
			Ctx(ctx).
			Err(err).
			Msg("FAILED to Kafka CommitTransaction = KAFKA ROLLBACK COMMIT !!!")

		errMskAbort := s.workerEvent.ProducerWorker.AbortTransaction(ctx)
		if errMskAbort != nil {
			span.RecordError(errMskAbort) 
			span.SetStatus(codes.Error, errMskAbort.Error())
			s.logger.Error().
				Ctx(ctx).
				Err(errMskAbort).
				Msg("FAILED to kafka AbortTransaction during CommitTransaction")
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