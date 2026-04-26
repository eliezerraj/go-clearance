package service

import (
	"fmt"
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
	go_core_middleware "github.com/eliezerraj/go-core/v2/middleware"


	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/codes"
)

type WorkerService struct {
	workerRepository 	*database.WorkerRepository
	logger 				*zerolog.Logger
	tracerProvider 		*go_core_otel_trace.TracerProvider
	httpService			*go_core_http.HttpService
	endpoint			*[]model.Endpoint
	workerEvent			*event.WorkerEvent	
	mutex    			sync.Mutex 	 	
}

// About new worker service
func NewWorkerService(	workerRepository *database.WorkerRepository, 
						appLogger 		*zerolog.Logger,
						tracerProvider 	*go_core_otel_trace.TracerProvider,
						endpoint		*[]model.Endpoint, 
					  	workerEvent		*event.WorkerEvent) *WorkerService {

	logger := appLogger.With().
						Str("package", "domain.service").
						Logger()
	logger.Info().
			Str("func","NewWorkerService").Send()

	httpService := go_core_http.NewHttpService(&logger)					

	return &WorkerService{
		workerRepository: workerRepository,
		logger: &logger,
		tracerProvider: tracerProvider,
		httpService: httpService,
		endpoint: endpoint,
		workerEvent: workerEvent,
	}
}

// Helper: Get service endpoint by index with error handling
func (s *WorkerService) getServiceEndpoint(index int) (*model.Endpoint, error) {
	if s.endpoint == nil || len(*s.endpoint) <= index {
		return nil, fmt.Errorf("service endpoint at index %d not found", index)
	}
	return &(*s.endpoint)[index], nil
}

// Helper: Build HTTP headers with request ID
func (s *WorkerService) buildHeaders(ctx context.Context) map[string]string {
	requestID := go_core_middleware.GetRequestID(ctx)
	return map[string]string{
		"Content-Type":  "application/json;charset=UTF-8",
		"X-Request-Id":  requestID,
	}
}

// about do http call 
func (s *WorkerService) doHttpCall(ctx context.Context,	httpClientParameter go_core_http.HttpClientParameter) (interface{}, error) {
	s.logger.Info().
			 Ctx(ctx).
			 Str("func","doHttpCall").Send()

	resPayload, statusCode, err := s.httpService.DoHttp(ctx, httpClientParameter)

	if err != nil {
		s.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, err
	}

	s.logger.Debug().
		Interface("+++++++++++++++++> httpClientParameter.Url:",httpClientParameter.Url).
		Interface("+++++++++++++++++> resPayload:",resPayload).
		Interface("+++++++++++++++++> statusCode:",statusCode).
		Interface("+++++++++++++++++> err:", err).
		Send()

		switch (statusCode) {
			case 200:
				return resPayload, nil
			case 201:
				return resPayload, nil	
			case 400:
			case 401:
			case 403:
			case 404:
			case 500:
				return nil, fmt.Errorf("internal server error (status code %d) - (process: %s)", statusCode, httpClientParameter.Url)
			default:
		}

		// marshal response payload
		jsonString, err := json.Marshal(resPayload)
		if err != nil {
			s.logger.Error().
				Ctx(ctx).
				Err(err).Send()
			return nil, fmt.Errorf("FAILED to marshal http response: %w (process: %s)", err, httpClientParameter.Url)
		}

		// parse error message
		message := model.APIError{}
		if err := json.Unmarshal(jsonString, &message); err != nil {
			s.logger.Error().
				Ctx(ctx).
				Err(err).Send()
			return nil, fmt.Errorf("FAILED to unmarshal error response: %w (process: %s)", err, httpClientParameter.Url)
		}

		newErr := fmt.Errorf("%s - (status code %d) - (process: %s)", message.Msg,statusCode, httpClientParameter.Url)
		s.logger.Error().
			Ctx(ctx).
			Err(newErr).Send()
		
	return nil, newErr
}

// About database stats
func (s *WorkerService) Stat(ctx context.Context) (go_core_db_pg.PoolStats){
	s.logger.Info().
			Ctx(ctx).
			Str("func","Stat").Send()

	return s.workerRepository.Stat(ctx)
}

// About check health service
func (s * WorkerService) HealthCheck(ctx context.Context) error {
	s.logger.Info().
			Str("func","HealthCheck").Send()

	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.HealthCheck", trace.SpanKindServer)
	defer span.End()

	// Check database health
	ctx, spanDB := s.tracerProvider.SpanCtx(ctx, "DatabasePG.Ping", trace.SpanKindInternal)
	err := s.workerRepository.DatabasePG.Ping()
	spanDB.End()

	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error().
			Ctx(ctx).
			Err(err).Msg("*** Database HEALTH CHECK FAILED ***")
		return erro.ErrHealthCheck
	}

	s.logger.Info().
		Ctx(ctx).
		Msg("*** Database HEALTH CHECK SUCCESSFULL ***")

	return nil
}