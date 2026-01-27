package http

import (
	"fmt"
	"time"
	"reflect"
	"net/http"
	"context"
	"strings"
	"strconv"
	"encoding/json"	

	"github.com/rs/zerolog"
	"github.com/gorilla/mux"

	"github.com/go-clearance/shared/erro"
	"github.com/go-clearance/internal/domain/model"
	"github.com/go-clearance/internal/domain/service"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/codes"

	go_core_midleware "github.com/eliezerraj/go-core/v2/middleware"
	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
)

// Global middleware reference for error handling
var (
	_ go_core_midleware.MiddleWare
)

type HttpRouters struct {
	workerService 	*service.WorkerService
	appServer		*model.AppServer
	logger			*zerolog.Logger
	tracerProvider 	*go_core_otel_trace.TracerProvider
}

// Helper to extract context with timeout and setup span
func (h *HttpRouters) withContext(req *http.Request, spanName string) (context.Context, context.CancelFunc, trace.Span) {
	ctx, cancel := context.WithTimeout(req.Context(), 
		time.Duration(h.appServer.Server.CtxTimeout) * time.Second)
	
	h.logger.Info().
			Ctx(ctx).
			Str("func", spanName).Send()
	
	ctx, span := h.tracerProvider.SpanCtx(ctx, "adapter."+spanName, trace.SpanKindInternal)
	return ctx, cancel, span
}

// Helper to get trace ID from context using middleware function
func (h *HttpRouters) getTraceID(ctx context.Context) string {
	return go_core_midleware.GetRequestID(ctx)
}

// Above create routers
func NewHttpRouters(appServer *model.AppServer,
					workerService *service.WorkerService,
					appLogger *zerolog.Logger,
					tracerProvider *go_core_otel_trace.TracerProvider) HttpRouters {
	logger := appLogger.With().
						Str("package", "adapter.http").
						Logger()
			
	logger.Info().
		Str("func","NewHttpRouters").Send()

	return HttpRouters{
		workerService: workerService,
		appServer: appServer,
		logger: &logger,
		tracerProvider: tracerProvider,
	}
}

// Helper to write JSON response
func (h *HttpRouters) writeJSON(w http.ResponseWriter, code int, data interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	return json.NewEncoder(w).Encode(data)
}

// Helper to parse ID parameter from URL variables
func (h *HttpRouters) parseIDParam(vars map[string]string) (int, error) {
	varID := vars["id"]
	varIDint, err := strconv.Atoi(varID)
	if err != nil {
		return 0, erro.ErrBadRequest
	}
	return varIDint, nil
}

// ErrorHandler creates an APIError with appropriate HTTP status based on error type
func (h *HttpRouters) ErrorHandler(traceID string, err error) *go_core_midleware.APIError {
	var httpStatusCode int = http.StatusInternalServerError

	if strings.Contains(err.Error(), "context deadline exceeded") {
		httpStatusCode = http.StatusGatewayTimeout
	}

	if strings.Contains(err.Error(), "check parameters") {
		httpStatusCode = http.StatusBadRequest
	}

	if strings.Contains(err.Error(), "not found") {
		httpStatusCode = http.StatusNotFound
	}

	if strings.Contains(err.Error(), "payment amount must be greater than zero") {
		httpStatusCode = http.StatusBadRequest
	}

	if strings.Contains(err.Error(), "duplicate key") || 
	   strings.Contains(err.Error(), "unique constraint") {
		httpStatusCode = http.StatusBadRequest
	}

	return go_core_midleware.NewAPIError(err, traceID, httpStatusCode)
}

// About return a health
func (h *HttpRouters) Health(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)

	json.NewEncoder(rw).Encode(model.MessageRouter{Message: "true"})
}

// About return a live
func (h *HttpRouters) Live(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)

	json.NewEncoder(rw).Encode(model.MessageRouter{Message: "true"})
}

// About show all header received
func (h *HttpRouters) Header(rw http.ResponseWriter, req *http.Request) {
	h.logger.Info().
		Str("func","Header").Send()
	
	json.NewEncoder(rw).Encode(req.Header)
}

// About show all context values
func (h *HttpRouters) Context(rw http.ResponseWriter, req *http.Request) {
	h.logger.Info().
		Str("func","Context").Send()
	
	contextValues := reflect.ValueOf(req.Context()).Elem()

	json.NewEncoder(rw).Encode(fmt.Sprintf("%v",contextValues))
}

// About info
func (h *HttpRouters) Info(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	
	_, cancel, span := h.withContext(req, "Info")
	defer cancel()
	defer span.End()

	json.NewEncoder(rw).Encode(h.appServer)
}

// About add payment
func (h *HttpRouters) AddPayment(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "AddPayment")
	defer cancel()
	defer span.End()
	
	// decode payload		
	payment := model.Payment{}
	err := json.NewDecoder(req.Body).Decode(&payment)
	defer req.Body.Close()
    
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		return h.ErrorHandler(h.getTraceID(ctx), erro.ErrBadRequest)
    }

	res, err := h.workerService.AddPayment(ctx, &payment)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusCreated, res)
}

// About get payment
func (h *HttpRouters) GetPayment(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "GetPayment")
	defer cancel()
	defer span.End()

	// decode payload			
	vars := mux.Vars(req)
	PaymentID, err := h.parseIDParam(vars)
    if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		return h.ErrorHandler(h.getTraceID(ctx), err)
    }

	payment := model.Payment{ID: PaymentID}

	res, err := h.workerService.GetPayment(ctx, &payment)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusOK, res)
}

// About get payment from order
func (h *HttpRouters) GetPaymentFromOrder(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "GetPaymentFromOrder")
	defer cancel()
	defer span.End()

	// decode payload			
	vars := mux.Vars(req)
	OrderID, err := h.parseIDParam(vars)
    if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		return h.ErrorHandler(h.getTraceID(ctx), err)
    }

	order := model.Order{ID: OrderID}

	res, err := h.workerService.GetPaymentFromOrder(ctx, &order)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusOK, res)
}
