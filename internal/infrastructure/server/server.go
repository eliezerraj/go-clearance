package server

import(
	"os"
	"time"
	"strconv"
	"context"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/gorilla/mux"

	go_core_midleware "github.com/eliezerraj/go-core/v2/middleware"

	"github.com/go-clearance/internal/domain/model"
	app_http_routers "github.com/go-clearance/internal/infrastructure/adapter/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	
	//metrics
	go_core_otel_metric "github.com/eliezerraj/go-core/v2/otel/metric"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/attribute"
)

// Route constants
const (
	routeHealth      = "/health"
	routeLive        = "/live"
	routeHeader      = "/header"
	routeContext     = "/context"
	routeInfo        = "/info"
	routeMetrics     = "/metrics"
	routePayment     = "/payment"
	routePaymentOrder = "/payment/order"
)

// ExcludedFromTracing routes that should not create spans
var ExcludedFromTracing = map[string]bool{
	routeHealth:   true,
	routeLive:     true,
	routeHeader:   true,
	routeContext:  true,
	routeMetrics:  true,
}

type HttpAppServer struct {
	appServer		*model.AppServer
	logger			*zerolog.Logger
	tpsMetric		metric.Int64Counter
	latencyMetric	metric.Float64Histogram
}

// About create new http server
func NewHttpAppServer(	appServer *model.AppServer,
						appLogger *zerolog.Logger) HttpAppServer {
	logger := appLogger.With().
						Str("package", "infrastructure.server").
						Logger()
	
	logger.Info().
			Str("func","NewHttpAppServer").Send()

	return HttpAppServer{
		appServer: appServer,
		logger: &logger,
	}
}

// About start http server
func (h *HttpAppServer) StartHttpAppServer(	ctx context.Context, 
											appHttpRouters app_http_routers.HttpRouters) {
	h.logger.Info().
			Ctx(ctx).
			Str("func","StartHttpAppServer").Send()

	// Setup metrics if enabled
	if h.appServer.Application.OtelTraces {
		if err := h.setupMetrics(ctx); err != nil {
			h.logger.Warn().Ctx(ctx).Err(err).Msg("FAILED to setup metrics")
		}
	}

	// Setup routes and middleware
	appRouter := h.setupRoutes(appHttpRouters)
		
	// -------   Server Http 
	srv := http.Server{
		Addr:         ":" +  strconv.Itoa(h.appServer.Server.Port),      	
		Handler:      appRouter,                	          
		ReadTimeout:  time.Duration(h.appServer.Server.ReadTimeout) * time.Second,   
		WriteTimeout: time.Duration(h.appServer.Server.WriteTimeout) * time.Second,  
		IdleTimeout:  time.Duration(h.appServer.Server.IdleTimeout) * time.Second, 
	}

	h.logger.Info().
			Ctx(ctx).
			Str("Service Port", strconv.Itoa(h.appServer.Server.Port)).Send()

	// start server in goroutine
	serverErrors := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			h.logger.Error().Err(err).Msg("Server error")
			serverErrors <- err
		}
	}()

	// Get SIGNALS and handle shutdown
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-ch:
			switch sig {
			case syscall.SIGHUP:
				h.logger.Info().
						Ctx(ctx).
						Msg("Received SIGHUP: Reloading Configuration...")
			case syscall.SIGINT, syscall.SIGTERM:
				h.logger.Info().
						Ctx(ctx).
						Msg("Received SIGINT/SIGTERM: Http Server shutting down...")
				
				// Graceful shutdown with timeout
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				
				if err := srv.Shutdown(shutdownCtx); err != nil && err != http.ErrServerClosed {
					h.logger.Error().
							Ctx(ctx).
							Err(err).
							Msg("Error during shutdown")
				}
				return
			default:
				h.logger.Info().
						Ctx(ctx).
						Interface("Received signal", sig).Send()
			}
		case err := <-serverErrors:
			h.logger.Error().Err(err).Msg("Server stopped unexpectedly")
			return
		}
	}
}

// Helper function to setup metrics
func (h *HttpAppServer) setupMetrics(ctx context.Context) error {
	appInfoMetric := go_core_otel_metric.InfoMetric{
		Name:    h.appServer.Application.Name,
		Version: h.appServer.Application.Version,
	}

	metricProvider, err := go_core_otel_metric.NewMeterProvider(ctx, appInfoMetric, h.logger)
	if err != nil {
		return err
	}
	
	otel.SetMeterProvider(metricProvider)
	meter := metricProvider.Meter(h.appServer.Application.Name)

	tpsMetric, err := meter.Int64Counter("transaction_request_custom")
	if err != nil {
		return err
	}
	h.tpsMetric = tpsMetric

	latencyMetric, err := meter.Float64Histogram("latency_request_custom")
	if err != nil {
		return err
	}
	h.latencyMetric = latencyMetric

	return nil
}

// Helper function to wrap handler with metrics
func (h *HttpAppServer) withMetrics(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.tpsMetric != nil && h.latencyMetric != nil {
			start := time.Now()

			h.tpsMetric.Add(r.Context(), 1,
				metric.WithAttributes(
					attribute.String("method", r.Method),
					attribute.String("path", r.URL.Path),
				),
			)

			next(w, r)

			duration := time.Since(start).Seconds()
			h.latencyMetric.Record(r.Context(), duration,
				metric.WithAttributes(
					attribute.String("method", r.Method),
					attribute.String("path", r.URL.Path),
				),
			)
		} else {
			next(w, r)
		}
	}
}

// Helper function to setup routes and middleware
func (h *HttpAppServer) setupRoutes(appHttpRouters app_http_routers.HttpRouters) *mux.Router {
	appRouter := mux.NewRouter().StrictSlash(true)
	appMiddleWare := go_core_midleware.NewMiddleWare(h.logger)
	
	// Apply common middleware
	appRouter.Use(appMiddleWare.MiddleWareHandlerHeader)

	// Register metrics handler before OTEL middleware
	appRouter.Handle("/metrics", promhttp.Handler())

	// Apply OTEL middleware with filter to exclude certain endpoints
	appRouter.Use(otelmux.Middleware(h.appServer.Application.Name, 
		otelmux.WithFilter(func(req *http.Request) bool {
			_, excluded := ExcludedFromTracing[req.URL.Path]
			return !excluded
		})))
	
	// Register health check endpoints
	health := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
	health.HandleFunc(routeHealth, appHttpRouters.Health)

	live := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
	live.HandleFunc(routeLive, appHttpRouters.Live)

	header := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
	header.HandleFunc(routeHeader, appHttpRouters.Header)

	wk_ctx := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
	wk_ctx.HandleFunc(routeContext, appHttpRouters.Context)

	info := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
	info.HandleFunc(routeInfo, appHttpRouters.Info)

	// Register business logic routes with metrics middleware
	add := appRouter.Methods(http.MethodPost, http.MethodOptions).Subrouter()
	add.HandleFunc(routePayment, h.withMetrics(appMiddleWare.MiddleWareErrorHandler(appHttpRouters.AddPayment)))

	get := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
	get.HandleFunc(routePayment+"/{id}", h.withMetrics(appMiddleWare.MiddleWareErrorHandler(appHttpRouters.GetPayment)))

	get_payment_order := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
	get_payment_order.HandleFunc(routePaymentOrder+"/{id}", h.withMetrics(appMiddleWare.MiddleWareErrorHandler(appHttpRouters.GetPaymentFromOrder)))

	return appRouter
}
