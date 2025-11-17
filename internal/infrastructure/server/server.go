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

	go_core_midleware "github.com/eliezerraj/go-core/middleware"

	"github.com/go-clearance/internal/domain/model"
	app_http_routers "github.com/go-clearance/internal/infrastructure/adapter/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
)

var(
	go_core_middleware go_core_midleware.ToolsMiddleware
)

type HttpAppServer struct {
	appServer	*model.AppServer
	logger		*zerolog.Logger
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

func (h *HttpAppServer) StartHttpAppServer(	ctx context.Context, 
											appHttpRouters app_http_routers.HttpRouters,
											) {
	h.logger.Info().
			Str("func","StartHttpAppServer").Send()

	appRouter := mux.NewRouter().StrictSlash(true)
	appRouter.Use(go_core_middleware.MiddleWareHandlerHeader)

	appRouter.Handle("/metrics", promhttp.Handler())

	health := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
    health.HandleFunc("/health", appHttpRouters.Health)

	live := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
    live.HandleFunc("/live", appHttpRouters.Live)

	header := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
    header.HandleFunc("/header", appHttpRouters.Header)

	wk_ctx := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
    wk_ctx.HandleFunc("/context", appHttpRouters.Context)

	info := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
    info.HandleFunc("/info", appHttpRouters.Info)
	info.Use(otelmux.Middleware(h.appServer.Application.Name))

	add := appRouter.Methods(http.MethodPost, http.MethodOptions).Subrouter()
	add.HandleFunc("/payment", go_core_middleware.MiddleWareErrorHandler(appHttpRouters.AddPayment))		
	add.Use(otelmux.Middleware(h.appServer.Application.Name))

	get := appRouter.Methods(http.MethodGet, http.MethodOptions).Subrouter()
	get.HandleFunc("/payment/{id}",go_core_middleware.MiddleWareErrorHandler(appHttpRouters.GetPayment))		
	get.Use(otelmux.Middleware(h.appServer.Application.Name))

	// -------   Server Http 

	srv := http.Server{
		Addr:         ":" +  strconv.Itoa(h.appServer.Server.Port),      	
		Handler:      appRouter,                	          
		ReadTimeout:  time.Duration(h.appServer.Server.ReadTimeout) * time.Second,   
		WriteTimeout: time.Duration(h.appServer.Server.WriteTimeout) * time.Second,  
		IdleTimeout:  time.Duration(h.appServer.Server.IdleTimeout) * time.Second, 
	}

	h.logger.Info().
			Str("Service Port", strconv.Itoa(h.appServer.Server.Port)).Send()

	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			h.logger.Warn().
					Err(err).Msg("Canceling http mux server !!!")
		}
	}()

	// Get SIGNALS
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for {
		sig := <-ch

		switch sig {
		case syscall.SIGHUP:
			h.logger.Info().
					Msg("Received SIGHUP: Reloading Configuration...")
		case syscall.SIGINT, syscall.SIGTERM:
			h.logger.Info().
					Msg("Received SIGINT/SIGTERM: Http Server Exit ...")
			return
		default:
			h.logger.Info().
					Interface("Received signal:", sig).Send()
		}
	}

	if err := srv.Shutdown(ctx); err != nil && err != http.ErrServerClosed {
		h.logger.Warn().
				Err(err).
				Msg("Dirty shutdown WARNING !!!")
		return
	}
}	