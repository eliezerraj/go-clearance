package main

import(
	"fmt"
	"os"
	"io"
	"time"
	"context"

	"github.com/rs/zerolog"

	"github.com/go-clearance/shared/log"
	"github.com/go-clearance/internal/domain/model"
	"github.com/go-clearance/internal/infrastructure/adapter/http"
	"github.com/go-clearance/internal/infrastructure/adapter/event"
	"github.com/go-clearance/internal/infrastructure/server"
	"github.com/go-clearance/internal/infrastructure/config"
	"github.com/go-clearance/internal/infrastructure/repo/database"
	"github.com/go-clearance/internal/domain/service"

	go_core_otel_trace 	"github.com/eliezerraj/go-core/v2/otel/trace"
	go_core_db_pg 		"github.com/eliezerraj/go-core/v2/database/postgre"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Global variables
var ( 
	appLogger 	zerolog.Logger
	logger		zerolog.Logger
	appServer	model.AppServer
	appDatabasePGServer go_core_db_pg.DatabasePGServer

	appInfoTrace 		go_core_otel_trace.InfoTrace
	appTracerProvider 	go_core_otel_trace.TracerProvider
	sdkTracerProvider *sdktrace.TracerProvider
)

// About init
func init(){
	// Load application info

	application := config.GetApplicationInfo()
	appServer.Application = &application
	
	// Log setup	
	writers := []io.Writer{os.Stdout}

	if	application.StdOutLogGroup {
		file, err := os.OpenFile(application.LogGroup, 
								os.O_APPEND|os.O_CREATE|os.O_WRONLY, 
								0644)
		if err != nil {
			panic(fmt.Sprintf("Failed to open log file: %v", err))
		}
		writers = append(writers, file)
	} 
	multiWriter := io.MultiWriter(writers...)

	// log level
	switch application.LogLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "warn": 
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error": 
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// prepare log
	// assign to package-level appLogger (avoid := which would shadow it)
	appLogger = zerolog.New(multiWriter).
						With().
						Timestamp().
						Str("component", application.Name).
						Logger().
						Hook(log.TraceHook{}) // hook the app shared log

	// set a logger
	logger = appLogger.With().
						Str("package", "main").
						Logger()


	// load configs					
	server 		:= config.GetHttpServerEnv()
	otelTrace 	:= config.GetOtelEnv()
	databaseConfig := config.GetDatabaseEnv()
	apiEndpoint := config.GetEndpointEnv() 
	event, topics := config.GetEventKafkaEnv() 

	appServer.Server = &server
	appServer.EnvTrace = &otelTrace
	appServer.DatabaseConfig = &databaseConfig 	
	appServer.Endpoint = &apiEndpoint
	appServer.KafkaConfigurations = &event
	appServer.Topics = topics 
}

// About main
func main (){
	logger.Info().
			Msgf("STARTING APP version: %s",appServer.Application.Version)
	logger.Info().
			Interface("appServer", appServer).Send()

	// create context and otel log provider
	ctx, cancel := context.WithCancel(context.Background())

	if appServer.Application.OtelTraces {
		appInfoTrace.Name = appServer.Application.Name
		appInfoTrace.Version = appServer.Application.Version
		appInfoTrace.ServiceType = "k8-workload"
		appInfoTrace.Env = appServer.Application.Env
		appInfoTrace.Account = appServer.Application.Account

		sdkTracerProvider = appTracerProvider.NewTracerProvider(ctx, 
															*appServer.EnvTrace, 
															appInfoTrace,
															&appLogger)

		otel.SetTextMapPropagator(propagation.TraceContext{})
		otel.SetTracerProvider(sdkTracerProvider)
		sdkTracerProvider.Tracer(appServer.Application.Name)
	}

	// Open prepare database
	count := 1
	var err error
	for {
		appDatabasePGServer, err = appDatabasePGServer.NewDatabasePG(ctx, 
																	*appServer.DatabaseConfig,
																	&appLogger)
		if err != nil {
			if count < 3 {
				logger.Warn().
						Err(err).Msg("error open database... trying again WARNING")
			} else {
				logger.Fatal().
						Err(err).Msg("Fatal Error open Database ABORTING")
				panic(err)
			}
			time.Sleep(3 * time.Second) //backoff
			count = count + 1
			continue
		}
		break
	}

	// wire
	workerEventProducer, err := event.NewWorkerEventTX(ctx, 
													   appServer.Topics, 
													   appServer.KafkaConfigurations,
													   &appLogger)
	if err != nil {
		logger.Error().
				Err(err).
				Msg("Error create Kafka Producer ERROR")
	} else {
		logger.Info().
				Msg("KAFKA Producer SUCCESSFULL")
	}

	repository := database.NewWorkerRepository(&appDatabasePGServer,
												&appLogger)
	
	workerService := service.NewWorkerService(&appServer,
											 repository,
											 workerEventProducer,
											  &appLogger)

	httpRouters := http.NewHttpRouters(&appServer,
										workerService,
										&appLogger)

	httpServer := server.NewHttpAppServer(&appServer,
										  &appLogger,)

	// Services/dependevies health check
	err = workerService.HealthCheck(ctx)
	if err != nil {
		logger.Error().
					Err(err).
					Msg("Error health check support services ERROR")
	} else {
		logger.Info().
				Msg("SERVICES HEALTH CHECK OK")
	}

	// Cancel everything
	defer func() {
		// cancel log provider
		if sdkTracerProvider != nil {
			err := sdkTracerProvider.Shutdown(ctx)
			if err != nil{
				logger.Error().
						Err(err).
						Msg("Erro to shutdown tracer provider")
			}
		}

		// cancel kafka
		if workerEventProducer != nil{
			workerEventProducer.Close(ctx)
		}

		// cancel database		
		appDatabasePGServer.CloseConnection()
		
		// cancel context		
		cancel()

		logger.Info().
				Msgf("App %s Finalized SUCCESSFULL !!!", appServer.Application.Name)
	}()

	// start http server
	httpServer.StartHttpAppServer(ctx, 
								  httpRouters,)
}