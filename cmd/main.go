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
	go_core_event 		"github.com/eliezerraj/go-core/v2/event/kafka" 

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// AppContext holds all application dependencies and state
type AppContext struct {
	Logger           zerolog.Logger
	Server           *model.AppServer
	Database         *go_core_db_pg.DatabasePGServer
	TracerProvider   *go_core_otel_trace.TracerProvider
	KafkaProducer    *event.WorkerEvent
}

// Global logger for init and main entry point only
var initLogger zerolog.Logger

// init sets up global logger for startup
func init(){
	// Load application info
	application := config.GetApplicationInfo()
	
	// Log setup	
	writers := []io.Writer{os.Stdout}

	if application.StdOutLogGroup {
		file, err := os.OpenFile(application.LogGroup, 
								os.O_APPEND|os.O_CREATE|os.O_WRONLY, 
								0644)
		if err != nil {
			panic(fmt.Sprintf("FAILED to open log file: %v", err))
		}
		writers = append(writers, file)
	} 
	multiWriter := io.MultiWriter(writers...)

	// log level
	switch application.LogLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "warning": 
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error": 
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// prepare log
	initLogger = zerolog.New(multiWriter).
						With().
						Timestamp().
						Str("component", application.Name).
						Logger().
						Hook(log.TraceHook{}) // hook the app shared log
}

// setupAppContext initializes all application dependencies
func setupAppContext(ctx context.Context) (*AppContext, error) {
	logger := initLogger.With().
				Str("package", "main").
				Logger()

	// Load all configurations with proper error handling
	configLoader := config.NewConfigLoader(&initLogger)
	allConfigs, err := configLoader.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("configuration loading FAILED: %w", err)
	}

	// Build AppServer
	appServer := &model.AppServer{
		Application:    allConfigs.Application,
		Server:         allConfigs.Server,
		EnvTrace:       allConfigs.OtelTrace,
		DatabaseConfig: allConfigs.Database,
		KafkaConfigurations: allConfigs.Kafka,
		Endpoint:       allConfigs.Endpoints,
		Topics:         allConfigs.Topics,
	}

	// Setup OTEL tracer if enabled
	var tracerProvider *go_core_otel_trace.TracerProvider
	if appServer.Application.OtelTraces {
		tracerProvider = setupTracerProvider(ctx, appServer, &logger)
	}

	// Connect to database with retry and timeout
	databaseServer, err := connectDatabase(ctx, *appServer.DatabaseConfig, &logger)
	if err != nil {
		return nil, fmt.Errorf("database connection FAILED: %w", err)
	}

	// Connect to Kafka producer
	workerEventProducer, err := connectKafkaProducer(ctx, *appServer.KafkaConfigurations, *appServer.Topics, &logger)
	if err != nil {
		//return nil, fmt.Errorf("Kafka producer connection FAILED: %w", err)
		logger.Warn().
			Msgf("Kafka producer connection FAILED: %s", err.Error())
	}

	return &AppContext{
		Logger:         logger,
		Server:         appServer,
		Database:       &databaseServer,
		TracerProvider: tracerProvider,
		KafkaProducer:	workerEventProducer,
	}, nil
}

// setupTracerProvider initializes OpenTelemetry tracer
func setupTracerProvider(ctx context.Context, appServer *model.AppServer, logger *zerolog.Logger) *go_core_otel_trace.TracerProvider {
	appInfoTrace := go_core_otel_trace.InfoTrace{
		Name:        appServer.Application.Name,
		Version:     appServer.Application.Version,
		ServiceType: "k8-workload",
		Env:         appServer.Application.Env,
		Account:     appServer.Application.Account,
	}

	tracerProvider := go_core_otel_trace.NewTracerProvider(	ctx,
															*appServer.EnvTrace,
															appInfoTrace,
															logger)

	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tracerProvider.TracerProvider)

	return tracerProvider
}

// connectDatabase establishes database connection with retry logic and timeout
func connectDatabase(ctx context.Context, dbCfg go_core_db_pg.DatabaseConfig, logger *zerolog.Logger) (go_core_db_pg.DatabasePGServer, error) {
	// Create context with timeout for connection attempts
	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	const maxRetries = 3
	const retryDelay = 3 * time.Second

	var dbServer go_core_db_pg.DatabasePGServer
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		var err error
		dbServer, err = dbServer.NewDatabasePG(connCtx, dbCfg, logger)
		if err == nil {
			logger.Info().
				Ctx(connCtx).
				Int("attempt", attempt).
				Msg("connected to database SUCCESSFULL")
			return dbServer, nil
		}

		lastErr = err
		if attempt < maxRetries {
			logger.Warn().
				Ctx(connCtx).
				Err(err).
				Int("attempt", attempt).
				Msg("FAILED to connect to database, retrying...")
			select {
			case <-connCtx.Done():
				return go_core_db_pg.DatabasePGServer{}, fmt.Errorf("connection timeout after %d attempts: %w", attempt, err)
			case <-time.After(retryDelay):
				// Continue to next attempt
			}
		}
	}

	return go_core_db_pg.DatabasePGServer{}, fmt.Errorf("FAILED to connect to database after %d attempts: %w", maxRetries, lastErr)
}

// connectKafkaProducer establishes Kafka producer connection
func connectKafkaProducer(ctx context.Context, kafkaCfgs go_core_event.KafkaConfigurations, topics []string, logger *zerolog.Logger) (*event.WorkerEvent, error) {
	logger.Info().
		Msg("Trying connected to kafka...")

	workerEventProducer, err := event.NewWorkerEventTX(ctx, 
													   topics, 
													   &kafkaCfgs,
													   logger)
	if err != nil {
		return nil, fmt.Errorf("FAILED to create Kafka producer: %w", err)
	}

	logger.Info().
		Msg("Successfully connected to kafka SUCCESSFULL")

	return workerEventProducer, nil
}	

// main is the application entry point
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize all dependencies
	appCtx, err := setupAppContext(ctx)
	if err != nil {
		initLogger.Fatal().
			Err(err).
			Msg("FAILED to initialize application context")
	}

	appCtx.Logger.Info().
		Msgf("STARTING workload version: %s", appCtx.Server.Application.Version)

	appCtx.Logger.Info().
		Interface("server", appCtx.Server).
		Send()

	// Setup graceful shutdown and cleanup
	defer func() {
		appCtx.Logger.Info().
			Msg("Shutting down application")

		// Close database first (highest dependency)
		appCtx.Database.CloseConnection()

		// Shutdown tracer provider
		if appCtx.TracerProvider != nil && appCtx.TracerProvider.TracerProvider != nil {
			if err := appCtx.TracerProvider.TracerProvider.Shutdown(ctx); err != nil {
				appCtx.Logger.Error().
					Ctx(ctx).
					Err(err).
					Msg("Error shutting down tracer provider")
			}
		}

		// Cancel context
		cancel()

		appCtx.Logger.Info().
			Msgf("workload ** %s ** shutdown completed SUCCESSFULLY", appCtx.Server.Application.Name)
	}()

	// Wire dependencies
	repository := database.NewWorkerRepository(
		appCtx.Database,
		&appCtx.Logger,
		appCtx.TracerProvider)

	workerService := service.NewWorkerService(
		repository,
		&appCtx.Logger,
		appCtx.TracerProvider, 
		appCtx.Server.Endpoint,
		appCtx.KafkaProducer)

	httpRouters := http.NewHttpRouters(
		appCtx.Server,
		workerService,
		&appCtx.Logger,
		appCtx.TracerProvider)

	httpServer := server.NewHttpAppServer(
		appCtx.Server,
		&appCtx.Logger)

	// Health check all dependencies
	if err := workerService.HealthCheck(ctx); err != nil {
		appCtx.Logger.Error().
			Ctx(ctx).
			Err(err).
			Msg("Health check FAILED for support services")
		//return // ENABLE this line to exit application
	}

	appCtx.Logger.Info().
		Ctx(ctx).
		Msg("All services health check passed")

	// Start web server (blocking)
	httpServer.StartHttpAppServer(ctx, httpRouters)
}
