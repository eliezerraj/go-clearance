package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"net"

	"github.com/rs/zerolog"
	"github.com/joho/godotenv"

	"github.com/go-clearance/internal/domain/model"
	go_core_db_pg "github.com/eliezerraj/go-core/v2/database/postgre"
	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
	go_core_event "github.com/eliezerraj/go-core/v2/event/kafka" 
)

var (
	envOnce sync.Once
	envLoaded bool
)

// AllConfig aggregates all configuration
type AllConfig struct {
	Application *model.Application
	Server      *model.Server
	Database    *go_core_db_pg.DatabaseConfig
	OtelTrace   *go_core_otel_trace.EnvTrace
	Kafka       *go_core_event.KafkaConfigurations
	Endpoints   *[]model.Endpoint
	Topics   	*[]string
}

// ConfigLoader handles loading and validating all configurations
type ConfigLoader struct {
	logger *zerolog.Logger
}

// NewConfigLoader creates a new config loader and loads .env once
func NewConfigLoader(logger *zerolog.Logger) *ConfigLoader {
	envOnce.Do(func() {
		err := godotenv.Load(".env")
		if err != nil {
			logger.Info().
				Err(err).
				Msg("No .env file found, using environment variables")
		}
		envLoaded = true
	})

	return &ConfigLoader{
		logger: logger,
	}
}

// LoadAll loads and validates all configurations
func (cl *ConfigLoader) LoadAll() (*AllConfig, error) {
	cl.logger.Info().Msg("Loading all configurations")

	app, err := cl.loadApplication()
	if err != nil {
		return nil, fmt.Errorf("FAILED to load application config: %w", err)
	}

	server, err := cl.loadServer()
	if err != nil {
		return nil, fmt.Errorf("FAILED to load server config: %w", err)
	}

	database, err := cl.loadDatabase()
	if err != nil {
		return nil, fmt.Errorf("FAILED to load database config: %w", err)
	}

	otel, err := cl.loadOtel()
	if err != nil {
		return nil, fmt.Errorf("FAILED to load OTEL config: %w", err)
	}

	endpoints, err := cl.loadEndpoints()
	if err != nil {
		cl.logger.Warn().Err(err).Msg("FAILED to load endpoint config, continuing without endpoints")
	}

	topics, err := cl.loadTopics()
	if err != nil {
		cl.logger.Warn().Err(err).Msg("FAILED to load topics config, continuing without topics")
	}

	kafka, err := cl.loadKafka()
	if err != nil {
		cl.logger.Warn().Err(err).Msg("FAILED to load topics config, continuing without topics")
	}

	return &AllConfig{
		Application: app,
		Server:      server,
		Database:    database,
		OtelTrace:   otel,
		Kafka:       kafka,
		Endpoints:   endpoints,
		Topics:      topics,
	}, nil
}

// loadApplication loads application configuration
func (cl *ConfigLoader) loadApplication() (*model.Application, error) {
	cl.logger.Debug().Msg("Loading application configuration")

	app := &model.Application{
		Version:       getEnvString("VERSION", "unknown"),
		Name:          getEnvString("APP_NAME", "go-cart"),
		Account:       getEnvString("ACCOUNT", ""),
		Env:           getEnvString("ENV", "dev"),
		StdOutLogGroup: getEnvBool("OTEL_STDOUT_LOG_GROUP", false),
		LogGroup:      getEnvString("LOG_GROUP", ""),
		LogLevel:      getEnvString("LOG_LEVEL", "info"),
		OtelTraces:    getEnvBool("OTEL_TRACES", false),
		OtelLogs:      getEnvBool("OTEL_LOGS", false),
		OtelMetrics:   getEnvBool("OTEL_METRICS", false),
	}

	// Get IP address
	ipAddr, err := getLocalIPAddress()
	if err != nil {
		cl.logger.Warn().Err(err).Msg("FAILED to get local IP address")
		app.IPAddress = "unknown"
	} else {
		app.IPAddress = ipAddr
	}

	app.OsPid = fmt.Sprintf("%d", os.Getpid())

	cl.logger.Info().
		Interface("application", app).
		Msg("Application configuration loaded SUCCESSFULLY")

	return app, nil
}

// loadServer loads HTTP server configuration
func (cl *ConfigLoader) loadServer() (*model.Server, error) {
	cl.logger.Debug().Msg("Loading server configuration")

	port, err := getEnvInt("PORT", 8080)
	if err != nil {
		return nil, fmt.Errorf("invalid PORT: %w", err)
	}

	readTimeout, err := getEnvInt("READ_TIMEOUT", 60)
	if err != nil {
		return nil, fmt.Errorf("invalid READ_TIMEOUT: %w", err)
	}

	writeTimeout, err := getEnvInt("WRITE_TIMEOUT", 60)
	if err != nil {
		return nil, fmt.Errorf("invalid WRITE_TIMEOUT: %w", err)
	}

	idleTimeout, err := getEnvInt("IDLE_TIMEOUT", 60)
	if err != nil {
		return nil, fmt.Errorf("invalid IDLE_TIMEOUT: %w", err)
	}

	ctxTimeout, err := getEnvInt("CTX_TIMEOUT", 5)
	if err != nil {
		return nil, fmt.Errorf("invalid CTX_TIMEOUT: %w", err)
	}

	server := &model.Server{
		Port:         port,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
		CtxTimeout:   ctxTimeout,
	}

	cl.logger.Info().
		Interface("server", server).
		Msg("Server configuration loaded SUCCESSFULLY")

	return server, nil
}

// loadDatabase loads database configuration
func (cl *ConfigLoader) loadDatabase() (*go_core_db_pg.DatabaseConfig, error) {
	cl.logger.Debug().Msg("Loading database configuration")

	maxConn, err := getEnvInt("DB_MAX_CONNECTION", 10)
	if err != nil {
		return nil, fmt.Errorf("invalid DB_MAX_CONNECTION: %w", err)
	}

	// Get credentials with fallbacks
	user, pass, err := getDatabaseCredentials(cl.logger)
	if err != nil {
		return nil, fmt.Errorf("FAILED to load database credentials: %w", err)
	}

	dbCfg := &go_core_db_pg.DatabaseConfig{
		Host:            getEnvString("DB_HOST", "localhost"),
		Port:            getEnvString("DB_PORT", "5432"),
		DatabaseName:    getEnvString("DB_NAME", "postgres"),
		User:            strings.TrimSpace(user),
		Password:        strings.TrimSpace(pass),
		DBMaxConnection: maxConn,
	}

	cl.logger.Info().
		Interface("dbCfg", dbCfg).
		Msg("Database configuration loaded SUCCESSFULLY")

	return dbCfg, nil
}

// loadOtel loads OTEL configuration
func (cl *ConfigLoader) loadOtel() (*go_core_otel_trace.EnvTrace, error) {
	cl.logger.Debug().Msg("Loading OTEL configuration")

	otel := &go_core_otel_trace.EnvTrace{
		OtelExportEndpoint:      getEnvString("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		UseStdoutTracerExporter: getEnvBool("OTEL_STDOUT_TRACER", false),
		UseOtlpCollector:        getEnvBool("OTEL_COLLECTOR", true),
		TimeInterval:            1,
		TimeAliveIncrementer:    1,
		TotalHeapSizeUpperBound: 100,
		ThreadsActiveUpperBound: 10,
		CpuUsageUpperBound:      100,
		SampleAppPorts:          []string{},
		AWSCloudWatchLogGroup:   []string{},
	}

	if logGroup := os.Getenv("LOG_GROUP"); logGroup != "" {
		otel.AWSCloudWatchLogGroup = strings.Split(logGroup, ",")
	}

	cl.logger.Info().
		Interface("otel", otel).
		Msg("OTEL configuration loaded SUCCESSFULLY")

	return otel, nil
}

// loadEndpoints loads service endpoint configurations from environment variables
func (cl *ConfigLoader) loadEndpoints() (*[]model.Endpoint, error) {
	cl.logger.Debug().Msg("Loading endpoint configuration")

	endpoints := []model.Endpoint{}
	maxEndpoints := 10 // Support up to 10 services

	for i := 0; i < maxEndpoints; i++ {
		endpoint := model.Endpoint{
			HttpTimeout: 5 * time.Second, // default timeout
		}

		// Generate env var names with zero-padded index (00, 01, 02, etc.)
		indexStr := fmt.Sprintf("%02d", i)

		// Load required field: URL
		url := getEnvString(fmt.Sprintf("URL_SERVICE_%s", indexStr), "")
		if url == "" {
			// No URL means end of service list
			break
		}
		endpoint.Url = url

		// Load optional fields
		endpoint.Name = getEnvString(fmt.Sprintf("NAME_SERVICE_%s", indexStr), "")
		endpoint.XApigwApiId = getEnvString(fmt.Sprintf("X_APIGW_API_ID_SERVICE_%s", indexStr), "")
		endpoint.HostName = getEnvString(fmt.Sprintf("HOST_SERVICE_%s", indexStr), "")

		// Load timeout with validation
		if timeoutStr := os.Getenv(fmt.Sprintf("CLIENT_HTTP_TIMEOUT_%s", indexStr)); timeoutStr != "" {
			timeout, err := strconv.Atoi(timeoutStr)
			if err != nil {
				cl.logger.Warn().
					Err(err).
					Str("env_var", fmt.Sprintf("CLIENT_HTTP_TIMEOUT_%s", indexStr)).
					Msg("Invalid timeout value, using default 5s")
			} else if timeout > 0 {
				endpoint.HttpTimeout = time.Duration(timeout) * time.Second
			}
		}

		endpoints = append(endpoints, endpoint)
	}

	if len(endpoints) == 0 {
		cl.logger.Warn().Msg("No endpoint configurations found in environment")
	}

	cl.logger.Info().
		Interface("endpoints", endpoints).
		Msg("Endpoint configurations loaded SUCCESSFULLY")

	return &endpoints, nil
}

// loadTopics loads Kafka topics configuration
func (cl *ConfigLoader) loadTopics() (*[]string, error) {
	cl.logger.Debug().Msg("Loading topics configuration")

	topics := []string{}
	maxTopics := 10 // Support up to 10 topics

	for i := 0; i < maxTopics; i++ {
		// Generate env var names with zero-padded index (00, 01, 02, etc.)
		indexStr := fmt.Sprintf("%02d", i)

		topic := getEnvString(fmt.Sprintf("TOPIC_EVENT_%s", indexStr), "")
		if topic == "" {
			// No topic means end of topic list
			break
		}
		topics = append(topics, topic)
	}

	if len(topics) == 0 {
		cl.logger.Warn().Msg("No topics configurations found in environment")
	}

	cl.logger.Info().
		Interface("topics", topics).
		Msg("Topics configurations loaded SUCCESSFULLY")

	return &topics, nil
}

// loadKafka loads Kafka configuration
func (cl *ConfigLoader) loadKafka() (*go_core_event.KafkaConfigurations, error) {
	cl.logger.Debug().Msg("Loading Kafka configuration")

	partition, err := getEnvInt("KAFKA_PARTITION", 1)
	if err != nil {
		return nil, fmt.Errorf("invalid KAFKA_PARTITION: %w", err)
	}

	replicationFactor, err := getEnvInt("KAFKA_REPLICATION", 3)
	if err != nil {
		return nil, fmt.Errorf("invalid KAFKA_REPLICATION: %w", err)
	}

	kafka := &go_core_event.KafkaConfigurations{
		Username: getEnvString("KAFKA_USER", ""),
		Password: getEnvString("KAFKA_PASSWORD", ""),
		Protocol: getEnvString("KAFKA_PROTOCOL", "SASL_SSL"),
		Mechanisms: getEnvString("KAFKA_MECHANISM", "SCRAM-SHA-512"),
		Clientid: getEnvString("KAFKA_CLIENT_ID", "go-clearance-client"),
		Groupid: getEnvString("KAFKA_GROUP_ID", "go-clearance-group"),	
		Partition: partition,
		ReplicationFactor: replicationFactor,
		Brokers1: getEnvString("KAFKA_BROKER_1", "localhost:9092"),
		Brokers2: getEnvString("KAFKA_BROKER_2", "localhost:9092"),
		Brokers3: getEnvString("KAFKA_BROKER_3", "localhost:9092"),
	}

	cl.logger.Info().
		Interface("kafka", kafka).
		Msg("Kafka configuration loaded SUCCESSFULLY")

	return kafka, nil
}

// Helper functions
// getEnvString retrieves environment variable as string with default
func getEnvString(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// getEnvBool retrieves environment variable as boolean with default
func getEnvBool(key string, defaultVal bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return strings.ToLower(val) == "true"
}

// getEnvInt retrieves environment variable as integer with error handling
func getEnvInt(key string, defaultVal int) (int, error) {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal, nil
	}

	intVal, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("FAILED to parse %s as integer: %w", key, err)
	}

	return intVal, nil
}

// getDatabaseCredentials retrieves database credentials with fallbacks
func getDatabaseCredentials(logger *zerolog.Logger) (user, pass string, err error) {
	// Try reading from Kubernetes secret volume
	userFile := "/var/pod/secret/username"
	passFile := "/var/pod/secret/password"

	if userData, err := os.ReadFile(userFile); err == nil {
		user = string(userData)
		logger.Debug().Str("source", "k8s_secret_volume").Msg("Loaded database user from secret")
	} else {
		// Fallback to environment variable
		user = os.Getenv("DB_USER")
		if user == "" {
			return "", "", fmt.Errorf("database user not found in secret or DB_USER environment variable")
		}
		logger.Debug().Str("source", "environment").Msg("Loaded database user from environment")
	}

	if passData, err := os.ReadFile(passFile); err == nil {
		pass = string(passData)
		logger.Debug().Str("source", "k8s_secret_volume").Msg("Loaded database password from secret")
	} else {
		// Fallback to environment variable
		pass = os.Getenv("DB_PASS")
		if pass == "" {
			return "", "", fmt.Errorf("database password not found in secret or DB_PASS environment variable")
		}
		logger.Debug().Str("source", "environment").Msg("Loaded database password from environment")
	}

	return user, pass, nil
}

// getLocalIPAddress retrieves the local non-loopback IP address
func getLocalIPAddress() (string, error) {

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no non-loopback IP address found")
}
