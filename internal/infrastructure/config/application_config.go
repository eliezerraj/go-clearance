package config

import(
	"os"
	"strconv"
	"net"

	"github.com/rs/zerolog"
	"github.com/joho/godotenv"

	"github.com/go-clearance/internal/domain/model"
)

var	logger = zerolog.New(os.Stdout).
					With().
					Str("component","go-clearance").
					Str("package","infrastructure.config").
					Timestamp().
					Logger()

// Load the Application configuration
func GetApplicationInfo() (model.Application) {
	logger.Info().
		Str("func","GetApplicationInfo").Send()

	err := godotenv.Load(".env")
	if err != nil {
		logger.Warn().
			Err(err).Send()
	}

	var application model.Application

	if os.Getenv("VERSION") !=  "" {
		application.Version = os.Getenv("VERSION")
	}
	if os.Getenv("APP_NAME") !=  "" {
		application.Name = os.Getenv("APP_NAME")
	}
	if os.Getenv("ACCOUNT") !=  "" {	
		application.Account = os.Getenv("ACCOUNT")
	}
	if os.Getenv("ENV") !=  "" {	
		application.Env = os.Getenv("ENV")
	}

	if os.Getenv("OTEL_STDOUT_LOG_GROUP") ==  "true" {
		application.StdOutLogGroup = true
	} else {
		application.StdOutLogGroup = false
	}	
	if os.Getenv("LOG_GROUP") !=  "" {	
		application.LogGroup = os.Getenv("LOG_GROUP")
	}
	if os.Getenv("LOG_LEVEL") !=  "" {	
		application.LogLevel = os.Getenv("LOG_LEVEL")
	}

	if os.Getenv("OTEL_TRACES") ==  "true" {
		application.OtelTraces = true
	} else {
		application.OtelTraces = false
	}

	if os.Getenv("OTEL_LOGS") ==  "true" {
		application.OtelLogs = true
	} else {
		application.OtelLogs = false
	}
	if os.Getenv("OTEL_METRICS") ==  "true" {
		application.OtelMetrics = true
	} else {
		application.OtelMetrics = false
	}
		
	// Get IP
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		logger.Error().
				Err(err).Send()
		os.Exit(3)
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				application.IPAddress = ipnet.IP.String()
			}
		}
	}
	application.OsPid = strconv.Itoa(os.Getpid())

	return application
}
