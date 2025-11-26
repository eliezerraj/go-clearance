package config

import(
	"os"
	"strings"
	
	"github.com/joho/godotenv"
	
	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
)

func GetOtelEnv() go_core_otel_trace.EnvTrace {
	
	logger.Info().
			Str("func","GetOtelEnv").Send()

	err := godotenv.Load(".env")
	if err != nil {
		logger.Info().
				Err(err).Send()
	}

	var envTrace	go_core_otel_trace.EnvTrace

	envTrace.TimeInterval = 1
	envTrace.TimeAliveIncrementer = 1
	envTrace.TotalHeapSizeUpperBound = 100
	envTrace.ThreadsActiveUpperBound = 10
	envTrace.CpuUsageUpperBound = 100
	envTrace.SampleAppPorts = []string{}

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") !=  "" {	
		envTrace.OtelExportEndpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}

	if os.Getenv("OTEL_STDOUT_TRACER") ==  "true" {
		envTrace.UseStdoutTracerExporter = true
	} else {
		envTrace.UseStdoutTracerExporter = false
	}

	if os.Getenv("LOG_GROUP") !=  "" {	
		envTrace.AWSCloudWatchLogGroup = strings.Split(os.Getenv("LOG_GROUP"),",")
	}
	
	return envTrace
}