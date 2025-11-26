package config

import(
	"os"
	"time"
	"strconv"
	
	"github.com/joho/godotenv"

	"github.com/go-clearance/internal/domain/model"
)

// About get services endpoints env var
func GetEndpointEnv() []model.Endpoint {
	logger.Info().
			Str("func","GetEndpointEnv").Send()

	err := godotenv.Load(".env")
	if err != nil {
		logger.Info().
				Err(err).Send()
	}
	
	var endpoint []model.Endpoint

	var endpoint00 model.Endpoint
	if os.Getenv("URL_SERVICE_00") !=  "" {
		endpoint00.Url = os.Getenv("URL_SERVICE_00")
	}
	if os.Getenv("X_APIGW_API_ID_SERVICE_00") !=  "" {
		endpoint00.XApigwApiId = os.Getenv("X_APIGW_API_ID_SERVICE_00")
	}
	if os.Getenv("NAME_SERVICE_00") !=  "" {
		endpoint00.Name = os.Getenv("NAME_SERVICE_00")
	}
	if os.Getenv("HOST_SERVICE_00") !=  "" {
		endpoint00.HostName = os.Getenv("HOST_SERVICE_00")
	}
	if os.Getenv("CLIENT_HTTP_TIMEOUT_00") !=  "" {
		intVar, _ := strconv.Atoi(os.Getenv("CLIENT_HTTP_TIMEOUT_00"))	
		endpoint00.HttpTimeout = time.Duration(intVar) * time.Second
	} else {
		endpoint00.HttpTimeout =(5 * time.Second) // default
	}
	endpoint = append(endpoint, endpoint00)

	var endpoint01 model.Endpoint
	if os.Getenv("URL_SERVICE_01") !=  "" {
		endpoint01.Url = os.Getenv("URL_SERVICE_01")
	}
	if os.Getenv("X_APIGW_API_ID_SERVICE_01") !=  "" {
		endpoint01.XApigwApiId = os.Getenv("X_APIGW_API_ID_SERVICE_01")
	}
	if os.Getenv("NAME_SERVICE_01") !=  "" {
		endpoint01.Name = os.Getenv("NAME_SERVICE_01")
	}
	if os.Getenv("HOST_SERVICE_02") !=  "" {
		endpoint01.HostName = os.Getenv("HOST_SERVICE_01")
	}
	if os.Getenv("CLIENT_HTTP_TIMEOUT_01") !=  "" {
		intVar, _ := strconv.Atoi(os.Getenv("CLIENT_HTTP_TIMEOUT_01"))	
		endpoint01.HttpTimeout = time.Duration(intVar) * time.Second
	} else {
		endpoint01.HttpTimeout =(5 * time.Second) // default
	}
	endpoint = append(endpoint, endpoint01)

	var endpoint02 model.Endpoint
	if os.Getenv("URL_SERVICE_02") !=  "" {
		endpoint02.Url = os.Getenv("URL_SERVICE_02")
	}
	if os.Getenv("X_APIGW_API_ID_SERVICE_02") !=  "" {
		endpoint02.XApigwApiId = os.Getenv("X_APIGW_API_ID_SERVICE_02")
	}
	if os.Getenv("NAME_SERVICE_02") !=  "" {
		endpoint02.Name = os.Getenv("NAME_SERVICE_02")
	}
	if os.Getenv("HOST_SERVICE_02") !=  "" {
		endpoint02.HostName = os.Getenv("HOST_SERVICE_02")
	}
	if os.Getenv("CLIENT_HTTP_TIMEOUT_02") !=  "" {
		intVar, _ := strconv.Atoi(os.Getenv("CLIENT_HTTP_TIMEOUT_02"))	
		endpoint02.HttpTimeout = time.Duration(intVar) * time.Second
	} else {
		endpoint02.HttpTimeout =(5 * time.Second) // default
	}
	endpoint = append(endpoint, endpoint02)

	var endpoint03 model.Endpoint
	if os.Getenv("URL_SERVICE_03") !=  "" {
		endpoint03.Url = os.Getenv("URL_SERVICE_03")
	}
	if os.Getenv("X_APIGW_API_ID_SERVICE_03") !=  "" {
		endpoint03.XApigwApiId = os.Getenv("X_APIGW_API_ID_SERVICE_03")
	}
	if os.Getenv("NAME_SERVICE_03") !=  "" {
		endpoint03.Name = os.Getenv("NAME_SERVICE_03")
	}
	if os.Getenv("HOST_SERVICE_03") !=  "" {
		endpoint03.HostName = os.Getenv("HOST_SERVICE_03")
	}
	if os.Getenv("CLIENT_HTTP_TIMEOUT_03") !=  "" {
		intVar, _ := strconv.Atoi(os.Getenv("CLIENT_HTTP_TIMEOUT_03"))	
		endpoint03.HttpTimeout = time.Duration(intVar) * time.Second
	} else {
		endpoint03.HttpTimeout =(5 * time.Second) // default
	}
	endpoint = append(endpoint, endpoint03)

	return endpoint
}