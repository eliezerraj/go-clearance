package config

import(
	"os"
	"strconv"

	"github.com/joho/godotenv"

	"github.com/go-clearance/internal/domain/model"
)

func GetHttpServerEnv() model.Server {
	logger.Info().
				Str("func","GetHttpServerEnv").Send()

	err := godotenv.Load(".env")
	if err != nil {
		logger.Info().
				Err(err).Send()
	}
	
	var server	model.Server

	if os.Getenv("PORT") !=  "" {
		intVar, _ := strconv.Atoi(os.Getenv("PORT"))
		server.Port = intVar
	}

	if os.Getenv("CTX_TIMEOUT") !=  "" {
		intVar, _ := strconv.Atoi(os.Getenv("CTX_TIMEOUT"))
		server.CtxTimeout = intVar
	}

	server.ReadTimeout = 60
	server.WriteTimeout = 60
	server.IdleTimeout = 60
	server.CtxTimeout = 5

	return server
}