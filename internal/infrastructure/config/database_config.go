package config

import(
	"os"
	"strconv"

	"github.com/joho/godotenv"
	
	go_core_db_pg "github.com/eliezerraj/go-core/database/postgre"
)

func GetDatabaseEnv() go_core_db_pg.DatabaseConfig {
	logger.Info().
				Str("func","GetDatabaseEnv").Send()

	err := godotenv.Load(".env")
	if err != nil {
		logger.Info().
					Err(err).Send()
	}
	
	var databaseConfig	go_core_db_pg.DatabaseConfig

	if os.Getenv("DB_HOST") !=  "" {
		databaseConfig.Host = os.Getenv("DB_HOST")
	}
	if os.Getenv("DB_PORT") !=  "" {
		databaseConfig.Port = os.Getenv("DB_PORT")
	}
	if os.Getenv("DB_NAME") !=  "" {	
		databaseConfig.DatabaseName = os.Getenv("DB_NAME")
	}
	if os.Getenv("DB_MAX_CONNECTION") !=  "" {
		intVar, _ := strconv.Atoi(os.Getenv("DB_MAX_CONNECTION"))
		databaseConfig.DBMaxConnection = intVar
	}

	// Get Database Secrets
	file_user, err := os.ReadFile("/var/pod/secret/username")
	if err != nil {
		logger.Fatal().
				Err(err).Send()
		os.Exit(3)
	}
	file_pass, err := os.ReadFile("/var/pod/secret/password")
	if err != nil {
		logger.Fatal().
				Err(err).Send()
		os.Exit(3)
	}
	
	databaseConfig.User = string(file_user)
	databaseConfig.Password = string(file_pass)

	return databaseConfig
}