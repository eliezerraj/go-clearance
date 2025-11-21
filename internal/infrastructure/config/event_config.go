package config

import(
	"os"
	"strconv"

	"github.com/joho/godotenv"
	go_core_event "github.com/eliezerraj/go-core/v2/event/kafka" 
)

// About get kafka env var
func GetEventKafkaEnv() (go_core_event.KafkaConfigurations, []string) {

	logger.Info().
			Str("func","GetOtelEnv").Send()

	err := godotenv.Load(".env")
	if err != nil {
		logger.Info().
				Err(err).Send()
	}

	var kafkaConfigurations go_core_event.KafkaConfigurations
	
	kafkaConfigurations.RequiredAcks = 1

	if os.Getenv("KAFKA_USER") !=  "" {
		kafkaConfigurations.Username = os.Getenv("KAFKA_USER")
	}
	if os.Getenv("KAFKA_PASSWORD") !=  "" {
		kafkaConfigurations.Password = os.Getenv("KAFKA_PASSWORD")
	}
	if os.Getenv("KAFKA_PROTOCOL") !=  "" {
		kafkaConfigurations.Protocol = os.Getenv("KAFKA_PROTOCOL")
	}
	if os.Getenv("KAFKA_MECHANISM") !=  "" {
		kafkaConfigurations.Mechanisms = os.Getenv("KAFKA_MECHANISM")
	}
	if os.Getenv("KAFKA_CLIENT_ID") !=  "" {
		kafkaConfigurations.Clientid = os.Getenv("KAFKA_CLIENT_ID")
	}
	if os.Getenv("KAFKA_GROUP_ID") !=  "" {
		kafkaConfigurations.Groupid = os.Getenv("KAFKA_GROUP_ID")
	}
	if os.Getenv("KAFKA_BROKER_1") !=  "" {
		kafkaConfigurations.Brokers1 = os.Getenv("KAFKA_BROKER_1")
	}
	if os.Getenv("KAFKA_BROKER_2") !=  "" {
		kafkaConfigurations.Brokers2 = os.Getenv("KAFKA_BROKER_2")
	}
	if os.Getenv("KAFKA_BROKER_3") !=  "" {
		kafkaConfigurations.Brokers3 = os.Getenv("KAFKA_BROKER_3")
	}
	if os.Getenv("KAFKA_PARTITION") !=  "" {
		intVar, _ := strconv.Atoi(os.Getenv("KAFKA_PARTITION"))
		kafkaConfigurations.Partition = intVar
	}
	if os.Getenv("KAFKA_REPLICATION") !=  "" {
		intVar, _ := strconv.Atoi(os.Getenv("KAFKA_REPLICATION"))
		kafkaConfigurations.ReplicationFactor = intVar
	}

	list_topics := []string{}

	if os.Getenv("TOPIC_EVENT") !=  "" {
		list_topics = append(list_topics, os.Getenv("TOPIC_EVENT"))
	}

	return kafkaConfigurations, list_topics
}