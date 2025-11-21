package event

import (
	"context"

	"github.com/rs/zerolog"

	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
	go_core_event "github.com/eliezerraj/go-core/v2/event/kafka"
)

var (
	tracerProvider go_core_otel_trace.TracerProvider
	producerWorker go_core_event.ProducerWorker
)

type WorkerEvent struct {
	Topics				[]string
	ProducerWorker 		*go_core_event.ProducerWorker 
	KafkaConfigurations *go_core_event.KafkaConfigurations
	logger 				*zerolog.Logger
}

// About create a worker producer kafka with transaction
func NewWorkerEventTX(ctx context.Context, 
					 topics []string, 
					 kafkaConfigurations *go_core_event.KafkaConfigurations,
					 appLogger *zerolog.Logger) (*WorkerEvent, error) {
	logger := appLogger.With().
						Str("component", "adapter.event").
						Logger()
	logger.Debug().
			Str("func","NewWorkerEventTX").Send()

	producerWorker, err := go_core_event.NewProducerWorkerTX(kafkaConfigurations,
															appLogger)
	if err != nil {
		logger.Error().
				Err(err).Send()
		return nil, err
	}

	// Start Kafka InitTransactions
	err = producerWorker.InitTransactions(ctx)
	if err != nil {
		logger.Error().
						Err(err).Send()
		return nil, err
	}

	return &WorkerEvent{
		Topics: topics,
		ProducerWorker: producerWorker,
		KafkaConfigurations: kafkaConfigurations,
		logger: &logger,
	},nil
}

// Above destroy ans create a new producer in case of failed abort
func (w *WorkerEvent) DestroyWorkerEventProducerTx(ctx context.Context) (error) {
	w.logger.Info().
			Str("func","DestroyWorkerEventProducerTx").Send()

	w.ProducerWorker.Close()	

	newProducerWorker, err := go_core_event.NewProducerWorkerTX(w.KafkaConfigurations,
															  w.logger)
	if err != nil {
		w.logger.Error().
			Err(err).Send()
		return err
	}

	// Start Kafka InitTransactions
	/*err = new_workerKafka.InitTransactions(ctx)
	if err != nil {
		w.logger.Error().
			Err(err).Send()
		return  err
	}*/

	w.ProducerWorker = newProducerWorker

	return nil
}

// About close producer connection kafka
func (w *WorkerEvent) Close(ctx context.Context){
	w.logger.Info().
			Str("func","Close").Send()
	
			w.ProducerWorker.Close()
}