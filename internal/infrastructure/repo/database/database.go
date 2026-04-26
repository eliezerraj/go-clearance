package database

import (
	"time"

	"context"
	"database/sql"

	"github.com/rs/zerolog"

	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
	go_core_db_pg "github.com/eliezerraj/go-core/v2/database/postgre"
)

type WorkerRepository struct {
	DatabasePG 		*go_core_db_pg.DatabasePGServer
	logger			*zerolog.Logger
	tracerProvider 	*go_core_otel_trace.TracerProvider
}

// Above new worker
func NewWorkerRepository(databasePG *go_core_db_pg.DatabasePGServer,
						appLogger *zerolog.Logger,
						tracerProvider *go_core_otel_trace.TracerProvider) *WorkerRepository{
	logger := appLogger.With().
						Str("package", "repo.database").
						Logger()
	logger.Info().
			Str("func","NewWorkerRepository").Send()

	return &WorkerRepository{
		DatabasePG: databasePG,
		logger: &logger,
		tracerProvider: tracerProvider,
	}
}

// Helper function to convert nullable time to pointer
func (w *WorkerRepository) pointerTime(nt sql.NullTime) *time.Time {
	if nt.Valid {
		return &nt.Time
	}
	return nil
}

// Above get stats from database
func (w *WorkerRepository) Stat(ctx context.Context) (go_core_db_pg.PoolStats){
	w.logger.Info().
			Ctx(ctx).
			Str("func","Stat").Send()
	
	stats := w.DatabasePG.Stat()

	resPoolStats := go_core_db_pg.PoolStats{
		AcquireCount:         stats.AcquireCount(),
		AcquiredConns:        stats.AcquiredConns(),
		CanceledAcquireCount: stats.CanceledAcquireCount(),
		ConstructingConns:    stats.ConstructingConns(),
		EmptyAcquireCount:    stats.EmptyAcquireCount(),
		IdleConns:            stats.IdleConns(),
		MaxConns:             stats.MaxConns(),
		TotalConns:           stats.TotalConns(),
	}

	return resPoolStats
}
