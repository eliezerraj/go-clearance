package model

import (
	"time"
	go_core_db_pg 		"github.com/eliezerraj/go-core/v2/database/postgre"
	go_core_otel_trace  "github.com/eliezerraj/go-core/v2/otel/trace"
	go_core_event 		"github.com/eliezerraj/go-core/v2/event/kafka" 
)

type AppServer struct {
	Application 		*Application	 				`json:"application"`
	Server     			*Server     					`json:"server"`
	EnvTrace			*go_core_otel_trace.EnvTrace	`json:"env_trace"`
	DatabaseConfig		*go_core_db_pg.DatabaseConfig  	`json:"database_config"`
	KafkaConfigurations	*go_core_event.KafkaConfigurations  `json:"kafka_configurations"`
	Topics 				[]string						`json:"topics"`
	Endpoint 			*[]Endpoint						`json:"endpoints"`
}

type MessageRouter struct {
	Message			string `json:"message"`
}

type Application struct {
	Name				string 	`json:"name"`
	Version				string 	`json:"version"`
	Account				string 	`json:"account,omitempty"`
	OsPid				string 	`json:"os_pid"`
	IPAddress			string 	`json:"ip_address"`
	Env					string 	`json:"enviroment,omitempty"`
	LogLevel			string 	`json:"log_level,omitempty"`
	OtelTraces			bool   	`json:"otel_traces"`
	OtelMetrics			bool   	`json:"otel_metrics"`
	OtelLogs			bool   	`json:"otel_logs"`
	StdOutLogGroup 		bool   	`json:"stdout_log_group"`
	LogGroup			string 	`json:"log_group,omitempty"`
}

type Server struct {
	Port 			int `json:"port"`
	ReadTimeout		int `json:"readTimeout"`
	WriteTimeout	int `json:"writeTimeout"`
	IdleTimeout		int `json:"idleTimeout"`
	CtxTimeout		int `json:"ctxTimeout"`
}

type Endpoint struct {
	Name			string `json:"name_service"`
	Url				string `json:"url"`
	Method			string `json:"method"`
	XApigwApiId		string `json:"x-apigw-api-id,omitempty"`
	HostName		string `json:"host_name"`
	HttpTimeout		time.Duration `json:"httpTimeout"`
}

type Payment struct {
	ID			int			`json:"id,omitempty"`
	Order		*Order		`json:"order,omitempty"`
	Transaction string		`json:"transaction_id,omitempty"`
	Type		string 		`json:"type,omitempty"`
	Status		string 		`json:"status,omitempty"`
	Currency	string 		`json:"currency,omitempty"`
	Amount		float64 	`json:"amount,omitempty"`
	CreatedAt	time.Time 	`json:"created_at,omitempty"`
	UpdatedAt	*time.Time 	`json:"update_at,omitempty"`	
}

type Order struct {
	ID		int	`json:"id,omitempty"`	
}

type Event struct{
	ID			string		`json:"event_id,omitempty"`
	Type		string		`json:"event_type,omitempty"`
	EventAt		time.Time 		`json:"event_date,omitempty"`
	EventData	interface {}	`json:"event_data,omitempty"`
}