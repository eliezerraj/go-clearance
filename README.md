# go-clearance

   This is workload for POC purpose such as load stress test, gitaction, etc.

   The main purpose is create an clearance for an order and send an event in kafka topic.

## Integration

    This is workload requires go-order and a kafka cluster.

    The integrations are made via http api request and event (producer).

    To local test you should use the work space:
    Create a work space (root)
    go work init ./cmd ../go-core

    Add module (inside /cmd)
    go work use ..

## Enviroment variables

   To run in local machine for local tests creat a .env in /cmd folder

    VERSION=1.0
    ACCOUNT=aws:localhost
    APP_NAME=go-clearance.localhost
    PORT=7003
    ENV=dev

    DB_HOST= 127.0.0.1 
    DB_PORT=5432
    DB_NAME=postgres
    DB_MAX_CONNECTION=30
    CTX_TIMEOUT=10

    LOG_LEVEL=info #info, error, warning, debug
    OTEL_EXPORTER_OTLP_ENDPOINT = localhost:4317

    OTEL_METRICS=true
    OTEL_STDOUT_TRACER=false
    OTEL_TRACES=true

    OTEL_LOGS=true
    OTEL_STDOUT_LOG_GROUP=true
    LOG_GROUP=/mnt/c/Eliezer/log/go-clearance.log

    KAFKA_USER=admin
    KAFKA_PASSWORD=admin
    KAFKA_PROTOCOL=SASL_SSL
    KAFKA_MECHANISM=SCRAM-SHA-512
    KAFKA_CLIENT_ID=GO-CLEARANCE-LOCAL
    KAFKA_BROKER_1=b-1.mskarch01.x25pj7.c3.kafka.us-east-2.amazonaws.com:9096 
    KAFKA_BROKER_2=b-3.mskarch01.x25pj7.c3.kafka.us-east-2.amazonaws.com:9096 
    KAFKA_BROKER_3=b-2.mskarch01.x25pj7.c3.kafka.us-east-2.amazonaws.com:9096 
    KAFKA_PARTITION=3
    KAFKA_REPLICATION=1
    TOPIC_EVENT=topic.clearance.local

    NAME_SERVICE_00=go-order
    URL_SERVICE_00=http://localhost:7004
    HOST_SERVICE_00=go-order
    CLIENT_HTTP_TIMEOUT_00=5

## Enpoints

    curl --location 'http://localhost:7003/info'
    curl --location 'http://localhost:7003/payment/393'
    curl --location 'http://localhost:7003/payment/order/6'

    curl --location 'http://localhost:7003/payment' \
    --data '{
        "transaction_id": "abc.123",
        "order": {
            "id": 70
        },
        "type": "CREDIT",
        "status": "PENDING",
        "currency": "BRL",
        "amount": 500.00
    }'

# tables

    CREATE TABLE public.clearance (
        id 				BIGSERIAL	NOT NULL,
        fk_order_id		BIGSERIAL	NOT NULL,
        type 			VARCHAR(100) NOT NULL,
        transaction_id	VARCHAR(100) NULL,
        status 			VARCHAR(100) NOT NULL,
        currency 		VARCHAR(100) NOT NULL,
        amount 			DECIMAL(10,2) NOT null DEFAULT 0,
        created_at 		timestamptz 	NOT NULL,
        updated_at 		timestamptz  NULL,
        CONSTRAINT clearance_pkey PRIMARY KEY (id)
    );

    ALTER TABLE clearance ADD constraint clearance_fk_order_id_fkey
    FOREIGN KEY (fk_order_id) REFERENCES public.order(id);
