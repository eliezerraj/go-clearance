# go-clearance

This is workload for POC purpose such as load stress test, gitaction, etc.

The main purpose is create an clearance for an order and send an event in kafka topic.

To local test you should use the work space:
Create a work space (root)

    go work init ./cmd ../go-core

Add module (inside /cmd)

    go work use ..

## Sequence Diagram

![alt text](clearance.png)

    title clearance

    participant user
    participant clearance
    participant order
    participant kafka

    entryspacing 1.1
    alt addPayment
        user->clearance:POST /clearance
        postData
    clearance->order:GET /order/{id}
    clearance<--order:http 200 (JSON)\ngetData
    clearance->clearance:create\nPayment
    clearance->kafka:EVENT topic.clearance
        user<--clearance:http 200 (JSON)\npostData
        queryData
    end
    alt getPayment
    user->clearance:GET /payment/{id}
    user<--clearance:http 200 (JSON)\nqueryData
    end
    alt getPaymentFromOrder
    user->clearance:GET /payment/order/{id}
    user<--clearance:http 200 (JSON)\nqueryData
    end


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
    KAFKA_PARTITION=3
    KAFKA_REPLICATION=1
    TOPIC_EVENT=topic.clearance.local

    #KAFKA_PROTOCOL=SASL_SSL
    #KAFKA_MECHANISM=SCRAM-SHA-512
    #KAFKA_BROKER_1=b-1.mskarch01.x25pj7.c3.kafka.us-east-2.amazonaws.com:9096 
    #KAFKA_BROKER_2=b-3.mskarch01.x25pj7.c3.kafka.us-east-2.amazonaws.com:9096 
    #KAFKA_BROKER_3=b-2.mskarch01.x25pj7.c3.kafka.us-east-2.amazonaws.com:9096 

    # Change from PLAINTEXT to SASL_PLAINTEXT to enable authentication
    KAFKA_PROTOCOL=SASL_PLAINTEXT
    KAFKA_MECHANISM=PLAIN
    KAFKA_BROKER_1=localhost:9092
    KAFKA_BROKER_2=localhost:9092
    KAFKA_BROKER_3=localhost:9092

    NAME_SERVICE_00=go-order
    URL_SERVICE_00=http://localhost:7004
    HOST_SERVICE_00=go-order
    CLIENT_HTTP_TIMEOUT_00=5

## Enpoints

curl --location 'http://localhost:7003/health'

curl --location 'http://localhost:7003/live'

curl --location 'http://localhost:7003/header'

curl --location 'http://localhost:7003/context'

curl --location 'http://localhost:7003/info'

curl --location 'http://localhost:7003/metrics'

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

## Monitoring

Logs: JSON structured logging via zerolog

Metrics: Available through endpoint /metrics via otel/sdk/metric

Trace: The x-request-id is extract from header and is ingest into context, in order the x-request-id do not exist a new one is generated (uuid)

Errors: Structured error handling with custom error types

## Security

Security Headers: Is implement via go-core midleware

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
