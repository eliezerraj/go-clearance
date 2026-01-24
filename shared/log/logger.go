//---------------------------------------
// Component is charge of ingest data in log
//---------------------------------------
package log

import(
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
	go_core_midleware "github.com/eliezerraj/go-core/v2/middleware"
)

type TraceHook struct{}

func (h TraceHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	ctx := e.GetCtx() 

    requestID := go_core_midleware.GetRequestID(ctx)
    if requestID != "" {
        e = e.Str("request-id", requestID)
    } 

	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		// Add the OTel Trace ID and Span ID fields to the log event
		//fmt.Printf("===> TraceID: %d \n", spanCtx.TraceID().String())
		//fmt.Printf("===> SpanID: %d \n", spanCtx.SpanID().String())
		e.Str("traceID", spanCtx.TraceID().String())
		e.Str("spanID", spanCtx.SpanID().String())
	}
}