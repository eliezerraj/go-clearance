//---------------------------------------
// Component is charge of ingest data in log
//---------------------------------------
package log

import(
	"fmt"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

type TraceHook struct{}

func (h TraceHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	ctx := e.GetCtx() 

	traceRequestId := fmt.Sprintf("%v",ctx.Value("trace-request-id"))
	
	if traceRequestId != "<nil>"{
		e = e.Str("trace-request-id", traceRequestId)
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