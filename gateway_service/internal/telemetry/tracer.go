package telemetry

import (
	"context"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

func InitTracer(ctx context.Context, serviceName, endpoint string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		res = resource.Default()
	}

	cleanEndpoint := strings.TrimSpace(endpoint)
	if cleanEndpoint == "" {
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
		)
		otel.SetTracerProvider(tp)
		slog.Info("OTLP endpoint not configured, using local tracer provider", "service", serviceName)
		return tp.Shutdown, nil
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(cleanEndpoint),
	}
	if !strings.HasPrefix(cleanEndpoint, "https://") {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		slog.Warn("Failed to initialize OTLP HTTP exporter, falling back to local provider", "error", err)
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
		)
		otel.SetTracerProvider(tp)
		return tp.Shutdown, nil
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	slog.Info("OpenTelemetry tracer initialized", "service", serviceName, "endpoint", cleanEndpoint)
	return tp.Shutdown, nil
}

type correlationIDKey struct{}

func ContextWithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, correlationID)
}

func CorrelationIDFromContext(ctx context.Context) string {
	if val, ok := ctx.Value(correlationIDKey{}).(string); ok {
		return val
	}
	return ""
}

func InjectGRPCMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}
	if corrID := CorrelationIDFromContext(ctx); corrID != "" {
		md.Set("x-correlation-id", corrID)
	}
	carrier := metadataCarrier{md: md}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return metadata.NewOutgoingContext(ctx, carrier.md)
}

func ExtractGRPCMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	carrier := metadataCarrier{md: md}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

type metadataCarrier struct {
	md metadata.MD
}

func (m metadataCarrier) Get(key string) string {
	vals := m.md.Get(strings.ToLower(key))
	if len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (m metadataCarrier) Set(key, value string) {
	m.md.Set(strings.ToLower(key), value)
}

func (m metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(m.md))
	for k := range m.md {
		keys = append(keys, k)
	}
	return keys
}

func Tracer(name string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name)
}
