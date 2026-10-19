package proxy

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"gateway_service/internal/auth"
)

type OrdersProxy struct {
	target           *url.URL
	reverseProxy     *httputil.ReverseProxy
	internalAPIToken string
}

func NewOrdersProxy(targetURL string, internalAPIToken string) (*OrdersProxy, error) {
	return NewOrdersProxyWithTransport(targetURL, internalAPIToken, nil)
}

func NewOrdersProxyWithTransport(targetURL string, internalAPIToken string, transport http.RoundTripper) (*OrdersProxy, error) {
	u, err := url.Parse(targetURL)
	if err != nil {
		return nil, err
	}

	p := &OrdersProxy{
		target:           u,
		internalAPIToken: internalAPIToken,
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			// Rewrite URL target and path: /api/v1/orders/* -> /orders/*
			pr.SetURL(u)
			pr.Out.URL.Path = strings.TrimPrefix(pr.In.URL.Path, "/api/v1")
			pr.Out.URL.RawPath = strings.TrimPrefix(pr.In.URL.RawPath, "/api/v1")
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery

			// 1. Strip ALL incoming client X-* headers to prevent spoofing
			for name := range pr.In.Header {
				if strings.HasPrefix(strings.ToLower(name), "x-") {
					pr.Out.Header.Del(name)
				}
			}

			// 2. Inject verified user context from authenticated JWT
			claims, ok := auth.UserFromContext(pr.In.Context())
			if ok && claims != nil {
				pr.Out.Header.Set("X-User-Id", claims.UserID.String())
				pr.Out.Header.Set("X-User-Role", string(claims.Role))

				// 3. For warehouse operators and admins, inject secret internal API token
				if claims.Role == auth.RoleWarehouseOperator || claims.Role == auth.RoleAdmin {
					pr.Out.Header.Set("X-Internal-Token", p.internalAPIToken)
				}
			}

			// 4. Inject W3C traceparent into outgoing request to order_service
			otel.GetTextMapPropagator().Inject(pr.In.Context(), propagation.HeaderCarrier(pr.Out.Header))
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Error("Reverse proxy to order_service failed", "error", err, "path", r.URL.Path)
			http.Error(w, `{"error":"order service unavailable"}`, http.StatusBadGateway)
		},
	}
	if transport != nil {
		rp.Transport = transport
	}

	p.reverseProxy = rp
	return p, nil
}

func (p *OrdersProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.reverseProxy.ServeHTTP(w, r)
}
