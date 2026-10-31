package inventory_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"gateway_service/internal/inventory"
	pb "gateway_service/internal/pb/inventoryv1"
	"gateway_service/internal/telemetry"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type mockGRPCInventoryService struct {
	pb.UnimplementedInventoryServiceServer
	listProductsFn           func(ctx context.Context, req *pb.ListProductsRequest) (*pb.ListProductsResponse, error)
	checkStockAvailabilityFn func(ctx context.Context, req *pb.CheckStockAvailabilityRequest) (*pb.CheckStockAvailabilityResponse, error)
	getProductStockFn        func(ctx context.Context, req *pb.GetProductStockRequest) (*pb.GetProductStockResponse, error)
}

func (m *mockGRPCInventoryService) ListProducts(ctx context.Context, req *pb.ListProductsRequest, opts ...grpc.CallOption) (*pb.ListProductsResponse, error) {
	if m.listProductsFn != nil {
		return m.listProductsFn(ctx, req)
	}
	return &pb.ListProductsResponse{}, nil
}

func (m *mockGRPCInventoryService) CheckStockAvailability(ctx context.Context, req *pb.CheckStockAvailabilityRequest, opts ...grpc.CallOption) (*pb.CheckStockAvailabilityResponse, error) {
	if m.checkStockAvailabilityFn != nil {
		return m.checkStockAvailabilityFn(ctx, req)
	}
	return &pb.CheckStockAvailabilityResponse{}, nil
}

func (m *mockGRPCInventoryService) GetProductStock(ctx context.Context, req *pb.GetProductStockRequest, opts ...grpc.CallOption) (*pb.GetProductStockResponse, error) {
	if m.getProductStockFn != nil {
		return m.getProductStockFn(ctx, req)
	}
	return nil, status.Error(codes.NotFound, "product not found")
}

func TestInventoryHandler_ListProducts(t *testing.T) {
	prodID := uuid.New().String()
	mockSvc := &mockGRPCInventoryService{
		listProductsFn: func(ctx context.Context, req *pb.ListProductsRequest) (*pb.ListProductsResponse, error) {
			return &pb.ListProductsResponse{
				Products: []*pb.GetProductStockResponse{
					{
						ProductId: prodID,
						Name:      "Wireless Keyboard",
						Price:     "1500.50",
						Available: 40,
						Reserved:  5,
					},
				},
				Total: 1,
			}, nil
		},
	}

	client := inventory.NewClientWithService(mockSvc)
	h := inventory.NewHandler(client)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/inventory/products?limit=10&offset=0", nil)
	w := httptest.NewRecorder()
	h.ListProducts(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp inventory.ListProductsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.Total != 1 || len(resp.Products) != 1 {
		t.Fatalf("expected 1 product, got total=%d, count=%d", resp.Total, len(resp.Products))
	}
	if resp.Products[0].ProductID != prodID || resp.Products[0].Name != "Wireless Keyboard" {
		t.Fatalf("unexpected product content: %+v", resp.Products[0])
	}
}

func TestInventoryHandler_CheckStockAvailability(t *testing.T) {
	prodID := uuid.New().String()
	mockSvc := &mockGRPCInventoryService{
		checkStockAvailabilityFn: func(ctx context.Context, req *pb.CheckStockAvailabilityRequest) (*pb.CheckStockAvailabilityResponse, error) {
			return &pb.CheckStockAvailabilityResponse{
				Eligible:      true,
				OverallStatus: pb.AvailabilityStatus_AVAILABILITY_STATUS_OK,
				Reason:        "",
				Items: []*pb.StockItemResult{
					{
						ProductId:      prodID,
						Status:         pb.AvailabilityStatus_AVAILABILITY_STATUS_OK,
						CatalogPrice:   "1500.50",
						AvailableStock: 50,
						Reason:         "",
					},
				},
			}, nil
		},
	}

	client := inventory.NewClientWithService(mockSvc)
	h := inventory.NewHandler(client)

	reqBody, _ := json.Marshal(inventory.CheckRequest{
		Items: []inventory.CheckItemRequest{
			{
				ProductID:     prodID,
				Quantity:      2,
				ExpectedPrice: "1500.50",
			},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/inventory/check", bytes.NewReader(reqBody))
	w := httptest.NewRecorder()
	h.CheckStockAvailability(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp inventory.CheckResponseJSON
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if !resp.Eligible || resp.OverallStatus != "OK" {
		t.Fatalf("expected eligible OK, got: %+v", resp)
	}
	if len(resp.Items) != 1 || resp.Items[0].ProductID != prodID {
		t.Fatalf("unexpected items: %+v", resp.Items)
	}
}

func TestInventoryHandler_GetProductStock(t *testing.T) {
	prodID := uuid.New().String()
	mockSvc := &mockGRPCInventoryService{
		getProductStockFn: func(ctx context.Context, req *pb.GetProductStockRequest) (*pb.GetProductStockResponse, error) {
			if req.GetProductId() == prodID {
				return &pb.GetProductStockResponse{
					ProductId: prodID,
					Name:      "Mouse",
					Price:     "300.00",
					Available: 10,
					Reserved:  2,
				}, nil
			}
			return nil, status.Error(codes.NotFound, "product not found")
		},
	}

	client := inventory.NewClientWithService(mockSvc)
	h := inventory.NewHandler(client)

	// Found
	reqFound := httptest.NewRequest(http.MethodGet, "/api/v1/inventory/products/"+prodID, nil)
	wFound := httptest.NewRecorder()
	h.GetProductStock(wFound, reqFound)
	if wFound.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", wFound.Code)
	}

	// Not Found
	reqNotFound := httptest.NewRequest(http.MethodGet, "/api/v1/inventory/products/"+uuid.New().String(), nil)
	wNotFound := httptest.NewRecorder()
	h.GetProductStock(wNotFound, reqNotFound)
	if wNotFound.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", wNotFound.Code)
	}
}

func TestInventoryClient_CorrelationIDMetadataPropagation(t *testing.T) {
	var capturedMD metadata.MD
	mockSvc := &mockGRPCInventoryService{
		listProductsFn: func(ctx context.Context, req *pb.ListProductsRequest) (*pb.ListProductsResponse, error) {
			if md, ok := metadata.FromOutgoingContext(ctx); ok {
				capturedMD = md
			}
			return &pb.ListProductsResponse{}, nil
		},
	}
	client := inventory.NewClientWithService(mockSvc)
	ctx := telemetry.ContextWithCorrelationID(context.Background(), "test-corr-id-999")
	_, err := client.ListProducts(ctx, 10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vals := capturedMD.Get("x-correlation-id"); len(vals) == 0 || vals[0] != "test-corr-id-999" {
		t.Fatalf("expected x-correlation-id in gRPC metadata, got %v", vals)
	}
}
