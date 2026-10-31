package grpcserver_test

import (
	"context"
	"net"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"inventory_service/internal/domain"
	"inventory_service/internal/grpcserver"
	pb "inventory_service/internal/pb/inventoryv1"
	"inventory_service/internal/repository"
)

type mockQuerier struct {
	getProductStockFn        func(ctx context.Context, productID uuid.UUID) (*domain.ProductStock, error)
	listProductsFn           func(ctx context.Context, limit, offset int) ([]domain.ProductStock, int, error)
	checkStockAvailabilityFn func(ctx context.Context, items []domain.StockItemCheck) (*domain.AvailabilityCheckResult, error)
}

func (m *mockQuerier) GetProductStock(ctx context.Context, productID uuid.UUID) (*domain.ProductStock, error) {
	if m.getProductStockFn != nil {
		return m.getProductStockFn(ctx, productID)
	}
	return nil, repository.ErrProductNotFound
}

func (m *mockQuerier) ListProducts(ctx context.Context, limit, offset int) ([]domain.ProductStock, int, error) {
	if m.listProductsFn != nil {
		return m.listProductsFn(ctx, limit, offset)
	}
	return nil, 0, nil
}

func (m *mockQuerier) CheckStockAvailability(ctx context.Context, items []domain.StockItemCheck) (*domain.AvailabilityCheckResult, error) {
	if m.checkStockAvailabilityFn != nil {
		return m.checkStockAvailabilityFn(ctx, items)
	}
	return &domain.AvailabilityCheckResult{Eligible: true, OverallStatus: 1}, nil
}

func setupTestGRPCServer(t *testing.T, querier repository.StockQuerier) (pb.InventoryServiceClient, func()) {
	t.Helper()
	bufferSize := 1024 * 1024
	lis := bufconn.Listen(bufferSize)

	s := grpc.NewServer(
		grpc.UnaryInterceptor(grpcserver.UnaryLoggingInterceptor()),
	)
	pb.RegisterInventoryServiceServer(s, grpcserver.NewServer(querier))

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	client := pb.NewInventoryServiceClient(conn)
	cleanup := func() {
		_ = conn.Close()
		s.Stop()
		_ = lis.Close()
	}

	return client, cleanup
}

func TestCheckStockAvailability_Eligible(t *testing.T) {
	prodID := uuid.New()
	querier := &mockQuerier{
		checkStockAvailabilityFn: func(ctx context.Context, items []domain.StockItemCheck) (*domain.AvailabilityCheckResult, error) {
			return &domain.AvailabilityCheckResult{
				Eligible:      true,
				OverallStatus: 1, // AVAILABILITY_STATUS_OK
				Items: []domain.StockItemCheckResult{
					{
						ProductID:      prodID,
						StatusCode:     1,
						CatalogPrice:   "1500.50",
						AvailableStock: 50,
					},
				},
			}, nil
		},
	}

	client, cleanup := setupTestGRPCServer(t, querier)
	defer cleanup()

	resp, err := client.CheckStockAvailability(context.Background(), &pb.CheckStockAvailabilityRequest{
		Items: []*pb.StockItemRequest{
			{
				ProductId:     prodID.String(),
				Quantity:      2,
				ExpectedPrice: "1500.50",
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if !resp.GetEligible() {
		t.Fatalf("expected eligible=true, got %v", resp.GetEligible())
	}
	if resp.GetOverallStatus() != pb.AvailabilityStatus_AVAILABILITY_STATUS_OK {
		t.Fatalf("expected status OK, got %v", resp.GetOverallStatus())
	}
	if len(resp.GetItems()) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.GetItems()))
	}
}

func TestCheckStockAvailability_InsufficientStock(t *testing.T) {
	prodID := uuid.New()
	querier := &mockQuerier{
		checkStockAvailabilityFn: func(ctx context.Context, items []domain.StockItemCheck) (*domain.AvailabilityCheckResult, error) {
			return &domain.AvailabilityCheckResult{
				Eligible:      false,
				OverallStatus: 4, // AVAILABILITY_STATUS_INSUFFICIENT_STOCK
				Reason:        "insufficient stock",
				Items: []domain.StockItemCheckResult{
					{
						ProductID:      prodID,
						StatusCode:     4,
						CatalogPrice:   "1500.50",
						AvailableStock: 1,
						Reason:         "insufficient stock: requested 5, available 1",
					},
				},
			}, nil
		},
	}

	client, cleanup := setupTestGRPCServer(t, querier)
	defer cleanup()

	resp, err := client.CheckStockAvailability(context.Background(), &pb.CheckStockAvailabilityRequest{
		Items: []*pb.StockItemRequest{
			{
				ProductId:     prodID.String(),
				Quantity:      5,
				ExpectedPrice: "1500.50",
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if resp.GetEligible() {
		t.Fatalf("expected eligible=false, got %v", resp.GetEligible())
	}
	if resp.GetOverallStatus() != pb.AvailabilityStatus_AVAILABILITY_STATUS_INSUFFICIENT_STOCK {
		t.Fatalf("expected status INSUFFICIENT_STOCK, got %v", resp.GetOverallStatus())
	}
}

func TestCheckStockAvailability_EmptyItems(t *testing.T) {
	querier := &mockQuerier{}
	client, cleanup := setupTestGRPCServer(t, querier)
	defer cleanup()

	resp, err := client.CheckStockAvailability(context.Background(), &pb.CheckStockAvailabilityRequest{
		Items: nil,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetEligible() {
		t.Fatalf("expected eligible=false on empty items")
	}
	if resp.GetOverallStatus() != pb.AvailabilityStatus_AVAILABILITY_STATUS_INVALID_QUANTITY {
		t.Fatalf("expected INVALID_QUANTITY status, got %v", resp.GetOverallStatus())
	}
}

func TestGetProductStock_Success(t *testing.T) {
	prodID := uuid.New()
	querier := &mockQuerier{
		getProductStockFn: func(ctx context.Context, productID uuid.UUID) (*domain.ProductStock, error) {
			return &domain.ProductStock{
				ProductID: prodID,
				Name:      "Mechanical Keyboard",
				Price:     "1500.50",
				Available: 42,
				Reserved:  8,
			}, nil
		},
	}

	client, cleanup := setupTestGRPCServer(t, querier)
	defer cleanup()

	resp, err := client.GetProductStock(context.Background(), &pb.GetProductStockRequest{
		ProductId: prodID.String(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetProductId() != prodID.String() {
		t.Fatalf("expected product_id=%s, got %s", prodID, resp.GetProductId())
	}
	if resp.GetName() != "Mechanical Keyboard" || resp.GetPrice() != "1500.50" {
		t.Fatalf("unexpected product info: name=%s, price=%s", resp.GetName(), resp.GetPrice())
	}
	if resp.GetAvailable() != 42 || resp.GetReserved() != 8 {
		t.Fatalf("expected stock 42/8, got %d/%d", resp.GetAvailable(), resp.GetReserved())
	}
}

func TestGetProductStock_NotFound(t *testing.T) {
	querier := &mockQuerier{
		getProductStockFn: func(ctx context.Context, productID uuid.UUID) (*domain.ProductStock, error) {
			return nil, repository.ErrProductNotFound
		},
	}

	client, cleanup := setupTestGRPCServer(t, querier)
	defer cleanup()

	_, err := client.GetProductStock(context.Background(), &pb.GetProductStockRequest{
		ProductId: uuid.New().String(),
	})
	if err == nil {
		t.Fatalf("expected error for not found product, got nil")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected code NotFound, got %v", status.Code(err))
	}
}

func TestGetProductStock_InvalidUUID(t *testing.T) {
	querier := &mockQuerier{}
	client, cleanup := setupTestGRPCServer(t, querier)
	defer cleanup()

	_, err := client.GetProductStock(context.Background(), &pb.GetProductStockRequest{
		ProductId: "invalid-uuid-format",
	})
	if err == nil {
		t.Fatalf("expected error for invalid UUID, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected code InvalidArgument, got %v", status.Code(err))
	}
}

func TestListProducts_Success(t *testing.T) {
	p1 := uuid.New()
	p2 := uuid.New()
	querier := &mockQuerier{
		listProductsFn: func(ctx context.Context, limit, offset int) ([]domain.ProductStock, int, error) {
			return []domain.ProductStock{
				{ProductID: p1, Name: "Keyboard", Price: "1500.00", Available: 10, Reserved: 2},
				{ProductID: p2, Name: "Mouse", Price: "300.00", Available: 20, Reserved: 1},
			}, 2, nil
		},
	}

	client, cleanup := setupTestGRPCServer(t, querier)
	defer cleanup()

	resp, err := client.ListProducts(context.Background(), &pb.ListProductsRequest{
		Limit:  10,
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetTotal() != 2 || len(resp.GetProducts()) != 2 {
		t.Fatalf("expected 2 products, got total=%d, items=%d", resp.GetTotal(), len(resp.GetProducts()))
	}
	if resp.GetProducts()[0].GetName() != "Keyboard" {
		t.Fatalf("expected first product Keyboard, got %s", resp.GetProducts()[0].GetName())
	}
}
