package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"inventory_service/internal/domain"
	pb "inventory_service/internal/pb/inventoryv1"
	"inventory_service/internal/repository"
)

type Server struct {
	pb.UnimplementedInventoryServiceServer
	querier repository.StockQuerier
}

func NewServer(querier repository.StockQuerier) *Server {
	return &Server{
		querier: querier,
	}
}

func (s *Server) CheckStockAvailability(ctx context.Context, req *pb.CheckStockAvailabilityRequest) (*pb.CheckStockAvailabilityResponse, error) {
	if req == nil || len(req.GetItems()) == 0 {
		return &pb.CheckStockAvailabilityResponse{
			Eligible:      false,
			OverallStatus: pb.AvailabilityStatus_AVAILABILITY_STATUS_INVALID_QUANTITY,
			Reason:        "items list cannot be empty",
		}, nil
	}

	items := make([]domain.StockItemCheck, 0, len(req.GetItems()))
	for _, item := range req.GetItems() {
		prodUUID, err := uuid.Parse(item.GetProductId())
		if err != nil {
			prodUUID = uuid.Nil
		}
		items = append(items, domain.StockItemCheck{
			ProductID:     prodUUID,
			Quantity:      int(item.GetQuantity()),
			ExpectedPrice: item.GetExpectedPrice(),
		})
	}

	res, err := s.querier.CheckStockAvailability(ctx, items)
	if err != nil {
		slog.Error("CheckStockAvailability query failed", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to check stock availability: %v", err)
	}

	respItems := make([]*pb.StockItemResult, 0, len(res.Items))
	for _, it := range res.Items {
		respItems = append(respItems, &pb.StockItemResult{
			ProductId:      it.ProductID.String(),
			Status:         pb.AvailabilityStatus(it.StatusCode),
			CatalogPrice:   it.CatalogPrice,
			AvailableStock: int32(it.AvailableStock),
			Reason:         it.Reason,
		})
	}

	return &pb.CheckStockAvailabilityResponse{
		Eligible:      res.Eligible,
		OverallStatus: pb.AvailabilityStatus(res.OverallStatus),
		Items:         respItems,
		Reason:        res.Reason,
	}, nil
}

func (s *Server) GetProductStock(ctx context.Context, req *pb.GetProductStockRequest) (*pb.GetProductStockResponse, error) {
	if req == nil || req.GetProductId() == "" {
		return nil, status.Error(codes.InvalidArgument, "product_id is required")
	}

	prodUUID, err := uuid.Parse(req.GetProductId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid product_id UUID format")
	}

	stock, err := s.querier.GetProductStock(ctx, prodUUID)
	if err != nil {
		if errors.Is(err, repository.ErrProductNotFound) {
			return nil, status.Errorf(codes.NotFound, "product %s not found in catalog", prodUUID)
		}
		return nil, status.Errorf(codes.Internal, "failed to query product: %v", err)
	}

	return &pb.GetProductStockResponse{
		ProductId: stock.ProductID.String(),
		Name:      stock.Name,
		Price:     stock.Price,
		Available: int32(stock.Available),
		Reserved:  int32(stock.Reserved),
	}, nil
}

func (s *Server) ListProducts(ctx context.Context, req *pb.ListProductsRequest) (*pb.ListProductsResponse, error) {
	limit := int(req.GetLimit())
	offset := int(req.GetOffset())
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	products, total, err := s.querier.ListProducts(ctx, limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list products: %v", err)
	}

	resList := make([]*pb.GetProductStockResponse, 0, len(products))
	for _, p := range products {
		resList = append(resList, &pb.GetProductStockResponse{
			ProductId: p.ProductID.String(),
			Name:      p.Name,
			Price:     p.Price,
			Available: int32(p.Available),
			Reserved:  int32(p.Reserved),
		})
	}

	return &pb.ListProductsResponse{
		Products: resList,
		Total:    int32(total),
	}, nil
}

// UnaryLoggingInterceptor extracts correlation ID from incoming metadata and logs duration and status.
func UnaryLoggingInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		var correlationID string
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get("x-correlation-id"); len(vals) > 0 {
				correlationID = vals[0]
			}
		}

		resp, err := handler(ctx, req)
		duration := time.Since(start)

		grpcCode := status.Code(err)
		if err != nil {
			slog.Warn("gRPC request failed",
				"method", info.FullMethod,
				"code", grpcCode.String(),
				"duration", duration,
				"correlation_id", correlationID,
				"error", err,
			)
		} else {
			slog.Info("gRPC request handled",
				"method", info.FullMethod,
				"code", grpcCode.String(),
				"duration", duration,
				"correlation_id", correlationID,
			)
		}

		return resp, err
	}
}
