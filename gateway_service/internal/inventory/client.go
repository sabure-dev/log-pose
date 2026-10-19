package inventory

import (
	"context"
	"fmt"
	"time"

	pb "gateway_service/internal/pb/inventoryv1"
	"gateway_service/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn    pb.InventoryServiceClient
	rawConn *grpc.ClientConn
}

func NewClient(target string) (*Client, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to inventory grpc service at %s: %w", target, err)
	}

	return &Client{
		conn:    pb.NewInventoryServiceClient(conn),
		rawConn: conn,
	}, nil
}

func NewClientWithService(service pb.InventoryServiceClient) *Client {
	return &Client{
		conn: service,
	}
}

func (c *Client) Close() error {
	if c.rawConn != nil {
		return c.rawConn.Close()
	}
	return nil
}

func (c *Client) ListProducts(ctx context.Context, limit, offset int32) (*pb.ListProductsResponse, error) {
	ctx = telemetry.InjectGRPCMetadata(ctx)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.conn.ListProducts(ctx, &pb.ListProductsRequest{
		Limit:  limit,
		Offset: offset,
	})
}

func (c *Client) CheckStockAvailability(ctx context.Context, req *pb.CheckStockAvailabilityRequest) (*pb.CheckStockAvailabilityResponse, error) {
	ctx = telemetry.InjectGRPCMetadata(ctx)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.conn.CheckStockAvailability(ctx, req)
}

func (c *Client) GetProductStock(ctx context.Context, productID string) (*pb.GetProductStockResponse, error) {
	ctx = telemetry.InjectGRPCMetadata(ctx)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.conn.GetProductStock(ctx, &pb.GetProductStockRequest{
		ProductId: productID,
	})
}
