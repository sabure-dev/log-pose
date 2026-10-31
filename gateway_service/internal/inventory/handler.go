package inventory

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	pb "gateway_service/internal/pb/inventoryv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	client *Client
}

func NewHandler(client *Client) *Handler {
	return &Handler{
		client: client,
	}
}

type CheckItemRequest struct {
	ProductID     string `json:"product_id"`
	Quantity      int32  `json:"quantity"`
	ExpectedPrice string `json:"expected_price,omitempty"`
}

type CheckRequest struct {
	Items []CheckItemRequest `json:"items"`
}

type ProductStockResponse struct {
	ProductID string `json:"product_id"`
	Name      string `json:"name"`
	Price     string `json:"price"`
	Available int32  `json:"available"`
	Reserved  int32  `json:"reserved"`
}

type ListProductsResponse struct {
	Products []ProductStockResponse `json:"products"`
	Total    int32                  `json:"total"`
}

type StockItemResultJSON struct {
	ProductID      string `json:"product_id"`
	Status         string `json:"status"`
	CatalogPrice   string `json:"catalog_price,omitempty"`
	AvailableStock int32  `json:"available_stock"`
	Reason         string `json:"reason,omitempty"`
}

type CheckResponseJSON struct {
	Eligible      bool                  `json:"eligible"`
	OverallStatus string                `json:"overall_status"`
	Reason        string                `json:"reason,omitempty"`
	Items         []StockItemResultJSON `json:"items"`
}

func cleanStatusName(s pb.AvailabilityStatus) string {
	return strings.TrimPrefix(s.String(), "AVAILABILITY_STATUS_")
}

func (h *Handler) ListProducts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := int32(50)
	offset := int32(0)

	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = int32(l)
		}
	}
	if oStr := r.URL.Query().Get("offset"); oStr != "" {
		if o, err := strconv.Atoi(oStr); err == nil && o >= 0 {
			offset = int32(o)
		}
	}

	res, err := h.client.ListProducts(r.Context(), limit, offset)
	if err != nil {
		handleGRPCError(w, err)
		return
	}

	prods := make([]ProductStockResponse, 0, len(res.GetProducts()))
	for _, p := range res.GetProducts() {
		prods = append(prods, ProductStockResponse{
			ProductID: p.GetProductId(),
			Name:      p.GetName(),
			Price:     p.GetPrice(),
			Available: p.GetAvailable(),
			Reserved:  p.GetReserved(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ListProductsResponse{
		Products: prods,
		Total:    res.GetTotal(),
	})
}

func (h *Handler) CheckStockAvailability(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "malformed request payload", http.StatusBadRequest)
		return
	}

	pbItems := make([]*pb.StockItemRequest, 0, len(req.Items))
	for _, it := range req.Items {
		pbItems = append(pbItems, &pb.StockItemRequest{
			ProductId:     it.ProductID,
			Quantity:      it.Quantity,
			ExpectedPrice: it.ExpectedPrice,
		})
	}

	res, err := h.client.CheckStockAvailability(r.Context(), &pb.CheckStockAvailabilityRequest{
		Items: pbItems,
	})
	if err != nil {
		handleGRPCError(w, err)
		return
	}

	itemResults := make([]StockItemResultJSON, 0, len(res.GetItems()))
	for _, it := range res.GetItems() {
		itemResults = append(itemResults, StockItemResultJSON{
			ProductID:      it.GetProductId(),
			Status:         cleanStatusName(it.GetStatus()),
			CatalogPrice:   it.GetCatalogPrice(),
			AvailableStock: it.GetAvailableStock(),
			Reason:         it.GetReason(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(CheckResponseJSON{
		Eligible:      res.GetEligible(),
		OverallStatus: cleanStatusName(res.GetOverallStatus()),
		Reason:        res.GetReason(),
		Items:         itemResults,
	})
}

func (h *Handler) GetProductStock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/v1/inventory/products/")
	id = strings.TrimSpace(id)
	if id == "" {
		http.Error(w, "product_id is required", http.StatusBadRequest)
		return
	}

	res, err := h.client.GetProductStock(r.Context(), id)
	if err != nil {
		handleGRPCError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ProductStockResponse{
		ProductID: res.GetProductId(),
		Name:      res.GetName(),
		Price:     res.GetPrice(),
		Available: res.GetAvailable(),
		Reserved:  res.GetReserved(),
	})
}

func handleGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	switch st.Code() {
	case codes.NotFound:
		http.Error(w, st.Message(), http.StatusNotFound)
	case codes.InvalidArgument:
		http.Error(w, st.Message(), http.StatusBadRequest)
	case codes.Unavailable:
		http.Error(w, `{"error":"inventory service unavailable"}`, http.StatusBadGateway)
	default:
		http.Error(w, st.Message(), http.StatusInternalServerError)
	}
}
