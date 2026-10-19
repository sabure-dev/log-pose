package domain

import "github.com/google/uuid"

type ProductStock struct {
	ProductID uuid.UUID
	Name      string
	Price     string
	Available int
	Reserved  int
}

type StockItemCheck struct {
	ProductID     uuid.UUID
	Quantity      int
	ExpectedPrice string
}

type StockItemCheckResult struct {
	ProductID      uuid.UUID
	StatusCode     int32
	CatalogPrice   string
	AvailableStock int
	Reason         string
}

type AvailabilityCheckResult struct {
	Eligible      bool
	OverallStatus int32
	Items         []StockItemCheckResult
	Reason        string
}
