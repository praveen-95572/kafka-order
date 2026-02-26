package model

type OrderStatus string

const (
	Created  OrderStatus = "CREATED"
	Verified OrderStatus = "VERIFIED"
	Settled  OrderStatus = "SETTLED"
)

type Order struct {
	OrderID string      `json:"order_id"`
	Status  OrderStatus `json:"status"`
}
