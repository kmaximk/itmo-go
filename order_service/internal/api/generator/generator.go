package generator

import (
	"context"

	"order_service/internal/api/model"
	"order_service/pkg/generated/proto/order_service"
)

type OrderGenerator interface {
	GenerateOrdersStream(ctx context.Context, orders []model.OrderInitialized) <-chan model.OrderInitialized
}

type OrderGeneratorImplementation struct{}

func NewOrderGeneratorImplementation() *OrderGeneratorImplementation {
	return &OrderGeneratorImplementation{}
}

func (o *OrderGeneratorImplementation) GenerateOrdersStream(ctx context.Context, orders []*order_service.ProcessOrdersRequest_OrderRequest) <-chan model.OrderInitialized {
	result := make(chan model.OrderInitialized)
	go func() {
		defer close(result)
		for _, elem := range orders {
			order := model.OrderInitialized{
				OrderID:     int(elem.OrderId),
				ProductID:   int(elem.ProductId),
				OrderStates: make([]model.OrderState, 0),
				Error:       nil,
			}
			select {
			case <-ctx.Done():
				return
			case result <- order:
			}
		}
	}()
	return result
}
