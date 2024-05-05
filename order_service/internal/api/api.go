package api

import (
	"context"
	"order_service/pkg/generated/proto/data_manager"

	"fmt"
	"google.golang.org/protobuf/types/known/emptypb"
	"order_service/internal/api/generator"
	"order_service/internal/api/model"
	"order_service/internal/api/workerpool"
	"order_service/pkg/generated/proto/order_service"
)

var workersCount = 5

type OrderServiceApi struct {
	dataClient data_manager.DataManagerClient
	order_service.UnimplementedOrderServiceServer
}

func NewOrderServiceApi(client data_manager.DataManagerClient) *OrderServiceApi {
	return &OrderServiceApi{
		dataClient:                      client,
		UnimplementedOrderServiceServer: order_service.UnimplementedOrderServiceServer{},
	}
}

func (orderService *OrderServiceApi) ProcessOrders(ctx context.Context, orders *order_service.ProcessOrdersRequest) (*order_service.ProcessOrdersResponse, error) {
	fmt.Println(orders.Orders)
	generatorImpl := generator.NewOrderGeneratorImplementation()
	ordersStream := generatorImpl.GenerateOrdersStream(ctx, orders.Orders)
	wp := workerpool.NewOrderWorkerPoolImplementation()
	for i := range orders.Orders {
		fmt.Println(i)
	}

	startedToFinishedExternalInteraction := func(orderStarted model.OrderProcessStarted) (int, int, error) {
		OrderID := orderStarted.OrderInitialized.OrderID
		_, err := orderService.dataClient.GetOrderDataCallback(ctx, &data_manager.GetOrderDataCallbackRequest{OrderId: int32(OrderID)})
		if err != nil {
			return 0, 0, err
		}
		t, got := model.OrderDestinationInfo.Data[OrderID]
		if !got {
			return 0, 0, fmt.Errorf("cannot get Storage and PickupPoint")
		}
		return t.StorageID, t.PickupPointID, nil
	}

	orderActions := model.OrderActions{
		InitToStarted:                                func() {},
		StartedToFinishedExternalInteraction:         startedToFinishedExternalInteraction,
		FinishedExternalInteractionToProcessFinished: func() {},
	}
	result := wp.StartWorkerPool(ctx, ordersStream, orderActions, workersCount)
	response := make([]*order_service.ProcessOrdersResponse_OrderResponse, 0)
	for i := range result {
		response = append(response, &order_service.ProcessOrdersResponse_OrderResponse{
			OrderId:       int32(i.OrderFinishedExternalInteraction.OrderProcessStarted.OrderInitialized.OrderID),
			ProductId:     int32(i.OrderFinishedExternalInteraction.OrderProcessStarted.OrderInitialized.ProductID),
			StorageId:     int32(i.OrderFinishedExternalInteraction.StorageID),
			PickupPointId: int32(i.OrderFinishedExternalInteraction.PickupPointID),
			IsProcessed:   i.Error == nil,
		})
	}
	return &order_service.ProcessOrdersResponse{
		ProcessedOrders: response,
	}, nil
}

func (orderService *OrderServiceApi) SendOrderDataCallback(_ context.Context, order *order_service.SendOrderDataCallbackRequest) (*emptypb.Empty, error) {
	model.OrderDestinationInfo.Mutex.Lock()
	model.OrderDestinationInfo.Data[int(order.OrderId)] = model.OrderDestinationID{
		StorageID:     int(order.StorageId),
		PickupPointID: int(order.PickupPointId),
	}
	model.OrderDestinationInfo.Mutex.Unlock()
	return &emptypb.Empty{}, nil
}
