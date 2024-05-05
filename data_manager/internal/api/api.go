package api

import (
	"context"
	"data_manager/internal/model"
	"data_manager/internal/repository"
	"data_manager/pkg/generated/proto/data_manager"
	"data_manager/pkg/generated/proto/order_service"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"math"
	"math/rand"
)

type DataManagerApi struct {
	repository         repository.OrderRepository
	orderServiceClient order_service.OrderServiceClient
	data_manager.UnimplementedDataManagerServer
}

func NewDataManagerApi(repository repository.OrderRepository, orderServiceClient order_service.OrderServiceClient) *DataManagerApi {
	return &DataManagerApi{
		repository:         repository,
		orderServiceClient: orderServiceClient,
	}
}

func (d *DataManagerApi) GetOrderDataCallback(ctx context.Context, data *data_manager.GetOrderDataCallbackRequest) (*emptypb.Empty, error) {
	orderData := model.OrderData{
		OrderID:       int(data.OrderId),
		StorageID:     rand.Intn(math.MaxInt32-1) + 1,
		PickupPointID: rand.Intn(math.MaxInt32-1) + 1,
	}

	err := d.repository.InsertOrder(orderData)

	if err != nil {
		log.Println(err)
		return nil, status.Errorf(codes.Unknown, fmt.Sprintf("error - %v", err))
	}

	_, err = d.orderServiceClient.SendOrderDataCallback(ctx, &order_service.SendOrderDataCallbackRequest{
		OrderId:       int32(orderData.OrderID),
		StorageId:     int32(orderData.StorageID),
		PickupPointId: int32(orderData.PickupPointID),
	})

	if err != nil {
		log.Println(err)
		return nil, status.Errorf(codes.Unknown, fmt.Sprintf("error - %v", err))
	}

	return &emptypb.Empty{}, nil
}

func (d *DataManagerApi) GetOrdersData(ctx context.Context, empty *emptypb.Empty) (*data_manager.GetOrdersDataResponse, error) {
	return &data_manager.GetOrdersDataResponse{
		Orders: d.toGrpcOrders(d.repository.GetAllOrders()),
	}, nil
}

func (d *DataManagerApi) toGrpcOrders(orders []model.OrderData) []*data_manager.GetOrdersDataResponse_OrderData {
	result := make([]*data_manager.GetOrdersDataResponse_OrderData, 0, len(orders))

	for _, o := range orders {
		result = append(result, &data_manager.GetOrdersDataResponse_OrderData{
			OrderId:       int32(o.OrderID),
			StorageId:     int32(o.StorageID),
			PickupPointId: int32(o.PickupPointID),
		})
	}

	return result
}
