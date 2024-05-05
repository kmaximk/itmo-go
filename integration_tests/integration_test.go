package main

import (
	"context"
	"data_manager/pkg/generated/proto/data_manager"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"math"
	"math/rand"
	"order_service/pkg/generated/proto/order_service"
	"os/exec"
	"slices"
	"testing"
	"time"
)

func TestSuccessfulScenario(t *testing.T) {
	ctx := context.Background()

	orderServiceStart := exec.Command("../order_service/bin/app", "-data_manager_address", "localhost:8086", "-app_port", "8085")
	dataManagerServiceStart := exec.Command("../data_manager/bin/app", "-order_address", "localhost:8085", "-app_port", "8086")

	defer func() {
		err := orderServiceStart.Process.Kill()

		if err != nil {
			log.Println(err)
		}
	}()

	go func() {
		require.NoError(t, orderServiceStart.Run())
	}()

	defer func() {
		err := dataManagerServiceStart.Process.Kill()

		if err != nil {
			log.Println(err)
		}
	}()

	go func() {
		code := dataManagerServiceStart.Run()
		require.NoError(t, code)
	}()

	time.Sleep(time.Second * 10)

	orderServiceConnect, err := grpc.Dial("localhost:8085", []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(),
	}...)

	require.NoError(t, err)

	if err != nil {
		t.Failed()
	}

	orderServiceClient := order_service.NewOrderServiceClient(orderServiceConnect)

	dataManagerConnect, err := grpc.Dial("localhost:8086", []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(),
	}...)

	if err != nil {
		t.Failed()
	}

	dataManagerClient := data_manager.NewDataManagerClient(dataManagerConnect)

	requestOrders := make([]*order_service.ProcessOrdersRequest_OrderRequest, 0)

	for i := 0; i < 5; i++ {
		requestOrders = append(requestOrders, &order_service.ProcessOrdersRequest_OrderRequest{
			OrderId:   int32(rand.Intn(math.MaxInt32-1) + 1),
			ProductId: int32(rand.Intn(math.MaxInt32-1) + 1),
		})
	}

	response, err := orderServiceClient.ProcessOrders(ctx, &order_service.ProcessOrdersRequest{
		Orders: requestOrders,
	})

	require.NoError(t, err)

	innerResponse, err := dataManagerClient.GetOrdersData(ctx, &emptypb.Empty{})

	require.NoError(t, err)

	require.Equal(t, len(response.ProcessedOrders), len(innerResponse.Orders))

	slices.SortFunc(response.ProcessedOrders, func(a, b *order_service.ProcessOrdersResponse_OrderResponse) int {
		return int(a.OrderId - b.OrderId)
	})

	slices.SortFunc(innerResponse.Orders, func(a, b *data_manager.GetOrdersDataResponse_OrderData) int {
		return int(a.OrderId - b.OrderId)
	})

	for i := 0; i < len(response.ProcessedOrders); i++ {
		responseOrder := response.ProcessedOrders[i]
		innerResponseOrder := innerResponse.Orders[i]

		require.Equal(t, responseOrder.OrderId, innerResponseOrder.OrderId)
		require.Equal(t, responseOrder.StorageId, innerResponseOrder.StorageId)
		require.Equal(t, responseOrder.PickupPointId, innerResponseOrder.PickupPointId)
		require.True(t, responseOrder.IsProcessed)
	}
}
