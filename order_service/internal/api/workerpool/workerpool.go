package workerpool

import (
	"context"
	"order_service/internal/api/model"
	"order_service/internal/api/pipeline"
	"sync"
)

type OrderWorkerPool interface {
	StartWorkerPool(ctx context.Context, orders <-chan model.OrderInitialized, additionalActions model.OrderActions, workersCount int) <-chan model.OrderProcessFinished
}

type OrderWorkerPoolImplementation struct{}

func NewOrderWorkerPoolImplementation() *OrderWorkerPoolImplementation {
	return &OrderWorkerPoolImplementation{}
}

func (o *OrderWorkerPoolImplementation) StartWorkerPool(ctx context.Context, orders <-chan model.OrderInitialized, additionalActions model.OrderActions, workersCount int) <-chan model.OrderProcessFinished {
	results := make(chan model.OrderProcessFinished)
	var wg sync.WaitGroup
	wg.Add(workersCount)
	for i := 0; i < workersCount; i++ {
		go func() {
			defer wg.Done()
			pipeline.NewOrderPipelineImplementation().Start(ctx, additionalActions, orders, results)
		}()
	}
	go func() {
		defer close(results)
		wg.Wait()
	}()
	return results
}
