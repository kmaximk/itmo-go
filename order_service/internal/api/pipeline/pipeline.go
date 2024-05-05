package pipeline

import (
	"context"
	"fmt"
	"order_service/internal/api/model"
	"sync"
)

const fanLimit = 10

type OrderPipeline interface {
	Start(ctx context.Context, actions model.OrderActions, orders <-chan model.OrderInitialized, processed chan<- model.OrderProcessFinished)
}

type OrderPipelineImplementation struct{}

func NewOrderPipelineImplementation() *OrderPipelineImplementation {
	return &OrderPipelineImplementation{}
}

func (o *OrderPipelineImplementation) Start(
	ctx context.Context,
	actions model.OrderActions,
	orders <-chan model.OrderInitialized,
	processed chan<- model.OrderProcessFinished,
) {
	ch1 := make(chan model.OrderProcessStarted)
	ch2 := make(chan model.OrderFinishedExternalInteraction)
	wg := sync.WaitGroup{}
	addInitToStarted(ctx, orders, ch1, actions, &wg)
	fanOut := make([]chan model.OrderFinishedExternalInteraction, fanLimit)
	for i := 0; i < fanLimit; i++ {
		fanOut[i] = make(chan model.OrderFinishedExternalInteraction)
		addStartedToFEI(ctx, ch1, fanOut[i], actions, &wg)
	}
	fanIn(ctx, fanOut, ch2)
	addFEITOFinished(ctx, ch2, processed, actions, &wg)
	wg.Wait()
}

func fanIn(ctx context.Context, chans []chan model.OrderFinishedExternalInteraction, result chan<- model.OrderFinishedExternalInteraction) {
	wg := sync.WaitGroup{}
	for _, ch := range chans {
		wg.Add(1)
		go func(ch <-chan model.OrderFinishedExternalInteraction) {
			defer wg.Done()
			for elem := range ch {
				select {
				case <-ctx.Done():
					return
				case result <- elem:
				}
			}
		}(ch)
	}
	go func() {
		wg.Wait()
		close(result)
	}()
}
func addInitToStarted(ctx context.Context, stream <-chan model.OrderInitialized, result chan<- model.OrderProcessStarted, actions model.OrderActions, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(result)
		for v := range stream {
			func() {
				defer func(initialized model.OrderInitialized) {
					if r := recover(); r != nil {
						result <- model.OrderProcessStarted{
							OrderInitialized: v,
							OrderStates:      v.OrderStates,
							Error:            fmt.Errorf("panic when processing init to started, cause: %s", r),
						}
					}
				}(v)
				actions.InitToStarted()
				select {
				case <-ctx.Done():
					return
				case result <- model.OrderProcessStarted{
					OrderInitialized: v,
					OrderStates:      append(v.OrderStates, model.ProcessStarted),
					Error:            v.Error,
				}:
				}
			}()
		}
	}()
}

func addStartedToFEI(ctx context.Context, stream <-chan model.OrderProcessStarted, result chan<- model.OrderFinishedExternalInteraction, actions model.OrderActions, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(result)
		for v := range stream {
			func() {
				defer func(initialized model.OrderProcessStarted) {
					if r := recover(); r != nil {
						result <- model.OrderFinishedExternalInteraction{
							OrderProcessStarted: v,
							StorageID:           0,
							PickupPointID:       0,
							OrderStates:         v.OrderStates,
							Error:               fmt.Errorf("panic when processing started to FEI, cause: %s", r),
						}
					}
				}(v)
				if v.Error != nil {
					result <- model.OrderFinishedExternalInteraction{
						OrderProcessStarted: v,
						StorageID:           0,
						PickupPointID:       0,
						OrderStates:         v.OrderStates,
						Error:               v.Error,
					}
					return
				}
				storageID, pickupPointID, err := actions.StartedToFinishedExternalInteraction(v)
				if err != nil {
					result <- model.OrderFinishedExternalInteraction{
						OrderProcessStarted: v,
						StorageID:           0,
						PickupPointID:       0,
						OrderStates:         v.OrderStates,
						Error:               err,
					}
					return
				}
				select {
				case <-ctx.Done():
					return
				case result <- model.OrderFinishedExternalInteraction{
					OrderProcessStarted: v,
					StorageID:           storageID,
					PickupPointID:       pickupPointID,
					OrderStates:         append(v.OrderStates, model.FinishedExternalInteraction),
					Error:               v.Error,
				}:
				}
			}()
		}
	}()
}

func addFEITOFinished(ctx context.Context, stream <-chan model.OrderFinishedExternalInteraction, result chan<- model.OrderProcessFinished, actions model.OrderActions, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range stream {
			func() {
				defer func(initialized model.OrderFinishedExternalInteraction) {
					if r := recover(); r != nil {
						result <- model.OrderProcessFinished{
							OrderFinishedExternalInteraction: v,
							OrderStates:                      v.OrderStates,
							Error:                            fmt.Errorf("panic when processing FEI to finished, cause:, %s", r),
						}
					}
				}(v)
				if v.Error != nil {
					result <- model.OrderProcessFinished{
						OrderFinishedExternalInteraction: v,
						OrderStates:                      v.OrderStates,
						Error:                            v.Error,
					}
					return
				}
				actions.FinishedExternalInteractionToProcessFinished()
				select {
				case <-ctx.Done():
					return
				case result <- model.OrderProcessFinished{
					OrderFinishedExternalInteraction: v,
					OrderStates:                      append(v.OrderStates, model.ProcessFinished),
					Error:                            v.Error,
				}:
				}
			}()
		}
	}()
}
