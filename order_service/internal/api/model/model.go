package model

import "sync"

type OrderState string

const (
	Initialized                 OrderState = "order_initialized"
	ProcessStarted              OrderState = "order_process_started"
	FinishedExternalInteraction OrderState = "order_finished_external_interaction"
	ProcessFinished             OrderState = "order_process_finished"
)

type OrderDestinationID struct {
	StorageID     int
	PickupPointID int
}

var OrderDestinationInfo = struct {
	Data  map[int]OrderDestinationID
	Mutex sync.Mutex
}{
	Data:  make(map[int]OrderDestinationID),
	Mutex: sync.Mutex{},
}

type OrderActions struct {
	InitToStarted                                func()
	StartedToFinishedExternalInteraction         func(OrderProcessStarted) (int, int, error)
	FinishedExternalInteractionToProcessFinished func()
}

type OrderStates interface {
	OrderInitialized | OrderProcessStarted | OrderFinishedExternalInteraction | OrderProcessFinished
}

type OrderInitialized struct {
	OrderID     int
	ProductID   int
	OrderStates []OrderState
	Error       error
}

type OrderProcessStarted struct {
	OrderInitialized OrderInitialized
	OrderStates      []OrderState
	Error            error
}

type OrderFinishedExternalInteraction struct {
	OrderProcessStarted OrderProcessStarted
	StorageID           int
	PickupPointID       int
	OrderStates         []OrderState
	Error               error
}

type OrderProcessFinished struct {
	OrderFinishedExternalInteraction OrderFinishedExternalInteraction
	OrderStates                      []OrderState
	Error                            error
}

type Order struct {
	OrderID       int
	ProductID     int
	StorageID     int
	PickupPointID int
	IsProcessed   bool
	OrderStates   []OrderState
}
