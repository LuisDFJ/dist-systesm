package shared

type WorkerId int;
type WorkerState int;

const (
	IDLE   WorkerState = iota
	MAP
	REDUCE
	EXIT
)

type TaskId int

type TaskParameters struct {
	Id TaskId
	File string
	Bucket WorkerId
}

type ArgGetTask struct {
	Id WorkerId
}

type ResGetTask struct {
	State 	WorkerState
	Params 	TaskParameters
}

type ArgFinishTask struct {
	Id 		TaskId
	Type 	WorkerState
}
type ResFinishTask struct {}


type CoordinatorRCP interface {
	GetTask( arg *ArgGetTask, res *ResGetTask ) error
	FinishTask( arg *ArgFinishTask, res *ResFinishTask ) error
}

