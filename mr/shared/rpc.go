package shared

type WorkerState int;

const (
	IDLE   WorkerState = iota
	MAP
	REDUCE
	EXIT
)

type ReqGetWork struct {

}

type ResGetWork struct {

}

type ReqEndWork struct {

}

type ResEndWork struct {

}


type CoordinatorRCP interface {
	GetWork( req ReqGetWork, res *ResGetWork ) error
	EndWork( req ReqEndWork, res *ResEndWork ) error
}

