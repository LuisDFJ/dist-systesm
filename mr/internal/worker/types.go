package worker

import (
	"mr/shared"
)

type Worker struct {
	socket string
	id shared.WorkerId
	n  int
}

