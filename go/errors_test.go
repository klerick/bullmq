package bullmq

import (
	"errors"
	"strings"
	"testing"
)

// Error-code mapping ported from python scripts.py::finishedErrors + error_code.py.
func TestFinishedError(t *testing.T) {
	cases := []struct {
		code ScriptErrorCode
		ctx  errorContext
		want string
	}{
		{CodeJobNotExist, errorContext{jobID: "1", command: "moveToFinished"}, "Missing key for job 1"},
		{CodeJobLockNotExist, errorContext{jobID: "1", command: "moveToFinished"}, "Missing lock for job 1"},
		{CodeJobNotInState, errorContext{jobID: "1", command: "moveToFinished", state: "active"}, "not in the active state"},
		{CodeJobPendingDependencies, errorContext{jobID: "1", command: "moveToFinished"}, "pending dependencies"},
		{CodeParentJobNotExist, errorContext{parentKey: "bull:q:2", command: "addJob"}, "Missing key for parent job bull:q:2"},
		{CodeJobLockMismatch, errorContext{jobID: "1", command: "moveToFinished", state: "active"}, "Lock mismatch for job 1"},
		{CodeJobHasFailedChildren, errorContext{jobID: "1", command: "moveToFinished"}, "at least one failed child"},
	}
	for _, c := range cases {
		err := finishedError(c.code, c.ctx)
		if err == nil {
			t.Errorf("code %d: got nil error", c.code)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("code %d: %q does not contain %q", c.code, err.Error(), c.want)
		}
	}
}

// A failed-children code is unrecoverable — the worker must not retry it.
func TestFinishedErrorUnrecoverable(t *testing.T) {
	err := finishedError(CodeJobHasFailedChildren, errorContext{jobID: "1", command: "moveToFinished"})
	var ue *UnrecoverableError
	if !errors.As(err, &ue) {
		t.Errorf("CodeJobHasFailedChildren should yield *UnrecoverableError, got %T", err)
	}
}

func TestErrInvalidConfigIs(t *testing.T) {
	err := ValidateQueueName("a:b")
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("ValidateQueueName error should wrap ErrInvalidConfig, got %v", err)
	}
}
