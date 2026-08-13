package bullmq

import (
	"errors"
	"fmt"
)

// Sentinel errors. Callers match them with errors.Is.
var (
	// ErrInvalidConfig is returned for invalid queue/connection configuration.
	ErrInvalidConfig = errors.New("bullmq: invalid config")

	// ErrWaitingChildren is a signal returned by a processor (or moveToWaitingChildren)
	// telling the worker not to finalize the job because it now waits for children.
	ErrWaitingChildren = errors.New("bullmq: job is waiting for children")

	// ErrExclusiveParentOptions is returned when more than one parent-failure
	// policy (failParentOnFailure, continueParentOnFailure,
	// ignoreDependencyOnFailure, removeDependencyOnFailure) is enabled on the same
	// job. Mirrors the ValueError raised by python/bullmq/job.py:73-77.
	ErrExclusiveParentOptions = errors.New("bullmq: parent-failure options cannot be used together")
)

// UnrecoverableError marks a failure that must not be retried. A processor can
// return it to force the job straight to the failed set, bypassing backoff.
type UnrecoverableError struct{ msg string }

// NewUnrecoverableError builds an UnrecoverableError with the given message.
func NewUnrecoverableError(msg string) *UnrecoverableError { return &UnrecoverableError{msg: msg} }

func (e *UnrecoverableError) Error() string { return e.msg }

// ScriptErrorCode is a negative status code returned by BullMQ Lua scripts.
// Values ported from python/bullmq/error_code.py (kept identical across ports).
type ScriptErrorCode int

const (
	CodeJobNotExist               ScriptErrorCode = -1
	CodeJobLockNotExist           ScriptErrorCode = -2
	CodeJobNotInState             ScriptErrorCode = -3
	CodeJobPendingDependencies    ScriptErrorCode = -4
	CodeParentJobNotExist         ScriptErrorCode = -5
	CodeJobLockMismatch           ScriptErrorCode = -6
	CodeParentJobCannotBeReplaced ScriptErrorCode = -7
	CodeJobHasFailedChildren      ScriptErrorCode = -9
)

// errorContext carries the details needed to render a script error message,
// mirroring the dict passed to python scripts.py::finishedErrors.
type errorContext struct {
	jobID     string
	parentKey string
	command   string
	state     string
}

// finishedError maps a negative Lua status code to a Go error, mirroring
// python scripts.py::finishedErrors. Message text is kept close to the reference
// so cross-port debugging reads the same.
func finishedError(code ScriptErrorCode, ctx errorContext) error {
	switch code {
	case CodeJobNotExist:
		return fmt.Errorf("Missing key for job %s. %s", ctx.jobID, ctx.command)
	case CodeJobLockNotExist:
		return fmt.Errorf("Missing lock for job %s. %s", ctx.jobID, ctx.command)
	case CodeJobNotInState:
		return fmt.Errorf("Job %s is not in the %s state. %s", ctx.jobID, ctx.state, ctx.command)
	case CodeJobPendingDependencies:
		return fmt.Errorf("Job %s has pending dependencies. %s", ctx.jobID, ctx.command)
	case CodeParentJobNotExist:
		return fmt.Errorf("Missing key for parent job %s. %s", ctx.parentKey, ctx.command)
	case CodeJobLockMismatch:
		return fmt.Errorf("Lock mismatch for job %s. Cmd %s from %s", ctx.jobID, ctx.command, ctx.state)
	case CodeParentJobCannotBeReplaced:
		return fmt.Errorf("The parent job %s cannot be replaced. %s", ctx.jobID, ctx.command)
	case CodeJobHasFailedChildren:
		return NewUnrecoverableError(fmt.Sprintf(
			"Cannot complete job %s because it has at least one failed child. %s", ctx.jobID, ctx.command))
	default:
		return fmt.Errorf("Unknown code %d error for %s. %s", code, ctx.jobID, ctx.command)
	}
}
