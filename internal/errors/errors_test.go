package errors_test

import (
	"errors"
	"testing"
	ve "vxdb/internal/errors"
)

func TestErrorCreationAndFormatting(t *testing.T) {
	err := ve.New(ve.ErrorCodeNotFound, "missing")
	if err.Code != ve.ErrorCodeNotFound {
		t.Fatalf("expected code %v, got %v", ve.ErrorCodeNotFound, err.Code)
	}
	if err.Message != "missing" {
		t.Fatalf("unexpected message: %s", err.Message)
	}
	msg := err.Error()
	if msg != "NOT_FOUND: missing" {
		t.Fatalf("unexpected formatted message: %s", msg)
	}
}

func TestWrapAndIsErrorCode(t *testing.T) {
	base := errors.New("boom")
	err := ve.Wrap(base, ve.ErrorCodeInternal, "wrap")
	if err.Cause != base {
		t.Fatalf("expected cause %v, got %v", base, err.Cause)
	}
	if !ve.IsErrorCode(err, ve.ErrorCodeInternal) {
		t.Fatalf("expected IsErrorCode to detect ErrorCodeInternal")
	}
	if ve.IsErrorCode(err, ve.ErrorCodeNotFound) {
		t.Fatalf("unexpected error code match")
	}
}

func TestWithDetailAndDetails(t *testing.T) {
	err := ve.New(ve.ErrorCodeInvalidArgument, "bad")
	err = err.WithDetail("field", "name")
	err = err.WithDetails(map[string]interface{}{"other": 1})
	if err.Details["field"] != "name" || err.Details["other"] != 1 {
		t.Fatalf("details not set correctly: %#v", err.Details)
	}
	err = err.WithStack("trace")
	if err.Stack != "trace" {
		t.Fatalf("expected stack to be set")
	}
}

func TestErrorCodeString(t *testing.T) {
	if ve.ErrorCodeInvalidArgument.String() != "INVALID_ARGUMENT" {
		t.Fatalf("unexpected string for invalid argument")
	}
	if ve.ErrorCode(999).String() != "UNKNOWN" {
		t.Fatalf("unexpected string for unknown code")
	}
}
