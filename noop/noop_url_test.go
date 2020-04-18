package noop_test

import (
	"testing"

	"github.com/uw-labs/substrate/suburl"
)

func TestNoopURLSink(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedErr bool
	}{
		{
			name:        "noop without host",
			input:       "noop://",
			expectedErr: false,
		},
		{
			name:        "noop",
			input:       "noop://local.xyz",
			expectedErr: false,
		},
		{
			name:        "noop with unknown schema",
			input:       "noop",
			expectedErr: true,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			_, err := suburl.NewSink(tst.input)

			if tst.expectedErr == (err == nil) {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}
		})
	}
}
