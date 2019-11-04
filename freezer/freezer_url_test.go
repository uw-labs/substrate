package freezer

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/freezer"
	"github.com/uw-labs/straw"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func TestFreezerSink(t *testing.T) {
	assert := assert.New(t)

	strawOpen = func(url string) (straw.StreamStore, error) {
		return &mockStore{url}, nil
	}

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSinkConfig
		expectedErr error
	}{
		{
			name:  "simple-dir",
			input: "freezer+dir:///foo/1",
			expected: AsyncMessageSinkConfig{
				FreezerConfig: freezer.MessageSinkConfig{
					CompressionType: freezer.CompressionTypeNone,
					Path:            "/foo/1",
				},
				StreamStore: &mockStore{"file:///"},
			},
			expectedErr: nil,
		},
		{
			name:  "everything-dir",
			input: "freezer+dir:///foo/bar2/baz/?compression=snappy",
			expected: AsyncMessageSinkConfig{
				FreezerConfig: freezer.MessageSinkConfig{
					CompressionType: freezer.CompressionTypeSnappy,
					Path:            "/foo/bar2/baz/",
				},
				StreamStore: &mockStore{"file:///"},
			},
			expectedErr: nil,
		},
		{
			name:  "simple-s3",
			input: "freezer+s3://foo/1",
			expected: AsyncMessageSinkConfig{
				FreezerConfig: freezer.MessageSinkConfig{
					CompressionType: freezer.CompressionTypeNone,
					Path:            "/1",
				},
				StreamStore: &mockStore{"s3://foo"},
			},
			expectedErr: nil,
		},
		{
			name:  "everything-s3",
			input: "freezer+s3://foo/bar2/baz/?sse=aes256&compression=snappy",
			expected: AsyncMessageSinkConfig{
				FreezerConfig: freezer.MessageSinkConfig{
					CompressionType: freezer.CompressionTypeSnappy,
					Path:            "/bar2/baz/",
				},
				StreamStore: &mockStore{"s3://foo?sse=AES256"},
			},
			expectedErr: nil,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {

			var conf AsyncMessageSinkConfig
			sinker = func(c AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
				conf = c
				return nil, nil
			}
			_, err := suburl.NewSink(tst.input)

			if tst.expectedErr != err {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, conf)
		})
	}

}

func TestFreezerSource(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSourceConfig
		expectedErr error
	}{
		{
			name:  "simple-dir",
			input: "freezer+dir:///foo/baz1/",
			expected: AsyncMessageSourceConfig{
				FreezerConfig: freezer.MessageSourceConfig{
					CompressionType: freezer.CompressionTypeNone,
					Path:            "/foo/baz1/",
					PollPeriod:      10 * time.Second,
				},
				StreamStore: &mockStore{"file:///"},
			},
			expectedErr: nil,
		},
		{
			name:  "everything-dir",
			input: "freezer+dir:///foo/baz3/?compression=snappy",
			expected: AsyncMessageSourceConfig{
				FreezerConfig: freezer.MessageSourceConfig{
					CompressionType: freezer.CompressionTypeSnappy,
					Path:            "/foo/baz3/",
					PollPeriod:      10 * time.Second,
				},
				StreamStore: &mockStore{"file:///"},
			},
			expectedErr: nil,
		},
		{
			name:  "simple-s3",
			input: "freezer+s3://foo/baz1/",
			expected: AsyncMessageSourceConfig{
				FreezerConfig: freezer.MessageSourceConfig{
					CompressionType: freezer.CompressionTypeNone,
					Path:            "/baz1/",
					PollPeriod:      10 * time.Second,
				},
				StreamStore: &mockStore{"s3://foo"},
			},
			expectedErr: nil,
		},
		{
			name:  "everything-s3",
			input: "freezer+s3://foo/baz3/?compression=snappy&sse=aes256",
			expected: AsyncMessageSourceConfig{
				FreezerConfig: freezer.MessageSourceConfig{
					CompressionType: freezer.CompressionTypeSnappy,
					Path:            "/baz3/",
					PollPeriod:      10 * time.Second,
				},
				StreamStore: &mockStore{"s3://foo?sse=AES256"},
			},
			expectedErr: nil,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {

			var conf AsyncMessageSourceConfig
			sourcer = func(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
				conf = c
				return nil, nil
			}
			_, err := suburl.NewSource(tst.input)

			if tst.expectedErr != err {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, conf)
		})
	}

}

type mockStore struct {
	url string
}

func (m *mockStore) Close() error {
	return nil
}

func (m *mockStore) OpenReadCloser(name string) (straw.StrawReader, error) {
	panic("not implemented")
}

func (m *mockStore) CreateWriteCloser(name string) (straw.StrawWriter, error) {
	panic("not implemented")
}

func (m *mockStore) Lstat(path string) (os.FileInfo, error) {
	panic("not implemented")
}

func (m *mockStore) Stat(path string) (os.FileInfo, error) {
	panic("not implemented")
}

func (m *mockStore) Readdir(path string) ([]os.FileInfo, error) {
	panic("not implemented")
}

func (m *mockStore) Mkdir(path string, mode os.FileMode) error {
	panic("not implemented")
}

func (m *mockStore) Remove(path string) error {
	panic("not implemented")
}
