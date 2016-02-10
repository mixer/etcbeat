package etcd

import (
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	etcd "github.com/coreos/etcd/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type testEtcdAPI struct {
	mock.Mock
}

func (t *testEtcdAPI) Get(ctx context.Context, key string, opts *etcd.GetOptions) (*etcd.Response, error) {
	args := t.Called(ctx, key, opts)
	return nil, args.Error(1)
}

func (t *testEtcdAPI) Set(ctx context.Context, key, value string, opts *etcd.SetOptions) (*etcd.Response, error) {
	args := t.Called(ctx, key, value, opts)
	return nil, args.Error(1)
}

func (t *testEtcdAPI) Delete(ctx context.Context, key string, opts *etcd.DeleteOptions) (*etcd.Response, error) {
	args := t.Called(ctx, key, opts)
	return nil, args.Error(1)
}

func pause() {
	time.Sleep(time.Millisecond * 500)
}

func TestEtcdKeyMaintainer(t *testing.T) {
	api := new(testEtcdAPI)
	clk := clock.NewMock()
	m := NewKeyMaintainer(api, "/test", "asdf")
	m.clock = clk

	api.On("Set", mock.Anything, "/test", "asdf", mock.Anything).Return(nil, nil)
	go m.Maintain()
	pause()
	api.AssertNumberOfCalls(t, "Set", 1)
	clk.Add(m.Interval - time.Nanosecond)
	api.AssertNumberOfCalls(t, "Set", 1)
	clk.Add(time.Nanosecond + 100)
	api.AssertNumberOfCalls(t, "Set", 2)

	api.On("Delete", mock.Anything, "/test", mock.Anything).Return(nil, nil)
	m.Close()
	pause()

	clk.Add(m.Interval)

	api.AssertNumberOfCalls(t, "Set", 2)
	del := api.Calls[2]
	assert.Equal(t, "Delete", del.Method)
	assert.Equal(t, "/test", del.Arguments.String(1))
}

func TestSendsErrorsDownChannels(t *testing.T) {
	api := new(testEtcdAPI)
	m := NewKeyMaintainer(api, "/test", "asdf")
	err := errors.New("oh noes!")
	api.On("Set", mock.Anything, "/test", "asdf", mock.Anything).Return(nil, err)
	api.On("Delete", mock.Anything, "/test", mock.Anything).Return(nil, nil)

	go m.Maintain()
	defer m.Close()

	assert.Equal(t, <-m.Errors(), err)
}

func TestEtcdDirMaintainer(t *testing.T) {
	api := new(testEtcdAPI)
	clk := clock.NewMock()
	m := NewDirMaintainer(api, "/test", func(m *Maintainer) error {
		_, err := m.API.Set(m.Context, m.Key+"/asdf", "42", nil)
		return err
	})
	m.clock = clk

	api.On("Set", mock.Anything, "/test", "", &etcd.SetOptions{
		TTL:       time.Second * 9,
		Dir:       true,
		PrevExist: etcd.PrevExist,
	}).Return(nil, nil).Once()

	go m.Maintain()
	pause()
	api.AssertNumberOfCalls(t, "Set", 1)

	api.On("Set", mock.Anything, "/test", "", &etcd.SetOptions{
		TTL:       time.Second * 9,
		Dir:       true,
		PrevExist: etcd.PrevExist,
	}).Return(nil, etcd.Error{Code: etcd.ErrorCodeKeyNotFound}).Once()
	api.On("Set", mock.Anything, "/test", "", &etcd.SetOptions{
		TTL: time.Second * 9,
		Dir: true,
	}).Return(nil, nil).Once()
	var opts *etcd.SetOptions
	api.On("Set", mock.Anything, "/test/asdf", "42", opts).Return(nil, nil).Once()

	clk.Add(m.Interval)
	api.AssertNumberOfCalls(t, "Set", 4)

	api.On("Delete", mock.Anything, "/test", mock.Anything).Return(nil, nil)
	m.Close()
}
