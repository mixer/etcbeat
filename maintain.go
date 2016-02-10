package etcd

import (
	"log"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	etcd "github.com/coreos/etcd/client"
)

// Default TTL interval overlap time.
const ttlDelta time.Duration = time.Second

// Default TTL for maintainance.
const ttlDefault time.Duration = time.Second * 8

// Updater function to be used in a maintainer.
type Updater func(m *Maintainer) error

// Function to delete the key.
type Deleter func(m *Maintainer) error

// The maintainer is used to keep a key set on an interval.
type Maintainer struct {
	API GetSetAPI
	// How often keys "live" for in etcd.
	TTL time.Duration
	// How often we attempt to refresh etcd keys. This is slightly shorter
	// than the TTL so they we never "lose" keys.
	Interval time.Duration
	// Context used for making calls to etcd.
	Context context.Context
	// Etcd key this maintainer operates on.
	Key string

	clock  clock.Clock
	errors chan error
	closer chan bool
	touch  Updater
	del    Deleter
}

// Subset of the etcd.KeysAPI that includes CRUD functions.
type GetSetAPI interface {
	Get(ctx context.Context, key string, opts *etcd.GetOptions) (*etcd.Response, error)
	Set(ctx context.Context, key, value string, opts *etcd.SetOptions) (*etcd.Response, error)
	Delete(ctx context.Context, key string, opts *etcd.DeleteOptions) (*etcd.Response, error)
}

// Creates and returns a new maintainer, which calls the updater function
// periodically with a ttl.
func NewMaintainer(api GetSetAPI, key string, updater Updater, deleter Deleter) *Maintainer {
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(ttlDelta))

	return &Maintainer{
		API:      api,
		TTL:      ttlDefault + ttlDelta,
		Interval: ttlDefault,
		Context:  ctx,
		Key:      key,

		clock:  clock.New(),
		errors: make(chan error, 4),
		closer: make(chan bool),
		touch:  updater,
		del:    deleter,
	}
}

// Channel of errors that can occur as a result of etcd operations.
func (m *Maintainer) Errors() <-chan error {
	return m.errors
}

func (m *Maintainer) logError(err error) {
	if err == nil {
		return
	}

	select {
	case m.errors <- err:
	default:
		log.Printf("Uncaught error maintaining etcd key %s: %s", m.Key, err.Error())
	}
}

// Creates and returns a new maintainer to keep a kv pair alive.
func NewKeyMaintainer(api GetSetAPI, key, value string) *Maintainer {
	return NewMaintainer(api, key, func(m *Maintainer) error {
		_, err := m.API.Set(m.Context, m.Key, value, &etcd.SetOptions{TTL: m.TTL})
		return err
	}, func(m *Maintainer) error {
		_, err := m.API.Delete(context.Background(), key, nil)
		return err
	})
}

// Creates and returns a new maintainer to keep a directory alive. It takes
// a function used to store things in the directory, in case the ttl gets
// missed.
func NewDirMaintainer(api GetSetAPI, dir string, setContents func(m *Maintainer) error) *Maintainer {
	return NewMaintainer(api, dir, func(m *Maintainer) error {
		_, err := m.API.Set(m.Context, m.Key, "", &etcd.SetOptions{
			TTL:       m.TTL,
			Dir:       true,
			PrevExist: etcd.PrevExist,
		})

		if err == nil {
			return nil
		}

		if !isErrCode(err, etcd.ErrorCodeKeyNotFound) {
			return err
		}

		if _, err := m.API.Set(m.Context, m.Key, "", &etcd.SetOptions{TTL: m.TTL, Dir: true}); err != nil {
			return err
		}

		return setContents(m)
	}, func(m *Maintainer) error {
		_, err := m.API.Delete(context.Background(), m.Key, &etcd.DeleteOptions{Recursive: true, Dir: true})
		return err
	})
}

func (m *Maintainer) doTouch() {
	m.logError(m.touch(m))
}

// Maintains the etcd key. Blocks until Close() is called. If an error occurs
// with etcd, we'll log it, you don't have to worry about it.
func (m *Maintainer) Maintain() {
	m.doTouch()

	for {
		select {
		case <-m.closer:
			if err := m.del(m); err != nil && !isErrCode(err, etcd.ErrorCodeKeyNotFound) {
				m.logError(err)
			}
			return
		case <-m.clock.After(m.Interval):
			m.doTouch()
		}
	}
}

// Closes the maintainer, deleting the key.
func (m *Maintainer) Close() {
	close(m.closer)
}

// Returns whether the error is an etcd error and it results from a key
// not being found.
func isErrCode(err error, code int) bool {
	if err == nil {
		return false
	}

	e, ok := err.(etcd.Error)

	return ok && e.Code == code
}
