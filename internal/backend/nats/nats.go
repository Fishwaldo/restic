package nats

import (
	"bytes"
	"context"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/restic/restic/internal/backend"
	"github.com/Fishwaldo/restic-nats"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

// make sure the nats backend implements restic.Backend
var _ restic.Backend = &Backend{}

// Backend uses the nats protocol to access workers that interface with the actual repository on behalf of restic
type Backend struct {
	sem *backend.Semaphore
	backend.Layout
	cfg  Config
	rns *rns.ResticNatsClient
}

func Open(ctx context.Context, cfg Config) (*Backend, error) {
	debug.Log("open nats backend at %s", cfg.Server.String())

	sem, err := backend.NewSemaphore(cfg.Connections)
	if err != nil {
		return nil, err
	}

	be := &Backend{
		sem: sem,
		cfg: cfg,
		Layout: &backend.DefaultLayout{Join: path.Join},
	}

	host, _ := os.Hostname()
	be.rns, err = rns.New(*be.cfg.Server, rns.WithName(host), rns.WithLogger(&resticLogger{}))
	if err != nil {
		return nil, err
	}
	fmt.Printf("Connected to Nats Server: %s (Cluster: %s)\n", be.rns.Conn.ConnectedServerName(), be.rns.Conn.ConnectedClusterName())

	hostname, _ := os.Hostname()

	result, err := be.rns.OpenRepo(ctx, hostname)
	if err != nil {
		// Communication Error
		return nil, err
	}
	if !result.Ok {
		// Backend Returned a Error
		return nil, result.Err
	}
	return be, nil
}

// Create creates all the necessary files and directories for a new local
// backend at dir. Afterwards a new config blob should be created.
func Create(ctx context.Context, cfg Config) (*Backend, error) {
	debug.Log("create nats backend at %s Repo %s", cfg.Server.String(), cfg.Repo)
	be, err := Open(ctx, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "Create Repo")
	}
	exist, _ := be.Test(ctx, restic.Handle{Type: restic.ConfigFile})
	if exist {
		return nil, errors.Errorf("config file already exists")
	}

	for _, d := range be.Paths() {
		err := be.Mkdir(ctx, d)
		if err != nil {
			return nil, err
		}
	}

	return be, nil
}

// Location returns a string that describes the type and location of the
// repository.
func (b *Backend) Location() string {
	return b.cfg.Server.String()
}

// Hasher may return a hash function for calculating a content hash for the backend
func (b *Backend) Hasher() hash.Hash {
	return nil
}

// Test a boolean value whether a File with the name and type exists.
func (b *Backend) Test(ctx context.Context, h restic.Handle) (bool, error) {
	debug.Log("Test %s - %s", b.cfg.Server.String(), b.Filename(h))
	res, err := b.Stat(ctx, h)
	if err != nil {
		return false, err
	}
	if res.Name == b.Filename(h) {
		return true, nil
	} 
	return false, nil
}

// Remove removes a File described  by h.
func (b *Backend) Remove(ctx context.Context, h restic.Handle) error {
	debug.Log("Remove %s - %s", b.cfg.Server.String(), b.Filename(h))
	result, err := b.rns.Remove(ctx, b.Dirname(h), filepath.Base(b.Filename(h)))
	if err != nil {
		//Communication Error
		return errors.Wrap(err, "save")
	}
	if !result.Ok {
		//Backend returned a Error
		return result.Err
	}
	return nil
}

// Close the backend
func (b *Backend) Close() error {
	debug.Log("Close %s", b.cfg.Server.String())
	result, err := b.rns.Close(context.Background())
	if err != nil {
		// Communication Error
		return errors.Wrap(err, "close")
	}
	if !result.Ok {
		// Backend Returned a Error
		return result.Err
	}
	return nil
}

// Save stores the data from rd under the given handle.
func (b *Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	debug.Log("Save %s - %s", b.cfg.Server.String(), b.Filename(h))

	result, err := b.rns.Save(ctx, b.Dirname(h), filepath.Base(b.Filename(h)), rd)
	if err != nil {
		// Communication Error
		return errors.Wrap(err, "save")
	}
	if !result.Ok {
		// Backend Returned a Error
		return result.Err
	}
	return nil
}

// Load runs fn with a reader that yields the contents of the file at h at the
// given offset. If length is larger than zero, only a portion of the file
// is read.
//
// The function fn may be called multiple times during the same Load invocation
// and therefore must be idempotent.
//
// Implementations are encouraged to use backend.DefaultLoad
func (b *Backend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	debug.Log("Load %s - %s (start %d length %d)", b.cfg.Server.String(), b.Filename(h), offset, length)

	result, err := b.rns.Load(ctx, b.Dirname(h), filepath.Base(b.Filename(h)), length, offset)
	if err != nil {
		//Communication Error
		return errors.Wrap(err, "Load")
	}
	if !result.Ok {
		// Backend Returned a Error
		return result.Err
	}
	rd := bytes.NewReader(result.Data)
	if err := fn(rd); err != nil {
		return err
	}
	return nil
}

// Stat returns information about the File identified by h.
func (b *Backend) Stat(ctx context.Context, h restic.Handle) (restic.FileInfo, error) {
	fmt.Printf("Backend: %+v\n", b)
	debug.Log("Stat %s", b.Filename(h))
	
	result, err := b.rns.Stat(ctx, b.Dirname(h), b.Filename(h))

	if err != nil {
		//Communication Error
		return restic.FileInfo{}, errors.Wrap(err, "Stat")
	}
	if !result.Ok {
		// Backend Returned a Error
		return restic.FileInfo{}, result.Err
	}
	return restic.FileInfo{Size: result.Size, Name: h.Name}, nil
}

// List runs fn for each file in the backend which has the type t. When an
// error occurs (or fn returns an error), List stops and returns it.
//
// The function fn is called exactly once for each file during successful
// execution and at most once in case of an error.
//
// The function fn is called in the same Goroutine that List() is called
// from.
func (b *Backend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {
	dir, recursive := b.Basedir(t)
	debug.Log("List %s - %s - Subdirs? %t", b.cfg.Server.String(),dir, recursive)
	result, err := b.rns.List(ctx, dir, recursive)
	if err != nil {
		//Communication Error
		return errors.Wrap(err, "List")
	}
	if !result.Ok {
		//Backend Returned a Error
		return result.Err
	}
	for _, fi := range result.FI {
		rfi := restic.FileInfo{Name: fi.Name, Size: fi.Size}
		if err := fn(rfi); err != nil {
			return err
		}
	}
	return nil
}

// IsNotExist returns true if the error was caused by a non-existing file
// in the backend.
func (b *Backend) IsNotExist(err error) bool {
	debug.Log("IsNotExist %s (TODO) - %T", b.cfg.Server.String(), err)

	fmt.Printf("IsNotExist Called\n")
	return false
}

func (b *Backend) removeKeys(ctx context.Context, t restic.FileType) error {
	return b.List(ctx, t, func(fi restic.FileInfo) error {
		return b.Remove(ctx, restic.Handle{Type: t, Name: fi.Name})
	})
}

// Delete removes all data in the backend.
func (b *Backend) Delete(ctx context.Context) error {
	alltypes := []restic.FileType{
		restic.PackFile,
		restic.KeyFile,
		restic.LockFile,
		restic.SnapshotFile,
		restic.IndexFile}

	for _, t := range alltypes {
		err := b.removeKeys(ctx, t)
		if err != nil {
			return nil
		}
	}

	err := b.Remove(ctx, restic.Handle{Type: restic.ConfigFile})
	if err != nil && !b.IsNotExist(err) {
		return err
	}

	return nil
}

func (b *Backend) Mkdir(ctx context.Context, dir string) error {
	debug.Log("Mkdir %s - %s", b.cfg.Server.String(), dir)
	result, err := b.rns.Mkdir(ctx, dir)
	if err != nil {
		//Communication Error
		return errors.Wrap(err, "Mkdir")
	}
	if !result.Ok {
		//Backend Returned a Error
		return result.Err
	}
			return nil
}
