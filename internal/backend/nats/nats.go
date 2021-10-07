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
	"time"

	"github.com/Fishwaldo/restic-nats-server/protocol"
	"github.com/nats-io/nats.go"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

// make sure the rest backend implements restic.Backend
var _ restic.Backend = &Backend{}

const defaultLayout = "default"

// Backend uses the REST protocol to access data stored on a server.
type Backend struct {
	sem *backend.Semaphore
	backend.Layout
	cfg  Config
	conn *nats.Conn
	enc  nats.Encoder
}

func connectNats(be *Backend) error {
	server := be.cfg.Server.Hostname()
	port := be.cfg.Server.Port()
	if port == "" {
		port = "4222"
	}
	url := fmt.Sprintf("nats://%s:%s", server, port)

	/* Check Credential File Exists */
	_, err := os.Stat(be.cfg.Credential)
	if err != nil {
		return errors.Wrap(err, "credential file missing")
	}

	var options []nats.Option
	options = append(options, nats.UserCredentials(be.cfg.Credential))
	options = append(options, nats.ClosedHandler(natsClosedCB))
	options = append(options, nats.DisconnectHandler(natsDisconnectedCB))

	be.conn, err = nats.Connect(url, options...)
	if err != nil {
		return errors.Wrap(err, "nats connection failed")
	}

	if size := be.conn.MaxPayload(); size < 8388608 {
		return errors.New("NATS Server Max Payload Size is below 8Mb")
	}

	if !be.conn.HeadersSupported() {
		return errors.New("server does not support Headers")
	}

	be.enc = nats.EncoderForType("gob")
	if be.enc == nil {
		return errors.New("Can't Load json Decoder")
	}
	be.sem, err = backend.NewSemaphore(be.cfg.Connections)

	fmt.Printf("Connected to Nats Server: %s (Cluster: %s)\n", be.conn.ConnectedServerName(), be.conn.ConnectedClusterName())

	if err != nil {
		return err
	}
	return nil
}

func natsClosedCB(conn *nats.Conn) {

}

func natsDisconnectedCB(conn *nats.Conn) {

}

func (be *Backend) SendMsgWithReply(ctx context.Context, op protocol.NatsCommand, send interface{}, recv interface{}) error {
	var operation string
	be.sem.GetToken()
	defer be.sem.ReleaseToken()
	start := time.Now()
	switch op {
	case protocol.NatsOpenCmd:
		if _, ok := send.(protocol.OpenOp); !ok {
			return errors.Errorf("Struct Supplied to SendMsgWithReply is not a openOP: %T", send)
		}
		if _, ok := recv.(*protocol.OpenResult); !ok {
			return errors.Errorf("Recv Struct supplied to SendMsgWithReply is not a openResult: %T", recv)
		}
		operation = "open"
	case protocol.NatsStatCmd:
		if _, ok := send.(protocol.StatOp); !ok {
			return errors.Errorf("Struct Supplied to SendMsgWithReply is not a statOp: %T", send)
		}
		if _, ok := recv.(*protocol.StatResult); !ok {
			return errors.Errorf("Recv Struct supplied to SendMsgWithReply is not a statResult %T", recv)
		}
		operation = "stat"
	case protocol.NatsMkdirCmd:
		if _, ok := send.(protocol.MkdirOp); !ok {
			return errors.Errorf("Struct Supplied to SendMsgWithReply is not a mkdirOp: %T", send)
		}
		if _, ok := recv.(*protocol.MkdirResult); !ok {
			return errors.Errorf("Recv Struct supplied to SendMsgWithReply is not a mkdirResult %T", recv)
		}
		operation = "mkdir"
	case protocol.NatsSaveCmd:
		if _, ok := send.(protocol.SaveOp); !ok {
			return errors.Errorf("Struct Supplied to SendMsgWithReply is not a saveOp: %T", send)
		}
		if _, ok := recv.(*protocol.SaveResult); !ok {
			return errors.Errorf("Recv Struct supplied to SendMsgWithReply is not a saveResult %T", recv)
		}
		operation = "save"
	case protocol.NatsListCmd:
		if _, ok := send.(protocol.ListOp); !ok {
			return errors.Errorf("Struct Supplied to SendMsgWithReply is not a listOp: %T", send)
		}
		if _, ok := recv.(*protocol.ListResult); !ok {
			return errors.Errorf("Recv Struct supplied to SendMsgWithReply is not a listResult %T", recv)
		}
		operation = "list"
	case protocol.NatsLoadCmd:
		if _, ok := send.(protocol.LoadOp); !ok {
			return errors.Errorf("Struct Supplied to SendMsgWithReply is not a loadOp: %T", send)
		}
		if _, ok := recv.(*protocol.LoadResult); !ok {
			return errors.Errorf("Recv Struct supplied to SendMsgWithReply is not a loadResult %T", recv)
		}
		operation = "load"
	case protocol.NatsRemoveCmd:
		if _, ok := send.(protocol.RemoveOp); !ok {
			return errors.Errorf("Struct Supplied to SendMsgWithReply is not a removeOp: %T", send)
		}
		if _, ok := recv.(*protocol.RemoveResult); !ok {
			return errors.Errorf("Recv Struct supplied to SendMsgWithReply is not a removeOp %T", recv)
		}
		operation = "remove"

	}
	var err error
	subject := fmt.Sprintf("repo.commands.%s", operation)
	msg := protocol.NewRNSMsg(subject)

	defer func() {
		if err == nil {
			debug.Log("SendMsgWithReply (%s - %s) Took %s - %d Bytes", operation, msg.Header.Get("X-RNS-MSGID"), time.Since(start), len(msg.Data))
		} else {
			debug.Log("SendMsgWithREply (%s - %s) Failed: %s (Took %s - %d Bytes", operation, msg.Header.Get("X-RNS-MSGID"), err, time.Since(start), len(msg.Data))
		}
	}()

	msg.Header.Set("X-RNS-OP", operation)

	msg.Data, err = be.enc.Encode(subject, send)
	if err != nil {
		return errors.Wrap(err, "Encoding Failed")
	}
	msg.Reply = nats.NewInbox()
	//debug.Log("Sending %s %d\n", msg.Subject, len(msg.Data))

	/* check the size of the Data Field. If its close to our NATS max payload size
	 * then we will chunk the transfer instead
	 */
	var chunkedmsg *nats.Msg

	chunkedmsg, err = protocol.ChunkSendRequestMsgWithContext(ctx, be.conn, msg, debug.Log)
	if err != nil {
		return errors.Wrapf(err, "ChunkRequestMsgWithContext Error: %d", len(msg.Data))
	}
	if err := be.enc.Decode(chunkedmsg.Subject, chunkedmsg.Data, recv); err != nil {
		return errors.Wrapf(err, "Decode Failed %s %s %d", chunkedmsg.Header.Get("X-RNS-MSGID"), chunkedmsg.Header, len(chunkedmsg.Data))
	}
	return nil
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
	}

	l, err := backend.ParseLayout(ctx, be, "default", defaultLayout, "")
	if err != nil {
		return nil, err
	}

	be.Layout = l

	err = connectNats(be)
	if err != nil {
		return nil, errors.Wrap(err, "open nats failed")
	}

	co := protocol.OpenOp{Bucket: be.cfg.Repo}
	var result protocol.OpenResult
	if err := be.SendMsgWithReply(ctx, protocol.NatsOpenCmd, co, &result); err != nil {
		return nil, errors.Wrap(err, "OpenOp Failed")
	}
	debug.Log("Open Result: %+v\n", result)

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
	_, err = be.Stat(ctx, restic.Handle{Type: restic.ConfigFile})
	if err == nil {
		return nil, errors.New("config file already exists")
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
	_, err := b.Stat(ctx, h)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// Remove removes a File described  by h.
func (b *Backend) Remove(ctx context.Context, h restic.Handle) error {
	debug.Log("Remove %s - %s", b.cfg.Server.String(), b.Filename(h))
	ro := protocol.RemoveOp{Bucket: b.cfg.Repo, Dir: b.Dirname(h), Name: filepath.Base(b.Filename(h))}
	var result protocol.RemoveResult
	if err := b.SendMsgWithReply(context.Background(), protocol.NatsRemoveCmd, ro, &result); err != nil {
		return errors.Wrap(err, "Remove: SendMsgWithReply Failed")
	}
	if result.Ok {
		return nil
	}
	return errors.Errorf("Remove Failed")
}

// Close the backend
func (b *Backend) Close() error {
	debug.Log("Close %s", b.cfg.Server.String())
	return nil
}

// Save stores the data from rd under the given handle.
func (b *Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	debug.Log("Save %s - %s", b.cfg.Server.String(), b.Filename(h))
	var so protocol.SaveOp
	so.Dir = b.Dirname(h)
	so.Name = filepath.Base(b.Filename(h))
	so.Filesize = rd.Length()
	so.Bucket = b.cfg.Repo

	var err error
	so.Data, err = io.ReadAll(rd)
	so.PacketSize = len(so.Data)
	if err != nil {
		return errors.Wrap(err, "Save")
	}
	so.Offset = 0
	var result protocol.SaveResult
	if err := b.SendMsgWithReply(context.Background(), protocol.NatsSaveCmd, so, &result); err != nil {
		return errors.Wrap(err, "Save: SendMsgWithReply Failed")
	}
	if result.Ok {
		return nil
	}
	return errors.Errorf("Save Failed")
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
	lo := protocol.LoadOp{Bucket: b.cfg.Repo, Dir: b.Dirname(h), Name: filepath.Base(b.Filename(h)), Length: length, Offset: offset}
	var result protocol.LoadResult
	if err := b.SendMsgWithReply(context.Background(), protocol.NatsLoadCmd, lo, &result); err != nil {
		return errors.Wrap(err, "Save: SendMsgWithReply Failed")
	}
	if !result.Ok {
		return errors.Errorf("Load Failed")
	}
	rd := bytes.NewReader(result.Data)
	if err := fn(rd); err != nil {
		return errors.Wrap(err, "Load Read")
	}
	return nil
}

// Stat returns information about the File identified by h.
func (b *Backend) Stat(ctx context.Context, h restic.Handle) (restic.FileInfo, error) {
	debug.Log("Stat %s - %s", b.cfg.Server.String(), b.Filename(h))
	op := protocol.StatOp{Bucket: b.cfg.Repo, Filename: b.Filename(h)}
	var result protocol.StatResult
	if err := b.SendMsgWithReply(context.Background(), protocol.NatsStatCmd, op, &result); err != nil {
		return restic.FileInfo{}, errors.Wrap(err, "statOp Failed")
	}
	if result.Ok {
		return restic.FileInfo{Size: result.Size, Name: h.Name}, nil
	} else {
		return restic.FileInfo{}, errors.New("File does not exist")
	}
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
	basedir, subdirs := b.Basedir(t)
	debug.Log("List %s - %s - Subdirs? %t", b.cfg.Server.String(), basedir, subdirs)
	op := protocol.ListOp{Bucket: b.cfg.Repo, BaseDir: basedir, SubDir: subdirs}
	var result protocol.ListResult
	if err := b.SendMsgWithReply(context.Background(), protocol.NatsListCmd, op, &result); err != nil {
		return errors.Wrap(err, "listOp Failed")
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
	debug.Log("IsNotExist %s (TODO) - %s", b.cfg.Server.String(), err)

	fmt.Printf("IsNotExist Called\n")
	return false
}

// Delete removes all data in the backend.
func (b *Backend) Delete(ctx context.Context) error {
	debug.Log("Delete %s (TODO)", b.cfg.Server.String())
	return errors.Errorf("TODO Delete")
}

func (b *Backend) Join(p ...string) string {
	return path.Join(p...)
}

func (b *Backend) ReadDir(ctx context.Context, dir string) ([]os.FileInfo, error) {
	debug.Log("ReadDir %s (TODO) - %s", b.cfg.Server.String(), dir)
	return nil, errors.Errorf("TODO: ReadDir")
}

func (b *Backend) Mkdir(ctx context.Context, dir string) error {
	debug.Log("Mkdir %s - %s", b.cfg.Server.String(), dir)
	op := protocol.MkdirOp{Bucket: b.cfg.Repo, Dir: dir}
	var result protocol.MkdirResult
	if err := b.SendMsgWithReply(context.Background(), protocol.NatsMkdirCmd, op, &result); err != nil {
		return errors.Wrap(err, "natsMkdirCmd Failed")
	}
	if result.Ok {
		return nil
	} else {
		return errors.New("mkdir Failed")
	}
}
