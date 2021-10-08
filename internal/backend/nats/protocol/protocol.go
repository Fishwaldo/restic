package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type NatsCommand int

const (
	NatsOpenCmd NatsCommand = iota
	NatsStatCmd
	NatsMkdirCmd
	NatsSaveCmd
	NatsListCmd
	NatsLoadCmd
	NatsRemoveCmd
)

type OpenOp struct {
	Bucket string `json:"bucket"`
}
type OpenResult struct {
	Ok bool `json:"ok"`
}

type StatOp struct {
	Bucket   string `json:"bucket"`
	Filename string `json:"filename"`
}

type StatResult struct {
	Ok   bool   `json:"ok"`
	Size int64  `json:"size"`
	Name string `json:"name"`
}

type MkdirOp struct {
	Bucket string `json:"bucket"`
	Dir    string `json:"dir"`
}

type MkdirResult struct {
	Ok bool `json:"ok"`
}

type SaveOp struct {
	Bucket     string `json:"bucket"`
	Dir        string `json:"dir"`
	Name       string `json:"name"`
	Filesize   int64  `json:"size"`
	PacketSize int    `json:"packet_size"`
	Offset     int64  `json:"offset"`
	Data       []byte `json:"data"`
}

type SaveResult struct {
	Ok bool `json:"ok"`
}

type ListOp struct {
	Bucket  string `json:"bucket"`
	BaseDir string `json:"base_dir"`
	SubDir  bool   `json:"sub_dir"`
}

type FileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type ListResult struct {
	Ok bool       `json:"ok"`
	FI []FileInfo `json:"fi"`
}

type LoadOp struct {
	Bucket string `json:"bucket"`
	Dir    string `json:"dir"`
	Name   string `json:"name"`
	Length int    `json:"length"`
	Offset int64  `json:"offset"`
}

type LoadResult struct {
	Ok   bool   `json:"ok"`
	Data []byte `json:"data"`
}

type RemoveOp struct {
	Bucket string `json:"bucket"`
	Dir    string `json:"dir"`
	Name   string `json:"name"`
}

type RemoveResult struct {
	Ok bool `json:"ok"`
}

const (
	msgHeaderID           string = "X-RNS-MSGID"
	msgHeaderChunk        string = "X-RNS-CHUNKS"
	msgHeaderChunkSubject string = "X-RNS-CHUNK-SUBJECT"
	msgHeaderChunkSeq     string = "X-RNS-CHUNKS-SEQ"
	msgHeaderOperation    string = "X-RNS-OP"
	msgHeaderNRI          string = "Nats-Request-Info"
)

func copyHeader(msg *nats.Msg) (hdr nats.Header) {
	hdr = make(nats.Header)
	hdr.Add(msgHeaderID, msg.Header.Get(msgHeaderID))
	hdr.Add(msgHeaderChunk, msg.Header.Get(msgHeaderChunk))
	hdr.Add(msgHeaderOperation, msg.Header.Get(msgHeaderOperation))
	return hdr
}

type nriT struct {
	Acc string `json:"acc"`
	Rtt int    `json:"rtt"`
}

func getNRI(msg *nats.Msg) (*nriT, bool) {
	nri := msg.Header.Get(msgHeaderNRI)
	if nri == "" {
		return nil, false
	}
	var res nriT
	if err := json.Unmarshal([]byte(nri), &res); err != nil {
		return nil, false
	}
	return &res, true
}

// NewRNSMSG Returns a New RNS Message (for each "Transaction")
func NewRNSMsg(subject string) *nats.Msg {
	msg := nats.NewMsg(subject)
	msg.Header.Set(msgHeaderID, randStringBytesMaskImprSrcSB(16))
	return msg
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func randStringBytesMaskImprSrcSB(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

func ChunkSendReplyMsgWithContext(ctx context.Context, conn *nats.Conn, replyto *nats.Msg, msg *nats.Msg, log func(string, ...interface{})) error {
	if len(msg.Header.Get(msgHeaderID)) == 0 {
		return errors.New("MessageID Not Set")
	}

	var maxchunksize int = int(0.95 * float32(conn.MaxPayload()))
	maxchunksize = 1024000 * 0.95
	datasize := len(msg.Data)
	log("ChunkSendReplyMsgWithContext: MsgID %s - Headers %s Size: %d", msg.Header.Get(msgHeaderID), msg.Header, len(msg.Data))

	if len(msg.Data) < maxchunksize {
		/* data is less then our maxchunksize, so we can just send it */
		log("ChunkSendReplyMsgWithContext: Short Reply Message %s", msg.Header.Get(msgHeaderID))
		err := replyto.RespondMsg(msg)
		return errors.Wrap(err, "Short Reply Message Send Failure")
	}

	/* need to Split the Data into Chunks
	 * we will end up sending pages + 1 messages
	 * as the initial message contains data as well
	 */
	pages := datasize / maxchunksize
	initialchunk := nats.NewMsg(msg.Subject)
	initialchunk.Header = copyHeader(msg)
	initialchunk.Header.Set(msgHeaderChunk, fmt.Sprintf("%d", pages))
	if len(msg.Data) < maxchunksize {
		maxchunksize = len(msg.Data)
	}
	initialchunk.Data = msg.Data[:maxchunksize]
	log("Chunking Initial Reply Message %s (%s)- pages %d, len %d First Chunk %d", initialchunk.Header.Get(msgHeaderID), initialchunk.Header, pages, len(msg.Data), len(initialchunk.Data))
	chunkchannelmsg, err := conn.RequestMsgWithContext(ctx, initialchunk)
	if err != nil {
		return errors.Wrap(err, "ChunkSendReplyMsgWithContext")
	}
	/* Reply Message just has a header with the subject we send the rest of the chunks to */
	chunkid := chunkchannelmsg.Header.Get(msgHeaderChunkSubject)
	if chunkid == "" {
		return errors.New("Chunked Reply Response didn't include ChunkID")
	}
	var chunksubject string
	if nri, ok := getNRI(replyto); ok {
		chunksubject = fmt.Sprintf("chunk.%s.send.%s", nri.Acc, chunkid)
	} else {
		chunksubject = fmt.Sprintf("chunk.send.%s", chunkid)
	}
	log("Chunk Reply Subject %s", chunksubject)
	for i := 1; i <= pages; i++ {
		chunkmsg := nats.NewMsg(chunksubject)
		chunkmsg.Header = copyHeader(msg)
		chunkmsg.Header.Set(msgHeaderChunkSeq, fmt.Sprintf("%d", i))
		start := maxchunksize * i
		end := maxchunksize * (i + 1)
		/* make sure we don't overrun our slice */
		if end > len(msg.Data) {
			end = len(msg.Data)
		}
		chunkmsg.Data = msg.Data[start:end]
		log("Sending Reply Chunk %s - Page: %d of %d (%d:%d)", chunkmsg.Header.Get(msgHeaderID), i, pages, start, end)
		var chunkack *nats.Msg
		if i < pages {
			log("Sending Chunk to %s", chunkmsg.Subject)
			chunkack, err = conn.RequestMsgWithContext(ctx, chunkmsg)
			if err != nil {
				return errors.Wrap(err, "ChunkSendReplyMsgWithContext")
			}
			log("Chunk Ack Reply: %s %s - Page %d", chunkack.Header.Get(msgHeaderID), chunkack.Header, i)
		} else {
			err := conn.PublishMsg(chunkmsg)
			if err != nil {
				return errors.Wrap(err, "ChunkSendReplyMsgWithContext")
			}
		}

		/* all chunkackorreply */
		if i == pages {
			return nil
		}
	}
	return errors.New("Failed")
}

func ChunkSendRequestMsgWithContext(ctx context.Context, conn *nats.Conn, msg *nats.Msg, log func(string, ...interface{})) (*nats.Msg, error) {
	if len(msg.Header.Get(msgHeaderID)) == 0 {
		return nil, errors.New("MessageID Not Set")
	}

	var maxchunksize int = int(0.95 * float32(conn.MaxPayload()))
	maxchunksize = 1024000 * 0.95
	datasize := len(msg.Data)
	log("ChunkSendRequestMsgWithContext: MsgID %s - Headers %s Size: %d", msg.Header.Get(msgHeaderID), msg.Header, len(msg.Data))

	if len(msg.Data) < maxchunksize {
		/* data is less then our maxchunksize, so we can just send it */
		log("Short SendRequest MsgID %s - %s Size: %d", msg.Header.Get(msgHeaderID), msg.Header, len(msg.Data))
		reply, err := conn.RequestMsgWithContext(ctx, msg)
		if err != nil {
			return nil, errors.Wrap(err, "Short Message Send Failure")
		}
		log("Short ReplyRequest MsgID %s Headers %s Size: %d", reply.Header.Get(msgHeaderID), reply.Header, len(reply.Data))
		return ChunkReadRequestMsgWithContext(ctx, conn, reply, log)
	}

	/* need to Split the Data into Chunks
	 * we will end up sending pages + 1 messages
	 * as the initial message contains data as well
	 */
	pages := datasize / maxchunksize

	initialchunk := nats.NewMsg(msg.Subject)
	initialchunk.Header = copyHeader(msg)
	initialchunk.Header.Set(msgHeaderChunk, fmt.Sprintf("%d", pages))

	initialchunk.Data = msg.Data[:maxchunksize]
	log("Chunking Send Request MsgID %s - %s- pages %d, len %d First Chunk %d", initialchunk.Header.Get(msgHeaderID), initialchunk.Header, pages, len(msg.Data), len(initialchunk.Data))
	chunkchannelmsg, err := conn.RequestMsgWithContext(ctx, initialchunk)
	if err != nil {
		return nil, errors.Wrap(err, "chunkRequestMsgWithContext")
	}
	/* Reply Message just has a header with the subject we send the rest of the chunks to */
	chunkid := chunkchannelmsg.Header.Get(msgHeaderChunkSubject)
	if chunkid == "" {
		return nil, errors.New("Chunked Reply Response didn't include ChunkID")
	}
	var chunksubject string
	if nri, ok := getNRI(chunkchannelmsg); ok {
		chunksubject = fmt.Sprintf("chunk.%s.send.%s", nri.Acc, chunkid)
	} else {
		chunksubject = fmt.Sprintf("chunk.send.%s", chunkid)
	}

	for i := 1; i <= pages; i++ {
		chunkmsg := nats.NewMsg(chunksubject)
		chunkmsg.Header = copyHeader(msg)
		chunkmsg.Header.Set(msgHeaderChunkSeq, fmt.Sprintf("%d", i))
		start := maxchunksize * i
		end := maxchunksize * (i + 1)
		/* make sure we don't overrun our slice */
		if end > len(msg.Data) {
			end = len(msg.Data)
		}
		chunkmsg.Data = msg.Data[start:end]
		log("Sending Request Chunk %s %s to %s- Page: %d (%d:%d)", chunkmsg.Header.Get(msgHeaderID), chunkmsg.Header, chunkmsg.Subject, i, start, end)
		var chunkackorreply *nats.Msg
		chunkackorreply, err = conn.RequestMsgWithContext(ctx, chunkmsg)
		if err != nil {
			return nil, errors.Wrap(err, "Chunk Send")
		}
		log("got Result %s - %s", chunkmsg.Header.Get(msgHeaderID), chunkmsg.Header)
		/* only the last Chunk Reply will contain the actual Response from the other side */
		if i == pages {
			log("SendRequest Chunk Reply: MsgID %s Headers %s Size: %d", chunkackorreply.Header.Get(msgHeaderID), chunkackorreply.Header, len(chunkackorreply.Data))
			return ChunkReadRequestMsgWithContext(ctx, conn, chunkackorreply, log)
		}
	}
	return nil, errors.New("Failed")
}

func ChunkReadRequestMsgWithContext(ctx context.Context, conn *nats.Conn, msg *nats.Msg, log func(string, ...interface{})) (*nats.Msg, error) {
	if len(msg.Header.Get(msgHeaderID)) == 0 {
		return nil, errors.New("MessageID Not Set")
	}
	log("ChunkReadRequestMsgWithContext: MsgID %s - Headers %s Size: %d", msg.Header.Get(msgHeaderID), msg.Header, len(msg.Data))
	chunked := msg.Header.Get(msgHeaderChunk)
	if chunked != "" {
		pages, err := strconv.Atoi(chunked)
		if err != nil {
			return nil, errors.Wrap(err, "Couldn't get Chunk Page Count")
		}
		log("Chunked Message Recieved: %s - %s - %d pages", msg.Header.Get(msgHeaderID), msg.Header, pages)
		chunktransfer := randStringBytesMaskImprSrcSB(16)
		chunkchan := make(chan *nats.Msg, 10)
		var chunktransfersubject string
		if nri, ok := getNRI(msg); ok {
			chunktransfersubject = fmt.Sprintf("chunk.%s.recieve.%s", nri.Acc, chunktransfer)
		} else {
			chunktransfersubject = fmt.Sprintf("chunk.recieve.%s", chunktransfer)
		}
		sub, err := conn.QueueSubscribeSyncWithChan(chunktransfersubject, chunktransfer, chunkchan)
		if err != nil {
			return nil, errors.Wrap(err, "Couldn't Subscribe to Chunk Channel")
		}
		sub.SetPendingLimits(1000, 64*1024*1024)
		log("Subscription: %+v", sub)
		defer sub.Unsubscribe()
		defer close(chunkchan)
		chunksubmsg := nats.NewMsg(msg.Reply)
		chunksubmsg.Header = copyHeader(msg)
		chunksubmsg.Header.Add(msgHeaderChunkSubject, chunktransfer)
		msg.RespondMsg(chunksubmsg)
		/* pages - 1 because we got first Chunk in original message */
		for i := 1; i <= pages; i++ {
			log("Pending MsgID %s Chunk %d of %d on %s", chunksubmsg.Header.Get(msgHeaderID), i, pages, chunktransfersubject)
			select {
			case chunk := <-chunkchan:
				seq, _ := strconv.Atoi(chunk.Header.Get(msgHeaderChunkSeq))
				log("Got MsgID %s - %s Chunk %d %d", chunk.Header.Get(msgHeaderID), chunk.Header, seq, i)
				msg.Data = append(msg.Data, chunk.Data...)
				if i < pages {
					ackChunk := nats.NewMsg(chunk.Subject)
					ackChunk.Header = copyHeader(chunk)
					log("sending ack %d %d", i, pages)
					err := chunk.RespondMsg(ackChunk)
					if err != nil {
						return nil, errors.Wrap(err, "Chunk Reply Error")
					}
				} else {
					log("Chunked Messages.... %d - %d", i, pages)
					msg.Reply = chunk.Reply
				}
			case <-ctx.Done():
				log("Context Canceled")
				return nil, context.DeadlineExceeded
			}
		}
		log("Chunked Messages Done - %s - %s Final Size %d", msg.Header.Get(msgHeaderID), msg.Header, len(msg.Data))
	}
	return msg, nil
}
