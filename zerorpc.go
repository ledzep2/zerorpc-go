
package zerorpc

import (
        "fmt"
        "bytes"
        zmq "github.com/alecthomas/gozmq"
        "github.com/ugorji/go/codec"
        "log"
        "github.com/nu7hatch/gouuid"
        "os"
       )


var (
        PAIR   = zmq.PAIR
        PUB    = zmq.PUB
        SUB    = zmq.SUB
        REQ    = zmq.REQ
        REP    = zmq.REP
        DEALER = zmq.DEALER
        ROUTER = zmq.ROUTER
        PULL   = zmq.PULL
        PUSH   = zmq.PUSH
        XPUB   = zmq.XPUB
        XSUB   = zmq.XSUB
    )

var GlobalContext *zmq.Context

type Socket interface {
    Connect(endpoint string)
    Invoke(method string, args ...interface{}) interface{}
    Close()
}

type zerorpcSocket struct {
    context zmq.Context
    socket zmq.Socket
    logger *log.Logger 
    Verbose bool
}

func NewContext() (*zmq.Context, error) {
    return zmq.NewContext()
}

func NewSocket(context *zmq.Context, t zmq.SocketType) Socket {
    socket, _ := context.NewSocket(t)
    return &zerorpcSocket{*context, *socket, log.New(os.Stderr, "zerorpc", log.LstdFlags) , false}
}

func (c *zerorpcSocket) Connect(endpoint string) {
    if c.Verbose {
        c.logger.Printf("Connecting to \"%v\"\n", endpoint)
    }
    c.socket.Connect(endpoint)
}


func (c *zerorpcSocket) Close() {
    c.socket.Close()
}

func buildMessage(method string, args []interface{}) ([]byte, error) {
    buf := []byte{}
    headers := make(map[string]string)
    //TODO: implement message_id with uuid
    uuid, _ := uuid.NewV4()
    headers["message_id"] = uuid.String()
    data := make([]interface{}, 3)
    data[0] = headers
    data[1] = method
    data[2] = args
    enc := codec.NewEncoderBytes(&buf, &codec.MsgpackHandle{})
    err := enc.Encode(data)
    return buf, err
}

func (c *zerorpcSocket) Invoke(method string, args ...interface{}) interface{} {
    message, err := buildMessage(method, args)
    if err != nil {
        panic("Unable to build message:" + err.Error())
    }
    if (c.Verbose) {
        c.logger.Println("request:", message)
    }
    c.socket.Send(message, 0)
    raw, _ := c.socket.Recv(0)
    buf := bytes.NewBuffer(raw)
    if (c.Verbose) {
        c.logger.Print("response:", buf)
    }
    dec := codec.NewDecoderBytes(raw, &codec.MsgpackHandle{})
    var value interface{}
    dec.Decode(&value)
    data := value.([]interface{})
    if (c.Verbose) {
        for k, v := range data[0].(map[interface{}]interface{}) {
            fmt.Printf("%s = %s\n", k, v)
        }
        log.Printf("%s\n", data[1])
    }
    return data[2].([]interface{})[0]
}

func NewClient(endpoint string, context *zmq.Context) Socket {
    // There should be 1 zmq.Context per application.
    if context == nil {
        if GlobalContext == nil {
            GlobalContext, _ = NewContext()
        }
        context = GlobalContext
    }
    socket := NewSocket(context, REQ)
    socket.Connect(endpoint)
    return socket
}
