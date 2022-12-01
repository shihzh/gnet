// Copyright (c) 2021 Andy Pan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux || freebsd || dragonfly || darwin
// +build linux freebsd dragonfly darwin

package gnet

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/panjf2000/gnet/v2/internal/math"
	"github.com/panjf2000/gnet/v2/internal/netpoll"
	"github.com/panjf2000/gnet/v2/internal/socket"
	"github.com/panjf2000/gnet/v2/pkg/buffer/ring"
	gerrors "github.com/panjf2000/gnet/v2/pkg/errors"
	"github.com/panjf2000/gnet/v2/pkg/logging"
)

// Client of gnet.
type Client struct {
	opts     *Options
	ev       *cliEventHandler
	logFlush func() error
}

type cliEventHandler struct {
	ev     EventHandler
	engine Engine
}

func (ev *cliEventHandler) OnBoot(eng Engine) (action Action) {
	ev.engine = eng
	return ev.ev.OnBoot(eng)
}

func (ev *cliEventHandler) OnShutdown(eng Engine) {
	ev.ev.OnShutdown(eng)
}

func (ev *cliEventHandler) OnOpen(c Conn) (out []byte, action Action) {
	return ev.ev.OnOpen(c)
}

func (ev *cliEventHandler) OnClose(c Conn, err error) (action Action) {
	return ev.ev.OnClose(c, err)
}

func (ev *cliEventHandler) OnTraffic(c Conn) (action Action) {
	return ev.ev.OnTraffic(c)
}

func (ev *cliEventHandler) OnTick() (delay time.Duration, action Action) {
	return ev.ev.OnTick()
}

// NewClient creates an instance of Client.
func NewClient(eventHandler EventHandler, opts ...Option) (cli *Client, err error) {
	options := loadOptions(opts...)
	cli = new(Client)
	cli.opts = options
	var logger logging.Logger
	if options.LogPath != "" {
		if logger, cli.logFlush, err = logging.CreateLoggerAsLocalFile(options.LogPath, options.LogLevel); err != nil {
			return
		}
	} else {
		logger = logging.GetDefaultLogger()
	}
	if options.Logger == nil {
		options.Logger = logger
	}

	// The maximum number of operating system threads that the Go program can use is initially set to 10000,
	// which should also be the maximum amount of I/O event-loops locked to OS threads that users can start up.
	if options.LockOSThread && options.NumEventLoop > 10000 {
		logging.Errorf("too many event-loops under LockOSThread mode, should be less than 10,000 "+
			"while you are trying to set up %d\n", options.NumEventLoop)
		return nil, gerrors.ErrTooManyEventLoopThreads
	}

	cli.ev = &cliEventHandler{ev: eventHandler}

	rbc := options.ReadBufferCap
	switch {
	case rbc <= 0:
		options.ReadBufferCap = MaxStreamBufferCap
	case rbc <= ring.DefaultBufferSize:
		options.ReadBufferCap = ring.DefaultBufferSize
	default:
		options.ReadBufferCap = math.CeilToPowerOfTwo(rbc)
	}
	wbc := options.WriteBufferCap
	switch {
	case wbc <= 0:
		options.WriteBufferCap = MaxStreamBufferCap
	case wbc <= ring.DefaultBufferSize:
		options.WriteBufferCap = ring.DefaultBufferSize
	default:
		options.WriteBufferCap = math.CeilToPowerOfTwo(wbc)
	}

	return
}

// Start starts the client event-loop, handing IO events.
func (cli *Client) Start() (err error) {
	ln := &listener{network: "cli"}
	return run(cli.ev, ln, cli.opts, "")
}

// Stop stops the client event-loop.
func (cli *Client) Stop() (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return cli.ev.engine.Stop(ctx)
}

// Dial is like net.Dial().
func (cli *Client) Dial(network, address string) (Conn, error) {
	return cli.DialTimeout(network, address, "", 0)
}

// DialTimeout is like net.DialTimeout() with localAddr bind.
func (cli *Client) DialTimeout(network, address, local string, timeout time.Duration) (Conn, error) {
	var (
		localAddr net.Addr
		c         net.Conn
		err       error
	)
	if local != "" {
		if strings.HasPrefix(network, "udp") {
			localAddr, err = net.ResolveUDPAddr(network, local)
			if err != nil {
				return nil, err
			}
		} else if strings.HasPrefix(network, "tcp") {
			localAddr, err = net.ResolveTCPAddr(network, local)
			if err != nil {
				return nil, err
			}
		}
	}

	dialer := net.Dialer{Timeout: timeout, LocalAddr: localAddr,
		Control: func(network, address string, c syscall.RawConn) (err error) {
			var dupFD int
			err = c.Control(func(fd uintptr) {
				dupFD = int(fd)
			})
			if err != nil {
				return
			}
			if cli.opts.ReuseAddr {
				err = unix.SetsockoptInt(dupFD, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
			}
			return
		}}

	c, err = dialer.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return cli.Enroll(c)
}

// Enroll converts a net.Conn to gnet.Conn and then adds it into Client.
func (cli *Client) Enroll(c net.Conn) (Conn, error) {
	defer c.Close()

	sc, ok := c.(syscall.Conn)
	if !ok {
		return nil, errors.New("failed to convert net.Conn to syscall.Conn")
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return nil, errors.New("failed to get syscall.RawConn from net.Conn")
	}

	var dupFD int
	e := rc.Control(func(fd uintptr) {
		dupFD, err = unix.Dup(int(fd))
	})
	if err != nil {
		return nil, err
	}
	if e != nil {
		return nil, e
	}

	var sockOpts []socket.Option
	if cli.opts.SocketRecvBuffer > 0 {
		sockOpt := socket.Option{SetSockOpt: socket.SetRecvBuffer, Opt: cli.opts.SocketRecvBuffer}
		sockOpts = append(sockOpts, sockOpt)
	}
	if cli.opts.SocketSendBuffer > 0 {
		sockOpt := socket.Option{SetSockOpt: socket.SetSendBuffer, Opt: cli.opts.SocketSendBuffer}
		sockOpts = append(sockOpts, sockOpt)
	}
	switch c.(type) {
	case *net.TCPConn:
		if cli.opts.TCPNoDelay == TCPDelay {
			sockOpt := socket.Option{SetSockOpt: socket.SetNoDelay, Opt: 0}
			sockOpts = append(sockOpts, sockOpt)
		}
		if cli.opts.TCPLinger >= 0 {
			sockOpt := socket.Option{SetSockOpt: socket.SetLinger, Opt: cli.opts.TCPLinger}
			sockOpts = append(sockOpts, sockOpt)
		}
		if cli.opts.TCPKeepAlive > 0 {
			err = socket.SetKeepAlivePeriod(dupFD, int(cli.opts.TCPKeepAlive.Seconds()))
		}
	}

	for _, sockOpt := range sockOpts {
		if err = sockOpt.SetSockOpt(dupFD, sockOpt.Opt); err != nil {
			return nil, err
		}
	}

	if cli.ev.engine.eng == nil {
		return nil, gerrors.ErrEmptyEngine
	}

	var (
		sockAddr unix.Sockaddr
		gc       Conn
		el       *eventloop
	)
	switch c.(type) {
	case *net.UnixConn:
		if sockAddr, _, _, err = socket.GetUnixSockAddr(c.RemoteAddr().Network(), c.RemoteAddr().String()); err != nil {
			return nil, err
		}
		ua := c.LocalAddr().(*net.UnixAddr)
		ua.Name = c.RemoteAddr().String() + "." + strconv.Itoa(dupFD)
		el = cli.ev.engine.eng.lb.next(c.LocalAddr())
		gc = newTCPConn(dupFD, el, sockAddr, c.LocalAddr(), c.RemoteAddr())
	case *net.TCPConn:
		if sockAddr, _, _, _, err = socket.GetTCPSockAddr(c.RemoteAddr().Network(), c.RemoteAddr().String()); err != nil {
			return nil, err
		}
		el = cli.ev.engine.eng.lb.next(c.LocalAddr())
		gc = newTCPConn(dupFD, el, sockAddr, c.LocalAddr(), c.RemoteAddr())
	case *net.UDPConn:
		if sockAddr, _, _, _, err = socket.GetUDPSockAddr(c.RemoteAddr().Network(), c.RemoteAddr().String()); err != nil {
			return nil, err
		}
		el = cli.ev.engine.eng.lb.next(c.LocalAddr())
		gc = newUDPConn(dupFD, el, c.LocalAddr(), sockAddr, true)
	default:
		return nil, gerrors.ErrUnsupportedProtocol
	}
	err = el.poller.UrgentTrigger(el.register, gc)
	if err != nil {
		gc.Close()
		return nil, err
	}
	return gc, nil
}
