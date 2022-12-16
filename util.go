package gnet

import (
	"github.com/panjf2000/gnet/v2/internal/socket"
)

var (
	// export SockAddr function.
	GetUDPSockAddr = socket.GetUDPSockAddr
	GetTCPSockAddr = socket.GetTCPSockAddr
)
