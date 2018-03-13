package metadata

import (
	"sync"

	"github.com/go-graphite/carbonzipper/zipper/types"
)

var Metadata struct {
	sync.RWMutex
	SupportedProtocols       map[string]struct{}
	ProtocolInits            map[string]types.NewServerClient
	ProtocolInitsWithLimiter map[string]types.NewServerClientWithLimiter
}
