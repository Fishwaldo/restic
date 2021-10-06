package nats

import (
	"net/url"
	"strings"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/options"
)

// Config contains all configuration necessary to connect to a REST server.
type Config struct {
	Server	*url.URL
	Credential string `option:"credentialfile" help:"Path to the NatsIO Credential File"`
	Connections uint `option:"connections" help:"set a limit for the number of concurrent connections (default: 5)"`
	Repo string
}

func init() {
	options.Register("natsio", Config{})
}

// NewConfig returns a new Config with the default values filled in.
func NewConfig() Config {
	return Config{
		Connections: 5,
	}
}

// ParseConfig parses the string s and extracts the REST server URL.
func ParseConfig(s string) (interface{}, error) {
	if !strings.HasPrefix(s, "nats:") {
		return nil, errors.New("invalid REST backend specification")
	}

	u, err := url.Parse(s)

	if err != nil {
		return nil, errors.Wrap(err, "url.Parse")
	}

	cfg := NewConfig()
	cfg.Server = u
	var repo string
	if cfg.Server.Path[0] == '/' {
		repo = cfg.Server.Path[1:]
	}
	if repo[len(repo)-1] == '/' {
		repo = repo[0:len(repo)-1]
	}
	// replace any further slashes with . to specify a nested queue
	repo = strings.Replace(repo, "/", ".", -1)

	cfg.Repo = repo
	return cfg, nil
}