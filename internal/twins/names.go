package twins

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// maxIdentLen is Postgres' NAMEDATALEN-1. Anything longer is silently
// truncated by the server, which would make our bookkeeping disagree with
// pg_database — so we refuse to generate names that long in the first place.
const maxIdentLen = 63

// validPrefix constrains the configured prefix to characters that are safe in
// an unquoted identifier. Twin names are never derived from user input, but the
// prefix comes from config, and a sloppy prefix would poison both the generated
// name and the `starts_with(datname, prefix)` orphan sweep.
var validPrefix = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// newTwinName builds a unique database name: prefix + UTC timestamp + random
// suffix. The timestamp makes logs and psql \l readable during a demo; the
// random suffix is what actually guarantees uniqueness, since two clones can
// start inside the same second.
func newTwinName(prefix string) (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("twins: generate twin name suffix: %w", err)
	}
	name := prefix + time.Now().UTC().Format("20060102t150405") + "_" + hex.EncodeToString(b[:])
	if len(name) > maxIdentLen {
		return "", fmt.Errorf("twins: generated name %q exceeds %d bytes; shorten NamePrefix", name, maxIdentLen)
	}
	return name, nil
}

// quoteIdent renders a database name for interpolation into DDL. Postgres does
// not allow database names to be passed as bind parameters, so every DDL string
// in this package is built by hand — this is the only sanctioned way to put an
// identifier into one.
func quoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// dsnForDB rewrites the admin connection string to point at a different
// database, so callers get a ready-to-use conn string for their twin without
// re-deriving credentials.
//
// When the admin URL is already a postgres:// URL we only swap the path,
// because that preserves every option verbatim — sslmode above all, which
// pgconn.Config resolves into an opaque *tls.Config we could not faithfully
// turn back into a string. Keyword/value DSNs fall back to reconstruction.
func dsnForDB(adminURL string, cfg *pgconn.Config, dbname string) string {
	if u, err := url.Parse(adminURL); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		u.Path = "/" + dbname
		return u.String()
	}
	return rebuildDSN(cfg, dbname)
}

func rebuildDSN(cfg *pgconn.Config, dbname string) string {
	u := url.URL{
		Scheme: "postgres",
		Path:   "/" + dbname,
	}
	q := url.Values{}
	if strings.HasPrefix(cfg.Host, "/") {
		// Unix socket directory: it cannot live in the URL authority, so it
		// goes in the query string the way libpq expects.
		q.Set("host", cfg.Host)
		u.Host = ":" + strconv.Itoa(int(cfg.Port))
	} else {
		u.Host = net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port)))
	}
	switch {
	case cfg.User != "" && cfg.Password != "":
		u.User = url.UserPassword(cfg.User, cfg.Password)
	case cfg.User != "":
		u.User = url.User(cfg.User)
	}
	// pgconn resolves sslmode into a TLS config at parse time; a nil TLSConfig
	// means the admin conn negotiated plaintext, and the twin DSN must say so
	// explicitly or libpq-style clients will default back to "prefer".
	if cfg.TLSConfig == nil {
		q.Set("sslmode", "disable")
	}
	if len(q) > 0 {
		u.RawQuery = q.Encode()
	}
	return u.String()
}
