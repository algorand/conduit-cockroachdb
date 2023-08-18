package testing

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/cockroachdb"
	"github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach/internal/schema"
	"github.com/stretchr/testify/require"
)

var testpg string

func init() {
	flag.StringVar(&testpg, "test-pg", "", "postgres connection string; resets the database")
	if testpg == "" {
		// Note: tests across packages run in parallel, so this does not work
		// very well unless you use "-p 1".
		testpg = os.Getenv("TEST_PG")
	}
}

// SetupDatabase starts a gnomock postgres DB then returns the database object,
// the connection string and a shutdown function.
func SetupDatabase(t *testing.T) (*pgxpool.Pool, string, func()) {
	p := cockroachdb.Preset(
		cockroachdb.WithVersion("v22.2.12"),
		cockroachdb.WithDatabase("mydb"),
	)
	container, err := gnomock.Start(p)
	require.NoError(t, err, "Error starting gnomock")

	connStr := fmt.Sprintf(
		"host=%s port=%d user=root dbname=%s sslmode=disable",
		container.Host, container.DefaultPort(), "mydb",
	)

	db, err := pgxpool.Connect(context.Background(), connStr)
	require.NoError(t, err, "Error opening database connection")

	shutdownFunc := func() {
		db.Close()
		err = gnomock.Stop(container)
		require.NoError(t, err, "Error stoping gnomock")
	}

	return db, connStr, shutdownFunc
}

// SetupPostgresWithSchema is equivalent to SetupDatabase() but also creates the
// indexer schema.
func SetupPostgresWithSchema(t *testing.T) (*pgxpool.Pool, string, func()) {
	db, connStr, shutdownFunc := SetupDatabase(t)

	_, err := db.Exec(context.Background(), schema.SetupCockroachSql)
	require.NoError(t, err)

	return db, connStr, shutdownFunc
}
