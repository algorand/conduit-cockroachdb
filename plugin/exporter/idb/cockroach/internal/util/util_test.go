package util_test

import (
	"fmt"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	test "github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach/internal/testing"
	"github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxWithRetry(t *testing.T) {
	count := 3
	f := func(pgx.Tx) error {
		if count == 0 {
			return nil
		}

		count--

		pgerr := pgconn.PgError{
			Code: pgerrcode.SerializationFailure,
		}
		return fmt.Errorf("database error: %w", &pgerr)
	}

	db, _, shutdownFunc := test.SetupDatabase(t)
	defer shutdownFunc()

	err := util.TxWithRetry(db, pgx.TxOptions{}, f, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
