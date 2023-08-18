// You can build without postgres by `go build --tags nopostgres` but it's on by default

package cockroach

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/algorand/go-algorand-sdk/v2/protocol"
	"github.com/algorand/go-algorand-sdk/v2/protocol/config"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb"
	"github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach/internal/encoding"
	"github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach/internal/schema"
	"github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach/internal/types"
	iutil "github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach/internal/util"
	"github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach/internal/writer"
	log "github.com/sirupsen/logrus"

	"github.com/algorand/indexer/idb/migration"
	itypes "github.com/algorand/indexer/v3/types"

	sdk "github.com/algorand/go-algorand-sdk/v2/types"
)

var serializable = pgx.TxOptions{IsoLevel: pgx.Serializable} // be a real ACID database
var readonlyRepeatableRead = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

// IndexerDb is an idb.IndexerDB implementation
type IndexerDb struct {
	readonly bool
	log      *log.Logger

	db             *pgxpool.Pool
	migration      *migration.Migration
	accountingLock sync.Mutex
}

// Close is part of idb.IndexerDb.
func (db *IndexerDb) Close() {
	db.db.Close()
}

func Init(connection string, opts idb.IndexerDbOptions, log *log.Logger) (*IndexerDb, chan struct{}, error) {
	postgresConfig, err := pgxpool.ParseConfig(connection)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't parse config: %v", err)
	}
	conn, err := pgxpool.ConnectConfig(context.Background(), postgresConfig)
	//defer conn.Close(context.Background())
	if err != nil {
		log.Fatal("failed to connect database", err)
	}

	idb := &IndexerDb{
		readonly: false,
		log:      log,
		db:       conn,
	}
	idb.init(opts)
	ch := make(chan struct{})
	close(ch)
	return idb, ch, nil
}

func (db *IndexerDb) isSetup() (bool, error) {
	query := `SELECT 0 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'metastate'`
	row := db.db.QueryRow(context.Background(), query)

	var tmp int
	err := row.Scan(&tmp)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("isSetup() err: %w", err)
	}
	return true, nil
}

// Returns an error object and a channel that gets closed when blocking migrations
// finish running successfully.
func (db *IndexerDb) init(opts idb.IndexerDbOptions) (chan struct{}, error) {
	setup, err := db.isSetup()
	if err != nil {
		return nil, fmt.Errorf("init() err: %w", err)
	}

	if !setup {
		// new database, run setup
		_, err = db.db.Exec(context.Background(), schema.SetupCockroachSql)
		if err != nil {
			return nil, fmt.Errorf("unable to setup postgres: %v", err)
		}

		ch := make(chan struct{})
		close(ch)
		return ch, nil
	}

	ch := make(chan struct{})
	close(ch)
	return ch, nil
}

// txWithRetry is a helper function that retries the function `f` in case the database
// transaction in it fails due to a serialization error. `f` is provided
// a transaction created using `opts`. If `f` experiences a database error, this error
// must be included in `f`'s return error's chain, so that a serialization error can be
// detected.
func (db *IndexerDb) txWithRetry(opts pgx.TxOptions, f func(pgx.Tx) error) error {
	return iutil.TxWithRetry(db.db, opts, f, db.log)
}

// AddBlock is part of idb.IndexerDb.
func (db *IndexerDb) AddBlock(vb *itypes.ValidatedBlock) error {
	protoVersion := protocol.ConsensusVersion(vb.Block.CurrentProtocol)
	_, ok := config.Consensus[protoVersion]
	if !ok {
		return fmt.Errorf("unknown protocol (%s) detected, this usually means you need to upgrade", protoVersion)
	}

	block := vb.Block
	round := block.BlockHeader.Round
	db.log.Printf("adding block %d", round)

	db.accountingLock.Lock()
	defer db.accountingLock.Unlock()

	f := func(tx pgx.Tx) error {
		// Check and increment next round counter.
		importstate, err := db.getImportState(context.Background(), tx)
		if err != nil {
			return fmt.Errorf("AddBlock() err: %w", err)
		}
		if round != sdk.Round(importstate.NextRoundToAccount) {
			return fmt.Errorf(
				"AddBlock() adding block round %d but next round to account is %d",
				round, importstate.NextRoundToAccount)
		}
		importstate.NextRoundToAccount++
		err = db.setImportState(tx, &importstate)
		if err != nil {
			return fmt.Errorf("AddBlock() err: %w", err)
		}

		w, err := writer.MakeWriter(tx)
		if err != nil {
			return fmt.Errorf("AddBlock() err: %w", err)
		}
		defer w.Close()

		if round == sdk.Round(0) {
			err = w.AddBlock0(&block)
			if err != nil {
				return fmt.Errorf("AddBlock() err: %w", err)
			}
			return nil
		}

		var wg sync.WaitGroup
		defer wg.Wait()

		var err0 error
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := func(tx pgx.Tx) error {
				err := writer.AddTransactions(&block, block.Payset, tx)
				if err != nil {
					return err
				}
				return writer.AddTransactionParticipation(&block, tx)
			}
			err0 = db.txWithRetry(serializable, f)
		}()

		err = w.AddBlock(&block, vb.Delta)
		if err != nil {
			return fmt.Errorf("AddBlock() err: %w", err)
		}

		// Wait for goroutines to finish and check for errors. If there is an error, we
		// return our own error so that the main transaction does not commit. Hence,
		// `txn` and `txn_participation` tables can only be ahead but not behind
		// the other state.
		wg.Wait()
		isUniqueViolationFunc := func(err error) bool {
			var pgerr *pgconn.PgError
			return errors.As(err, &pgerr) && (pgerr.Code == pgerrcode.UniqueViolation)
		}
		if (err0 != nil) && !isUniqueViolationFunc(err0) {
			return fmt.Errorf("AddBlock() err0: %w", err0)
		}

		return nil
	}
	err := db.txWithRetry(serializable, f)
	if err != nil {
		return fmt.Errorf("AddBlock() err: %w", err)
	}

	return nil
}

// LoadGenesis is part of idb.IndexerDB
func (db *IndexerDb) LoadGenesis(genesis sdk.Genesis) error {
	f := func(tx pgx.Tx) error {
		// check genesis hash
		network, err := db.getNetworkState(context.Background(), tx)
		if err == idb.ErrorNotInitialized {
			networkState := types.NetworkState{
				GenesisHash: genesis.Hash(),
			}
			err = db.setNetworkState(tx, &networkState)
			if err != nil {
				return fmt.Errorf("LoadGenesis() err: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("LoadGenesis() err: %w", err)
		} else {
			if network.GenesisHash != genesis.Hash() {
				return fmt.Errorf("LoadGenesis() genesis hash not matching")
			}
		}
		setAccountStatementName := "set_account"
		query := `INSERT INTO account (addr, microalgos, rewardsbase, account_data, rewards_total, created_at, deleted) VALUES ($1, $2, 0, $3, $4, 0, false)`
		_, err = tx.Prepare(context.Background(), setAccountStatementName, query)
		if err != nil {
			return fmt.Errorf("LoadGenesis() prepare tx err: %w", err)
		}
		defer tx.Conn().Deallocate(context.Background(), setAccountStatementName)

		for ai, alloc := range genesis.Allocation {
			addr, err := sdk.DecodeAddress(alloc.Address)
			if err != nil {
				return fmt.Errorf("LoadGenesis() decode address err: %w", err)
			}
			accountData := accountToAccountData(alloc.State)
			_, err = tx.Exec(
				context.Background(), setAccountStatementName,
				addr[:], alloc.State.MicroAlgos,
				encoding.EncodeTrimmedLcAccountData(encoding.TrimLcAccountData(accountData)), 0)
			if err != nil {
				return fmt.Errorf("LoadGenesis() error setting genesis account[%d], %w", ai, err)
			}

		}

		importstate := types.ImportState{
			NextRoundToAccount: 0,
		}
		err = db.setImportState(tx, &importstate)
		if err != nil {
			return fmt.Errorf("LoadGenesis() err: %w", err)
		}

		return nil
	}
	err := db.txWithRetry(serializable, f)
	if err != nil {
		return fmt.Errorf("LoadGenesis() err: %w", err)
	}

	return nil
}

func accountToAccountData(acct sdk.Account) sdk.AccountData {
	return sdk.AccountData{
		AccountBaseData: sdk.AccountBaseData{
			Status:     sdk.Status(acct.Status),
			MicroAlgos: 0,
		},
		VotingData: sdk.VotingData{
			VoteID:          acct.VoteID,
			SelectionID:     acct.SelectionID,
			StateProofID:    acct.StateProofID,
			VoteLastValid:   sdk.Round(acct.VoteLastValid),
			VoteKeyDilution: acct.VoteKeyDilution,
		},
	}
}

// Returns `idb.ErrorNotInitialized` if uninitialized.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getMetastate(ctx context.Context, tx pgx.Tx, key string) (string, error) {
	return iutil.GetMetastate(ctx, db.db, tx, key)
}

// If `tx` is nil, use a normal query.
func (db *IndexerDb) setMetastate(tx pgx.Tx, key, jsonStrValue string) (err error) {
	return iutil.SetMetastate(db.db, tx, key, jsonStrValue)
}

// Returns idb.ErrorNotInitialized if uninitialized.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getImportState(ctx context.Context, tx pgx.Tx) (types.ImportState, error) {
	importStateJSON, err := db.getMetastate(ctx, tx, schema.StateMetastateKey)
	if err == idb.ErrorNotInitialized {
		return types.ImportState{}, idb.ErrorNotInitialized
	}
	if err != nil {
		return types.ImportState{}, fmt.Errorf("unable to get import state err: %w", err)
	}

	state, err := encoding.DecodeImportState([]byte(importStateJSON))
	if err != nil {
		return types.ImportState{},
			fmt.Errorf("unable to parse import state v: \"%s\" err: %w", importStateJSON, err)
	}

	return state, nil
}

// If `tx` is nil, use a normal query.
func (db *IndexerDb) setImportState(tx pgx.Tx, state *types.ImportState) error {
	return db.setMetastate(
		tx, schema.StateMetastateKey, string(encoding.EncodeImportState(state)))
}

// Returns idb.ErrorNotInitialized if uninitialized.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getNetworkState(ctx context.Context, tx pgx.Tx) (types.NetworkState, error) {
	networkStateJSON, err := db.getMetastate(ctx, tx, schema.NetworkMetaStateKey)
	if err == idb.ErrorNotInitialized {
		return types.NetworkState{}, idb.ErrorNotInitialized
	}
	if err != nil {
		return types.NetworkState{}, fmt.Errorf("unable to get network state err: %w", err)
	}

	state, err := encoding.DecodeNetworkState([]byte(networkStateJSON))
	if err != nil {
		return types.NetworkState{},
			fmt.Errorf("unable to parse network state v: \"%s\" err: %w", networkStateJSON, err)
	}

	return state, nil
}

// If `tx` is nil, use a normal query.
func (db *IndexerDb) setNetworkState(tx pgx.Tx, state *types.NetworkState) error {
	return db.setMetastate(
		tx, schema.NetworkMetaStateKey, string(encoding.EncodeNetworkState(state)))
}

// Returns ErrorNotInitialized if genesis is not loaded.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getNextRoundToAccount(ctx context.Context, tx pgx.Tx) (uint64, error) {
	state, err := db.getImportState(ctx, tx)
	if err == idb.ErrorNotInitialized {
		return 0, err
	}
	if err != nil {
		return 0, fmt.Errorf("getNextRoundToAccount() err: %w", err)
	}

	return state.NextRoundToAccount, nil
}

// GetNextRoundToAccount is part of idb.IndexerDB
// Returns ErrorNotInitialized if genesis is not loaded.
func (db *IndexerDb) GetNextRoundToAccount() (uint64, error) {
	return db.getNextRoundToAccount(context.Background(), nil)
}

// Returns ErrorNotInitialized if genesis is not loaded.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getMaxRoundAccounted(ctx context.Context, tx pgx.Tx) (uint64, error) {
	round, err := db.getNextRoundToAccount(ctx, tx)
	if err != nil {
		return 0, err
	}

	if round > 0 {
		round--
	}
	return round, nil
}

// SetNetworkState is part of idb.IndexerDB
func (db *IndexerDb) SetNetworkState(gh sdk.Digest) error {
	networkState := types.NetworkState{
		GenesisHash: gh,
	}
	return db.setNetworkState(nil, &networkState)
}

// GetNetworkState is part of idb.IndexerDB
func (db *IndexerDb) GetNetworkState() (idb.NetworkState, error) {
	state, err := db.getNetworkState(context.Background(), nil)
	if err != nil {
		return idb.NetworkState{}, fmt.Errorf("GetNetworkState() err: %w", err)
	}
	networkState := idb.NetworkState{
		GenesisHash: state.GenesisHash,
	}
	return networkState, nil
}

// Health is part of idb.IndexerDB
func (db *IndexerDb) Health(ctx context.Context) (idb.Health, error) {
	migrationRequired := false
	migrating := false
	blocking := false
	errString := ""
	var data = make(map[string]interface{})

	if db.readonly {
		data["read-only-mode"] = true
	}

	if db.migration != nil {
		state := db.migration.GetStatus()

		if state.Err != nil {
			errString = state.Err.Error()
		}
		if state.Status != "" {
			data["migration-status"] = state.Status
		}

		migrationRequired = state.Running
		migrating = state.Running
		blocking = state.Blocking
	}

	data["migration-required"] = migrationRequired

	round, err := db.getMaxRoundAccounted(ctx, nil)

	// We'll just have to set the round to 0
	if err == idb.ErrorNotInitialized {
		err = nil
		round = 0
	}

	return idb.Health{
		Data:        &data,
		Round:       round,
		IsMigrating: migrating,
		DBAvailable: !blocking,
		Error:       errString,
	}, err
}
