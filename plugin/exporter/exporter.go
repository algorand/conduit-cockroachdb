package exporter

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb"
	_ "github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/exporters"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/algorand/indexer/v3/types"
)

//go:embed sample.yaml
var sampleConfig string
var errMissingDelta = errors.New("ledger state delta is missing from block, ensure algod importer is using 'follower' mode")

// metadata contains information about the plugin used for CLI helpers.
var metadata = plugins.Metadata{
	Name:         "cockroachdb",
	Description:  "CockroachDB exporter.",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func init() {
	exporters.Register(metadata.Name, exporters.ExporterConstructorFunc(func() exporters.Exporter {
		return &cockroachdbExporter{}
	}))
}

type ExporterConfig struct {
	ConnectionString string `yaml:"connection-string"`
	Test             bool   `yaml:"test"`
}

// ExporterTemplate is the object which implements the exporter plugin interface.
type cockroachdbExporter struct {
	log    *logrus.Logger
	cfg    ExporterConfig
	ctx    context.Context
	cf     context.CancelFunc
	logger *logrus.Logger
	db     idb.IndexerDb
	round  uint64
}

func (exp *cockroachdbExporter) Metadata() plugins.Metadata {
	return metadata
}

func (exp *cockroachdbExporter) Config() string {
	ret, _ := yaml.Marshal(exp.cfg)
	return string(ret)
}

func (exp *cockroachdbExporter) Close() error {
	if exp.db != nil {
		exp.db.Close()
	}
	return nil
}

// createIndexerDB common code for creating the IndexerDb instance.
func createIndexerDB(logger *logrus.Logger, readonly bool, cfg plugins.PluginConfig) (idb.IndexerDb, chan struct{}, error) {
	var eCfg ExporterConfig
	if err := cfg.UnmarshalConfig(&eCfg); err != nil {
		return nil, nil, fmt.Errorf("connect failure in unmarshalConfig: %v", err)
	}

	// Inject a dummy db for unit testing
	dbName := "cockroachdb"
	if eCfg.Test {
		dbName = "dummy"
	}

	// for some reason when ConnectionString is empty, it's automatically
	// connecting to a local instance that's running.
	// this behavior can be reproduced in TestConnectDbFailure.
	if !eCfg.Test && eCfg.ConnectionString == "" {
		return nil, nil, fmt.Errorf("connection string is empty for %s", dbName)
	}

	var opts idb.IndexerDbOptions
	opts.ReadOnly = readonly

	db, ready, err := idb.IndexerDbByName(dbName, eCfg.ConnectionString, opts, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("connect failure constructing db, %s: %v", dbName, err)
	}

	return db, ready, nil
}

func (exp *cockroachdbExporter) Init(ctx context.Context, ip data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	exp.ctx, exp.cf = context.WithCancel(ctx)
	exp.logger = logger

	db, ready, err := createIndexerDB(exp.logger, false, cfg)
	if err != nil {
		return fmt.Errorf("db create error: %v", err)
	}
	<-ready

	exp.db = db
	_, err = idb.EnsureInitialImport(exp.db, *ip.GetGenesis())
	if err != nil {
		return fmt.Errorf("error importing genesis: %v", err)
	}
	dbRound, err := db.GetNextRoundToAccount()
	if err != nil {
		return fmt.Errorf("error getting next db round : %v", err)
	}
	if uint64(ip.NextDBRound()) != dbRound {
		return fmt.Errorf("initializing block round %d but next round to account is %d", ip.NextDBRound(), dbRound)
	}
	exp.round = uint64(ip.NextDBRound())

	return nil

}

func (exp *cockroachdbExporter) Receive(exportData data.BlockData) error {
	if exportData.Delta == nil {
		if exportData.Round() == 0 {
			exportData.Delta = &sdk.LedgerStateDelta{}
		} else {
			return errMissingDelta
		}
	}
	vb := types.ValidatedBlock{
		Block: sdk.Block{BlockHeader: exportData.BlockHeader, Payset: exportData.Payset},
		Delta: *exportData.Delta,
	}
	if err := exp.db.AddBlock(&vb); err != nil {
		return err
	}
	exp.round = exportData.Round() + 1
	return nil
}
