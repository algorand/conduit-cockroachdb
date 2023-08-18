package exporter

import (
	"context"
	"testing"

	sdk "github.com/algorand/go-algorand-sdk/v2/types"
	_ "github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/cockroach"
	_ "github.com/shiqizng/cockroachdb-exporter/plugin/exporter/idb/dummy"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/algorand/conduit/conduit"
	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/exporters"
)

var cockroachConstructor = exporters.ExporterConstructorFunc(func() exporters.Exporter {
	return &cockroachdbExporter{}
})
var logger *logrus.Logger
var round = sdk.Round(0)

func init() {
	logger, _ = test.NewNullLogger()
}

func TestExporterMetadata(t *testing.T) {
	exporter := cockroachConstructor.New()
	meta := exporter.Metadata()
	assert.Equal(t, metadata.Name, meta.Name)
	assert.Equal(t, metadata.Description, meta.Description)
	assert.Equal(t, metadata.Deprecated, meta.Deprecated)
}

func TestConnectDisconnectSuccess(t *testing.T) {
	exporter := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("test: true\nconnection-string: ''")
	assert.NoError(t, exporter.Init(context.Background(), conduit.MakePipelineInitProvider(&round, &sdk.Genesis{}), cfg, logger))
	assert.NoError(t, exporter.Close())
}

func TestConnectUnmarshalFailure(t *testing.T) {
	exporter := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("'")
	assert.ErrorContains(t, exporter.Init(context.Background(), conduit.MakePipelineInitProvider(&round, nil), cfg, logger), "connect failure in unmarshalConfig")
}

func TestConnectDbFailure(t *testing.T) {
	exporter := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("")
	assert.ErrorContains(t, exporter.Init(context.Background(), conduit.MakePipelineInitProvider(&round, nil), cfg, logger), "connection string is empty for cockroachdb")
}

func TestReceiveInvalidBlock(t *testing.T) {
	exporter := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("test: true")
	assert.NoError(t, exporter.Init(context.Background(), conduit.MakePipelineInitProvider(&round, &sdk.Genesis{}), cfg, logger))
	invalidBlock := data.BlockData{
		BlockHeader: sdk.BlockHeader{
			Round: 1,
		},
		Payset:      sdk.Payset{},
		Certificate: &map[string]interface{}{},
		Delta:       nil,
	}
	err := exporter.Receive(invalidBlock)
	assert.ErrorIs(t, err, errMissingDelta)
}

func TestReceiveAddBlockSuccess(t *testing.T) {
	exporter := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("test: true")
	assert.NoError(t, exporter.Init(context.Background(), conduit.MakePipelineInitProvider(&round, &sdk.Genesis{}), cfg, logger))

	block := data.BlockData{
		BlockHeader: sdk.BlockHeader{},
		Payset:      sdk.Payset{},
		Certificate: &map[string]interface{}{},
		Delta:       &sdk.LedgerStateDelta{},
	}
	assert.NoError(t, exporter.Receive(block))
}

func TestPostgresqlExporterInit(t *testing.T) {
	exporter := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("test: true")

	// genesis hash mismatch
	initProvider := conduit.MakePipelineInitProvider(&round, &sdk.Genesis{})
	initProvider.SetGenesis(&sdk.Genesis{
		Network: "test",
	})
	err := exporter.Init(context.Background(), initProvider, cfg, logger)
	assert.Contains(t, err.Error(), "error importing genesis: genesis hash not matching")

	// incorrect round
	round = 1
	err = exporter.Init(context.Background(), conduit.MakePipelineInitProvider(&round, &sdk.Genesis{}), cfg, logger)
	assert.Contains(t, err.Error(), "initializing block round 1 but next round to account is 0")
}
