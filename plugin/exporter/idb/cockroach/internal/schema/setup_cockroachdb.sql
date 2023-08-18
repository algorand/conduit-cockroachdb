/* SQL Export using CockroachDB SQL Migration Tool */

-- Statement 1
SET default_int_size = 8;

-- Statement 2
CREATE TABLE IF NOT EXISTS block_header (
        round bigint primary key,
        realtime timestamp without time zone NOT NULL,
        rewardslevel bigint NOT NULL,
        header jsonb NOT NULL
);

-- Statement 3
CREATE INDEX IF NOT EXISTS block_header_time ON block_header (realtime);

-- Statement 4
CREATE TABLE IF NOT EXISTS txn (
               round bigint NOT NULL,
               intra integer NOT NULL,
               typeenum smallint NOT NULL,
               asset bigint NOT NULL, -- 0=Algos, otherwise AssetIndex
               txid bytea, -- base32 of [32]byte hash, or NULL for inner transactions.
               txn jsonb NOT NULL, -- json encoding of signed txn with apply data; inner txns exclude nested inner txns
               extra jsonb NOT NULL,
               primary key ( round, intra )
    );

-- Statement 5
CREATE INDEX IF NOT EXISTS txn_by_tixid ON txn ( txid );

-- Statement 6
CREATE TABLE IF NOT EXISTS txn_participation (
                                                 addr bytea NOT NULL,
                                                 round bigint NOT NULL,
                                                 intra integer NOT NULL
);

-- Statement 7
CREATE UNIQUE INDEX IF NOT EXISTS txn_participation_i ON txn_participation ( addr, round DESC, intra DESC );

-- Statement 8
CREATE TABLE IF NOT EXISTS account (
                                       addr bytea primary key,
                                       microalgos bigint NOT NULL, -- okay because less than 2^54 Algos
                                       rewardsbase bigint NOT NULL,
                                       rewards_total bigint NOT NULL,
                                       deleted bool NOT NULL, -- whether or not it is currently deleted
                                       created_at bigint NOT NULL, -- round that the account is first used
                                       closed_at bigint, -- round that the account was last closed
                                       keytype varchar(8), -- "sig", "msig", "lsig", or NULL if unknown
    account_data jsonb NOT NULL -- trimmed ledgercore.AccountData that excludes the fields above; SQL 'NOT NULL' is held though the json string will be "null" iff account is deleted
    );

-- Statement 9
CREATE TABLE IF NOT EXISTS account_asset (
                                             addr bytea NOT NULL, -- [32]byte
                                             assetid bigint NOT NULL,
                                             amount numeric(20) NOT NULL, -- need the full 18446744073709551615
    frozen boolean NOT NULL,
    deleted bool NOT NULL, -- whether or not it is currently deleted
    created_at bigint NOT NULL, -- round that the asset was added to an account
    closed_at bigint, -- round that the asset was last removed from the account
    primary key (addr, assetid)
    );

-- Statement 10
CREATE INDEX IF NOT EXISTS account_asset_by_addr_partial ON account_asset(addr) WHERE NOT deleted;

-- Statement 11
CREATE TABLE IF NOT EXISTS asset (
                                     id bigint primary key,
                                     creator_addr bytea NOT NULL,
                                     params jsonb NOT NULL, -- data.basics.AssetParams; json string "null" iff asset is deleted
                                     deleted bool NOT NULL, -- whether or not it is currently deleted
                                     created_at bigint NOT NULL, -- round that the asset was created
                                     closed_at bigint -- round that the asset was closed; cannot be recreated because the index is unique
);

-- Statement 12
CREATE INDEX IF NOT EXISTS asset_by_creator_addr_deleted ON asset(creator_addr, deleted);

-- Statement 13
CREATE TABLE IF NOT EXISTS metastate (
                                         k text primary key,
                                         v jsonb
);

-- Statement 14
CREATE TABLE IF NOT EXISTS app (
                                   id bigint primary key,
                                   creator bytea NOT NULL, -- account address
                                   params jsonb NOT NULL, -- json string "null" iff app is deleted
                                   deleted bool NOT NULL, -- whether or not it is currently deleted
                                   created_at bigint NOT NULL, -- round that the asset was created
                                   closed_at bigint -- round that the app was deleted; cannot be recreated because the index is unique
);

-- Statement 15
CREATE INDEX IF NOT EXISTS app_by_creator_deleted ON app(creator, deleted);

-- Statement 16
CREATE TABLE IF NOT EXISTS account_app (
                                           addr bytea,
                                           app bigint,
                                           localstate jsonb NOT NULL, -- json string "null" iff deleted from the account
                                           deleted bool NOT NULL, -- whether or not it is currently deleted
                                           created_at bigint NOT NULL, -- round that the app was added to an account
                                           closed_at bigint, -- round that the account_app was last removed from the account
                                           primary key (addr, app)
    );

-- Statement 17
CREATE INDEX IF NOT EXISTS account_app_by_addr_partial ON account_app(addr) WHERE NOT deleted;

-- Statement 18
CREATE TABLE IF NOT EXISTS app_box (
           app bigint NOT NULL,
           name bytea NOT NULL,
           value bytea NOT NULL, -- upon creation 'value' is 0x000...000 with length being the box'es size
           primary key (app, name)
);