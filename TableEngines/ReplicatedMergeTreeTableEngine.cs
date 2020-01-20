using ClickHouse.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.TableEngines
{
    public enum ReplicatedMergeTreeFamily
    {
        ReplicatedMergeTree = TableEngineType.ReplicatedMergeTree,
        ReplicatedSummingMergeTree = TableEngineType.ReplicatedSummingMergeTree,
        ReplicatedReplacingMergeTree = TableEngineType.ReplicatedReplacingMergeTree,
        ReplicatedAggregatingMergeTree = TableEngineType.ReplicatedAggregatingMergeTree,
        ReplicatedCollapsingMergeTree = TableEngineType.ReplicatedCollapsingMergeTree,
        ReplicatedVersionedCollapsingMergeTree = TableEngineType.ReplicatedVersionedCollapsingMergeTree,
        ReplicatedGraphiteMergeTree = TableEngineType.ReplicatedGraphiteMergeTree
    }
    // https://clickhouse.yandex/docs/en/operations/table_engines/replication/
    // https://cloud.yandex.com/docs/managed-clickhouse/concepts/replication
    // https://clickhouse-docs.readthedocs.io/en/latest/system_tables/system.replicas.html
    // https://www.altinity.com/blog/2018/5/10/circular-replication-cluster-topology-in-clickhouse
    public class ReplicatedMergeTreeTableEngine : MergeTreeTableEngine<ReplicatedMergeTreeFamily> 
    {
        public ReplicatedMergeTreeTableEngine(ReplicatedMergeTreeFamily type, string parameters
            , string partitionBy = null, string orderBy = null, string primaryKey = null, string sampleBy = null
            , DataLifeTime ttl = null , params (MergeTreeSettingsOption Option, object Value)[] settings)
            : base(type: type, partitionBy: partitionBy, orderBy: orderBy, primaryKey: primaryKey, sampleBy: sampleBy, ttl: ttl, settings: settings) 
        {
            Parameters = parameters;
        }
        // https://clickhouse.yandex/docs/en/operations/table_engines/replication/#creating-replicated-tables
        public static string GetParametersFromMacro(string tableName, bool withCluster = false, bool withLayer = false) => 
            $"'/clickhouse/{(withCluster ? Macros.Cluster + "/" : "")}tables/{(withLayer ? Macros.Layer + "-" : "")}{Macros.Shard}/{tableName}', '{Macros.Replica}'";
    }
    /*
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}')
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)     
     */
}