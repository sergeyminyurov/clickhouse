using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.TableEngines
{
    // https://clickhouse.yandex/docs/ru/operations/table_engines/distributed/
    // https://blog.uiza.io/replicated-vs-distributed-on-clickhouse-part-1/
    public sealed class DistributedTableEngine : ITableEngine
    {
        public TableEngineType Type => TableEngineType.Distributed;
        public string Cluster { get; }
        public string Database { get; }
        public string Table { get; }
        public string ShardingKey { get; }
        public DistributedTableEngine(string cluster, string database, string table, string shardingKey = null)
        {
            Cluster = cluster;
            Database = database;
            Table = table;
            ShardingKey = shardingKey;
        }
        // Distributed(logs, default, hits[, sharding_key])
        public string Text => $"Distributed({Cluster}, {(string.IsNullOrWhiteSpace(Database) ? "CurrentDatabase()" : Database)}, " 
            + $"{Table} {(!string.IsNullOrWhiteSpace(ShardingKey) ? $", {ShardingKey}": "")})";
    }
}