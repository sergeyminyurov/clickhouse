using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace ClickHouse.TableEngines
{
    // https://clickhouse.yandex/docs/en/operations/table_engines/
    public enum TableEngineType
    {
        MergeTree,
        ReplacingMergeTree,
        SummingMergeTree,
        AggregatingMergeTree,
        CollapsingMergeTree,
        VersionedCollapsingMergeTree,
        StripeLog,
        Log,
        TinyLog,
        ReplicatedMergeTree,
        ReplicatedSummingMergeTree,
        ReplicatedReplacingMergeTree,
        ReplicatedAggregatingMergeTree,
        ReplicatedCollapsingMergeTree,
        ReplicatedVersionedCollapsingMergeTree,
        ReplicatedGraphiteMergeTree,
        Distributed
    }
    public interface ITableEngine
    {
        TableEngineType Type { get; }
        string Text { get; }
    }
}