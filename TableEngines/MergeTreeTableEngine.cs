using System;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.TableEngines
{
    public enum MergeTreeFamily
    {
        MergeTree = TableEngineType.MergeTree,
        ReplacingMergeTree = TableEngineType.ReplacingMergeTree,
        SummingMergeTree = TableEngineType.SummingMergeTree,
        AggregatingMergeTree = TableEngineType.AggregatingMergeTree,
        CollapsingMergeTree = TableEngineType.CollapsingMergeTree,
        VersionedCollapsingMergeTree = TableEngineType.VersionedCollapsingMergeTree,
    }
    public enum MergeTreeSettingsOption
    {
        IndexGranularity,
        IndexGranularityBytes,
        EnableMixedGranularityParts,
        UseMinimalisticPartHeaderInZookeeper,
        MinMergeBytesToUseDirectIo,
        MergeWithTtlTimeout,
        WriteFinalMark,
        StoragePolicy,
    }
    public class MergeTreeTableEngine<TFamily> : ITableEngine where TFamily : Enum
    {
        public TFamily Type { get; }
        TableEngineType ITableEngine.Type => (TableEngineType)Convert.ChangeType(Type, typeof(TableEngineType));
        public string Parameters { get; set; }
        public string PartitionBy { get; set; }
        public string OrderBy { get; set; }
        public string PrimaryKey { get; set; }
        public string SampleBy { get; set; }
        public DataLifeTime TTL { get; set; }
        public Dictionary<MergeTreeSettingsOption, object> Settings { get; private set; }
        public MergeTreeTableEngine(TFamily type, string partitionBy = null, string orderBy = null, string primaryKey = null, string sampleBy = null
            , DataLifeTime ttl = null, params (MergeTreeSettingsOption Option, object Value)[] settings)
        {
            Type = type;
            PartitionBy = partitionBy;
            OrderBy = orderBy;
            PrimaryKey = primaryKey;
            SampleBy = sampleBy;
            TTL = ttl;
            Settings = new Dictionary<MergeTreeSettingsOption, object>();
            if (settings != null && settings.Length > 0)
            {
                for (int i = 0; i < settings.Length; i++)
                    Settings.Add(settings[i].Option, settings[i].Value);
            }
        }
        // https://github.com/ClickHouse/ClickHouse/blob/master/docs/ru/operations/table_engines/mergetree.md
        public static string GetText(string engine, string parameters = ""
            , string partitionBy = null, string orderBy = null, string primaryKey = null, string sampleBy = null, string ttl = null
            , params (MergeTreeSettingsOption Option, object Value)[] settings) =>
            $"{engine}({parameters}){(!string.IsNullOrWhiteSpace(partitionBy) ? " PARTITION BY " + partitionBy : "")}"
                + $"{(!string.IsNullOrWhiteSpace(orderBy) ? " ORDER BY " + orderBy : "")}"
                + $"{(!string.IsNullOrWhiteSpace(primaryKey) ? " PRIMARY KEY " + primaryKey : "")}"
                + $"{(!string.IsNullOrWhiteSpace(sampleBy) ? " SAMPLE BY " + sampleBy : "")}"
                + $"{(!string.IsNullOrWhiteSpace(ttl) ? " TTL " + ttl : "")}"
                + $"{(settings != null && settings.Length > 0 ? " SETTINGS " + string.Join(",", settings.Select(t => $"{t.Option.ToLowerUnderscore()}={t.Value}")) : "")}";
        public string Text
        {
            get
            {
                List<(MergeTreeSettingsOption Option, object Value)> settings = new List<(MergeTreeSettingsOption Option, object Value)>();
                if (Settings.Count > 0)
                {
                    foreach (var item in Settings)
                        settings.Add((item.Key, item.Value));
                }
                return GetText(engine: Type.ToString(), parameters: Parameters, partitionBy: PartitionBy, orderBy: OrderBy, primaryKey: PrimaryKey
                    , sampleBy: SampleBy, ttl: TTL?.GetText(), settings: settings.ToArray());
            }
        }
    }
    public class MergeTreeTableEngine : MergeTreeTableEngine<MergeTreeFamily> 
    {
        public MergeTreeTableEngine(MergeTreeFamily type = MergeTreeFamily.MergeTree
            , string partitionBy = null, string orderBy = null, string primaryKey = null, string sampleBy = null, DataLifeTime ttl = null
            , params (MergeTreeSettingsOption Option, object Value)[] settings)
            : base(type: type, partitionBy: partitionBy, orderBy: orderBy, primaryKey: primaryKey, sampleBy: sampleBy, ttl: ttl, settings: settings) { }
    }
}