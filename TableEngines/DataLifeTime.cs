using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.TableEngines
{
    // https://clickhouse.yandex/docs/en/operations/table_engines/mergetree/#table_engine-mergetree-ttl
    public enum DataLifeAction
    {
        Delete,
        MoveToDisk,
        MoveToVolume
    }
    public sealed class DataLifeTime
    {
        public string DateField { get; set; }
        public TimeInterval Interval { get; set; }
        public UInt16 Value { get; set; }
        public DataLifeAction Action { get; set; }
        public string Storage { get; set; }

        public DataLifeTime() { }
        public DataLifeTime(string dateField, TimeInterval interval, UInt16 value, DataLifeAction action, string storage = null) 
        {
            DateField = dateField;
            Interval = interval;
            Value = value;
            Action = action;
            Storage = storage;
        }

        public static string GetText(string dateField, TimeInterval interval, UInt16 value, DataLifeAction action, string storage = null) 
            => $"TTL {dateField} + {interval.ToString().ToUpper()} {value} {GetActionText(action, storage)}";
        public static string GetActionText(DataLifeAction action, string storage = null)
        {
            switch (action)
            {
                case DataLifeAction.MoveToDisk: return $"TO DISK '{storage}'";
                case DataLifeAction.MoveToVolume: return $"TO VOLUME '{storage}'";
                default: return "DELETE";
            }
        }
        public string GetText() => GetText(DateField, Interval, Value, Action, Storage);
    }
}