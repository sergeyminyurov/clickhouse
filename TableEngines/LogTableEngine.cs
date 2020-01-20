using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.TableEngines
{
    // https://github.com/ClickHouse/ClickHouse/blob/master/docs/ru/operations/table_engines/log_family.md
    public enum TableEngineLogFamily
    {
        StripeLog = TableEngineType.StripeLog,
        Log = TableEngineType.Log,
        TinyLog = TableEngineType.TinyLog,
    }
    public class LogTableEngine : ITableEngine
    {
        public TableEngineLogFamily Type { get; }
        TableEngineType ITableEngine.Type => (TableEngineType)Type;
        public LogTableEngine(TableEngineLogFamily type = TableEngineLogFamily.Log) { Type = type; }
        public string Text => Type.ToString();
    }
}