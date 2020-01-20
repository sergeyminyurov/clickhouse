using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.TableEngines
{
    // https://clickhouse.yandex/docs/en/query_language/operators/#operators-datetime
    public enum TimeInterval
    {
        Second,
        Minute,
        Hour,
        Day,
        Week,
        Month,
        Quarter,
        Year,
    }
}