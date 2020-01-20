using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse
{
    // https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_sources/
    public enum DictionarySourceType
    {
        File, Executable, Http, ODBC, MySql, Clickhouse, MongoDB, Redis
    }
    public interface IDictionarySource
    {
        DictionarySourceType Type { get; }
        string Expression { get; }
    }
}
