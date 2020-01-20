using ClickHouse.Ado;

namespace ClickHouse
{
    // https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_sources/#dicts-external_dicts_dict_sources-clickhouse
    public class DictionarySourceClickhouse : IDictionarySource
    {
        public DictionarySourceType Type => DictionarySourceType.Clickhouse;
        public ClickHouseConnectionSettings Settings { get; }
        public string Table { get; set; }
        public string Where { get; set; }
        public string InvalidateQuery { get; set; }

        public DictionarySourceClickhouse(ClickHouseConnectionSettings settings, string table, string where = null, string invalidateQuery = null)
        {
            Settings = settings;
            Table = table;
            Where = where;
            InvalidateQuery = invalidateQuery;
        }

        public string Expression => 
            $"SOURCE(CLICKHOUSE(host '{Settings.Host}' port {Settings.Port} user '{Settings.User}' password '{Settings.Password}' " 
                + $"db '{Settings.Database}' table '{Table}' where \"{Where}\" " 
                + $"{(!string.IsNullOrWhiteSpace(InvalidateQuery) ? $"invalidate_query '{InvalidateQuery}'" : "")}))";
    }
/*
SOURCE(CLICKHOUSE(
    host 'example01-01-1'
    port 9000
    user 'default'
    password ''
    db 'default'
    table 'ids'
    where 'id=10'
))

SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
*/
}