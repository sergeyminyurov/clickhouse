using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse
{
    // https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_sources/#dicts-external_dicts_dict_sources-odbc
    // https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_sources/#example-of-connecting-postgresql
    public class DictionarySourceOdbc : IDictionarySource
    {
        public DictionarySourceType Type => DictionarySourceType.ODBC;
        public string TableName { get; set; }
        public string ConnectionString { get; set; }
        public string DatabaseName { get; set; }
        public string InvalidateQuery { get; set; }

        public DictionarySourceOdbc(string connString, string table, string db = null, string invalidateQuery = null)
        {
            ConnectionString = connString;
            TableName = table;
            DatabaseName = db;
            InvalidateQuery = invalidateQuery;
        }
        // CREATE DICTIONARY TestDWH2.dimPeople () PRIMARY KEY  SOURCE(ODBC(table '{TableName}' connection_string '{ConnectionString}')) LAYOUT (HASHED()) LIFETIME (MIN 300 MAX 600)
        public string Expression => $"SOURCE(ODBC({(!string.IsNullOrWhiteSpace(DatabaseName) ? $"db '{DatabaseName}' " : "")}" 
            + "table '{TableName}' connection_string '{ConnectionString}'" 
            + $"{(!string.IsNullOrWhiteSpace(InvalidateQuery) ? $" invalidate_query '{InvalidateQuery}'" : "")}))";
    }
}