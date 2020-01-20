namespace ClickHouse.CommandText
{
    public class DatabaseCommandText : ClickHouseCommandText
    {
        public DatabaseCommandText(ClickHouseSchema schema) : base(schema) { }

        #region Create
        // https://clickhouse.yandex/docs/en/query_language/create/
        public static string CreateDatabase(string db, bool ifNotExists = true, string cluster = null) =>
            $"CREATE DATABASE {(ifNotExists ? "IF NOT EXISTS " : "")}{db}{(!string.IsNullOrWhiteSpace(cluster) ? " ON CLUSTER " + cluster : "")}";
        #endregion

        #region Drop
        // https://clickhouse.yandex/docs/en/query_language/misc/
        public static string DropDatabase(string db, bool ifExists = false, string cluster = null) =>
            $"DROP DATABASE {(ifExists ? "IF EXISTS " : "")}{db}{(!string.IsNullOrWhiteSpace(cluster) ? " ON CLUSTER " + cluster : "")}";
        #endregion
    }
}