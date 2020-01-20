using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using System.Linq;
using ClickHouse.TableEngines;

namespace ClickHouse.CommandText
{
    public class TableCommandText : ClickHouseCommandText, IFromExpression
    {
        public string TableName { get; set; }
        public bool IsTemporary { get; set; }
        string IFromExpression.GetText(string db) => $"{db}.{TableName}";

        public TableCommandText(ClickHouseSchema schema, string tableName, bool isTemporary) : base(schema) { TableName = tableName; }

        // https://clickhouse.yandex/docs/en/query_language/create/
        #region Create Table
        public static string CreateTable(string table, string columnsWithType, ITableEngine engine, string db = null, string indexes = null
            , bool temporary = false, bool ifNotExists = false, string cluster = null)
        {
            #region Guard
            if (string.IsNullOrWhiteSpace(table))
                throw new ArgumentNullException(nameof(table));
            if (string.IsNullOrWhiteSpace(columnsWithType))
                throw new ArgumentNullException(nameof(columnsWithType));
            if (engine == null)
                throw new ArgumentNullException(nameof(engine));
            if (db == null && !temporary)
                throw new ArgumentNullException(nameof(db));
            #endregion

            if (temporary)
                db = null;
            string cmdText = $"CREATE {(temporary ? "TEMPORARY " : "")}TABLE {(ifNotExists ? "IF NOT EXISTS " : "")}" 
                + $"{(!string.IsNullOrWhiteSpace(db) && !temporary ? db + "." : "")}{table}"
                + $"{(!string.IsNullOrWhiteSpace(cluster) ? " ON CLUSTER " + cluster : "")}"
                + $"({columnsWithType}{(!string.IsNullOrWhiteSpace(indexes) ? ", " + indexes : "")}) ENGINE = {engine.Text}";
            return cmdText;
        }
        //public static string CreateTable(ClickHouseTable table, bool temporary = false, bool ifNotExists = false) =>
        //    CreateTable(table: table.Name, columnsWithType: string.Join(",", table.Columns.Select(t => t.ToString())), engine: table.Engine
        //        , indexes: table.Indexes, temporary:temporary, ifNotExists: ifNotExists, db: db, cluster: table.Cluster);
        public string CreateTable(string columnsWithType, ITableEngine engine, string db = null, bool ifNotExists = false, string cluster = null) =>
            CreateTable(table:TableName, db:db, columnsWithType:columnsWithType, engine:engine, temporary:IsTemporary, ifNotExists:ifNotExists, cluster:cluster);
        #endregion

        #region Drop
        // https://clickhouse.yandex/docs/en/query_language/misc/
        public static string DropTable(string table, string db = null, bool temporary = false, bool ifExists = false, string cluster = null) =>
            $"DROP {(temporary ? "TEMPORARY " : "")}TABLE {(ifExists ? "IF EXISTS " : "")}" 
                + $"{(!string.IsNullOrWhiteSpace(db) && !temporary ? db + "." : "")}{table}"
                + $"{(!string.IsNullOrWhiteSpace(cluster) ? " ON CLUSTER " + cluster : "")}";
        public string DropTable(string db = null, bool ifExists = false, string cluster = null) => DropTable(TableName, db:db, ifExists:ifExists, cluster:cluster);
        #endregion

        #region Truncate
        public static string TruncateTable(string table, string db = null, bool ifExists = false, string cluster = null) =>
            $"TRUNCATE TABLE {(ifExists ? "IF EXISTS " : "")}{(!string.IsNullOrWhiteSpace(db) ? db + "." : "")}{table}"
                + $"{(!string.IsNullOrWhiteSpace(cluster) ? " ON CLUSTER " + cluster : "")}";
        public string TruncateTable(string db = null, bool ifExists = false, string cluster = null) => TruncateTable(TableName, db:(IsTemporary ? null : db), ifExists:ifExists, cluster:cluster);
        #endregion

        #region Exists
        // https://clickhouse.yandex/docs/en/query_language/misc/
        public static string ExistsTable(string table, string db = null, bool temporary = false) =>
            $"EXISTS {(temporary ? "TEMPORARY " : "")} TABLE {(!string.IsNullOrWhiteSpace(db) ? db + "." : "")}{table}";
        #endregion
    }
}