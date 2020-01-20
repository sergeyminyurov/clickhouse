using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using ClickHouse.Ado;
using ClickHouse.Net;
using System.Data;

namespace ClickHouse
{
    public static class ClickHouseExtensions
    {
        #region ConnectionString
        public static string GetDatabaseName(this string connectionString)
        {
            int startIndex = connectionString.ToLower().IndexOf("database");
            if (startIndex < 0)
                return null;
            startIndex += 9;
            int endIndex = connectionString.IndexOf(";", startIndex);
            return endIndex > 0
                ? connectionString.Substring(startIndex, endIndex - startIndex)
                : connectionString.Substring(startIndex);
        }
        public static string GetConnectionWithoutDb(this string connectionString)
        {
            int startIndex = connectionString.ToLower().IndexOf("database");
            if (startIndex < 0)
                return connectionString;
            int endIndex = connectionString.IndexOf(";", startIndex);
            if (endIndex <= 0 || endIndex == connectionString.Length - 1)
                return connectionString.Substring(0, startIndex);
            return connectionString.Substring(0, startIndex) + connectionString.Substring(endIndex + 1);
        }
        public static string GetConnectionWithoutDb(this ClickHouseConnectionSettings settings) =>
            $"Compress={settings.Compress};CheckCompressedHash={settings.CheckCompressedHash};Compressor={settings.Compressor};Host={settings.Host};Port={settings.Port};User={settings.User};Password={settings.Password};SocketTimeout={settings.SocketTimeout};";
        public static string GetClusterName(this ClickHouseConnectionSettings settings)
        {
            string commandText = $"SELECT cluster FROM system.clusters where is_local = 1 and host_name = '{settings.Host}' LIMIT 1";
            using (ClickHouseConnection conn = new ClickHouseConnection(settings.GetConnectionWithoutDb()))
            {
                conn.Open();
                return conn.Scalar<string>(commandText);
            }
        }
        public static IEnumerable<string> GetDatabaseList(this ClickHouseConnectionSettings settings)
        {
            string commandText = "SELECT name FROM system.databases";
            ProxyLog.Info(commandText);
            using (ClickHouseConnection conn = new ClickHouseConnection(settings))
            {
                conn.Open();
                using (ClickHouseCommand cmd = new ClickHouseCommand(conn, commandText))
                {
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return reader.GetString(0);
                        }
                        ProxyLog.Info($"{reader.RecordsAffected} row(s)");
                    }
                }
            }
        }
        public static bool CheckConnection(this ClickHouseConnectionSettings settings)
        {
            try
            {
                using (ClickHouseConnection conn = new ClickHouseConnection(settings))
                {
                    conn.Open();
                    return true;
                }
            }
            catch { return false; }
        }
        //public static string GetServerConnectionString(string host, string port = "9000", string user = "default", string password = "") =>
        //    $"Host={host};Port={port};User={user};Password={password};";
        #endregion

        #region ExecuteNonQuery
        public static int Execute(this ClickHouseConnection conn, string commandText)
        {
            ProxyLog.Info(commandText);
            using (ClickHouseCommand cmd = new ClickHouseCommand(conn, commandText))
            {
                int rowsAffected = cmd.ExecuteNonQuery();
                if (rowsAffected > 0)
                {
                    ProxyLog.Info($"{rowsAffected} row(s)");
                }
                return rowsAffected;
            }
        }
        #endregion

        #region ExecuteScalarQuery
        public static T Scalar<T>(this ClickHouseConnection conn, string commandText)
        {
            ProxyLog.Info(commandText);
            using (ClickHouseCommand cmd = new ClickHouseCommand(conn, commandText))
            {
                T result = (T)cmd.ExecuteScalar();
                ProxyLog.Info($"Result: {result}");
                return result;
            }
        }
        public static T GetMaxValue<T>(this ClickHouseConnection conn, string tableName, string columnName, string where = null)
        {
            string commandText = $"SELECT MAX({columnName}) FROM {tableName}"
                + $"{(!string.IsNullOrWhiteSpace(where) ? " WHERE " + where : "")}";
            T maxValue = conn.Scalar<T>(commandText);
            return maxValue;
        }
        public static ulong GetRowCount(this ClickHouseConnection conn, string tableName, string where = null)
        {
            string commandText = $"SELECT COUNT() FROM {tableName}"
                + $"{(!string.IsNullOrWhiteSpace(where) ? " WHERE " + where : "")}";
            ulong rowCount = conn.Scalar<ulong>(commandText);
            return rowCount;
        }
        #endregion 

        #region Read
        public static IEnumerable<T> Read<T>(this ClickHouseDatabase db, string commandText) where T : new()
        {
            ProxyLog.Info(commandText);
            var entities = db.ExecuteQueryMapping<T>(commandText);
            ProxyLog.Info($"{entities.Count()} row(s)");
            return entities;
        }
        public static ClickHouseDatabase CreateDatabase(this ClickHouseConnection conn) =>
            new ClickHouseDatabase(
                new ClickHouseConnectionSettings(conn.ConnectionString),
                new ClickHouseCommandFormatter(),
                new ClickHouseConnectionFactory(),
                null,
                new DefaultPropertyBinder());
        public static IEnumerable<T> Read<T>(this ClickHouseConnection conn, string commandText) where T : new()
        {
            using (ClickHouseDatabase db = conn.CreateDatabase())
            {
                return db.Read<T>(commandText);
            }
        }
        #endregion

        #region Insert
        public static void Insert(this ClickHouseConnection conn, string tableName, params ColumnValue[] values)
        {
            string commandText = $"INSERT INTO {tableName}({string.Join(",", values.Select(t => t.Name).ToArray())}) VALUES ({string.Join(",", values.Select(t => t.Value.AsClickValue()).ToArray())})";
            conn.Execute(commandText);
        }
        public static void Insert<T>(this ClickHouseConnection conn, ClickHouseTable table, params T[] entities)
        {
            BatchData<T> data = new BatchData<T>(entities);
            string cmdText = $"INSERT INTO {(table.CommandText.IsTemporary ? "" : conn.ConnectionSettings.Database + ".")}{table.Name} ({BatchData<T>.GetFieldNames()}) VALUES @bulk";
            ProxyLog.Info(cmdText);
            using (var command = conn.CreateCommand(cmdText))
            {
                command.Parameters.Add(new ClickHouseParameter
                {
                    ParameterName = "bulk",
                    Value = data
                });
                int rowsAffected = command.ExecuteNonQuery();
                if (rowsAffected == 0)
                    rowsAffected = data.Count();
                ProxyLog.Info($"{rowsAffected} row(s)");
            }
        }

        #endregion

        #region Update
        public static int Update(this ClickHouseConnection conn, string tableName, string where, params ColumnValue[] values)
        {
            string commandText = $"ALTER TABLE {tableName} "
                + $"UPDATE {string.Join(",", values.Select(t => $"{t.Name}={t.Value.AsClickValue()}").ToArray())} WHERE {where}";
            return conn.Execute(commandText);
        }
        #endregion

        #region Delete
        public static int Delete(this ClickHouseConnection conn, string tableName, string where)
        {
            string commandText = $"ALTER TABLE {tableName} DELETE WHERE {where}";
            return conn.Execute(commandText);
        }
        #endregion

        #region Common
        // https://clickhouse.yandex/docs/en/query_language/functions/date_time_functions/
        // toDateTime('2019-01-01 00:00:00')
        public static string AsClickValue(this object value)
        {
            if (value == null)
                return "NULL";
            if (value is String || value is Guid)
                return $"'{value}'";
            if (value is DateTime)
                return $"toDateTime('{((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss")}')";
            return value.ToString();
        }
        #endregion
    }
}