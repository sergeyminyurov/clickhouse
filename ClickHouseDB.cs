using ClickHouse.Ado;
using ClickHouse.CommandText;
using ClickHouse.TableEngines;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouse
{
    public enum ClickHouseNodeMode
    {
        Autonom,
        Replicated,
        Distributed
    }
    [Flags]
    public enum CreateDatabaseMode
    {
        OnlyDatabase = 0,
        Tables = 1,
        Views = 2,
        Dictionaries = 4,
        All = Tables | Views | Dictionaries
    }
    public sealed class CreateDatabaseParameters
    {
        public CreateDatabaseParameters(ClickHouseNodeMode nodeMode, string cluster = null, string distribitedSourceDatabaseName = null)
        {
            NodeMode = nodeMode;
            Cluster = cluster;
            DistribitedSourceDatabaseName = distribitedSourceDatabaseName;
        }
        public ClickHouseNodeMode NodeMode { get; }
        public string Cluster { get; set; }
        public string DistribitedSourceDatabaseName { get; set; }
    }

    public abstract class ClickHouseDB<TSchema> : ClickHouseObject<DatabaseCommandText>
        where TSchema : ClickHouseSchema
    {
        public ClickHouseConnectionSettings ConnectionSettings { get; private set; }
        public TSchema Schema { get; }
        public CreateDatabaseParameters CreateParameters { get; }
        public Dictionary<Type, ITableEngine> TableEngines { get; }

        public ClickHouseDB(TSchema schema, string сonnectionString, CreateDatabaseParameters createParams = null) 
            : this(schema, new ClickHouseConnectionSettings(сonnectionString), createParams) { }
        public ClickHouseDB(TSchema schema, ClickHouseConnectionSettings settings, CreateDatabaseParameters createParams = null)
        {
            ProxyLog.Info($"{GetType().Name}: Database={settings.Database}; Server={settings.Host}:{settings.Port}");
            ConnectionSettings = settings;
            if (createParams == null)
                CreateParameters = new CreateDatabaseParameters(ClickHouseNodeMode.Autonom);
            else
            {
                CreateParameters = createParams;
                if (CreateParameters.NodeMode != ClickHouseNodeMode.Autonom)
                {
                    if (string.IsNullOrWhiteSpace(CreateParameters.Cluster))
                        CreateParameters.Cluster = ConnectionSettings.GetClusterName();
                    if (string.IsNullOrWhiteSpace(CreateParameters.Cluster))
                        throw new ArgumentNullException(nameof(CreateParameters.Cluster));
                }
                if (CreateParameters.NodeMode == ClickHouseNodeMode.Distributed)
                {
                    if (string.IsNullOrWhiteSpace(CreateParameters.DistribitedSourceDatabaseName))
                        CreateParameters.DistribitedSourceDatabaseName = ConnectionSettings.Database;
                }
            }
            Schema = schema;
            CommandText = new DatabaseCommandText(schema);
            TableEngines = new Dictionary<Type, ITableEngine>();
            DefineTableEngines();
        }
        public void Reset(string сonnectionString) => ConnectionSettings = new ClickHouseConnectionSettings(сonnectionString);
        protected abstract void DefineTableEngines();

        #region Database Commands
        public void CreateDatabase(bool ifNotExists = false, CreateDatabaseMode mode = CreateDatabaseMode.All)
        {
            string cluster = CreateParameters.NodeMode == ClickHouseNodeMode.Replicated ? CreateParameters.Cluster : null;
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings.GetConnectionWithoutDb()))
            {
                conn.Open();
                conn.Execute(DatabaseCommandText.CreateDatabase(db: ConnectionSettings.Database, ifNotExists:ifNotExists, cluster:cluster));
                if ((mode & CreateDatabaseMode.Tables) != 0)
                    Schema.Tables.ToList().ForEach(t => t.Create(conn, TableEngines[t.EntityType], db:ConnectionSettings.Database, ifNotExists:ifNotExists, cluster:cluster));
                if ((mode & CreateDatabaseMode.Views) != 0)
                    Schema.Views.ForEach(t => t.Create(conn, ConnectionSettings.Database, ifNotExists: ifNotExists));
                if ((mode & CreateDatabaseMode.Dictionaries) != 0)
                    Schema.Dictionaries.ForEach(t => t.Create(conn, db: ConnectionSettings.Database, ifNotExists: ifNotExists));
            }
        }
        public void DropDatabase(bool ifExists = false)
        {
            string cluster = CreateParameters.NodeMode == ClickHouseNodeMode.Replicated ? CreateParameters.Cluster : null;
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings.GetConnectionWithoutDb()))
            {
                conn.Open();
                conn.Execute(DatabaseCommandText.DropDatabase(db: ConnectionSettings.Database, ifExists:ifExists, cluster:cluster));
            }
        }
        #endregion

        #region Table Commands
        public void CreateTable(ClickHouseTable table, bool ifNotExists = false)
        {
            string cluster = CreateParameters.NodeMode == ClickHouseNodeMode.Replicated ? CreateParameters.Cluster : null;
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                table.Create(conn, TableEngines[table.EntityType], db:ConnectionSettings.Database, ifNotExists:ifNotExists, cluster:cluster);
            }
        }
        public void CreateTable<T>(bool ifNotExists = false) => CreateTable(Schema.GetTable<T>(), ifNotExists:ifNotExists);
        public void DropTable(ClickHouseTable table, bool ifExists = false)
        {
            string cluster = CreateParameters.NodeMode == ClickHouseNodeMode.Replicated ? CreateParameters.Cluster : null;
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                table.Drop(conn, db:ConnectionSettings.Database, ifExists:ifExists, cluster:cluster);
            }
        }
        public void DropTable<T>(bool ifExists = false) => DropTable(Schema.GetTable<T>(), ifExists:ifExists);
        public void TruncateTable(ClickHouseTable table, bool ifExists = false)
        {
            string cluster = CreateParameters.NodeMode == ClickHouseNodeMode.Replicated ? CreateParameters.Cluster : null;
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                table.Truncate(conn, db:ConnectionSettings.Database, ifExists:ifExists, cluster:cluster);
            }
        }
        public void TruncateTable<T>(bool ifExists = false) => TruncateTable(Schema.GetTable<T>(), ifExists:ifExists);
        public void TruncateAllTables(bool ifExists = false)
        {
            string cluster = CreateParameters.NodeMode == ClickHouseNodeMode.Replicated ? CreateParameters.Cluster : null;
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                foreach (ClickHouseTable table in Schema.Tables)
                    table.Truncate(conn, db:ConnectionSettings.Database, ifExists:ifExists, cluster:cluster);
            }
        }
        #endregion

        #region Dictionary Commands
        public void CreateAllDictionaries(bool ifNotExists = false, bool dropIfExists = false)
        {
            if (Schema.Dictionaries.Count == 0)
                return;
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                if (dropIfExists)
                    Schema.Dictionaries.ForEach(dic => dic.Drop(conn, db: ConnectionSettings.Database, ifExists: true));
                Schema.Dictionaries.ForEach(dic => dic.Create(conn, db: ConnectionSettings.Database, ifNotExists: ifNotExists));
            }
        }
        #endregion

        #region View Commands
        public void CreateAllViews(bool ifNotExists = false, bool dropIfExists = false)
        {
            if (Schema.Views.Count == 0)
                return;
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                if (dropIfExists)
                    Schema.Views.ForEach(view => view.Drop(conn, ConnectionSettings.Database, ifExists: true));
                Schema.Views.ForEach(view => view.Create(conn, ConnectionSettings.Database, ifNotExists: ifNotExists));
            }
        }
        #endregion

        #region Read/Modify
        public IEnumerable<T> Read<T>(string where = null) where T : new()
        {
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                return Read<T>(conn, where: where);
            }
        }
        public IEnumerable<T> Read<T>(ClickHouseConnection conn, string where = null) where T : new()
        {
            var table = Schema.GetTable<T>();
            string commandText = SelectCommandText.Select(ConnectionSettings.Database, table.CommandText, where: where);
            return conn.Read<T>(commandText);
        }
        public void Insert<T>(params T[] data) where T : new()
        {
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                conn.Insert<T>(Schema.GetTable<T>(), data);
            }
        }
        public int Update(ClickHouseTable table, string where, params ColumnValue[] values)
        {
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                return conn.Update(table.Name, where: where, values: values);
            }
        }
        public int Update<T>(string where, params ColumnValue[] values) =>
            Update(Schema.GetTable<T>(), where, values);
        public int Delete<T>(string where)
        {
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                return conn.Delete(Schema.GetTable<T>().Name, where: where);
            }
        }
        #endregion

        #region Scalar
        public T Max<T>(ClickHouseTable table, string columnName, string where = null)
        {
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                return conn.GetMaxValue<T>(table.Name, columnName, where: where);
            }
        }
        public ulong RowCount(ClickHouseTable table)
        {
            using (ClickHouseConnection conn = new ClickHouseConnection(ConnectionSettings))
            {
                conn.Open();
                return conn.GetRowCount(table.Name);
            }
        }
        public ulong RowCount<T>() where T : new() => RowCount(Schema.GetTable<T>());
        #endregion
    }
}