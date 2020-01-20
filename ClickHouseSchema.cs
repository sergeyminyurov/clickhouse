using ClickHouse.TableEngines;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace ClickHouse
{
    public abstract class ClickHouseSchema
    {
        public ClickHouseSchema(bool defineObjects = true)
        {
            if (defineObjects)
                DefineObjects();
        }

        protected void DefineObjects()
        {
            DefineTables();
            DefineDictionaries();
            DefineViews();
        }

        #region Tables
        Dictionary<Type, ClickHouseTable> _tables = new Dictionary<Type, ClickHouseTable>();
        public IEnumerable<Type> EntityTypes => _tables.Keys;
        public IEnumerable<ClickHouseTable> Tables => _tables.Values;
        public ClickHouseTable this[Type entityType]
        {
            get { return _tables[entityType]; }
            set { _tables[entityType] = value; }
        }
        public ClickHouseTable GetTable<T>() => _tables[typeof(T)];
        public void SetTable<T>(ClickHouseTable table) => _tables[typeof(T)] = table;
        public void SetTable<TEntity>(string tableName) =>
            SetTable<TEntity>(new ClickHouseTable<TEntity>(schema:this, tableName:tableName));
        protected abstract void DefineTables();
        #endregion

        #region Views
        public List<ClickHouseView> Views = new List<ClickHouseView>();
        protected virtual void DefineViews() { }
        #endregion

        #region Dictionaries
        public List<ClickHouseDictionary> Dictionaries = new List<ClickHouseDictionary>();
        protected virtual void DefineDictionaries() { }
        #endregion

        #region Default columns for joins
        private Dictionary<Type, string[]> _defaultColumns = new Dictionary<Type, string[]>();
        public void SetDefaultColumns(Type entityType, params string[] columnNames) =>
            _defaultColumns[entityType] = columnNames;
        public void SetDefaultColumns<T>(params Expression<Func<T, object>>[] expressions) =>
            SetDefaultColumns(typeof(T), expressions.GetMemberNames().ToArray());

        public string[] GetDefaultColumns(Type entityType)
        {
            if (_defaultColumns.TryGetValue(entityType, out string[] columns))
                return columns;
            return new string[0];
        }
        public string[] GetDefaultColumns<T>() => GetDefaultColumns(typeof(T));
        #endregion
    }
}