using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using ClickHouse.Ado;
using ClickHouse.CommandText;
using ClickHouse.TableEngines;

namespace ClickHouse
{
    public class ClickHouseTable : ClickHouseObject<TableCommandText>
    {
        public virtual Type EntityType { get; }
        public string Name { get; set; }
        public List<ClickHouseColumn> Columns { get; set; }
        public ClickHouseColumn IdentityColumn { get; set; }
        public string GetColumnsWithTypes() => string.Join(",", Columns.Select(t => t.ToString()));
        public List<ClickHouseReference> References { get; }

        public ClickHouseTable(ClickHouseSchema schema, string tableName, string idColumnName = null, bool isTemporary = false, params ClickHouseColumn[] columns)
        {
            Name = tableName;
            Columns = new List<ClickHouseColumn>(columns);
            CommandText = new TableCommandText(schema, tableName, isTemporary);
            References = new List<ClickHouseReference>();
            if (!string.IsNullOrWhiteSpace(idColumnName))
                IdentityColumn = Columns.First(t => t.Name == idColumnName);
        }

        #region Columns
        public int GetColumnIndex(string columnName)
        {
            for (int i = 0; i < Columns.Count; i++)
            {
                if (Columns[i].Name.Equals(columnName, StringComparison.InvariantCultureIgnoreCase))
                    return i;
            }
            return -1;
        }
        public bool HasColumn(string columnName) => 
            Columns.FirstOrDefault(t => t.Name.Equals(columnName, StringComparison.InvariantCultureIgnoreCase)) != null;
        public ClickHouseColumn this[string columnName] => Columns.FirstOrDefault(t => t.Name.Equals(columnName, StringComparison.InvariantCultureIgnoreCase));
        #endregion

        #region DateColumn for MergeTree table engine
        public ClickHouseColumn AddDateColumn(string timeColumnName, string dateColumnName)
        {
            ClickHouseColumn column = new ClickHouseColumn(name: dateColumnName
                , valueType: ClickHouseValueType.Date
                , mappingSide: ColumnMappingSide.OnlyTarget)
            {
                DefaultExpression = $"toDate({timeColumnName})"
            };
            Columns.Add(column);
            return column;
        }
        public ClickHouseColumn GetDateColumn() =>
            Columns.FirstOrDefault(t => t.ValueType == ClickHouseValueType.Date);
        public ClickHouseColumn GetOrAddDateColumn(string timeColumnName, string dateColumnName)
        {
            ClickHouseColumn column = GetDateColumn();
            if (column == null)
                column = AddDateColumn(timeColumnName, dateColumnName);
            return column;
        }
        public string GetColumnNames(bool onlyUpdatable = false) =>
            string.Join(",", Columns.Where(t => !onlyUpdatable || !t.IsDefault).Select(t => t.Name).ToArray());
        #endregion

        #region Helper
        internal static string GetTableName<T>()
        {
            TableAttribute attr = typeof(T).GetAttribute<TableAttribute>();
            if (attr != null)
                return attr.Schema != null ? $"{attr.Schema}.{attr.Name}" : attr.Name;
            return typeof(T).Name;
        }
        internal static ClickHouseColumn[] GetColumns<T>() => typeof(T).GetProperties().Select(t => ClickHouseColumn.Create(t)).ToArray();
        #endregion

        #region Commands
        public void Create(ClickHouseConnection conn, ITableEngine engine, string db, bool ifNotExists = false, string cluster = null) =>
            conn.Execute(CommandText.CreateTable(GetColumnsWithTypes(), engine:engine, db:db, ifNotExists: ifNotExists, cluster:cluster));

        public void Drop(ClickHouseConnection conn, string db, bool ifExists = false, string cluster = null) =>
            conn.Execute(CommandText.DropTable(db:db, ifExists: ifExists));

        public void Truncate(ClickHouseConnection conn, string db, bool ifExists = false, string cluster = null) =>
            conn.Execute(CommandText.TruncateTable(db:db, ifExists:ifExists, cluster:cluster));
        #endregion

        public override string ToString() => $"{GetType().Name}: {Name}";
    }
    public class ClickHouseTable<T> : ClickHouseTable
    {
        public override Type EntityType => typeof(T);
        public ClickHouseTable(ClickHouseSchema schema, string tableName, string idColumnName = null, bool isTemporary = false) 
            : base(schema, tableName, idColumnName: idColumnName, isTemporary: isTemporary, columns:GetColumns<T>()) 
        {
            References.AddRange(GetReferences());
        }
        public ClickHouseColumn GetColumn(Expression<Func<T, object>> expression) => this[expression.GetMemberName()];
        internal IEnumerable<ClickHouseReference> GetReferences()
        {
            foreach(var prop in typeof(T).GetProperties())
            {
                var refAttr = prop.GetAttribute<ReferenceAttribute>();
                if (refAttr != null)
                {
                    if (!refAttr.EntityType.IsEnum)
                        yield return new ClickHouseReference(this[prop.Name], refAttr.EntityType, refAttr.Permanent);
                }
                //else
                //{
                //    var dicAttr = prop.GetAttribute<DictionaryAttribute>();
                //    if (dicAttr != null)
                //        yield return new ClickHouseReference(this[prop.Name], typeof(DictionaryEntity), dicAttr.Permanent);
                //}
            }
        }
    }
}