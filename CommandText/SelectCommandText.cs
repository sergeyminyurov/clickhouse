using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace ClickHouse.CommandText
{
    public class SelectCommandText : ClickHouseCommandText, IFromExpression
    {
        public IFromExpression FromExpression { get; }
        string IFromExpression.GetText(string db) => Select(db);

        public string Where { get; set; }
        public string OrderBy { get; set; }
        public string With { get; set; }
        public string LimitBy { get; set; }
        public int? Limit { get; set; }
        public bool Distinct { get; set; }
        public List<SelectResultColumn> ResultColumns { get; }
        public IEnumerable<SelectResultColumn> AllColumns
        {
            get
            {
                foreach (SelectResultColumn column in ResultColumns)
                    yield return column;
                foreach (ClickHouseJoin join in Joins)
                    foreach (SelectResultColumn column in join.ResultColumns)
                        yield return column;
            }
        }

        private List<ClickHouseJoin> _joins;
        public IEnumerable<ClickHouseJoin> Joins => _joins.AsReadOnly();

        public SelectCommandText(ClickHouseSchema schema, IFromExpression fromExpr, params (string Name, string Alias)[] columns)
            : base(schema)
        {
            FromExpression = fromExpr;
            ResultColumns = new List<SelectResultColumn>();
            if (columns != null && columns.Length > 0)
                ResultColumns.AddRange(columns.Select(t => new SelectResultColumn(this, t.Name, 0, alias: t.Alias)));
            _joins = new List<ClickHouseJoin>();
        }
        public SelectCommandText(ClickHouseTable table, bool addJoinsByReferences = false, bool hideForeignKeys = false)
            : this(table.CommandText.Schema, table.CommandText, columns:table.Columns.Select(t => (t.Name, t.Name)).ToArray())
        {
            if (hideForeignKeys)
            {
                foreach (ClickHouseReference reference in table.References)
                {
                    SelectResultColumn column = ResultColumns.First(t => t.PropertyName == reference.ForeignKey.Name);
                    ResultColumns.Remove(column);
                }
            }
            if (addJoinsByReferences)
                AddJoinsByReferences(table);
        }
        public ClickHouseJoin AddJoin(ClickHouseTable table, ILogicalExpression joinExpr, JoinType joinType
            , params (string Name, string Alias)[] columns)
        {
            ClickHouseJoin join = AddJoin(table.CommandText, joinExpr, joinType, columns);
            return join;
        }
        private void AddJoinsByReferences(ClickHouseTable table)
        {
            int joinIndex = 0;
            for (int refIndex = 0; refIndex < table.References.Count; refIndex++)
            {
                var reference = table.References[refIndex];

                var referencedTable = Schema[reference.EntityType];
                if (referencedTable.IdentityColumn == null)
                {
                    ProxyLog.Warning($"{nameof(SelectCommandText)}.{nameof(AddJoinsByReferences)}: {nameof(ClickHouseTable.IdentityColumn)} for table '{referencedTable.Name}' is undefined.");
                    continue;
                }
                string[] columnNames = Schema.GetDefaultColumns(reference.EntityType);
                AddJoin(referencedTable
                    , joinExpr: new ColumnComparison($"t0.{reference.ForeignKey.Name}", СomparisonOperator.Equal, $"t{++joinIndex}.{referencedTable.IdentityColumn.Name}")
                    , joinType: reference.Permanent ? JoinType.Inner : JoinType.Left
                    , columns: columnNames.Select(name => (name, $"{reference.ForeignKey.Name}_{name}")).ToArray());
            }
        }
        private void ResetEnums(Type entityType, List<SelectResultColumn> columns, int prefixIndex)
        {
            foreach (PropertyInfo propInfo in entityType.GetProperties())
            {
                Type enumType = null;
                if (propInfo.PropertyType.IsEnum)
                    enumType = propInfo.PropertyType;
                else
                {
                    ReferenceAttribute refAttr = propInfo.GetAttribute<ReferenceAttribute>();
                    if (refAttr != null && refAttr.EntityType.IsEnum)
                        enumType = refAttr.EntityType;
                }
            }
        }
        public ClickHouseJoin AddJoin(IFromExpression fromExpr, ILogicalExpression joinExpr, JoinType joinType, params (string Name, string Alias)[] columns)
        {
            ClickHouseJoin join = new ClickHouseJoin(fromExpr, joinExpr, joinType);
            _joins.Add(join);
            if (columns != null && columns.Length > 0)
            {
                for (int i = 0; i < columns.Length; i++)
                    join.ResultColumns.Add(new SelectResultColumn(fromExpr, columns[i].Name, _joins.Count, alias: columns[i].Alias));
            }
            return join;
        }

        #region Select
        // https://clickhouse.yandex/docs/en/query_language/select/
        public static string Select(string db, string from, string columns = null, bool distinct = false, string with = null
            , string where = null, string orderBy = null, string limitBy = null, int? limit = null, params ClickHouseJoin[] joins)
        {
            List<string> joinText = new List<string>();
            if (joins != null && joins.Length > 0)
            {
                for (int i = 0; i < joins.Length; i++)
                    joinText.Add(joins[i].ToString(db, i + 1));
            }
            string commandText = $"{(!string.IsNullOrWhiteSpace(with) ? "WITH " + with + " " : "")}"
                + $"SELECT {(distinct ? "DISTINCT " : "")}"
                + $"{(!string.IsNullOrWhiteSpace(columns) ? columns : "*")} FROM {from}"
                + $"{(joins != null && joins.Length > 0 ? " " + string.Join(" ", joinText) : "")}"
                + $"{(!string.IsNullOrWhiteSpace(where) ? " WHERE " + where : "")}"
                + $"{(!string.IsNullOrWhiteSpace(orderBy) ? " ORDER BY " + orderBy : "")}"
                + $"{(!string.IsNullOrWhiteSpace(limitBy) ? " LIMIT " + limitBy : "")}"
                + $"{(limit != null ? " LIMIT " + limit.ToString() : "")}";
            return commandText;
        }
        public static string Select(string db, IFromExpression fromExpr, string where = null) =>
            Select(db, from:fromExpr.GetText(db), where:where);
        private string GetColumnNames() => AllColumns.Count() > 0 ? string.Join(",", AllColumns.Select(t => t.ToString())) : "*";
        private string GetFromText(string db) => $"{FromExpression.GetText(db)} AS t0";
        public string Select(string db) => Select(db, from:GetFromText(db), columns:GetColumnNames(), distinct:Distinct
            , with:With, where:Where, orderBy:OrderBy, limitBy:LimitBy, limit:Limit, joins:_joins.ToArray());
        #endregion
    }
}