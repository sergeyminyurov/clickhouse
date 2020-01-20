namespace ClickHouse
{
    public class SelectResultColumn
    {
        public string PropertyName { get; }
        public string Alias { get; set; }
        public string ColumnExpression { get; set; }
        public IFromExpression FromExpression { get; }
        public int PrefixIndex { get; }
        public SelectResultColumn(IFromExpression fromExpr, string propName, int prefixIndex, string alias = null, string colExpr = null)
        {
            FromExpression = fromExpr;
            PropertyName = propName;
            PrefixIndex = prefixIndex;
            Alias = alias;
            ColumnExpression = colExpr;
        }
        public override string ToString() => 
            $"{(string.IsNullOrWhiteSpace(ColumnExpression) ? $"t{PrefixIndex}.{PropertyName}" : ColumnExpression)}{(!string.IsNullOrWhiteSpace(Alias) ? $" AS {Alias}" : "")}";
    }
}