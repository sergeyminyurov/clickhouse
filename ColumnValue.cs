using System.Collections.Generic;

namespace ClickHouse
{
    public enum СomparisonOperator
    {
        Equal,
        NotEqual,
        More,
        Less,
        MoreOrEqual,
        LessOrEqual
    }
    public class ColumnValue
    {
        protected static Dictionary<СomparisonOperator, string> __operatorMap = new Dictionary<СomparisonOperator, string>
        {
            [СomparisonOperator.Equal] = "=",
            [СomparisonOperator.NotEqual] = "<>",
            [СomparisonOperator.More] = ">",
            [СomparisonOperator.Less] = "<",
            [СomparisonOperator.MoreOrEqual] = ">=",
            [СomparisonOperator.LessOrEqual] = "<=",
        };
        public string Name { get; }
        public object Value { get; set; }
        public ColumnValue(string name, object value)
        {
            Name = name;
            Value = value;
        }
        public override string ToString() =>$"{Name} = {Value.AsClickValue()}";
        public string ToString(СomparisonOperator op) => $"{Name}{__operatorMap[op]}{Value.AsClickValue()}";
    }
    public interface ILogicalExpression
    {
        string Text { get; }
    }
    public class ColumnComparison : ColumnValue, ILogicalExpression
    {
        public СomparisonOperator Operator { get; set; }
        public string Text => ValueIsColumn
            ? $"{Name}{__operatorMap[Operator]}{Value}"
            : base.ToString(Operator);
        public ColumnComparison(string name, object value, СomparisonOperator op) : base(name, value)
        {
            Operator = op;
        }
        public readonly bool ValueIsColumn;
        public ColumnComparison(string name, СomparisonOperator op, string valueColumn) : base(name, valueColumn)
        {
            Operator = op;
            ValueIsColumn = true;
        }
    }
}