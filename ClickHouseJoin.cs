using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse
{
    public enum JoinType { Inner, Left, Right, Full, Cross }
    public class ClickHouseJoin
    {
        public IFromExpression FromExpression { get; }
        public JoinType JoinType { get; }
        public ILogicalExpression JoinExpression { get; }
        public List<SelectResultColumn> ResultColumns { get; }

        public ClickHouseJoin(IFromExpression fromExpr, ILogicalExpression joinExpr, JoinType joinType = JoinType.Inner)
        {
            FromExpression = fromExpr;
            JoinType = joinType;
            JoinExpression = joinExpr;
            ResultColumns = new List<SelectResultColumn>();
        }
        public string ToString(string db, int index) => $"{JoinType.ToString().ToUpper()} JOIN {FromExpression.GetText(db)} AS t{index} ON {JoinExpression.Text}";
    }
}