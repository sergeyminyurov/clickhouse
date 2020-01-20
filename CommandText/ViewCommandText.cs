using ClickHouse.TableEngines;

namespace ClickHouse.CommandText
{
    public class ViewCommandText : SelectCommandText
    {
        public string ViewName { get; }
        public ITableEngine Engine { get; }
        public bool IsMaterialized => Engine != null;
        public ViewCommandText(ClickHouseSchema schema, string viewName, IFromExpression fromExpr, ITableEngine engine = null, params (string Name, string Alias)[] columns) 
            : base(schema, fromExpr, columns:columns)
        {
            ViewName = viewName;
            Engine = engine;
        }
        public ViewCommandText(string viewName, ClickHouseTable table, bool addJoinsByReferences = false
            , ITableEngine engine = null, bool hideForeignKeys = false) 
            : base(table, addJoinsByReferences:addJoinsByReferences, hideForeignKeys:hideForeignKeys) 
        {
            ViewName = viewName;
            Engine = engine;
        }

        #region Create View
        public static string CreateView(string db, string view, SelectCommandText commandText, ITableEngine engine = null, bool ifNotExists = false) =>
            $"CREATE {(engine != null ? "MATERIALIZED " : "")}VIEW " 
                + $"{(ifNotExists ? "IF NOT EXISTS " : "")}{db}.{view} AS {commandText.Select(db)}"
                + $"{(engine != null ? $" ENGINE={engine.Text}" : "")}";
        public string CreateView(string db, bool ifNotExists = false) => 
            CreateView(db, view:ViewName, commandText:this, engine:Engine, ifNotExists:ifNotExists);
        #endregion

        #region Drop View
        // ClickHouse Feature (Bug): DROP VIEW isn't works
        public static string DropView(string db, string view, bool ifExists = false) => 
            $"DROP TABLE {(ifExists ? "IF EXISTS " : "")} {db}.{view}";
        public string DropView(string db, bool ifExists = false) => DropView(db, ViewName, ifExists:ifExists);
        #endregion
    }
}