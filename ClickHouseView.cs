using ClickHouse.Ado;
using ClickHouse.CommandText;
using ClickHouse.TableEngines;

namespace ClickHouse
{
    public class ClickHouseView : ClickHouseObject<ViewCommandText>
    {
        public ClickHouseTable Table { get; }
        public string ViewName { get; }
        public ClickHouseView(string viewName, ClickHouseTable table, bool addJoinsByReferences = false
            , ITableEngine engine = null, bool hideForeignKeys = false)
        {
            Table = table;
            ViewName = viewName;
            CommandText = new ViewCommandText(viewName, table, addJoinsByReferences:addJoinsByReferences
                , engine:engine, hideForeignKeys:hideForeignKeys);
        }
        public void Create(ClickHouseConnection conn, string db, bool ifNotExists = false) =>
            conn.Execute(CommandText.CreateView(db, ifNotExists:ifNotExists));

        public void Drop(ClickHouseConnection conn, string db, bool ifExists = false) =>
            conn.Execute(CommandText.DropView(db, ifExists:ifExists));
    }
}