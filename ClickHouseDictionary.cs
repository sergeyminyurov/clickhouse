using ClickHouse.Ado;
using ClickHouse.CommandText;
using System.Linq;

namespace ClickHouse
{
    public class ClickHouseDictionary : ClickHouseObject<DictionaryCommandText>
    {
        public ClickHouseDictionary(DictionaryCommandText commandText)
        {
            CommandText = commandText;
        }

        public void Create(ClickHouseConnection conn, string db, bool ifNotExists = false) =>
            conn.Execute(CommandText.CreateDictionary(db:db, ifNotExists: ifNotExists));

        public void Drop(ClickHouseConnection conn, string db, bool ifExists = false) =>
            conn.Execute(CommandText.DropDictionary(db:db, ifExists: ifExists));
    }
    public class ClickHouseDictionary<T> : ClickHouseDictionary
    {
        public ClickHouseDictionary(ClickHouseSchema schema, string dicName, string connString, string tableName, (int Min, int Max) lifetime, SimpleDictionaryLayout layout = SimpleDictionaryLayout.Hashed)
            : base(new DictionaryCommandText(schema, dicName, new DictionarySourceOdbc(connString, tableName), layout, lifetime, GetColumns()))
        { }
        internal static DictionaryColumn[] GetColumns() => typeof(T).GetProperties().Select(t => DictionaryColumn.Create(t)).ToArray();
    }
}