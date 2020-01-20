using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ClickHouse.CommandText
{
    public enum SimpleDictionaryLayout
    {
        Flat, Hashed, Sparse_Hashed, Complex_Key_Hashed
    }
    public class DictionaryColumn
    {
        public string Name { get; set; }
        public ClickHouseValueType ValueType { get; set; }
        public bool IsNullable { get; set; }
        public bool IsArray { get; set; }
        public bool IsKey { get; set; }
        public static DictionaryColumn Create(PropertyInfo prop)
        {
            DictionaryColumn column = new DictionaryColumn
            {
                Name = prop.Name
            };
            column.ValueType = ClickHouseColumn.MapType(prop.PropertyType, out bool isNullable, out bool isArray);
            column.IsNullable = isNullable;
            column.IsArray = isArray;
            return column;
        }
    }
    public class DictionaryCommandText : ClickHouseCommandText
    {
        public string DictionaryName { get; }
        public IDictionarySource Source { get; }
        public SimpleDictionaryLayout Layout { get; }
        public (int Min, int Max) Lifetime { get; }
        public List<DictionaryColumn> Columns { get; }
        public DictionaryCommandText(ClickHouseSchema schema, string dicName, IDictionarySource source
            , SimpleDictionaryLayout layout, (int Min, int Max) lifetime, params DictionaryColumn[] columns) 
            : base(schema) 
        {
            DictionaryName = dicName;
            Source = source;
            Layout = layout;
            Lifetime = lifetime;
            Columns = new List<DictionaryColumn>(columns);
        }

        public static string CreateDictionary(string db, string dic, IDictionarySource source, SimpleDictionaryLayout layout
            , (int Min, int Max) lifetime, bool ifNotExists = false, params DictionaryColumn[] columns) => 
            $"CREATE DICTIONARY {(ifNotExists ? "IF NOT EXISTS " : "")}{db}.{dic} " 
                + $"({string.Join(",", columns.Select(t => $"{t.Name} {t.ValueType}"))}) " 
                + $"PRIMARY KEY {string.Join(",", columns.Where(t => t.IsKey).Select(t => $"{t.Name}"))} " 
                + $"{source.Expression} LAYOUT ({layout.ToString().ToUpper()}()) "
                + $"LIFETIME ({(lifetime.Max > 0 ? $"MIN {lifetime.Min} MAX {lifetime.Max}" : $"{lifetime.Min}")})";
        public string CreateDictionary(string db, bool ifNotExists = false) => 
            CreateDictionary(db:db, dic:DictionaryName, source:Source, layout:Layout, lifetime:Lifetime, ifNotExists:ifNotExists, columns:Columns.ToArray());

        public static string DropDictionary(string db, string dic, bool ifExists = false) => 
            $"DROP DICTIONARY {(ifExists ? "IF EXISTS " : "")}{db}.{dic}";
        public string DropDictionary(string db, bool ifExists = false) => DropDictionary(db:db, dic:DictionaryName, ifExists:ifExists);
    }
    /* https://clickhouse.yandex/docs/en/query_language/create/#create-dictionary-query
     * https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_structure/
     * https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_layout/
     * https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_lifetime/
    CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name
    (
        key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
        key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
        attr1 type2 [DEFAULT|EXPRESSION expr3],
        attr2 type2 [DEFAULT|EXPRESSION expr4]
    )
    PRIMARY KEY key1, key2
    SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
    LAYOUT(LAYOUT_NAME([param_name param_value]))
    LIFETIME([MIN val1] MAX val2)
    */
}