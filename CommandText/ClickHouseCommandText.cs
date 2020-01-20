namespace ClickHouse.CommandText
{
    public class ClickHouseCommandText
    {
        public ClickHouseSchema Schema { get; }
        public ClickHouseCommandText(ClickHouseSchema schema) 
        { 
            Schema = schema;
        }
    }
}