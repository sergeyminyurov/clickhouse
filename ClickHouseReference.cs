using System;

namespace ClickHouse
{
    public class ClickHouseReference
    {
        public ClickHouseColumn ForeignKey { get; }
        public Type EntityType { get; }
        public bool Permanent { get; }
        public ClickHouseReference(ClickHouseColumn foreignKey, Type entityType, bool permanent) 
        {
            ForeignKey = foreignKey;
            EntityType = entityType;
            Permanent = permanent;
        }
        public override string ToString() => $"{ForeignKey.Name}: {EntityType.Name}";
    }
}