using System;

namespace ClickHouse
{
    [AttributeUsage(AttributeTargets.Property)]
    public class ReferenceAttribute : Attribute 
    {
        public Type EntityType { get; }
        public bool Permanent { get; }
        public ReferenceAttribute(Type entityType, bool permanent = false) 
        { 
            EntityType = entityType;
            Permanent = permanent;
        }
    }
}