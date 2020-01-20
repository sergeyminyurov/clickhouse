using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Reflection;
using System.Text;

namespace ClickHouse
{
    // https://clickhouse.yandex/docs/en/data_types/
    // https://clickhouse.yandex/docs/en/data_types/domains/ipv4/
    // https://www.altinity.com/blog/introducing-clickhouse-ipv4-and-ipv6-domains-for-ip-address-handling
    public enum ClickHouseValueType
    {
        Int8, Int16, Int32, Int64,
        UInt8, UInt16, UInt32, UInt64,
        Float32, Float64,
        Decimal, Decimal32, Decimal64, Decimal128,
        String, FixedString,
        UUID,
        Date, DateTime,
        Enum8 /*as Int8*/, Enum16 /*as Int16*/,
        IPv4,
    }
    [AttributeUsage(AttributeTargets.Property)]
    public class LowCardinalityAttribute : Attribute { }

    public enum ColumnMappingSide { Both, OnlySource, OnlyTarget }
    public class ClickHouseColumn
    {
        public string Name { get; set; }
        public ClickHouseValueType ValueType { get; set; }
        public bool IsNullable { get; set; }
        public bool IsArray { get; set; }
        // https://www.altinity.com/blog/2019/3/27/low-cardinality
        public bool IsLowCardinality { get; }
        public int? FixedStringLength { get; set; }
        public ColumnMappingSide MappingSide { get; set; }
        public string DefaultExpression { get; set; }
        public bool IsDefault => !string.IsNullOrWhiteSpace(DefaultExpression);

        public ClickHouseColumn(string name, ClickHouseValueType valueType, bool isNullable = false, bool isArray = false, int? fixedStringLength = null, ColumnMappingSide mappingSide = ColumnMappingSide.Both)
        {
            Name = name;
            ValueType = valueType;
            IsNullable = isNullable;
            IsArray = isArray;
            FixedStringLength = fixedStringLength;
            MappingSide = mappingSide;
        }
        public ClickHouseColumn(string name, Type valueType, ColumnMappingSide mappingSide = ColumnMappingSide.Both)
        {
            Name = name;
            ValueType = MapType(valueType, out bool isNullable, out bool isArray);
            IsNullable = isNullable;
            IsArray = isArray;
            MappingSide = mappingSide;
        }
        public ClickHouseColumn(string name, bool isRowCardinality)
        {
            Name = name;
            ValueType = ClickHouseValueType.String;
            IsLowCardinality = isRowCardinality;
            MappingSide = ColumnMappingSide.Both;
        }
        public static ClickHouseColumn Create(PropertyInfo property)
        {
            ColumnAttribute attr = (ColumnAttribute)property.GetCustomAttribute(typeof(ColumnAttribute));
            string columnName = attr != null ? attr.Name : property.Name;
            if (property.PropertyType.Equals(typeof(string)))
            {
                if (property.GetAttribute<LowCardinalityAttribute>() != null)
                    return new ClickHouseColumn(columnName, isRowCardinality:true);
            }
            return new ClickHouseColumn(columnName, property.PropertyType);
        }
        public static string ToString(string columnName, ClickHouseValueType valueType, bool isNullable = false, bool isLowCardinality = false
            , bool isArray = false, int? fixedStringLength = null, string defaultExpression = null)
        {
            string typeName;
            if (valueType == ClickHouseValueType.String)
            {
                typeName = isLowCardinality
                    ? $"LowCardinality({valueType})"
                    : valueType.ToString();

            }
            else if (valueType == ClickHouseValueType.FixedString)
            {
                if (fixedStringLength == null)
                    throw new ArgumentNullException(nameof(fixedStringLength));
                typeName = $"{valueType}({fixedStringLength})";
            }
            else
                typeName = valueType.ToString();

            if (isNullable)
                typeName = $"Nullable({typeName})";
            if (isArray)
                typeName = $"Array({typeName})";
            return $"{columnName} {typeName}"
                + (!string.IsNullOrWhiteSpace(defaultExpression) ? $" DEFAULT {defaultExpression}" : "");
        }
        static Dictionary<Type, ClickHouseValueType> __typeMap;
        static ClickHouseColumn()
        {
            __typeMap = new Dictionary<Type, ClickHouseValueType>
            {
                [typeof(sbyte)] = ClickHouseValueType.Int8,
                [typeof(short)] = ClickHouseValueType.Int16,
                [typeof(int)] = ClickHouseValueType.Int32,
                [typeof(long)] = ClickHouseValueType.Int64,
                [typeof(bool)] = ClickHouseValueType.UInt8,
                [typeof(byte)] = ClickHouseValueType.UInt8,
                [typeof(UInt16)] = ClickHouseValueType.UInt16,
                [typeof(UInt32)] = ClickHouseValueType.UInt32,
                [typeof(UInt64)] = ClickHouseValueType.UInt64,
                [typeof(Guid)] = ClickHouseValueType.UUID,
                [typeof(float)] = ClickHouseValueType.Float32,
                [typeof(double)] = ClickHouseValueType.Float64,
                [typeof(string)] = ClickHouseValueType.String,
                [typeof(DateTime)] = ClickHouseValueType.DateTime,
            };
        }
        public static ClickHouseValueType MapType(Type valueType, out bool isNullable, out bool isArray)
        {
            isNullable = false;
            isArray = false;
            Type elementType = valueType.GetElementType();
            if (elementType != null)
            {
                isArray = true;
                valueType = elementType;
            }
            Type underlyingType = Nullable.GetUnderlyingType(valueType);
            if (underlyingType != null)
            {
                isNullable = true;
                valueType = underlyingType;
            }
            if (valueType.IsEnum)
                valueType = typeof(String);

#if DEBUG
            try
            {
#endif
                return __typeMap[valueType];
#if DEBUG
            }
            catch (KeyNotFoundException ex)
            {
                Debug.WriteLine($"{nameof(ClickHouseColumn)}.{nameof(MapType)}: {nameof(KeyNotFoundException)} - {valueType}");
                throw ex;
            }
#endif
        }
        public override string ToString() => 
            ToString(columnName: Name, valueType: ValueType, isNullable: IsNullable, isLowCardinality:IsLowCardinality
                , isArray: IsArray, fixedStringLength: FixedStringLength, defaultExpression: DefaultExpression);
    }
}