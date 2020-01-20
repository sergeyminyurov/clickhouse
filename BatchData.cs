using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ClickHouse
{
    public class BatchData : List<BatchRow>
    {
        public void AddRow(params object[] values) => base.Add(new BatchRow(values));

        public void RemoveColumn(int columnIndex) => ForEach(t => t.Values.RemoveAt(columnIndex));
    }
    public class BatchRow : IEnumerable
    {
        public ArrayList Values { get; }
        public BatchRow(params object[] values) => Values = new ArrayList(values);
        public IEnumerator GetEnumerator()
        {
            for (int i = 0; i < Values.Count; i++)
                yield return Values[i];
        }
    }
    public class BatchData<T> : BatchData, IEnumerable
    {
        public BatchData(IEnumerable<T> entities) 
        {
            foreach(T entity in entities)
                AddRow(GetValues(entity));
        }

        static (PropertyInfo Property, Func<object, object> Convert)[] __properties;
        static BatchData()
        {
            var props = typeof(T).GetProperties();
            __properties = new (PropertyInfo Property, Func<object, object> Read)[props.Length];
            for (int i = 0; i < props.Length; i++)
            {
                PropertyInfo prop = props[i];
                __properties[i].Property = prop;
                if (prop.PropertyType.IsEnum)
                    __properties[i].Convert = (v) => v.ToString();
                else if(prop.PropertyType.Equals(typeof(Boolean)))
                    __properties[i].Convert = (v) => (byte)((bool)v ? 1 : 0);
            }
        }

        static object[] GetValues(T entity)
        {
            object[] values = new object[__properties.Length];
            for (int i = 0; i < __properties.Length; i++)
            {
                PropertyInfo prop = __properties[i].Property;
                object val = prop.GetValue(entity);
                if (val != null)
                {
                    if (__properties[i].Convert != null)
                        values[i] = __properties[i].Convert(val);
                    else
                        values[i] = val;
                }
            }
            return values;
        }
        public static string GetFieldNames() => string.Join(",", __properties.Select(t => t.Property.Name));
    }
}