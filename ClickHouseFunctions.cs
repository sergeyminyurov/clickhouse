using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace ClickHouse
{
    public static class ClickHouseFunctions
    {
        public static string multiIf(string columnName, Type enumType)
        {
            #region Guard
            if (string.IsNullOrWhiteSpace(columnName))
                throw new ArgumentNullException(nameof(columnName));
            if (enumType == null)
                throw new ArgumentNullException(nameof(enumType));
            if (!enumType.IsEnum)
                throw new ArgumentException($"{nameof(enumType)} must be Enum");
            #endregion

            Array values = Enum.GetValues(enumType);
            Type valueType = Enum.GetUnderlyingType(enumType);
            List<string> conditions = new List<string>();
            for (int i = 0; i < values.Length; i++)
                conditions.Add($"{columnName}={Convert.ChangeType(values.GetValue(i), valueType)},'{values.GetValue(i)}'");
            conditions.Add("NULL");
            return $"multiIf({string.Join(",", conditions)})";
        }

        public static string toInt8(string expr) => $"toInt8({expr})";
        public static string toInt16(string expr) => $"toInt16({expr})";
        public static string toInt32(string expr) => $"toInt32({expr})";
        public static string toInt64(string expr) => $"toInt64({expr})";

        public static string toUInt8(string expr) => $"toUInt8({expr})";
        public static string toUInt16(string expr) => $"toUInt16({expr})";
        public static string toUInt32(string expr) => $"toUInt32({expr})";
        public static string toUInt64(string expr) => $"toUInt64({expr})";

        public static string toDate(string dateColumn) => $"toDate({dateColumn})";
        public static string toDate(DateTime value) => $"toDate('{value.ToString("yyyy-MM-dd")}')";

        public static string toYYYYMM(string dateColumn) => $"toYYYYMM({dateColumn})";
        public static string toYYYYMM<T>(Expression<Func<T, object>> expression) => $"toYYYYMM({expression.GetMemberName()})";

        public static string dictGet(string dictName, string attrName, string idExpr) => $"dictGet('{dictName}','{attrName}',{idExpr})";
        public static string dictGet(string dictName, string attrName, string idExpr, ClickHouseValueType valueType) => $"dictGet{valueType}('{dictName}','{attrName}',{idExpr})";
        public static string dictGetOrDefault(string dictName, string attrName, string idExpr, object defaultValue) => 
            $"dictGetOrDefault('{dictName}','{attrName}',{idExpr},{defaultValue.AsClickValue()})";
        public static string dictGetOrDefault(string dictName, string attrName, string id, object defaultValue, ClickHouseValueType valueType) =>
            $"dictGet{valueType}OrDefault('{dictName}','{attrName}',{id},{defaultValue.AsClickValue()})";
        public static string dictHas(string dictName, string idExpr) => $"dictHas('{dictName}',{idExpr})";
        public static string dictGetHierarchy(string dictName, string idExpr) => $"dictGetHierarchy('{dictName}',{idExpr})";
        public static string dictIsIn(string dictName, string childIdExpr, string ancestorIdExpr) => $"dictIsIn('{dictName}',{childIdExpr},{ancestorIdExpr})";
    }
}