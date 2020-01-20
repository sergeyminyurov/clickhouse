using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ClickHouse
{
    public static class Helper
    {
        #region Attribute
        /// <summary>
        /// Gets an attribute on an enum field value
        /// </summary>
        /// <typeparam name="T">The type of the attribute you want to retrieve</typeparam>
        /// <param name="enumVal">The enum value</param>
        /// <returns>The attribute of type T that exists on the enum value</returns>
        /// <example>string desc = myEnumVariable.GetAttributeOfType<DescriptionAttribute>().Description;</example>
        /// https://stackoverflow.com/questions/1799370/getting-attributes-of-enums-value
        public static T GetAttribute<T>(this Enum enumVal) where T : Attribute
        {
            var type = enumVal.GetType();
            var memInfo = type.GetMember(enumVal.ToString());
            var attributes = memInfo[0].GetCustomAttributes(typeof(T), false);
            return (attributes.Length > 0) ? (T)attributes[0] : null;
        }
        public static T GetAttribute<T>(this Type type) where T : Attribute
        {
            var attributes = type.GetCustomAttributes(typeof(T), false);
            return (attributes.Length > 0) ? (T)attributes[0] : null;
        }
        public static T GetAttribute<T>(this PropertyInfo propInfo) where T : Attribute
        {
            var attributes = propInfo.GetCustomAttributes(typeof(T), false);
            return (attributes.Length > 0) ? (T)attributes[0] : null;
        }
        #endregion

        #region Property
        // https://www.automatetheplanet.com/get-property-names-using-lambda-expressions/
        // https://www.codeproject.com/Tips/301274/How-to-get-property-name-using-Expression-2
        public static string GetMemberName<T>(this Expression<Func<T, object>> expression) => GetMemberName(expression.Body);
        public static Type GetMemberType<T>(this Expression<Func<T, object>> expression) => expression.ReturnType;

        public static List<string> GetMemberNames<T>(this Expression<Func<T, object>>[] expressions)
        {
            List<string> memberNames = new List<string>();
            foreach (var cExpression in expressions)
            {
                memberNames.Add(GetMemberName(cExpression.Body));
            }
            return memberNames;
        }
        public static string GetMemberName<T>(this Expression<Action<T>> expression) => GetMemberName(expression.Body);

        private static readonly string expressionCannotBeNullMessage = "The expression cannot be null.";
        private static readonly string invalidExpressionMessage = "Invalid expression.";
        private static string GetMemberName(Expression expression)
        {
            if (expression == null)
                throw new ArgumentException(expressionCannotBeNullMessage);

            if (expression is MemberExpression)
            {
                // Reference type property or field
                var memberExpression = (MemberExpression)expression;
                return memberExpression.Member.Name;
            }
            if (expression is MethodCallExpression)
            {
                // Reference type method
                var methodCallExpression = (MethodCallExpression)expression;
                return methodCallExpression.Method.Name;
            }
            if (expression is UnaryExpression)
            {
                // Property, field of method returning value type
                var unaryExpression = (UnaryExpression)expression;
                return GetMemberName(unaryExpression);
            }
            throw new ArgumentException(invalidExpressionMessage);
        }

        private static string GetMemberName(UnaryExpression unaryExpression)
        {
            if (unaryExpression.Operand is MethodCallExpression)
            {
                var methodExpression = (MethodCallExpression)unaryExpression.Operand;
                return methodExpression.Method.Name;
            }
            return ((MemberExpression)unaryExpression.Operand).Member.Name;
        }
        /*
        // https://stackoverflow.com/questions/3341666/net-get-property-name
        String GetPropertyName<TValue>(Expression<Func<TValue>> propertyId)
        {
            return ((MemberExpression)propertyId.Body).Member.Name;
        }
        */
        #endregion

        #region String
        public static string ToLowerUnderscore(this string text) =>
            Regex.Replace(text, @"(?<!_|^)([A-Z])", "_$1").ToLower();
        public static string ToLowerUnderscore(this Enum value) => value.ToString().ToLowerUnderscore();
        #endregion

        #region DataColumn
        public static List<string> GetColumnNames(this DataColumnCollection columns, bool withTypes = false)
        {
            List<string> columnNames = new List<string>();
            foreach (DataColumn column in columns)
                columnNames.Add(column.ColumnName + (withTypes ? " " + column.DataType.ToString() : ""));
            return columnNames;
        }
        #endregion

        #region File
        public static void ToJsonFile<T>(this IEnumerable<T> data, string path)
        {
            using (StreamWriter outputFile = new StreamWriter(path))
            {
                foreach (T item in data)
                    outputFile.WriteLine(JsonSerializer.Serialize<T>(item));
            }
        }
        public static IEnumerable<T> FromJsonFile<T>(string path)
        {
            using (StreamReader reader = File.OpenText(path))
            {
                string text;
                while ((text = reader.ReadLine()) != null)
                {
                    yield return JsonSerializer.Deserialize<T>(text);
                }
            }
        }
        #endregion
    }
}