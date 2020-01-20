using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace ClickHouse
{
    public static class ProxyLog
    {
        public static Action<string> ErrorWriter { get; set; }
        public static Action<string> WarningWriter { get; set; }
        public static Action<string> InfoWriter { get; set; }

        public static void Error(string message) 
        {
            message = $"ClickHouse Error: {message}";
            Debug.WriteLine(message);
            ErrorWriter?.Invoke(message);
        }
        public static void Warning(string message) 
        {
            message = $"ClickHouse Warning: {message}";
            Debug.WriteLine(message);
            WarningWriter?.Invoke(message);
        }
        public static void Info(string message) 
        {
            message = $"ClickHouse Info: {message}";
            Debug.WriteLine(message);
            InfoWriter?.Invoke(message);
        }
    }
}