using ClickHouse.CommandText;
using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse
{
    public class ClickHouseObject<T> where T: ClickHouseCommandText
    {
        public T CommandText { get; protected set; }
    }
}