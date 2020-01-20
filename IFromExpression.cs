using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse
{
    public interface IFromExpression
    {
        string GetText(string db);
    }
}