using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Configuration
{
    public static class Macros
    {
        public static readonly string Cluster = "{cluster}";
        public static readonly string Layer = "{layer}";
        public static readonly string Shard = "{shard}";
        public static readonly string Replica = "{replica}";
    }
}