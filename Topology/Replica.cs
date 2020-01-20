using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Topology
{
    public sealed class Replica : Server
    {
        public UInt32 ShardNum { get; set; }
        public UInt32 ShardWeight { get; set; }
        public UInt32 ReplicaNum { get; set; }
    }
}