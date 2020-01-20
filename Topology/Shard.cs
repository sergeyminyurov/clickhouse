using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Topology
{
    public sealed class Shard : Server
    {
        public int Weight { get; set; }
        public bool InternalReplication { get; set; }
        public List<Replica> Replicas { get; }
        public Shard(int weight)
        {
            Weight = weight;
            Replicas = new List<Replica>();
        }
    }
}