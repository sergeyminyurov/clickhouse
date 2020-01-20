using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Topology
{
    // system.clusters
    public sealed class Cluster
    {
        public string Name { get; set; }
        public List<Shard> Shards { get; }
        public Cluster(string name)
        {
            Name = name;
            Shards = new List<Shard>();
        }
    }
}