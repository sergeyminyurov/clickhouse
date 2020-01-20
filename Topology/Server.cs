using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Topology
{
    public class Server
    {
        public string Host { get; set; }
        public ushort Port { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public bool Secure { get; set; }
        public bool Compression { get; set; }
    }
}
