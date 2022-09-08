/* ----------------------------------
*  @author suyame 2022-08-29 11:27:00
*  Crazy for Golang !!!
*  IDE: GoLand
*-----------------------------------*/

package loadbalance

type Node struct {
	name  string
	alive bool
}

func (n *Node) IsAlive() bool {
	return n.alive
}

type Request struct {
	ipstr string
}

// 定义一组servers
var servers = []Server{
	&Node{
		"node1",
		true,
	},
	&Node{
		"node2",
		true,
	},
	&Node{
		"node3",
		true,
	},
	&Node{
		"node4",
		true,
	},
	&Node{
		"node5",
		true,
	},
	&Node{
		"node6",
		false,
	},
}

//var servers2 = []Server{
//	ConsistentHash.NewServer("192.168.0.1"),
//	ConsistentHash.NewServer("192.168.0.6"),
//	ConsistentHash.NewServer("192.168.0.11"),
//	ConsistentHash.NewServer("192.168.0.16"),
//}

//定义一组请求
var requests = []Request{
	Request{
		"192.168.0.0",
	},
	Request{
		"192.168.0.2",
	},
	Request{
		"192.168.0.3",
	},
	Request{
		"192.168.0.4",
	},
	Request{
		"192.168.0.5",
	},
	Request{
		"192.168.0.8",
	},
	Request{
		"192.168.0.7",
	},
	Request{
		"192.168.0.8",
	},
	Request{
		"192.168.0.9",
	},
	Request{
		"192.168.0.19",
	},
	Request{
		"192.168.0.13",
	},
	Request{
		"192.168.0.12",
	},
	Request{
		"192.168.0.13",
	},
	Request{
		"192.168.0.14",
	},
	Request{
		"192.168.0.15",
	},
	Request{
		"192.168.0.15",
	},
	Request{
		"192.168.0.17",
	},
	Request{
		"192.168.0.18",
	},
}
