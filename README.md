# balancer

*This project was completed as a part of Bradfield's [Distributed Systems](https://bradfieldcs.com/courses/distributed-systems/) class.*

## Introduction

balancer is a distributed key-value store where the least recently-active node is selected for 'work', i.e. load-balancing mechanism.

## Description

The server consists of a network of nodes with set and get operations. If a node receives a set request, it will broadcast to its peers to check the key is not already stored elsewhere. If a node receives a get request, it will broadcast to its peers if the key is not stored at that node.

The client selects the least recently-active node to send a request to. Selection is made at random with weights distributed using a 'reverse softmax' function. Node activity passes back to the client via heartbeats.

The client-server connection is simulated as an unreliable network. In the event of a dropped packet, up to three attempts would be made. Node-to-node connection on the server side is assumed to be fast and reliable.

The message buffer is an indexed queue, and is used to ensure idempotent operations and fast lookups. Messages use [Bencode](https://en.wikipedia.org/wiki/Bencode) encoding, which is simple yet binary-safe.

The implementation consists of both [asynchronous](https://github.com/savarin/balancer/tree/master/linear) and [synchronous](https://github.com/savarin/balancer/tree/master/parallel) versions.

