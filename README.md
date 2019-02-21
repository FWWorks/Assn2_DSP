# Assn2_DSP
## Team member(T4)
Wei Fan(wei.fan@vanderbilt.edu Github id: FWWorks)  
Dingjie Su(dingjie.su@Vanderbilt.Edu Github id: sudmat)  
Zhehao Zeng(zhehao.zeng@vanderbilt.edu Github id: frankvandy2018) 

## Project code
https://github.com/FWWorks/Assn2_DSP

## Abstract
We built a layer upon the PUB/SUB model supported by ZMQ and ZooKeeper to support anonymity between publishers and subscribers. 
Based on what we did in assignment1, we use kazoo to support multiple brokers. Instead of publisher going for a broker, we now make a  publisher/subscriber goes to a zookeeper server to get the leader broker's address, then the data dissemination from publishers and subscribers is the same as assignment 1. eper. Join and Leave of entities is now handled via ZooKeeper via Watch mechanisms and the brokers do “leader election” using ZooKeeper. If Broker leadership changes, these entities will need to know the change. 
We also did experiments to get a sense of the impact on amount of data sent, latency of dissemination, pinpointing the source of bottlenecks.
The code is written in Python3.5 and we use Mininet to build single topologies to test our code, which runs on Linux Ubuntu.

## How to run our code

## Performance measurement
