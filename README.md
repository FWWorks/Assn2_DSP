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

### 1 If you want to quickly verify the correctness of our program

You can run our unit test by the following steps,  
1 Start zookeeper on 127.0.0.1:2181 (the unit test does not depend on mininet, so make sure it is running at localhost).  
2 Execute "pytest test" in project home.

The unit test has covered the following content for both modes,  
1 Starting up broker, publisher and subscriber.  
2 Sending and receiving messages.  
3 Reconnecting to new broker leader when the old dies.  

This is a convenient and suggested way to grade our assignment.

### 2 If you want to manually test our program using a singple topology in mininet

Do as the following steps,  
1 Execute "sudo mn --topo=single,4" to create a simple topology.  
2 Execute "xterm h1 h2 h3 h4" to open windows for nodes.  
3 In h1, start up your zookeeper, which means the zookeeper should be started on 10.0.0.1:2181.  
4 In h2, execute "python3 start_broker.py config/test.ini Broker1" to start the broker.  
5 In h3, execute "python3 start_pub.py config/test.ini Pub1" to start the publisher.  
6 In h4, execute "python3 start_sub.py config/test.ini Sub1" to start the subscriber.  
7 In h3, enter the message you want to publish, watch the subscriber in h4 receiving it.

### 3 If you want to manually test our program using other topologies in mininet

In case you want to make further verification, you need to do some configuration work as the following steps,  
1 Define your topology in mininet.  
2 Modifiy the config/test.ini such that all the ip addresses are consistent with your topology.  
3 Start up all the related entities (zookeeper, broker, publisher, subscriber) in proper windows. The commands are similar to what are described above, but make sure you are using the right name to start up broker, publisher and subscriber.  
4 Enter the message you want to publish in publisher window(s), watch the subscriber(s) receiving it.

## Performance measurement
