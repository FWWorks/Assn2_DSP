# Assn2_DSP
## Team member(T4)
Wei Fan(wei.fan@vanderbilt.edu Github id: FWWorks)  
Dingjie Su(dingjie.su@Vanderbilt.Edu Github id: sudmat)  
Zhehao Zeng(zhehao.zeng@vanderbilt.edu Github id: frankvandy2018) 

## Project code
https://github.com/FWWorks/Assn2_DSP

## Abstract
We built a layer upon the PUB/SUB model supported by ZMQ and ZooKeeper to support anonymity between publishers and subscribers. 
To be specific, we provide two ways as to how data disseminated from publishers and subscribers. One way is, that we wrote code to support the publisher’s middleware layer directly sending the data to the subscribers who are interested in the topic being published by this publisher. Another approach allows the publisher’s middleware always send the information to the broker, who then sends it to the subscribers for this topic. 
Our team also conducted performance measurement experiments to get a sense of the impact on amount of data sent, latency of dissemination, pinpointing the source of bottlenecks.
The code is written in Python3.5 and we use Mininet to build single topologies to test our code, which runs on Linux Ubuntu.
