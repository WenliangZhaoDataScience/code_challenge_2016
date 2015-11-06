Insight Data Engineering Coding Challenge!
===================

Wenliang Zhao
MS in CS at 

Courant Institute of Mathematical Sciences 

New York University

zhaowenliangpolyu@gmail.com

----------


Documents
=========

0. Outline:
-----------
I think about 3 strategies to obtain the average degree in this project. 
Here I define 2 parameters related to time and space complexity of this work, 
and all the strategies are considered based on these parameters. The first one 
is **active period**, determines how long we should consider on the active 
tweets (in original challenge it is 60s). The second one is 
**updating interval** (in original challenge it is 1s). 
    
It is clear that longer active period makes larger amount of data, so that 
the memory limit needs to be considered. Longer updating interval means more 
chance to have same tag pairs(edge in the graph) during the interval. So that 
a simple sequential algorithm will have many

In section 1, I will explain the **environment** and **dependencies** of 
the codes. In section 2, I will introduce a simple (but efficient) sequential 
algorithm exactly dealing with the original problem. In section 3, I will extend 
the algorithm by using hadoop. Consider longer interval, say more than 10 min or 
even several hours, MapReduce potentially works better than single core sequential 
methods. In section 4, I will use hive to build a data warehouse, and this work 
will potentially benefit huge amount data processing, day finding average degree 
with in 1 year and updating every month (due to time and computational limit I 
only finish data warehouse building for this work)     
    

1. Environment and Dependencies
-------------------------------
Mac OS 10.9.2 (64 bit)
Pyhton 2.7 (in Anaconda 2.3.0)
Hadoop 1.2.1
Hive 0.9.0 (with pyhs and python for thrift)
MySQL 5.7.9


2. Sequential Algorithm (single core)
-------------------------------------
(active period 60s, updating interval 1s)
Main file: code_challenge.py
Subfunctions (in utilities.py): tweets_cleaned,  clean_tag, 
average_degree (see a **flow map** below)
(1) Write to ft1.txt and ft2.txt in **same loop**

(2) **Tweets clean**: clean all unicode, substitute all escapes
    
(3) **Tag clean**: a. empty tag (only #); b. duplicated # symbols in 
front (maybe cause by removing unicode or just typos) c. tag like {#_}, maybe 
caused by removing unicode; d. multiple tags recognized as single tag (e.g. 
because of forgotten spaces between tags, or cause by removing unicode)  

(4)A small trick to avoid blank line in the edn of output: set a flag k=0. 
If k==0, write output directly, k=1; else write a "newline" first, and then 
write output. Save time for tracking the size of the file at the beginning.

(5) **Store by vertex**: A **hash map** (dictionary, "tag_dict" in the code) 
storing all active vertex (v1, as key) whose edge>0, value is **another hash map**. 
Key of the second map is vertex (v2) connecting with (v1), and value is count of 
the forming edge by E(v1, v2) (notice that number of edges will be doubled by this 
way, because we count the edge on v1, and also on v2.

(6) **Store by time**: A **deque** storing all active edges by second and element 
is again a **hash map** <key=v1, value=hash map<key=v2, value=count> > similar to (4) 
but only storing 1s data. (e.g. the first element will be all edges in the first 
seconds. The deque structure is actually can be normal queue. But because python 
queue ADT doesn't support checking first element without popping, we need deque 
in order to push at front (instead of back).

(7) **Time order**: by observation, there is possibility that tweets in the "next 
second" come before the "current second" in streaming. Because of this, if we reach 
the next second, say second 61s, we will delete all information in second 1s; but when 
streaming continue and tweets in second 60s come again, we already lost the information 
of 1s. To overcome, in stead of store 60s in deque, we store 61s (called backup dict 
in utilities). During looping of the deque, I check if the current element is backup 
second, if so, store this sub-dictionary and add back after loop. This procedure can 
promise no information in 1s deleted when dealing with 61s; but it doesn't solve the 
issue of incomplete information about 60s when dealing with 61s. We will see later 
in **hadoop** section, a simple way may be just parallel process the input and sort 
them. But this depends on the purpose of application. Here we just assume our target 
is to update every second as soon as we get records from streaming.  

**check result**: tweet_output/ft1.txt, tweet_output/ft2.txt

![GitHub Logo](/images/flow_map.png)


3. Hadoop
---------
(active period: 10 minutes, updating interval: 60s)
(1) 60s interval is just an example. Consider even longer interval, 
say we want to update the average degree every 1 hour, we can imagine 
that there are lots of overlapping edges (e.g. tweets with same 2 tags; 
or tweets only have 1 tag and the tag is same; or tweet without tag). 
To process these tweets in MapReduce can be more efficient. 

(2) Mapper: extract tags and clean tag, mapping time-tag-tag pairs to 
reducer (time is same within 1 minute here; the longer the interval, 
the better performance it can be)

(3) Reducer: combine same time-tag-tag pairs and aggregate counts.

(4) Process similar to average_degree in section 2, but now there are 
much less data to be processed.

(5) Real application: we need to consider the usefulness of this method. 
Consider tweets data is coming with streaming, we receive the data 
continuously and generate a separate text file every hour. And every 
hour, we want to update the average degree within 10 hour. In this case, 
the main function always stores the hash map for all active vertex, and 
when the information of the next hour arrives, we update to the next hour. 
And because tweets in 1 hour can be very large, hadoop map-reduce can 
improve the performance.

**check result**: tweet_output/ft1_hadoop.txt, tweet_output/unicode_count.txt 

**we tested 7 minutes data, but in theory larger dataset would perform better**

(for simplisity, we separate output1 to pure tweets and count), 
tweet_output/ft2_hadoop.txt


4. Hive data warehouse (fundamental codes done, data warehouse built)
-------------------------------------------------------------
What if we have even larger input? Say active period = 1 year, updating 
interval = 1 month, it even may not be possible to store a complete hash 
map for all vertex in a main function. We consider **Hive** to build data 
warehouse in this case. 

** Make sure Hive server is open for this step **

(1) Use python-thrift to remote connect the Hive server 

(2) MySQL as metastore database

(3) To create table from thrift, need to turn on Hive server first and 
require the following MySQL setting (can be done by calling from python 
or manually in MySQL: "**alter table SDS alter column IS_STOREDASSUBDIRECTORIES 
set default  0;**")

(4) Use the time-tag-tag-count (time now devided by year, month, day, hour, 
minute) information after hadoop as source data; create an external table 
(edge_source) based on the source data

(5) Create a table (edge) with formatted partition (year, month, day, hour, 
minute); dynamically partition bu selecting query from edge_source and insert 
into edge.  

**Check result**: (1) hive (2) select * from edge where minute=51; 



