#! /usr/bin/env python

# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 14:15:37 2015

@author: Wenliang Zhao
"""

import sys, os, subprocess, glob
import utilities as ut

def main(argv):
    if len(argv) != 4:
        print "4 args required!"
        print "Usage code_challenge_hadoop.py input.txt output1.txt output2.txt"
        exit(1)

    current_dir = os.getcwd()
    infile = sys.argv[1].strip()
    outfile1 = sys.argv[2].strip()
    outfile2 = sys.argv[3].strip()
    result_dir = current_dir + '/count_result'
    hadoop_home = os.getenv('HADOOP_HOME').strip()
    stream_jar = glob.glob(hadoop_home + '/contrib//streaming/hadoop-streaming*.jar')[0]
    mapper_file = current_dir + '/count_mapper_hive.py'
    reducer_file = current_dir + '/count_reducer_hive.py'
    
    ### clean tweets, output to ft1.txt and count for unicode
    OUT1 = open(outfile1, 'w')
    unicode_count = 0
    k = 0
    escapes = ''.join([chr(char) for char in range(1, 32)])
    temp_list = 31 * ' '
    with open(infile) as IN:
        for line in IN:
            (content, time1, unicode_flag) = ut.tweets_cleaned_json(line, escapes, temp_list)
            if not time1:
                continue
            
            if unicode_flag == 1:    unicode_count += 1 
                
            if k == 1:
                OUT1.write("\n" + content + " (timestamp: " + time1 + ")" )
            else:
                k = 1
                OUT1.write(content + " (timestamp: " + time1 + ")" )
     
    OUT1.close()           
    OUT2 = open(outfile2, 'w')                                   ### For convenience of hadoop streaming, we divide ft1.txt 
    OUT2.write(str(unicode_count) + " tweets contained unicode.")    ### into ft1_hadoop.txt (contain only tweet), and unicode_count.txt
    OUT2.close()     
    
    
    ### Extract tag and combine same tag with same time (within 1 minute) 
    input_file = outfile1
    call_str = 'hadoop jar ' + stream_jar + ' -mapper ' + mapper_file + ' -reducer ' + reducer_file + \
               ' -file ' + mapper_file + ' -file ' + reducer_file + ' -input ' + input_file + ' -output ' + \
               result_dir + ' -mapper cat -reducer aggregate'
               
    call_list = call_list = ['hadoop', 'jar', stream_jar, '-mapper', mapper_file, '-reducer', reducer_file, 
                             '-file', mapper_file, '-file', reducer_file, '-input', input_file, '-output', 
                             result_dir, '-mapper', 'cat', '-reducer', 'aggregate']
    print call_str
    subprocess.Popen(call_list).wait()
    

    ### Hive data warehouse building
    from hive_service import ThriftHive
    from thrift import Thrift
    from thrift.transport import TSocket
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol
    
    input_hive = result_dir + "/part-00000"
    if os.path.isfile(result_dir+"/_SUCCESS") == True:
        try:
            transport = TSocket.TSocket('localhost', 10000)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
 
            client = ThriftHive.Client(protocol)
            transport.open()
            print "Create table from thrift require change in MySQL as below:"
            print "alter table SDS alter column IS_STOREDASSUBDIRECTORIES set default  0;"            
            
            client.execute("drop table if exists edge_source")                             
            client.execute("""CREATE EXTERNAL TABLE if NOT EXISTS edge_source(year INT, month INT, day INT,
                           hour INT, minute INT, tag1 STRING, tag2 STRING, count INT) 
                           ROW FORMAT DELIMITED FIELDS TERMINATED by ','""")
            call_str = "LOAD DATA LOCAL INPATH '" + input_hive + "' INTO TABLE edge_source"
            client.execute(call_str)
                           
            client.execute("drop table if exists edge")          
            client.execute("""CREATE TABLE if NOT EXISTS edge(tag1 STRING, tag2 STRING, count INT) 
                           PARTITIONED by (year INT, month INT, day INT, hour INT, minute INT)
                           ROW FORMAT DELIMITED FIELDS TERMINATED by ','""")
                           
            client.execute("set hive.exec.dynamic.partition=true")
            client.execute("set hive.exec.dynamic.partition.mode=nonstrict")
            client.execute("""INSERT OVERWRITE TABLE edge PARTITION (year, month, day, hour, minute) 
                           SELECT tag1, tag2, count, year, month, day, hour, minute FROM edge_source""")
            
            
            #client.execute("select * from edge")
            #print client.fetchAll()
        
        except Thrift.TException, tx:
            print "Error occurs!"
            print '%s' % (tx.message)
        
    else:
        print "Error occurs in map reduce!"
        exit(1)


if __name__ == '__main__':
    main(sys.argv[:])
