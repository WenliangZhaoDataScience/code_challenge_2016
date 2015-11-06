#! /usr/bin/env python

# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 14:15:37 2015

@author: Wenliang Zhao
"""

import sys, os, subprocess, glob, collections
import utilities as ut
from datetime import datetime

def main(argv):
    if len(argv) != 5:
        print "5 args required!"
        print "Usage code_challenge_hadoop.py input.txt output1.txt output2.txt output3.txt"
        exit(1)

    current_dir = os.getcwd()
    infile = sys.argv[1].strip()
    outfile1 = sys.argv[2].strip()
    outfile2 = sys.argv[3].strip()
    outfile3 = sys.argv[4].strip()
    result_dir = current_dir + '/count_result'
    hadoop_home = os.getenv('HADOOP_HOME').strip()
    stream_jar = glob.glob(hadoop_home + '/contrib//streaming/hadoop-streaming*.jar')[0]
    mapper_file = current_dir + '/count_mapper.py'
    reducer_file = current_dir + '/count_reducer.py'
    
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
    
    
    if os.path.isfile(result_dir+"/_SUCCESS") == True:
        period = 10                        ### update average degree of last 10 min every 1 min
        tag_dict = {}
        tag_list = []
        tag_dq = collections.deque([], maxlen=period)
        last_time = ''
        N_v, d_total, k = 0, 0, 0
        OUT3 = open(outfile3, 'w')
        with open(result_dir+"/part-00000") as IN:
            for line in IN:
                temp_list = line.split('\\t')
                time_str, tag1, tag2, count = temp_list[0], temp_list[1], temp_list[2], temp_list[3]
                timestamp = datetime.strptime(time_str, '%Y-%m-%d %H:%M')
                
                if k == 0:    
                    k = 1                    
                    last_time = timestamp
                
                if (timestamp == last_time): 
                    if tag1 and tag2:
                        tag_list.append((tag1, tag2, int(count)))        ### if same time and not empty, add in tag list
                else:
                    N_v, d_total = ut.average_degree_hadoop(tag_list, last_time, N_v, d_total, tag_dict, tag_dq, period)
                    if N_v == 0:
                        av_degree = 0
                    else:
                        av_degree = float(d_total) / N_v
                    tag_list = [(tag1, tag2, int(count))]

                    OUT3.write(last_time.strftime("%Y-%m-%d %H:%M")+"\t%.2f\n" % av_degree)
                    last_time = timestamp
  
                    
        IN.close()
        N_v, d_total = ut.average_degree_hadoop(tag_list, last_time, N_v, d_total, tag_dict, tag_dq, period)
        if N_v == 0:
            av_degree = 0
        else:
            av_degree = float(d_total) / N_v
        OUT3.write(last_time.strftime("%Y-%m-%d %H:%M")+"\t%.2f" % av_degree)
        OUT3.close()          
                
    else:
        print "Error occurs in map reduce!"
        exit(1)


if __name__ == '__main__':
    main(sys.argv[:])
