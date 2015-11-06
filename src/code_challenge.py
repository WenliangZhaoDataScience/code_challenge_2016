#! /usr/bin/env python

# -*- coding: utf-8 -*-
"""
Created on Sun Nov  1 10:27:03 2015

@author: Wenliang Zhao
"""

import sys, collections, re
import utilities as ut
from datetime import datetime


def main(argv):
    infile = argv[1]
    outfile1 = argv[2]
    outfile2 = argv[3]
    
    OUT1 = open(outfile1, 'w')
    OUT2 = open(outfile2, 'w')
    unicode_count = 0
    period = 60
    tag_dict = {}
    tag_dq = collections.deque([], maxlen=period+1)      ### maximum length of deque is 61. +1 because the issue of un-order tweet within 2s 
    last_time = ''
    
    k = 0
    N_v = 0
    d_total = 0
    escapes = ''.join([chr(char) for char in range(1, 32)])
    temp_list = 31 * ' '
    
    with open(infile) as IN:
        for line in IN:
            (content, time1, unicode_flag) = ut.tweets_cleaned_json(line, escapes, temp_list)
            if not time1:
                continue
            
            if unicode_flag == 1:    unicode_count += 1 
                
            OUT1.write(content + " (timestamp: " + time1 + ")\n")
            
            tag_list = re.findall('#[^ ]*', content)                              ### find all potential tags in text
            tag_list = [ut.clean_tag(x.strip('#')) for x in tag_list]             ### remove all heading '#' in tags 
            tag_list = filter(None, [y.lower() for x in tag_list for y in x])     ### remove empty tags and change to lower case
            tag_list = ['#'+x for x in tag_list]                                  ### add '#' back in front of tags

            temp_str = time1.split(" ")
            temp_str1 = temp_str[3].split(":")
            time_str = temp_str[1].strip() + ' ' + temp_str[2].strip() + ' ' + temp_str[5].strip() + ' ' + temp_str1[0].strip() + ' ' + temp_str1[1].strip() + ' ' + temp_str[2].strip()
            time2 = datetime.strptime(time_str, '%b %d %Y %H %M %S')
            N_v, d_total, last_time = ut.average_degree(tag_list, time2, N_v, d_total, tag_dict, tag_dq, last_time, period)
            if N_v == 0:
                av_degree = 0
            else:
                av_degree = float(d_total) / N_v 
                
            if k == 1:                                                           ### except for the first line, (newline + printing)
                OUT2.write("\n%.2f" % av_degree)   
            else:
                k = 1                   
                OUT2.write("%.2f" % av_degree)
    
    OUT1.write(str(unicode_count) + " tweets contained unicode.")
    OUT1.close()
    OUT2.close()
           
if __name__ == '__main__':
    main(sys.argv[:]) 
