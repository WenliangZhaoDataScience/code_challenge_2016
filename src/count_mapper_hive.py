#!/usr/bin/env python  
       
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 14:15:37 2015

@author: Wenliang Zhao

mapper function to generate tag-tag edge with time
"""

import sys, re
import utilities as ut  
from datetime import datetime
from itertools import permutations
       
# input comes from STDIN (standard input)  
for line in sys.stdin:  
    line = line.strip() 
    temp_list = line.split("(timestamp:", 1)
    if len(temp_list) == 2:
        ts = temp_list[1].strip()      
        ts = ts.strip(")")    ### timestamp, -1 means delete the tailing ")"
        temp_str = ts.split(" ")
        temp_str1 = temp_str[3].split(":")
        time_str = temp_str[1].strip() + ' ' + temp_str[2].strip() + ' ' + temp_str[5].strip() + ' ' + temp_str1[0].strip() + ' ' + temp_str1[1].strip()
        timestamp = datetime.strptime(time_str, '%b %d %Y %H %M').strftime("%Y-%m-%d-%H-%M")
        
        content = temp_list[0].strip()                  ### a tweet text
        tag_list = re.findall('#[^ ]*', content)
        if tag_list:
            tag_list = [ut.clean_tag(x.strip('#')) for x in tag_list] 
            tag_list = filter(None, [y.lower() for x in tag_list for y in x])
            tag_list = ['#'+x for x in tag_list]

            for (tag1, tag2) in permutations(tag_list, 2): 
                edge = timestamp + "," + tag1 + "," + tag2
                print '%s\\t%s' % (edge, 1)
        else:
            edge = timestamp + ",," 
            print '%s\\t%s' % (edge, 1)
    else:
        pass
