#! /usr/bin/env python

# -*- coding: utf-8 -*-
"""
Created on Sun Nov  1 16:03:48 2015

@author: Wenliang Zhao
"""

from itertools import permutations
import re, string, json

'''
   function cleans tweet.
   input: One string record from JASON
   output: text of tweet, time of tweet, and flag indicating if the tweet contains unicode
'''



def is_unicode(s):
    try:
        s.decode('ascii')
    except:
        return True
    else:
        return False

'''
    function taking a tweet record, clean all escapes, ignore unicode
    input: a tweet (line), a list of escapes (escapes) and a temporal list with ' '
'''   
def tweets_cleaned_json(line, escapes, temp_list):
    json_dict = json.loads(line)
    count = 0
    try: 
        time1 = json_dict['created_at']
    except:
        return ('', '', count)           ### (text, time, count)
    else:
        try:
            text = json_dict['text']
        except:
            return ('', time1.strip(), count+1)   ### empty text also doesn't have unicode
        else:
            if is_unicode(text):
                count = 1
                text = text.encode('ascii', 'ignore')
            text = str(text).translate(string.maketrans(escapes, temp_list))
            return (text.strip(), time1.strip(), count)
            
                       
'''
    function cleans hashtag.
    input: a hashtag string without '#'
    output: cleaned hashtag list (1. if tag with only '_', return empty list; 2. if tag contains '#', split the tag and return list of tags)
'''
def clean_tag(tag):
    if tag == '_':    return []
    if re.search('#', tag):
        L = tag.split('#')
        L = filter(lambda x: (x==None or x=='_'), L)
        return L
    return [tag]

'''
    function maintains average degree of the hashtag graph.
    input: 1. list of hashtag for 1 record (tag_l); 
           2. time of the tweet (t, in python datetime format); 
           3. number of vertex in the graph (N);
           4. 2 times of number of edge or total degree of all vertex (d); 
           5. dictionary for all vertex in the graph <tag1, dict> (key is a tag, value is another dictionary whose key 
              is a tag connecting to tag1, and value is count for the connection); 
           6. a deque (dq) whose elements are dictionaries within 61 seconds (not 60 because there is possiblity that 
              tweet in the previous second is after newer tweet in the text streaming); 
           7. time of last tweet (l_t): if l_t = t, means the tweet is at the same time of the previous. And because 
              we removed tweets older than 61s in previous step, there is no need to consider 60s rule for current step; 
           8. period (p): determine the calculation during, 60s for this study
    output:
           1. updated N, d, and l_t (will be used for the next tweet)
'''
def average_degree(tag_l, t, N, d, tag_d, dq, l_t, p):
    if l_t and l_t != t:
        temp_dict = dq.pop()
        backup_dict = {}                          ### backup for info 61s from current, solving the bug of un-order
        temp_t = temp_dict.get("group_time")
        while ((t-temp_t).total_seconds() > (p-1)):
            if t == temp_t + p:
                backup_dict = temp_dict
            for key in temp_dict:
                temp_dict1 = temp_dict[key]
                for key1 in temp_dict1:
                    tag_d[key][key1] -= (temp_dict1[key1])
                    if not tag_d[key][key1]:
                        d -= 1
                        del tag_d[key][key1]
                    if not tag_d[key]:
                        N -= 1
                        del tag_d[key]
            if dq:
                temp_dict = dq.pop()
                temp_t = temp_dict.get("group_time")
            else:
                temp_dict = {}
                break
        
        if temp_dict:
            dq.append(temp_dict)
        if backup_dict:
            dq.append(backup_dict)
            
    if len(tag_l) > 1:
        temp_dict = {}
        temp_dict["group_time"] = t
        for (tag1, tag2) in permutations(tag_l, 2):
            if tag1 in temp_dict:
                if tag2 in temp_dict[tag1]:
                    temp_dict[tag1][tag2] += 1
                else:
                    temp_dict[tag1][tag2] = 1
            else:
                temp_dict[tag1] = {}
                temp_dict[tag1][tag2] = 1
            
            if tag1 in tag_d:
                if tag2 in tag_d[tag1]:
                    tag_d[tag1][tag2] += 1
                else:
                    d += 1
                    tag_d[tag1][tag2] = 1
            else:
                N += 1
                d += 1
                tag_d[tag1] = {}
                tag_d[tag1][tag2] = 1
    
        dq.appendleft(temp_dict)    
    return N, d, l_t
    

def average_degree_hadoop(tag_l, t, N, d, tag_d, dq, p):
    if dq:
        temp_dict = dq.pop()
        temp_t = temp_dict.get("group_time")
        while ((t-temp_t).total_seconds() > (p-1)*60):
            for key in temp_dict:
                temp_dict1 = temp_dict[key]
                for key1 in temp_dict1:
                    tag_d[key][key1] -= (temp_dict1[key1])
                    if not tag_d[key][key1]:
                        d -= 1
                        del tag_d[key][key1]
                    if not tag_d[key]:
                        N -= 1
                        del tag_d[key]
            if dq:
                temp_dict = dq.pop()
                temp_t = temp_dict.get("group_time")
            else:
                temp_dict = {}
                break
        
        if temp_dict:
            dq.append(temp_dict)
            

    temp_dict = {}
    temp_dict["group_time"] = t
    for T in tag_l:
        tag1, tag2, count = T[0], T[1], T[2]
        if tag1 in temp_dict:
            if tag2 in temp_dict[tag1]:
                temp_dict[tag1][tag2] += count
            else:
                temp_dict[tag1][tag2] = count
        else:
            temp_dict[tag1] = {}
            temp_dict[tag1][tag2] = count
            
        if tag1 in tag_d:
            if tag2 in tag_d[tag1]:
                tag_d[tag1][tag2] += count
            else:
                d += 1
                tag_d[tag1][tag2] = count
        else:
            N += 1
            d += 1
            tag_d[tag1] = {}
            tag_d[tag1][tag2] = count
    
    dq.appendleft(temp_dict)    
    return N, d
