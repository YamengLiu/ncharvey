from __future__ import print_function
from operator import add
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import re
import random
import numpy
from array import array
import math
import nltk
import pandas as pd
from nltk import TweetTokenizer
from spark_zihui import *
from ast import literal_eval as make_tuple
import string
import re

def comptime(t1,t2):
  t1=t1.replace(" ","/").replace(":","/").split("/")
  t2=t2.replace(" ","/").replace(":","/").split("/")
  for i in range(len(t1)):
    if int(t1[i])>int(t2[i]):
      return True
    elif int(t1[i])<int(t2[i]):
      return False
  return False

def keytime(t1):
  t1=t1.replace(" ","/").replace(":","/").split("/")
  time=int(t1[-1])+100*int(t1[1])+10000*int(t1[0])
  return time

def remove_non_ascii_1(text):
  text= ''.join(i for i in text if ord(i)<128)
  if all(c in string.punctuation or c.isdigit() for c in text):
    return ""
  return text

def mapwords(words):
  a=[]
  a.append(words[0])
  for wp in words[1]:
    a.append(wp[0])
    a.append(wp[1])
  return a

if __name__=="__main__":
  conf = SparkConf()
  conf.setMaster("local[8]").setAppName("YELP")
  sc = SparkContext(conf=conf)
  log4j = sc._jvm.org.apache.log4j
  log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
  print("Set log level to Error")
  num_partitions=10

  types=["NN","NNP","NNPS","NNS"]
  to_remove=["","|","~","\"","/","im","the","get","makes","seen","new"]
      #.filter(lambda x:x[1] in types)\
  words=sc.textFile("words/*").map(make_tuple)\
      .map(lambda x:(remove_non_ascii_1(x[0]),x[1],x[2],x[3]))\
      .filter(lambda x:x[0] not in to_remove and x[0] !="")
      
  print ("finished reading words")
  print(words.top(10))

  word_count=countWord(words)
  w_c=word_count.collect()
  w_c=sorted(w_c,key=lambda x:-x[1])
  print(w_c[0:50])
  f=open("freq.txt","w")
  for i in range(50):
    f.write("\""+w_c[i][0]+"\" "+str(w_c[i][1])+"\n")
  f.close()
  #word_count.saveAsTextFile("word_count")
  #word_count.saveAsPickleFile("word_countpickle")
  #print("top 50 frequenct words",word_count.count())
  #print(word_count.top(50))
  ##topwords=word_count.sortBy(keyfunc=lambda x:x[1],ascending=False,numPartitions=num_partitions).top(50)
  #topwords=word_count.takeOrdered(50,key=lambda x:x[1])
  #print(topwords)

  words=groupWordsByTime(words)
#(word,time),count
  words=mergeWords(words).collect()
  words=sorted(words,key=lambda x:keytime(x[0]))
  freqs=[]
  for i in range(5,9):
    freq={}
    for w in words:
      if((keytime(w[0])>=82000+i*100) and (keytime(w[0])<82000+i*100+100)):
        for pair in w[1]:
          if pair[0] in freq:
            freq[pair[0]]+=pair[1]
          else:
            freq[pair[0]]=pair[1]
    mf=[(k,freq[k]) for k in freq]
    mf=sorted(mf,key=lambda x:-x[1])
    freqs.append(mf[0:50])
  n=25
  for freq in freqs:
    f=open("w"+str(n)+".txt","w")
    for i in range(len(freq)):
      f.write("\""+freq[i][0]+"\" "+str(freq[i][1])+"\n")
    f.close()
    n+=1






  
  #print(words.top(10))
  #words.saveAsTextFile("period")


