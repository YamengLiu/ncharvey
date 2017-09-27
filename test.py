from __future__ import print_function
from operator import add
import findspark
findspark.init()
import pyspark
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
import sparkpickle
from nltk import TweetTokenizer
from operator import add
import os

if __name__=="__main__":
  conf = SparkConf()
  conf.setMaster("local[8]").setAppName("YELP")
  sc = SparkContext(conf=conf)
  log4j = sc._jvm.org.apache.log4j
  log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
  path = "words"
  data=sparkpickle.load("wordspickle/part-00000")
  print(data)
