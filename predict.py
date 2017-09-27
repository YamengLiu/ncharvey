import nltk.sentiment.vader as vader
import pandas as pd
import numpy as np
import nltk

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
  time=int(t1[-1])+100*int(t1[-2])+10000*int(t1[1])+1000000*int(t1[0])
  return time


reader=pd.read_csv("Harvey_tweets.csv",encoding="ISO-8859-1").dropna(how='any').as_matrix()
of=open("Harvey_tweets_senti.csv","w")
senti=vader.SentimentIntensityAnalyzer()
n=0
hourlydata=[]
reader=reader.tolist()
reader=sorted(reader, key=lambda x:keytime(x[-2]))
T=reader[0][-2].split(":")[0]
print "start time",T
sta=[0,0,0]
for tweet in reader:
  p=senti.polarity_scores(tweet[-1])
  time=tweet[-2].split(":")[0]
  if comptime(time,T):
    hourlydata.append(sta)
    print(time+","+str(sta[0])+","+str(sta[1])+","+str(sta[2]))
    sta=[0,0,0]
    T=time

  a="positive"
  if p["neu"]==1.0:
    a="neutral"
    sta[1]+=1
  elif p['pos']<p['neg']:
    a="negative"
    sta[0]+=1
  else:
    sta[2]+=1
  of.write(str(tweet[0])+","+a+"\n")

hourlydata.append(sta)

of.close()





