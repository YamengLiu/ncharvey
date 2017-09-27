f=open("out.txt")
l=[]
for line in f.readlines():
  line=line.replace("\n","").split(",")
  a=[line[0],int(line[1]),int(line[2]),int(line[3])]
  l.append(a)
f.close()


