import sys, shutil, os

def processDirectory(dirname):
  fileList = os.listdir(dirname)
  if(not dirname[-1] == "/"):
    dirname += "/"
  count = 0
  for filename in fileList:
    if(count % 3 == 2):
      shutil.copy(dirname + filename, sys.argv[3])
      print (dirname + filename) + " --> " + sys.argv[3]
    else:
      shutil.copy(dirname + filename, sys.argv[2])
      print (dirname + filename) + " --> " + sys.argv[2]
    count += 1

processDirectory(sys.argv[1])
