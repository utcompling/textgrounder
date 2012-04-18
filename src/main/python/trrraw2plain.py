import sys, os

outDir = sys.argv[2]
if(not outDir[-1] == "/"):
    outDir += "/"

def processFile(filename):
    global outDir
    inFile = open(filename,'r')
    newFilename = filename[filename.rfind("/")+1:-3] + ".txt"
    outFile = open(outDir + newFilename, 'w')
    wroteSomething = False
    while(True):
        curLine = inFile.readline()
        if(curLine == ""): break
        if(curLine.startswith(" ") or curLine.startswith("\t")): continue
        nextToken = curLine.split()[0]
        processedToken = nextToken.replace("&equo;", "'").replace("&dquo;", '"').replace("&dollar;", "$").replace("&dash;", "-").replace("&amp;", "&").replace("&times;", "*")
        if(processedToken[0].isalnum() and wroteSomething):
            outFile.write(" ") 
        outFile.write(processedToken)
        wroteSomething = True
    inFile.close()

def processDirectory(dirname):
  fileList = os.listdir(dirname)
  if(not dirname[-1] == "/"):
    dirname += "/"
  for filename in fileList:
    if(os.path.isdir(dirname + filename)):
      processDirectory(dirname + filename)
    elif(os.path.isfile(dirname + filename)):
      processFile(dirname + filename)

processDirectory(sys.argv[1])
