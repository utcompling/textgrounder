import sys, re

locationString = re.compile(r'(\w*/LOCATION(\s*\w*/LOCATION)*)')

def processFile(filename):
    inFile = open(filename,'r')
    curLine = inFile.readline()
    while(curLine != ""):
        for ls in locationString.findall(curLine):
            lineToPrint = ls[0].replace("/LOCATION", "").strip()
            if(len(lineToPrint) > 0):
                print lineToPrint
        curLine = inFile.readline()

def processDirectory(dirname):
  fileList = os.listdir(dirname)
  if(not dirname[-1] == "/"):
    dirname += "/"
  for filename in fileList:
    if(os.path.isdir(dirname + filename)):
      processDirectory(dirname + filename)
    elif(os.path.isfile(dirname + filename)):
      processFile(dirname + filename)

for filename in sys.argv[1:]:
    processFile(filename)
