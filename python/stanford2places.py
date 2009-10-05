import sys, re

locationString = re.compile(r'(\w*/LOCATION(\s*\w*/LOCATION)*)')

inFile = file(sys.argv[1])

curLine = inFile.readline()
while(curLine != ""):
    for ls in locationString.findall(curLine):
        lineToPrint = ls[0].replace("/LOCATION", "").strip()
        if(len(lineToPrint) > 0):
            print lineToPrint
    curLine = inFile.readline()
