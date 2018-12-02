from Model import Model
import os, sys
import zipfile
import requests, io, datetime
from config import *

# pull zip file from online
def Pull(url):
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    with open('ipgold-offline.zip', 'wb') as fh:
        fh.write(r.content)

# Extract file from zip to path(directory)
def Unzip(zip, path):
    with zipfile.ZipFile(zip,'r') as zip_ref:
        zip_ref.extractall(path)

# get all file name list in directory
def getCSVfilelist(dir):
    return os.listdir(dir)

# store csv data to mysql
def CSVtoDB(dir,file):
    filepath = "./{0}/{1}".format(dir, file)
    isHeader = True
    # create model with file name
    model = Model(file.split('.')[0])

    #read csv files line by line
    with open(filepath) as fp:
        for line in fp:
            if isHeader:
                model.set(line)
                model.creatTable()
                isHeader = False
            else:
                model.set(line)
                model.save()

if __name__ == "__main__":
    print("Start at %s" % datetime.datetime.now())
    ## pull zip file from url
    Pull(ZIP_URL)

    ## unzip zip file
    zipfileName = "ipgold-offline.zip"
    print("Extracting %s..." % (zipfileName))
    Unzip(zipfileName, '')
    print("Finish Extracting!")
    dirName = zipfileName.split('.')[0]

    print("Start process!")
    csvlist = getCSVfilelist(dirName)
    for csv in csvlist:
        print("Storing {0} to mysql...".format(csv))
        CSVtoDB(dirName, csv)

    print("Ended at %s" % datetime.datetime.now())