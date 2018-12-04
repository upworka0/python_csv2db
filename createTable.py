from Model import *
import os, sys
import zipfile
import requests, io, datetime
from config import *
import csv

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
def CSVtoDB(dir,file, model):
    filepath = "./{0}/{1}".format(dir, file)
    isHeader = True

    #read csv files line by line
    with open(filepath) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for line in csv_reader:
            if isHeader:
                # model.set(line)
                model.creatTable()
                isHeader = False
            else:
                model.set(line)
                model.save()
def getDate(text):
    return datetime.datetime.strptime(text, "%Y/%m/%d").strftime("%Y-%m-%d")
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
    for csvf in csvlist:
        print("Storing {0} to mysql...".format(csvf))
        modelName = csvf.split('.')[0]
        model = None
        if  '201' in modelName:
            model = IPGOLD201()
        elif '202' in modelName:
            model = IPGOLD202()
        elif '203' in modelName:
            model = IPGOLD203()
        elif '204' in modelName:
            model = IPGOLD204()
        elif '206' in modelName:
            model = IPGOLD206()
        elif '207' in modelName:
            model = IPGOLD207()
        elif '208' in modelName:
            model = IPGOLD208()
        elif '220' in modelName:
            model = IPGOLD220()
        elif '221' in modelName:
            model = IPGOLD221()
        elif '222' in modelName:
            model = IPGOLD222()

        if model != None:
            CSVtoDB(dirName, csvf, model)

    print("Ended at %s" % datetime.datetime.now())
