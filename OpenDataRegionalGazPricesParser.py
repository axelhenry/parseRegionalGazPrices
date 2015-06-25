#!/usr/bin/env python
# -*- coding: utf-8 -*-

import wget
import zipfile
import os
import json
from xml.dom.minidom import parse
import utilities.DatabaseHandler as db
# import xml.dom.minidom


class OpenDataRegionalGazPricesParser:

    def __init__(self, aUrl, aDirectory, aCfgDict):
        self.myTemplateName = 'FR_frenchGazPrices_'
        self.myGazDictTemplate = {
            'SP95': None, 'SP98': None, 'Gazole': None,
            'E10': None, 'E85': None, 'GPLc': None}
        self.myMatchingNames = {
            'SP95': 'Sans Plomb 95', 'SP98': 'Sans Plomb 98',
            'Gazole': 'Gazole', 'E10': 'Sans Plomb 95-E10',
            'E85': 'BioEthanol E85', 'GPLc': 'GPL'}
        myXmlFile = self.getExtractedFileFromUrl(aUrl, aDirectory)
        myPdvs = self.getPdvsFromXmlFile(myXmlFile)
        myDeps = self.getValuesForEachDep(myPdvs, self.myGazDictTemplate)
        self.computeAverageForEachDep(myDeps)
        # self.writeLocalizedPricesToJson(
        #     myDeps, aDirectory, self.myMatchingNames, self.myTemplateName)
        self.writeToDb(myDeps, self.myMatchingNames,
                       self.myTemplateName, aCfgDict['host'],
                       aCfgDict['port'], aCfgDict['dbname'],
                       aCfgDict['user'], aCfgDict['password'])

    def averageForDepartment(self, myDico):
        myTmpDico = myDico
        for key in myTmpDico:
            myList = myTmpDico[key]
            myAveragePrice = 0
            if myList is not None:
                try:
                    for myPrice in myList:
                        myAveragePrice = myAveragePrice + int(myPrice)
                    myTmpDico[key] = round(
                        myAveragePrice / (len(myList) * 1000), 3)
                except TypeError:
                    # print('we\'re changing nothin, so...')
                    pass
            else:
                myTmpDico[key] = 0
        return myTmpDico

    def computeAverageForEachDep(self, myDeps):
        for dep in myDeps:
            # print('computeAverageForEachDep :: ', dep)
            myTempAv = self.averageForDepartment(myDeps[dep])
            # print('computeAverageForEachDep :: ', dep, ' : ', myTempAv)
            myDeps[dep] = myTempAv
            # input("Press Enter to continue...")

    def getValuesForEachDep(self, myPdvs, aTemplate):
        myDeps = {}
        for pdv in myPdvs:
            myCp = pdv.getAttribute('cp')[0:2]
            if myCp not in myDeps:
                myDeps[myCp] = aTemplate.copy()
            tempTemplate = myDeps[myCp]
            for gaz in pdv.getElementsByTagName('prix'):
                myGazName = gaz.getAttribute('nom')
                if myGazName is not '':
                    if tempTemplate[myGazName] is None:
                        tempTemplate[myGazName] = [gaz.getAttribute('valeur')]
                    else:
                        myDict = tempTemplate[myGazName]
                        myDict.append(gaz.getAttribute('valeur'))
                        tempTemplate[myGazName] = myDict
            myDeps[myCp] = tempTemplate
        return myDeps

    def unzip(self, source_filename, dest_dir):
        with zipfile.ZipFile(source_filename) as zf:
            for member in zf.infolist():
                # Path traversal defense copied from
                # http://hg.python.org/cpython/file/tip/Lib/http/server.py#l789
                words = member.filename.split('/')
                path = dest_dir
                for word in words[:-1]:
                    drive, word = os.path.splitdrive(word)
                    head, word = os.path.split(word)
                    if word in (os.curdir, os.pardir, ''):
                        continue
                    path = os.path.join(path, word)
                zf.extract(member, path)

    def getExtractedFileFromUrl(self, aUrl, aDir):
        aFile = wget.download(aUrl)
        self.unzip(aFile, aDir)
        os.remove(aFile)
        myFileName = os.path.splitext(aFile)[0]
        # print('filename :', myFileName)
        return aDir + '/' + myFileName + '.xml'

    def getPdvsFromXmlFile(self, aFile):
        myTree = parse(aFile)
        os.remove(aFile)
        return myTree.documentElement.getElementsByTagName('pdv')

    def harmonizeGazNames(self, aDict, aMatchingTableNames):
        myHarmonizedDict = {}
        for key in aDict:
            myHarmonizedDict[aMatchingTableNames[key]] = aDict[key]
        return myHarmonizedDict

    def writeLocalizedPricesToJson(self, aDictOfDeps, aOutputDir,
                                   aMatchingTableNames, aTemplateName):
        if os.access(aOutputDir, os.W_OK):
            for dep, dict in aDictOfDeps.items():
                myDictToWrite = self.harmonizeGazNames(
                    dict, aMatchingTableNames)
                myFileName = aOutputDir + '/' + aTemplateName + dep + '.json'
                print('myFileName : ', myFileName)
                with open(myFileName, 'w', encoding='utf-8') as f:
                    json.dump(myDictToWrite, f, indent=4)

    def writeToDb(self, aDictofDeps, aMatchingTableNames,
                  aTemplateName, aHost, aPort, aDbname, aUser, aPassword=''):
        with db.DatabaseHandler(aHost, aPort,
                                aDbname, aUser, aPassword) as myDb:
            for dep, dict in aDictofDeps.items():
                myDictToWrite = self.harmonizeGazNames(dict,
                                                       aMatchingTableNames)
                myRegionalId = aTemplateName + dep
                myDb.addPrices(myRegionalId, myDictToWrite)
