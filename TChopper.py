#! /usr/bin/python
'''
Created on Jun 18, 2012

@author: mohsen
'''

from mimetools import Message
from StringIO import StringIO
from urlparse import urlparse
import os 
import gzip 
import random 
from random import shuffle
#from reportlab.lib.units import toLength

class IO(object):
    _maximumFileSize = 20000000
    _targetFolder = None
    _relevance = 'relevance'
    _data = 'data'
    _fileOpen = False 
    _workingFolder = 1 
    _fileName = 1
    _workingFilePath = None
    _numberOfTimeBeforCheckFileSize = None
    _iteration = 0 
    _maximumFileInOneFolder = 20
    _relevanceFileHandeler = None
    _dataFileHandeler = None
    _relevances = {}
    _gzipSupport = None
    _extension = ''
    
    def __init__(self,targetFolder, autoOpen = True,gzipSupport = True):
        if gzipSupport : 
            self._extension = '.gz'
            self._numberOfTimeBeforCheckFileSize = int(self._maximumFileSize/1000)
        else :
            self._numberOfTimeBeforCheckFileSize = int(self._maximumFileSize/10000)
        self._gzipSupport = gzipSupport
        self._targetFolder = targetFolder
        self._data = os.path.join(targetFolder,self._data)
        self._relevance =os.path.join(targetFolder,self._relevance)
        self._makeFolder(self._data)
        self._makeFolder(self._relevance)
        if (autoOpen):
            self.openFiles()
        
    def _makeFolder(self,address):
        if not os.path.exists(address): os.makedirs(address)
    
    def openFiles(self):
        self._fileOpen = True
        self._makeFolder(os.path.join(self._data,str(self._workingFolder)))
        dataFilePath = os.path.join(self._data,str(self._workingFolder),str(self._fileName)+self._extension)
        self._workingFilePath = dataFilePath
        relevanceFilePath = os.path.join(self._relevance,'rel')
        if self._gzipSupport :
            self._dataFileHandeler = gzip.open(dataFilePath,'w')
        else :
            self._dataFileHandeler = open(dataFilePath,'w')
        self._relevanceFileHandeler = open(relevanceFilePath,'w')
    
    def _check(self):
        self._dataFileHandeler.flush()
        size = os.path.getsize(self._workingFilePath)
        if(size >self._maximumFileSize):
            self._iteration = 0
            self._fileName += 1
            self._dataFileHandeler.close()
            if(self._fileName>self._maximumFileInOneFolder):
                self._fileName = 1
                self._workingFolder += 1
                self._makeFolder(os.path.join(self._data,str(self._workingFolder)))
            dataFilePath = os.path.join(self._data,str(self._workingFolder),str(self._fileName)+self._extension)
            self._workingFilePath = dataFilePath
            if self._gzipSupport:
                self._dataFileHandeler = gzip.open(self._workingFilePath,'w')
            else :
                self._dataFileHandeler = open(self._workingFilePath,'w')
                         
    def writeData(self,data,relevances = None):    
        self._iteration += 1 
        if(self._numberOfTimeBeforCheckFileSize<self._iteration):
            self._check()   
        self._dataFileHandeler.write(data)
        if(not relevances == None):
            for relevance in relevances :
                self._relevanceFileHandeler.write(relevance.getLine())
                
    def _sortRelevance(self):
        oldRelevanceAddress = os.path.join(self._relevance,'rel')
        newRelevanceAddress = os.path.join(self._relevance,'relevance')
        fileReader = open(oldRelevanceAddress,'r')
        fileWriter = open(newRelevanceAddress,'w')
        listRelevance = []
        for line in fileReader:
            listRelevance.append(Relevance(line))
        listRelevance.sort(key = lambda x : x.getQueryNumber())
        for rel in listRelevance:
            fileWriter.write(rel.getLine())
        fileReader.close()
        fileWriter.flush()
        fileWriter.close() 
        os.remove(oldRelevanceAddress)
        
    def closeFiles(self):
        if self._fileOpen :
            self._dataFileHandeler.flush()
            self._dataFileHandeler.close()
            self._relevanceFileHandeler.flush()
            self._relevanceFileHandeler.close()
            self._sortRelevance()
            self._fileOpen = False

class TRECWriter(object):
    _categories = {}
    _targetFolder = str()

    def __init__(self,targetFolder):
        self._targetFolder = targetFolder
        
    def writeToFile(self,category,data,relevances = None):
        
        if (self._categories.has_key(category)):
            io = self._categories.get(category)
        else :
            io = IO(os.path.join(self._targetFolder,category))
            self._categories[category] = io
        io.writeData(data,relevances)
        
    def closeAllFiles(self):
        listOfCategoris = self._categories.items()
        for category in listOfCategoris :
            category[1].closeFiles()
        
class Relevance(object):
    _line = None 
    def __init__(self,line):
        self._line = line 
    def getDocumentKey(self):
        return self._line.split()[2].lower().strip()
    def getUniqueKey(self):
        return self._line.split()[0]+' '+self._line.split()[2]
    def getRelevance(self):
        return int(self._line.split()[3])
    def getQueryNumber(self):
        return int(self._line.split()[0])
    def getLine(self):
        return self._line
    
class RelevanceBucket(object):
    
    _relevanceDictionaryBaseOnDocID = None
    'this dictionary contain same object with same reference to previous dictionary'
    _relevanceDictionaryBaseOnQueryNumber = None
    _querNumberList= None
    _numberOfRelevantDocuemnt = 0
    _relevanceDocumentDictinary = None
    def __init__(self,fileAddresses=None):
        self._relevanceDictionaryBaseOnDocID = {}
        self._relevanceDictionaryBaseOnQueryNumber = {}
        self._querNumberList= []
        self._relevanceDocumentDictinary = {}
        if fileAddresses != None:
            self.processFile(fileAddresses)
        
    def processFile(self,fileAddresses):
        for fileAddress in fileAddresses:
            fileReader = open(fileAddress,"r")
            for line in fileReader :
                relevance = Relevance(line)
                self.addRelevance(relevance)

    def addRelevance(self,relevance):
        documentKey = relevance.getDocumentKey()
        relevanceList = self._relevanceDictionaryBaseOnDocID.get(documentKey,[])
        relevanceList.append(relevance)
        self._relevanceDictionaryBaseOnDocID[relevance.getDocumentKey()] = relevanceList
        relevanceList = self._relevanceDictionaryBaseOnQueryNumber.get(relevance.getQueryNumber(),[])
        relevanceList.append(relevance)
        self._relevanceDictionaryBaseOnQueryNumber[relevance.getQueryNumber()] = relevanceList
        rel =self._relevanceDocumentDictinary.get(documentKey,0)
        if rel == 0 :
            rel = relevance.getRelevance()
            self._relevanceDocumentDictinary[documentKey] = rel
            if rel != 0: self._numberOfRelevantDocuemnt +=1
        if len(relevanceList) == 1:
            self._querNumberList.append(relevance.getQueryNumber())
            
    def addRelevanceList(self,relevanceList):
        for relevance in relevanceList:
            self.addRelevance(relevance)

    def writeToFile(self,fileAddress):
        fileHandler = open(fileAddress,'w')
        self._querNumberList.sort()
        for queryNumber in self._querNumberList :
            relevenceList = self._relevanceDictionaryBaseOnQueryNumber.get(queryNumber)
            for relevance in relevenceList:
                fileHandler.write(relevance.getLine())
                       
    def getRelevance(self,documentKey):
        '''
        Return relevances list base on documentKey 
        '''
        documentKey = documentKey.lower().strip()
        return self._relevanceDictionaryBaseOnDocID.get(documentKey,[])
    
    def getRelevanceByQueryNumber(self,queryNumber):
        '''
        Return relevances list base on query Number
        '''
        return self._relevanceDictionaryBaseOnQueryNumber.get(queryNumber,[])
    
    def getQueryNumberList(self):
        return self._querNumberList
    
    def getNumberOfQuery(self):
        return len(self._querNumberList)
    
    def getRelevancesListBySequencialNumber(self,number):
        queryNumber = self._querNumberList[number]
        return self._relevanceDictionaryBaseOnQueryNumber.get(queryNumber)
    
    def isRelevant(self,documentKey):
        documentKey = documentKey.lower().strip()
        rel = self._relevanceDocumentDictinary.get(documentKey)
        if rel : return True
        else : return False

    def isEvaluated(self,documentKey):
        documentKey = documentKey.lower().strip()
        relevantList = self.getRelevance(documentKey)
        if len(relevantList) == 0 :
            return False
        else: return True
        
    def getNumberOfRelevantDocuemnts(self):
        return self._numberOfRelevantDocuemnt
                        
class TRECParser(object): 
    ''' 
    document tags which you want to find in the document 
    ''' 
    _tags = [ "DOCNO","DOCOLDNO","DOCHDR","html"]
    #_tags = [ 'DOCNO']
    #_tags = [ "DOCHDR"]
    #_tags = []
    '''
    main Document tag
    '''
    _startDocTag = "DOC"
    _docDictinary = {}
    i = int()

    def __init__(self ):
        '''
        Constructor
        '''
    def _extractDocument(self,fileReader, DocTag):
        lines = str()
        startTag = "<"+DocTag+">"
        endTag = "</"+DocTag+">"
        flag = False
        for line in fileReader:
            if (line.find(startTag)!= -1):
                flag = True
            if(flag == True):
                lines= lines + line 
            if(line.find(endTag) !=-1):
                flag = False
                break
        return lines           
    
    def _extaractData(self,doc ,tags):
        docDictionary = {}
        for tag in tags :
            startTag = "<" + tag + ">"
            endTag = "</"+ tag + ">"
            startIndex = doc.find(startTag)
            endIndex = doc.rfind(endTag)
            if (startIndex != -1 and endIndex != -1):
                docDictionary[tag]=str.lstrip(doc[startIndex+len(startTag):endIndex])
        return docDictionary
    
    
    def _getTopLevelDomain(self,dc,docDictionary,listOfCategories):
        headers = Message(StringIO(docDictionary["DOCHDR"]))
        url = "http:" + headers.dict["http"].split()[0]
        o = urlparse(url)
        url =o[1] 
        listURLComponent = url.split(":")[0].split(".")
        #print listURLComponent
        listURLComponent.reverse()
        topLevelDomain = listURLComponent[0]
        if(listOfCategories.count(topLevelDomain) != 1):
            topLevelDomain = 'other'
        return topLevelDomain
        
        
    def _extractTopLevelDomain(self,doc,docDictionary):
        headers = Message(StringIO(docDictionary["DOCHDR"]))
        url = "http:" + headers.dict["http"].split()[0]
        o = urlparse(url)
        st =o[1]
        listURLComponent = st.split(":")[0].split(".")
        topLevelDomain = str()
        listURLComponent.reverse()
        counter = 0 
        isItIp = listURLComponent[0].isdigit()
        if(isItIp):
            for componet in listURLComponent:
                topLevelDomain = topLevelDomain +"." + componet
                if (not componet.isdigit()):
                    break
        else :
            for componet in listURLComponent:
                counter += 1
                if(counter < 2):
                    if (len(componet)<4):
                        topLevelDomain = "."+componet.lower() + topLevelDomain
                else:
                    break
        return topLevelDomain
    def _extractContentType(self,doc,docDictionary):
        header = Message(StringIO(docDictionary["DOCHDR"]))
        return header.gettype()
     
        
    def contentTypesStatics(self,targetFolder,resultFileAddress):
        self._tags = [ "DOCHDR"]
        dicContentTypes = {}
        totalNumberOfDocument = 0
        fileHandler = open(resultFileAddress,'w')
        for doc,docDictionary in self.parse(targetFolder):
            totalNumberOfDocument +=1
            pageType =self._extractContentType(doc, docDictionary)
            count = dicContentTypes.get(pageType,0L)
            count += 1
            dicContentTypes[pageType] = count
        listItems = dicContentTypes.items()
        fileHandler.write(str(totalNumberOfDocument)+os.linesep)
        totalNumberOfDocument = 0
        for item in listItems :
            totalNumberOfDocument += item[1]
            fileHandler.write(item[0]+' '+str(item[1])+os.linesep)
        fileHandler.write(str(totalNumberOfDocument)+os.linesep)
        fileHandler.close()
            
    def topLevelDomainStatics(self,targetFolder,resultFileAddressList):
        self._tags = [ "DOCHDR"] 
        dicTopLevelDomain = {}
        totalNumberOfDocument = 0
        fileHandler = open(resultFileAddressList,'w')
        for doc, docDictionary in self.parse(targetFolder):
            totalNumberOfDocument += 1
            topDomain = self._extractTopLevelDomain(doc,docDictionary)
            count = dicTopLevelDomain.get(topDomain,0L)
            count += 1
            dicTopLevelDomain[topDomain] = count
        fileHandler.write(str(totalNumberOfDocument)+os.linesep)
        listOfTopleveDomain = dicTopLevelDomain.items()
        totalFromFile = 0
        for item in listOfTopleveDomain:
            fileHandler.write(item[0]+' '+str(item[1]) + os.linesep)
            totalFromFile += item[1]
        fileHandler.write(str(totalFromFile))
        fileHandler.close()
        
    def test(self,relevanceFileAddresses):
        '''Calculate number of evaluated document in the sub-coprpus
        function get sub-corpus folder as targetFolder and relevance folder address
        and also out put target folder for results'''
        allRelevanceFileNames = relevanceFileAddresses
        allFileAddress = []
        
        for fileName in allRelevanceFileNames:
            allFileAddress.append(os.path.join(relevanceFileAddresses,fileName))
        
        relevanceFeedBack = RelevanceBucket(allFileAddress)
        print(relevanceFeedBack.getNumberOfRelevantDocuemnts())
           
    def evaluatedDocumentsStatics(self,targetFolder,relevanceFileAddresses,
                            outPutTarget,label='TREC'):
        '''Calculate number of evaluated document in the sub-coprpus
        function get sub-corpus folder as targetFolder and relevance folder address
        and also out put target folder for results'''
        files= {}
        if outPutTarget == None:
            outPutTarget = targetFolder
        allRelevanceFileNames = relevanceFileAddresses
        allFileAddress = []
        
        for fileName in allRelevanceFileNames:
            allFileAddress.append(os.path.join(relevanceFileAddresses,fileName))
        
        relevanceFeedBack = RelevanceBucket(allFileAddress)
        
        for allFileAndDirectoryNames in os.walk(targetFolder):
            dirName = allFileAndDirectoryNames[0]
            filenames = allFileAndDirectoryNames [2]
            for fileName in filenames:
                filePath= os.path.join(dirName, fileName)
                if not os.path.isdir(filePath):
                    files[fileName]=filePath
        # statistics 
        statistics  ={}            
        for fileName, filePath in files.items():
            print fileName 
            numberOfDocuments = 0
            numberOfEvaluatedDocuments = 0
            numberOfRelevanceDocuments = 0
            numberOfUnRelevanceDocuments = 0
            for doc,docDictionary in self.parseFile(filePath):
                # following line added to to ignore warning 
                doc = doc 
                numberOfDocuments = numberOfDocuments+1
                documentNumber = docDictionary.get('DOCNO')
                relevances = relevanceFeedBack.getRelevance(documentNumber)
                
                #print docDictionary.get('DOCNO') +str(len(relevances))
                if len(relevances)>0:
                    numberOfEvaluatedDocuments = numberOfEvaluatedDocuments+1
                    isRelevance = False 
                    for relevance in relevances:
                        if relevance.getRelevance()>0:
                            isRelevance  = True
                            break
                    if isRelevance:
                        numberOfRelevanceDocuments = numberOfRelevanceDocuments+1
                    else :
                        numberOfUnRelevanceDocuments = numberOfUnRelevanceDocuments+1
            fileName= fileName.split('.')[0]        
            statistics[fileName] = [numberOfDocuments,numberOfEvaluatedDocuments,numberOfRelevanceDocuments,numberOfUnRelevanceDocuments]
            print 'number of evaluated documents ' + str(numberOfEvaluatedDocuments)
            print 'number of relevance documents ' + str(numberOfRelevanceDocuments)
            print 'number of un-relevance documents ' + str(numberOfUnRelevanceDocuments)
            print 'number of documents ' + str(numberOfDocuments)
            
        outPutTarget = os.path.join(outPutTarget,'numberRelevanceDocument_'+label+'.txt')
        
        fileHandler = open(outPutTarget,'w+')
        fileHandler.write(label + os.linesep)
        header = 'name of sub-corpus in '+label+', number of documents, number of evaluated documents, number of relevance documents, number of un-relevance documents'
        fileHandler.write(header+os.linesep)
        for fileName,statisticPerFile in statistics.items():
            
            numberOfDocuments,numberOfEvaluatedDocuments,numberOfRelevanceDocuments,numberOfUnRelevanceDocuments = statisticPerFile  
            fileHandler.write(fileName+', '+str(numberOfDocuments)
                              +', '+str(numberOfEvaluatedDocuments)+'('+str(round(numberOfEvaluatedDocuments/float(numberOfDocuments),5))+'%'+')'
                              +', '+str(numberOfRelevanceDocuments)+'('+str(round(numberOfRelevanceDocuments/float(numberOfDocuments),5))+'%'+')'
                              +', '+str(numberOfUnRelevanceDocuments)+'('+str(round(numberOfUnRelevanceDocuments/float(numberOfDocuments),5))+'%'+')'
                              +os.linesep)
        fileHandler.write(os.linesep)
        
    def splitTopLevelDomain(self,targetFolder,relevanceFolderAddress,
                            outPutTarget):
        listOfCategories = ['com','org','edu','gov','uk','ca']
        allRelevanceFileNames = relevanceFolderAddress
        allFileAddress = []
        for fileName in allRelevanceFileNames:
            allFileAddress.append(os.path.join(relevanceFolderAddress,fileName))
        relevanceFeedBack = RelevanceBucket(allFileAddress)
        trecWriter  = TRECWriter(outPutTarget) 
        for doc,docDictionary in self.parse(targetFolder):
            category = self._getTopLevelDomain(doc, docDictionary,listOfCategories)
            relevances = relevanceFeedBack.getRelevance(docDictionary.get('DOCNO'))
            trecWriter.writeToFile(category, doc, relevances)
        trecWriter.closeAllFiles()
        
    def earlierTrecSpliter(self,targetFolder,relevanceFileAddresses,
                            outPutTarget):
        self._tags = ['DOCNO']
        relevanceFeedBack = RelevanceBucket(relevanceFileAddresses)
        trecWriter  = TRECWriter(outPutTarget)
        folderList = os.listdir(targetFolder)
        for folderName in folderList:
            insideFolderAddress = os.path.join(targetFolder,folderName) 
            fileNames = os.listdir(insideFolderAddress)
            for fileName in fileNames:
                fileAddress = os.path.join(insideFolderAddress,fileName)
                for doc,docDictionary in self.parseFile(fileAddress):
                    category = fileName.split('.')[0]
                    relevances = relevanceFeedBack.getRelevance(docDictionary.get('DOCNO'))
                    trecWriter.writeToFile(category, doc, relevances)
        trecWriter.closeAllFiles()
        
    def makeOneCollection(self,targetFolder,relevanceFileAddresses,
                            outPutTarget,label='TREC'):
        self._tags = ['DOCNO']
        relevanceFeedBack = RelevanceBucket(relevanceFileAddresses)
        trecWriter  = TRECWriter(outPutTarget)
        category = label
        for doc,docDictionary in self.parse(targetFolder):
            relevances = relevanceFeedBack.getRelevance(docDictionary.get('DOCNO'))
            trecWriter.writeToFile(category, doc, relevances)
        trecWriter.closeAllFiles()

    def makeNoneRelevantCorpus(self,targetFolder,relevanceFileAddresses,
                            outPutTarget,label='TREC'):
        self._tags = ['DOCNO']
        relevanceFeedBack = RelevanceBucket(relevanceFileAddresses)
        trecWriter  = TRECWriter(outPutTarget)
        category = label
        for doc,docDictionary in self.parse(targetFolder):
            if not relevanceFeedBack.isRelevant(docDictionary.get('DOCNO')):
                relevances = relevanceFeedBack.getRelevance(docDictionary.get('DOCNO'))
                trecWriter.writeToFile(category, doc, relevances)
        trecWriter.closeAllFiles()
        
    def makeRelevantCorpus(self,targetFolder,relevanceFileAddresses,
                            outPutTarget,label='TREC'):
        self._tags = ['DOCNO']
        relevanceFeedBack = RelevanceBucket(relevanceFileAddresses)
        trecWriter  = TRECWriter(outPutTarget)
        category = label
        for doc,docDictionary in self.parse(targetFolder):
            if relevanceFeedBack.isRelevant(docDictionary.get('DOCNO')):
                relevances = relevanceFeedBack.getRelevance(docDictionary.get('DOCNO'))
                trecWriter.writeToFile(category, doc, relevances)
        trecWriter.closeAllFiles()
        
    def randomTrecSpliter(self,targetFolder,relevanceFileAddresses,
                            outPutTarget,numberOfSpliting,randomDocLable=1):
        self._tags = ['DOCNO']
        relevanceBucket = RelevanceBucket(relevanceFileAddresses)
        numberOfEachCollection = \
        self.countNumberOfDocumentsInCollection(targetFolder)[1]
        numberOfSpliting += randomDocLable
        listDocumentAddress = numberOfEachCollection.items()
        numberOfFileToSplite = len(listDocumentAddress)
        print 'start splitting the data'
        for i in range(0,numberOfFileToSplite-1):
            for j in range(i+1,numberOfFileToSplite):
                totalNumberOfDocuments = listDocumentAddress[i][1]+listDocumentAddress[j][1]
                documentLabel = listDocumentAddress[i][0].split(os.path.sep).\
                pop().split('.')[0]
                documentLabel += '_'+listDocumentAddress[j][0].split(os.path.sep).\
                pop().split('.')[0]
                while ( randomDocLable < numberOfSpliting):
                    trecWriter  = TRECWriter(outPutTarget)
                    splittingPoint = [listDocumentAddress[i][1],totalNumberOfDocuments]
                    countFileToProcess = 0                
                    while countFileToProcess < 2 :
                        countFileToProcess += 1
                        randomHashMap = self.makeRandomHashMap(totalNumberOfDocuments,splittingPoint)
                        if countFileToProcess == 1 :
                            fileAddress = listDocumentAddress[i][0]
                        else : 
                            fileAddress = listDocumentAddress[j][0]
                        documentNumber = 1
                        for doc,docDictionary in self.parseFile(fileAddress):
                            if randomHashMap[documentNumber]:
                                category = str(randomDocLable)+'_'+documentLabel+'_10'
                            else : 
                                category = str(randomDocLable)+'_'+documentLabel+'_01'
                            relevances = relevanceBucket.getRelevance(docDictionary.get('DOCNO'))
                            trecWriter.writeToFile(category, doc, relevances)
                            documentNumber +=1
                    randomDocLable += 1
                    trecWriter.closeAllFiles()
    relevanceBucket = None
    def randomTrecSpliterWithQrls(self,targetFolder,relevanceFileAddresses,
                            outPutTarget,randomDocLable=1,numberOfSpliting=4,
                            equalCorpus=True,equalEvaluatedDocument=True):
        self._tags = ['DOCNO']
        if not self.relevanceBucket: 
            relevanceBucket = RelevanceBucket(relevanceFileAddresses)
            self.relevanceBucket = relevanceBucket
        else :
            relevanceBucket = self.relevanceBucket
        numberOfRelevantDocuments = relevanceBucket.getNumberOfRelevantDocuemnts()    
        numberOfDocuments = \
        self.countNumberOfDocumentsInCollection(targetFolder,False)
        numberOfUnevaluatedDocuments = numberOfDocuments - numberOfRelevantDocuments 
        if (equalCorpus and equalEvaluatedDocument)or(equalCorpus) :
            unrelevantRandomCorpusHashMap = self.makeRandomHashMap(numberOfUnevaluatedDocuments,numberOfSpliting)
            relevantRandomCorpusHashMap = self.makeRandomHashMap(numberOfRelevantDocuments, numberOfSpliting)
        elif equalEvaluatedDocument:
            splittingPoints = self.getExponensialSplittingPoints(numberOfUnevaluatedDocuments)
            unrelevantRandomCorpusHashMap=self.makeRandomHashMap(numberOfUnevaluatedDocuments, splittingPoints)
            relevantRandomCorpusHashMap=self.makeRandomHashMap(numberOfRelevantDocuments,numberOfSpliting)
        else :
            splittingPoints = self.getExponensialSplittingPoints(numberOfUnevaluatedDocuments)
            unrelevantRandomCorpusHashMap=self.makeRandomHashMap(numberOfUnevaluatedDocuments, splittingPoints)
            splittingPoints = self.getExponensialSplittingPoints(numberOfRelevantDocuments)
            relevantRandomCorpusHashMap=self.makeRandomHashMap(numberOfRelevantDocuments,splittingPoints)
        print 'start splitting the data'
        trecWriter  = TRECWriter(outPutTarget) 
        unrelDocsNumber = 0
        relDocsNumber = 0
        for doc,docDictionary in self.parse(targetFolder):
            category = str(randomDocLable)+'_'
            documentKey = docDictionary.get('DOCNO')
            isRelevant = relevanceBucket.isRelevant(documentKey)
            if isRelevant :
                relDocsNumber +=1
                category += str(relevantRandomCorpusHashMap[relDocsNumber])
            else :
                unrelDocsNumber +=1
                category += str(unrelevantRandomCorpusHashMap[unrelDocsNumber])
            relevances = relevanceBucket.getRelevance(documentKey)
            trecWriter.writeToFile(category, doc, relevances)
        trecWriter.closeAllFiles()

    def splitBasedOnSizeOfDocuments(self,targetFolder,relevanceFileAddresses,
                            outPutTarget,numberOfSplits):
        relevanceFeedBack = RelevanceBucket(relevanceFileAddresses)
        trecWriter  = TRECWriter(outPutTarget) 
        sizeList = self.getSizeOfDocuments(targetFolder)
        numberOfDocuments = len(sizeList)
        sizeOfEachPart = int(numberOfDocuments/numberOfSplits)
        self._tags =self._tags = ['DOCNO']
        for doc,docDictionary in self.parse(targetFolder):
            category = str(numberOfSplits)
            fromHere = 0
            toHere = sizeOfEachPart
            sizeOfDocument = len(doc)
            for i in range(1,numberOfSplits+1):
                fromHere = (i-1)*sizeOfEachPart
                toHere = (i)*sizeOfEachPart
                if sizeList[fromHere] <= sizeOfDocument and \
                            sizeList[toHere] >= sizeOfDocument:
                    category = str(i)
                    break   
            relevances = relevanceFeedBack.getRelevance(docDictionary.get('DOCNO'))
            trecWriter.writeToFile(category, doc, relevances)
        trecWriter.closeAllFiles()
        
    def parse(self,targetFolder):         
        for allFileAndDirectoryNames in os.walk(targetFolder):
            dirname = allFileAndDirectoryNames[0]
            filenames = allFileAndDirectoryNames [2]
            for filename in filenames:
                print filename
                file_name= os.path.join(dirname, filename)
                newFileName = file_name.lower()
                if(newFileName.endswith('.gz')):
                    fileReader = gzip.open(file_name)
                else : 
                    fileReader = open(file_name,'r')
                while (True)  :
                    doc = self._extractDocument(fileReader,self._startDocTag)
                    if(doc != ""):
                        self._docDictinary = self._extaractData(doc , self._tags)
                        yield [doc,self._docDictinary]
                    else :
                        break 
                fileReader.close()
        return 
    
    def parseFile(self,fileAddress):
        fileReader = None 
        newFileName = fileAddress.lower()
        if newFileName.endswith('.gz') :
            fileReader = gzip.open(fileAddress)
        else :
            fileReader = open(fileAddress,'r')
        while (True)  :
            doc = self._extractDocument(fileReader,self._startDocTag)
            if(doc != ""):
                self._docDictinary = self._extaractData(doc , self._tags)
                yield [doc,self._docDictinary]
            else :
                fileReader.close()
                break
            
    def sizeAnalysis(self,targetFolder,resultFileAddress):
        fileHandler = open(resultFileAddress,'w')
        self._tags= []
        sizeList = []
        for docInfo in self.parse(targetFolder):
            sizeList.append(len(docInfo[0]))    
        sizeList.sort()
        totalSize = 0
        for size in sizeList :
            totalSize += size
            fileHandler.write(str(size)+ os.linesep)
            
        sizeOfCorpus= len(sizeList)
        print('Minimum ' + str(sizeList[0]))
        print('Maximum ' + str(sizeList[sizeOfCorpus-1]))
        print('Medium ' + str(sizeList[int(sizeOfCorpus/2)]))
        print('Average ' + str(totalSize/sizeOfCorpus))
        eachCorpusSize = int(sizeOfCorpus/16)
        txtForPrint = str()
        for i in range(1,17):
            txtForPrint += str(i)+'. '+str(sizeList[i*eachCorpusSize]) + '  '
        print txtForPrint
        txtForPrint = ''
        eachCorpusSize = int(sizeOfCorpus/8)
        for i in range(1,9):
            txtForPrint += str(i)+'. '+str(sizeList[i*eachCorpusSize]) + '  '
        print txtForPrint
        txtForPrint = ''
        eachCorpusSize = int(sizeOfCorpus/4)
        for i in range(1,5):
            txtForPrint += str(i)+'. '+str(sizeList[i*eachCorpusSize]) + '  '
        print txtForPrint
        
    def getSizeOfDocuments(self,targetFolder):
        #self._tags = ['TEXT']
        self._tags= []
        sizeList = []
        for docInfo in self.parse(targetFolder):
            sizeList.append(len(docInfo[0]))    
        sizeList.sort()
        return sizeList

    def countNumberOfDocumentsInCollection(self,targetFolder,countEachCollection=True):
        if not self.totalNumberDocuments :
            print 'count the number of each collection'
            copyTags = list(self._tags)
            self._tags = []
            numberOfEachCollection = {}
            totalNumberDocuments = long()
            for directoryInfo in os.walk(targetFolder):
                r = directoryInfo[0]
                f = directoryInfo[2]
                for files in f:
                    fileAddress = os.path.join(r,files)
                    numberOfDocumentInOneFile = long()
                    for doc ,docDictionary in self.parseFile(fileAddress):
                        # these two line added to ignore compiler warning
                        doc = doc
                        docDictionary = docDictionary
                        numberOfDocumentInOneFile +=1
                        totalNumberDocuments +=1
                    if  countEachCollection :
                        numberOfEachCollection[fileAddress]= numberOfDocumentInOneFile
            self._tags = copyTags
            self.totalNumberDocuments = totalNumberDocuments
            self.numberOfEachCollection = numberOfEachCollection
            print numberOfEachCollection
            if countEachCollection :
                return totalNumberDocuments,numberOfEachCollection
            else:return totalNumberDocuments
        elif countEachCollection :
            return self.totalNumberDocuments , self.numberOfEachCollection
        else : return self.totalNumberDocuments
    # these two values hold the number of documents in the collection 
    # so next time doesn't count the number of document    
    totalNumberDocuments = None
    numberOfEachCollection = None  
    
    def makeRandomHashMapOldVersion(self,size):
        randomHashMap = {}
        ignoreList = []
        numberOfFalse = 0
        numberOfTure = 0
        for i in range(1,size +1):
            randomBool = bool(random.randint(0,1))
            randomHashMap[i] = randomBool
            if randomBool : numberOfTure +=1
            else : numberOfFalse += 1
        ommitType = False
        if numberOfFalse < numberOfTure :
            ommitType = True
        elif numberOfTure < numberOfFalse :
            ommitType = False
        times = 0 
        numberTimeToOmit = abs(numberOfFalse-numberOfTure)
        while (times < numberTimeToOmit):
            randomSelected = random.randint(1,size)
            if randomSelected not in ignoreList :
                if randomHashMap[randomSelected] == ommitType :
                    ignoreList.append(randomSelected)
                    times +=1
        return  randomHashMap ,ignoreList
    
    def getExponensialSplittingPoints(self,size):
        splittingPoints = []
        step = int(size/15)
        for i in range(1,4):
            splittingPoints.append(step*i)
        splittingPoints.append(size)
        return splittingPoints
    def makeRandomHashMap(self,size,splittingSize=-1):
        if type(splittingSize) == type([]) :
            splittingPoints = splittingSize
        elif splittingSize == -1 :
            splitPoint = int(size/2)
            splittingPoints = [splitPoint,size]
        else :
            splittingPoints = []
            splitPoint = int(size/splittingSize)
            for i in range(1,splittingSize):
                splittingPoints.append(i*splitPoint)
            splittingPoints.append(size)   
        indexList = []
        randomHashMap = {}
        for i in range(1,size +1):
            indexList.append(i)
        shuffle(indexList)
        spliteType = 0
        splittingPointIndex = 0
        for i in range(0,size):
            randomIndex = indexList[i]
            if splittingPoints[splittingPointIndex]>i:
                spliteType = splittingPointIndex
            else :
                splittingPointIndex +=1
                spliteType = splittingPointIndex
            randomHashMap[randomIndex] = spliteType
        return randomHashMap
    
    def randomQrelsSwapper(self,firstQrelsAddress,secondQrelsAddress):
        firstRelevanceBucket = RelevanceBucket([firstQrelsAddress])
        secondRelevanceBucket = RelevanceBucket([secondQrelsAddress])
        
        firstRandomBucket = RelevanceBucket()
        secondRandomBucket = RelevanceBucket()
        firstNumberQuery = firstRelevanceBucket.getNumberOfQuery()
        secondNumberOfQuery = secondRelevanceBucket.getNumberOfQuery()
        
        totalNumberOfQuery = firstNumberQuery + secondNumberOfQuery
    
        randomList = []
        for i in range(0,totalNumberOfQuery):
            randomList.append(i)
        shuffle(randomList)
        
        for i in range(0,totalNumberOfQuery):
            randomBucket = None 
            relevanceList = None
            if(i<firstNumberQuery):
                randomBucket = firstRandomBucket
            else: randomBucket = secondRandomBucket
            randomNumber = randomList[i]
            if randomNumber < firstNumberQuery:
                relevanceList = firstRelevanceBucket.\
                getRelevancesListBySequencialNumber(randomNumber)
            else :
                relevanceList = secondRelevanceBucket.\
                getRelevancesListBySequencialNumber(randomNumber-firstNumberQuery)
            randomBucket.addRelevanceList(relevanceList)
        return [firstRandomBucket,secondRandomBucket]
        
if __name__ == '__main__':
    #r'/research/remote/debunk/collections/text_collections/TREC/gov2/gov2-corpus'
    targetFolder = r'/home/mohsen/Research/Earlier_TREC/TREC4'
    #targetFolder = r'/home/mohsen/Research/Earlier_TREC/DataForProcessing/TREC7'
    #targetFolder = r'/media/My\ Passport/TREC/wt10g_uncompress'
    #targetFolder = r'/home/mohsen/Research/DATA/wt10g'
    #relevanceFolderAddress = r'/home/mohsen/Research/relevance'
    #relevanceFileAddresses = [r'/home/mohsen/Research/relevance/qrels.401-450.trec8.adhoc']
    relevanceFileAddresses = [r'/home/mohsen/Research/relevance/qrels.201-250.trec4.adhoc']
    
    outPutTarget = r'/home/mohsen/Research/Earlier_TREC/TREC'
    #outPutTarget = r'/home/mohsen/Research/TREC8'
    #outPutTarget = r'/home/mohsen/Research/Earlier_TREC/RandomSplitToFour'
    trec = TRECParser()
    #trec.randomTrecSpliterWithQrls(targetFolder, relevanceFileAddresses, outPutTarget)
    #trec.splitTopLevelDomain(targetFolder,relevanceFileAddresses,outPutTarget)
    #trec.earlierTrecSpliter(targetFolder, relevanceFileAddresses, outPutTarget)
    trec.makeOneCollection(targetFolder, relevanceFileAddresses, outPutTarget, 'TREC4_one_collection')
    #trec.makeNoneRelevantCorpus(targetFolder, relevanceFileAddresses, outPutTarget, 'TREC6')
    #relevance = RelevanceBucket(relevanceFileAddresses)
    #relevanceList = relevance._relevanceDictionaryBaseOnDocID
        
    #trec.sizeAnalysis(targetFolder, 'sizeAna')
    #print first._querNumberList
#    for number in first._querNumberList:
#        print number 
#    for test in first._relevanceDictionaryBaseOnQueryNumber:
#        print test
    #first.writeToFile(destinations)
#    numberOfSplits =2**3
#    outPutTarget = outPutTarget +'_'+str(numberOfSplits)
#    trec.splitBasedOnSizeOfDocuments(targetFolder, relevanceFileAddresses, outPutTarget, numberOfSplits)
###################################
###################################
#####   Calculate the number of relevances in each file
# 
#    targetFolder = r'/home/mohsen/Research/Earlier_TREC/TREC6'
#    outPutTarget = r'/home/mohsen/Research/Earlier_TREC/RelevanceAndRelevanceStatistics'
#    relevanceFileAddresses = [r'/home/mohsen/Research/relevance/qrels.301-350.trec6.adhoc']
#    trec.evaluatedDocumentsStatics(targetFolder, relevanceFileAddresses, outPutTarget,'TREC6')
#    
###################################
###################################
        
    #trec.topLevelDomainStatics(targetFolder,r'ext_w10g_top_level_domain')
    #trec.contentTypesStatics(targetFolder, 'content_types')
    #trec.countNumberOfDocumentsInCollection(targetFolder)
    #trec.randomTrecSpliter(targetFolder, relevanceFileAddresses, outPutTarget, 1 ,2)
        
