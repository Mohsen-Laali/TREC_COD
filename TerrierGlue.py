#! /usr/bin/python


import subprocess
import os 
import shutil
import glob 
from tkFont import names
from scipy import stats
import array
from ImImagePlugin import number
from cProfile import label

class CollectionSpecification(object):
    topicsAddress = None
    qrlsAddress = None
    collectionAddress = None
    resultsAddress = None
    
    def __init__(self,topicsAddress, qrlsAddress,collectionAddress,resultsAddress):
        self.topicsAddress = topicsAddress
        self.qrlsAddress = qrlsAddress
        self.collectionAddress = collectionAddress 
        self.resultsAddress = resultsAddress
    

class TerrierGlue(object):
    terrierFolderAddress = None
    evaluateTerierCommand = 'bin'+os.sep+'trec_terrier.sh -e'
    evaluateTerierCommandPerQuery = 'bin'+os.sep+'trec_terrier.sh -e -p'
    indexTerrierCommand = 'bin'+os.sep+'trec_terrier.sh -i'
    retrieveTerrierCommand = 'bin'+os.sep+'trec_terrier.sh -r' 
    builtCollectionCommand = 'bin'+os.sep+'trec_setup.sh'
    terrierPropertiesFileAddress = None
    terrierResultsFolderAddressConfig = 'trec.results='
    terrierRankingFunctionConfig = 'trec.model='
    terrierTopicsConfig = 'trec.topics='
    terrierQrelsConfig = 'trec.qrels='
    "'DFIO','PL2F','BM25F','ML2','MDL2'"
    allRankingSupportedModels = ['BB2','BM25','DFR_BM25','DLH','DLH13',
                                 'DPH','DFRee','Hiemstra_LM','IFB2','In_expB2',
                                 'In_expC2','InL2','LemurTF_IDF','LGD','PL2',
                                 'TF_IDF']
#    allRankingSupportedModels = ['BB2','BM25','DFR_BM25']
    '''  ,'PL2F','BM25F','ML2','MDL2'  '''
                                        
    logCommand = None
    _line = '------------------------'
    _hashLine= '########################'
    
    def __init__(self,terrierFolderAddress,logFileAddress='log'):
        self.terrierFolderAddress = terrierFolderAddress
        self.logCommand = ' >> '+logFileAddress+' 2>&1'
        self.indexTerrierCommand = os.path.join(terrierFolderAddress,
                                self.indexTerrierCommand)
        self.retrieveTerrierCommand = os.path.join(terrierFolderAddress,
                                self.retrieveTerrierCommand)
        self.evaluateTerierCommand = os.path.join(terrierFolderAddress,
                                self.evaluateTerierCommand)
        self.evaluateTerierCommandPerQuery = os.path.join(terrierFolderAddress,
                                            self.evaluateTerierCommandPerQuery)
        self.builtCollectionCommand = os.path.join(terrierFolderAddress,
                                self.builtCollectionCommand)
        self.terrierPropertiesFileAddress =os.path.join(terrierFolderAddress,'etc','terrier.properties')
        
    def _log(self,label):
        command ='echo '+ self._line +label+self._line + self.logCommand
        return command
        
    def indexTerrier(self,log=True,label='index'):
        self._log(label)
        command = str()
        if log :
            command = self.indexTerrierCommand + self.logCommand 
        else :
            command = self.indexTerrierCommand
        print command 
        p = subprocess.Popen(command,shell=True)
        p.communicate()
         
    def evaluateTerrier(self,log=True,label='evaluate',perQuery=False):
        self._log(label)
        command = str()
        evaluatedTerierCommand = None
        if perQuery :
            evaluatedTerierCommand =self.evaluateTerierCommandPerQuery
        else:
            evaluatedTerierCommand =self.evaluateTerierCommand
            
        if log :
            command = evaluatedTerierCommand + self.logCommand 
        else :
            command = evaluatedTerierCommand
        print command
        p = subprocess.Popen(command,shell=True)
        p.communicate()
    
    def retrieveTerrier(self,log=True,label='retrieve'):
        self._log(label)
        command = str() 
        if log :
            command = self.retrieveTerrierCommand+ self.logCommand 
        else :
            command = self.retrieveTerrierCommand

        print command 
        p = subprocess.Popen(command,shell=True)
        p.communicate()
    
    def builtTerrierCollection(self,CollectionFolderAddress,log=True,label='Built collection'):
        self._log(label)
        command = str() 
        if log :
            command = self.builtCollectionCommand + ' ' + CollectionFolderAddress+ self.logCommand
        else :
            command = self.builtCollectionCommand + ' ' + CollectionFolderAddress
        p = subprocess.Popen(command,shell=True)
        p.communicate()
        
    def makeFolder(self,address):
        if not os.path.exists(address): os.makedirs(address)
    
    def renameFolder(self,oldFolderAddress,newFolderAddress):
        if os.path.exists(oldFolderAddress):
            os.rename(oldFolderAddress, newFolderAddress)
            
    def deletFile(self,fileAddress):
        if os.path.exists(fileAddress):
            os.remove(fileAddress)
            
    def builtTerrierProperties(self,topicsAddress,qrlsAddress,rankingFunction,resultsAddress):
        #fileAddress = r'/home/mohsen/workspace/TChpper/TChopper/TerrierGlue/terrier.properties'
        fileAddress = r'/home/mohsen/workspace/TChpper/TChopper/TerrierGlue/terrier.properties(TREC4)'
        copyConfigFile = open(fileAddress,'r')
        configFile = open(self.terrierPropertiesFileAddress,'w')
        configFile.writelines(copyConfigFile.readlines())
        configFile.write(self.terrierTopicsConfig + topicsAddress + os.linesep)
        configFile.write(self.terrierQrelsConfig + qrlsAddress + os.linesep)
        configFile.write(self.terrierRankingFunctionConfig + rankingFunction + os.linesep)
        configFile.write(self.terrierResultsFolderAddressConfig + resultsAddress + os.linesep)
        configFile.flush()
        configFile.close()
        
    def getSize(self,path = '.'):
        total_size = 0
        for dirSpec in os.walk(path):
            dirpath = dirSpec[0]
            filenames = dirSpec[2]
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size
        
    def terrierRoundRobin(self,collectionSpec,log=True,useOldIndex=False,evaluatePerQuery=False):
        #built collection
        if(not useOldIndex):
            self.builtTerrierCollection(collectionSpec.collectionAddress,log)
            self.indexTerrier(log)
        #built Terrier properties 
        for rankFunciton in self.allRankingSupportedModels:
            self.builtTerrierProperties(collectionSpec.topicsAddress,collectionSpec.qrlsAddress, rankFunciton, 
                                        collectionSpec.resultsAddress)
            self.retrieveTerrier(log)
        self.evaluateTerrier(log,perQuery=evaluatePerQuery)
        
    def terrierRoundRobinSpilitedData(self,folderAddress,topicsAddress=None,
                                      log=True ,calculateKendallTau = False,
                                      keepOldIndex=True,withTopic = False,
                                      useOldIndex=False,evaluatePerQuery =False):
        
        indexAddress = os.path.join(self.terrierFolderAddress,'var','index')
        varAddress = os.path.join(self.terrierFolderAddress,'var')
        rootFolderName= folderAddress.split(os.sep).pop()
        madeIndexAddress = os.path.join(self.terrierFolderAddress,'var','index_'+rootFolderName)
        
        if self.getSize(indexAddress)>0 and not useOldIndex:
            oldIndexAdddress =os.path.join(self.terrierFolderAddress,'var','index_old')
            self.renameFolder(indexAddress, oldIndexAdddress)
            self.makeFolder(indexAddress)
            
        resultsAddress= os.path.join(folderAddress,'results')
        self.makeFolder(resultsAddress)
        
        directoryLists = os.listdir(folderAddress)
        directoryLists.sort()
        for directoryName in directoryLists:
            subResultsAddress = os.path.join(resultsAddress,'results_'+directoryName)
            collectionAddress = os.path.join(folderAddress,directoryName,'data')
            size = self.getSize(collectionAddress)
            if size == 0 or directoryName == 'results' : 
                continue 
            qrlsAddress = os.path.join(folderAddress,directoryName,'relevance','relevance')
            if withTopic :
                topicsAddress = os.path.join(folderAddress,directoryName,'topics','topics')
                
            collectionSpec = CollectionSpecification(topicsAddress, qrlsAddress,
                                                      collectionAddress, subResultsAddress)
            self.terrierRoundRobin(collectionSpec,log,useOldIndex,evaluatePerQuery=evaluatePerQuery)
            if not useOldIndex:
                if keepOldIndex :
                    desIndex = os.path.join(madeIndexAddress,'index_'+directoryName)
                    newIndexAddress = os.path.join(varAddress,'index_'+directoryName)
                    self.renameFolder(indexAddress, newIndexAddress)
                    self.makeFolder(desIndex)
                    shutil.move(newIndexAddress, desIndex)
                    self.makeFolder(indexAddress)
                else :
                    shutil.rmtree(indexAddress)
                    self.makeFolder(indexAddress)
        return self.concatenateAllTheResults(resultsAddress,calculateKendallTau)
    
    def calculateKendallTauMatrix(self, listAllData):
        import scipy.stats
        numberOfKanduallTau = len(self.importantDataList)
        firstResults = []
        secondResults = []
        maxIndex = len(listAllData)
        names = []
        for index in range(0, maxIndex):
            tagNameIndex = len(listAllData[index]) - 1
            comparereName = listAllData[index][tagNameIndex]
            names.append(comparereName)
            oneLineResults = []
            oneLineResults.append(comparereName)
            first = []
            second = []
            for compareIndex in range(0, maxIndex):
                for kandallTauTypeNumber in range(0, numberOfKanduallTau):
                    x = listAllData[index][kandallTauTypeNumber]
                    y = listAllData[compareIndex][kandallTauTypeNumber]
                    kandallTau = scipy.stats.kendalltau(x, y)
                    if kandallTauTypeNumber == 0:
                        first.append(kandallTau)
                    else:
                        second.append(kandallTau)
            
            firstResults.append(first)
            secondResults.append(second)
        
        return names, firstResults, secondResults

    def writeKenallTauToSperateFiles(self,names,firstResults,secondResults,folderAddress,testNumber):
        for i in range(0,len(names)):
            for j in range(i+1,len(names)):
                # make name uniform 
                if int(names[i].split('_')[1])<int(names[j].split('_')[1]):
                    firstName = names[i].split('_')[1]+'_'+names[j].split('_')[1]+\
                    '_'+self.importantDataList[0].replace(' ','')
                    secondName = names[i].split('_')[1]+'_'+names[j].split('_')[1]+\
                    '_'+self.importantDataList[1].replace(' ','')
                else :
                    firstName = names[j].split('_')[1]+'_'+names[i].split('_')[1]+\
                      '_'+self.importantDataList[0].replace(' ','')
                    secondName = names[j].split('_')[1]+'_'+names[i].split('_')[1]+\
                      '_'+self.importantDataList[1].replace(' ','')
                secondName = names[i].split('_')[1]+'_'+names[j].split('_')[1]+\
                '_'+self.importantDataList[1].replace(' ','')
                firstFileHandler = open(os.path.join(folderAddress,firstName),'a')
                firstFileHandler.write(str(testNumber)+' '+str(firstResults[i][j][0])+
                                       ' '+str(firstResults[i][j][1])+os.linesep)
                secondFileHandler = open(os.path.join(folderAddress,secondName),'a')
                secondFileHandler.write(str(testNumber)+' '+str(secondResults[i][j][0])+
                                       ' '+str(secondResults[i][j][1])+os.linesep)
                firstFileHandler.flush()
                firstFileHandler.close()
                secondFileHandler.flush()
                secondFileHandler.close()
                                   
    def writeKendallTauToDisk(self, resultsFolderAddress, names, firstResults, secondResults):
        firstResults = self.seprateTauProb(firstResults)[0]
        secondResults = self.seprateTauProb(secondResults)[0]
        resultAddress = os.path.join(resultsFolderAddress, 'KendallTau.txt')
        resultWriter = open(resultAddress, 'w')
        resultWriter.write(self.importantDataList[0] + os.linesep)
        resultWriter.write(os.linesep)
        maxLength = 0
        for name in names:
            if len(name) > maxLength:
                maxLength = len(name)
        
        resultWriter.write(maxLength * ' ' + str(names) + os.linesep)
        index = 0
        for line in firstResults:
            numberOfSpace = maxLength - len(names[index])
            resultWriter.write(names[index] + numberOfSpace * ' ' + str(line) + os.linesep)
            index += 1
        
        resultWriter.write(os.linesep + os.linesep)
        resultWriter.write(self.importantDataList[1] + os.linesep)
        resultWriter.write(os.linesep)
        maxLength = 0
        for name in names:
            if len(name) > maxLength:
                maxLength = len(name)
        
        resultWriter.write(maxLength * ' ' + str(names) + os.linesep)
        index = 0
        for line in secondResults:
            numberOfSpace = maxLength - len(names[index])
            resultWriter.write(names[index] + numberOfSpace * ' ' + str(line) + os.linesep)
            index += 1
            
    def seprateTauProb(self,matrix,roundKendal=2):
        probMatrix = []
        tauMatrix = []
        for line in matrix:
            tauLine =[]
            probLine = []
            for datum in line :
                tauLine.append(round(datum[0],2))
                probLine.append(datum[1])
            probMatrix.append(probLine)
            tauMatrix.append(tauLine)
        return tauMatrix,probMatrix
            
    def calculateKendallTau(self,listAllData,resultsFolderAddress):
        names,firstResults, secondResults = self.calculateKendallTauMatrix(listAllData)     
        self.writeKendallTauToDisk(resultsFolderAddress, names, firstResults, secondResults) 
                                  
    importantDataList = ['Average Precision','Precision at   10']
    def extractSpecialLines(self,fileHandler):
        fileHandler.seek(0)
        data = len(self.importantDataList)*[None]
        #importantDataList = ['Number of queries','Retrieved','Relevant','Relevant retrieved','Average Precision','Precision at   10 ']
        seenTag = 0
        for line in fileHandler:
            for tag in self.importantDataList:
                index = self.importantDataList.index(tag)
                if line.find(tag)!= -1:
                    seenTag += 1
                    datum = float(line.split(':')[1].replace(os.linesep,''))
                    data[index] = datum
            if seenTag == len(self.importantDataList) : break
        return data 
    
    def extractSpecialLinesOld(self,fileHandler):
        fileHandler.seek(0)
        lines = []
        data = len(self.importantDataList)*[None]
        #importantDataList = ['Number of queries','Retrieved','Relevant','Relevant retrieved','Average Precision','Precision at   10 ']
        seenTag = 0
        for line in fileHandler:
            for tag in self.importantDataList:
                index = self.importantDataList.index(tag)
                if line.find(tag)!= -1:
                    seenTag += 1
                    newTag = str(tag)
                    datum = float(line.split(':')[1].replace(os.linesep,''))
                    data[index] = datum
                    newTag = newTag.replace(' ','')
                    newTag = newTag.replace('at','@')
                    line =line.replace(tag,newTag)
                    lines.append(line)
            if seenTag == len(self.importantDataList) : break
        return [lines,data]
    
    def deletDirectory(self,address):
        subprocess.Popen('rm -r '+address,shell=True)
        
    def deletInsideDirectory(self,address):
        listFileOrDirectory = os.listdir(address)
        for fileOrDirectory in listFileOrDirectory:
            path = os.path.join(address,fileOrDirectory)
            if os.path.isdir(path):
                shutil.rmtree(path)
                
    def deletAllFileInsideDirectory(self,address):
        for fileInfo in os.walk(address):
            r = fileInfo[0]
            f = fileInfo[2]
            for files in f:
                os.remove(os.path.join(r,files))
                
    pAt10KendalTauFileName = 'Pat10KendalTau.txt'
    meanAveragePerKendalTauFileName = 'MeanAveragePerKendalTau.txt' 
    
    def randomSplitAndEvaluate(self,topicsAddress,numberOFTime,targetFolder,relevanceFileAddresses,
                            outPutTarget,randomDocLable=1,calculateKendallTau=False,log=False):
        from TChopper import TRECParser

        trecParcer = TRECParser()
        self.makeFolder(outPutTarget)
        outPutFactory = os.path.join(outPutTarget,'factory')
        self.makeFolder(outPutFactory) 
        #self.makeFolder(outPutFactory)
        if calculateKendallTau : 
            pAt10KendalTauFileHandler =    open(os.path.join(outPutTarget,self.pAt10KendalTauFileName),'a')
            meanAveragePerKendalTauFileHandler = open(os.path.join(outPutTarget,self.meanAveragePerKendalTauFileName),'a')
            
        selectedResultsFileHandler = open(os.path.join(outPutTarget,self.selectedResultFileName),'a')
        allResulFileHandler =        open(os.path.join(outPutTarget,self.allResultFileName),'a')
        
        firstTime = True 
        collectionName = str()
        if os.path.exists(os.path.join(outPutTarget,self.pAt10KendalTauFileName)):
            firstTime = False
        
        for i in range(randomDocLable,numberOFTime+randomDocLable):
            trecParcer.randomTrecSpliter(targetFolder,relevanceFileAddresses,
                            outPutFactory,1,randomDocLable=i)
            
            listAllData =self.terrierRoundRobinSpilitedData(outPutFactory, topicsAddress, False, False, False)
            names,firstResults,secondResults =self.calculateKendallTauMatrix(listAllData)
            factoryAllResultFileHandler = open(os.path.join(outPutFactory,'results',self.allResultFileName),'r')
            factorySelectedResultFileHandler =open(os.path.join(outPutFactory,'results',self.selectedResultFileName),'r')

            factorySelectedResultFileHandler.seek(0)
            allResulFileHandler.writelines(factoryAllResultFileHandler.readlines())
            selectedResultsFileHandler.writelines(factorySelectedResultFileHandler.readlines())
            nameInfo = names[0].split('_')
            number = nameInfo[0]
            collectionName = nameInfo[1]+'_'+nameInfo[2]
            
            if firstTime :
                meanAveragePerKendalTauFileHandler.write(collectionName+os.linesep)   
                meanAveragePerKendalTauFileHandler.write(self.importantDataList[0] + os.linesep)
                meanAveragePerKendalTauFileHandler.write(os.linesep)
                pAt10KendalTauFileHandler.write(os.linesep)
                pAt10KendalTauFileHandler.write(self.importantDataList[1] + os.linesep)
                pAt10KendalTauFileHandler.write(os.linesep)
                firstTime = False
            meanAveragePerKendalTauFileHandler.write(number+' '+str(firstResults[0][1][0])+os.linesep)
            pAt10KendalTauFileHandler.write(number+' '+str(secondResults[0][1][0]) + os.linesep)
            
            selectedResultsFileHandler.flush()
            meanAveragePerKendalTauFileHandler.flush()
            pAt10KendalTauFileHandler.flush()
            
            factoryAllResultFileHandler.close()
            factorySelectedResultFileHandler.close()
            self.deletAllFileInsideDirectory(outPutFactory)
            #shutil.rmtree(outPutFactory,'results')
#            shutil.rmtree(outPutFactory)
#            self.makeFolder(outPutFactory)
#            command = r'java -jar /home/mohsen/workspace/TChpper/TChopper/TerrierGlue/deletOrMakeFolder.jar '
#            p = subprocess.Popen(command+outPutFactory+' -d',shell=True)
#            p.communicate()
#            p = subprocess.Popen(command+outPutFactory+' -m',shell=True)
#            p.communicate()
            #self.deletInsideDirectory(outPutFactory)
        if calculateKendallTau:   
            pAt10KendalTauFileHandler.flush()
            pAt10KendalTauFileHandler.close()
            meanAveragePerKendalTauFileHandler.flush()
            meanAveragePerKendalTauFileHandler.close()
                
        allResulFileHandler.flush()
        allResulFileHandler.close()
        selectedResultsFileHandler.flush()
        selectedResultsFileHandler.close()
        
    def randomSplitAndEvaluateWithQrls(self,topicsAddress,numberOFTime,
                                       targetFolder,relevanceFileAddresses,
                            outPutTarget,randomDocLable=1,log=False,equalCorpus=True,equalEvaluatedDocument =True):
        from TChopper import TRECParser
        trecParcer = TRECParser()
        self.makeFolder(outPutTarget)
        outPutFactory = os.path.join(outPutTarget,'factory')
        self.makeFolder(outPutFactory) 
        selectedResultsFileHandler = open(os.path.join(outPutTarget,self.selectedResultFileName),'a')
        allResulFileHandler =        open(os.path.join(outPutTarget,self.allResultFileName),'a')
        for i in range(randomDocLable,numberOFTime+randomDocLable):
            trecParcer.randomTrecSpliterWithQrls(targetFolder, relevanceFileAddresses,
                                                  outPutFactory, randomDocLable=i,
                                                  numberOfSpliting=4,equalCorpus=equalCorpus,
                                                  equalEvaluatedDocument=equalEvaluatedDocument)
            listAllData =self.terrierRoundRobinSpilitedData(outPutFactory, topicsAddress,
                                                             False, False, False)
            names,firstResults,secondResults =self.calculateKendallTauMatrix(listAllData)
            self.writeKenallTauToSperateFiles(names, firstResults, secondResults,outPutTarget,testNumber=i)
            factoryAllResultFileHandler = open(os.path.join(outPutFactory,
                                                            'results',self.allResultFileName),'r')
            factorySelectedResultFileHandler =open(os.path.join(outPutFactory,
                                                                'results',self.selectedResultFileName),'r')
            factorySelectedResultFileHandler.seek(0)
            allResulFileHandler.writelines(factoryAllResultFileHandler.readlines())
            selectedResultsFileHandler.writelines(factorySelectedResultFileHandler.readlines())
            selectedResultsFileHandler.flush()
            allResulFileHandler.flush()
            factoryAllResultFileHandler.close()
            factorySelectedResultFileHandler.close()
            self.deletAllFileInsideDirectory(outPutFactory)
        allResulFileHandler.flush()
        allResulFileHandler.close()
        selectedResultsFileHandler.flush()
        selectedResultsFileHandler.close()   
        
    allResultFileName ='allTheResults.txt'
    selectedResultFileName = 'selectedResult.txt'
    def concatenateAllTheResults(self,resultsFolderAddress,calculateKendallTau =False ):
        allResultFileAddress = os.path.join(resultsFolderAddress,
                                            self.allResultFileName)
        specialResultsFileAddress = os.path.join(resultsFolderAddress,
                                                 self.selectedResultFileName)
        allResultFileHandler = open(allResultFileAddress,'a')
        specialResultsFileHandler = open(specialResultsFileAddress,'a')
        directoryLists = os.listdir(resultsFolderAddress)
        listAllData = []
        
        for directoryName in directoryLists:
            directoryAddress = os.path.join(resultsFolderAddress,directoryName)
            size = self.getSize(directoryAddress)
            if(os.path.isdir(directoryAddress) and size!=0):
                
                allResultFileHandler.write(self._hashLine*3 + os.linesep)
                allResultFileHandler.write(' '*10 + directoryName +os.linesep)
                allResultFileHandler.write(self._hashLine*3 + os.linesep)
                
                os.chdir(directoryAddress)
                
                fileNameList = []
                for fileName in glob.glob('*.eval'):
                    fileNameList.append(fileName)
                fileNameList.sort()
                oneWayData = [[],[],]
                #oneWayData = len(self.importantDataList)*[None]
                rankNamesList = []
                
                for fileName in fileNameList:
                    index = 0 
                    rankFunctionName = fileName.split('.')[0]
                    rankNamesList.append(rankFunctionName)
                    
                    allResultFileHandler.write(self._line+'( '+rankFunctionName+' )'+self._line+os.linesep)
                    
                    fileAddress = os.path.join(directoryAddress,fileName)
                    fileHandler = open(fileAddress,'r')
                    allResultFileHandler.writelines(fileHandler.readlines())
                    
                    data = self.extractSpecialLines(fileHandler)
                    
                    for datum in data :
                        listOfData =oneWayData[index]
                        listOfData.append(datum)
                        #print data ,index , datum , oneWayData[0],oneWayData[1]
                        index += 1   
                    #specialResultsFileHandler.writelines(lines)
                    fileHandler.close()
                    
                specialResultsFileHandler.write(self._line*4 + os.linesep )
                specialResultsFileHandler.write(directoryName.replace('results_','')
                                                    +os.linesep)
                for i in range(0,len(self.importantDataList)):
                    specialResultsFileHandler.write(self.importantDataList[i].replace(' ','_')
                                                    +os.linesep)
                    for j in range(0,len(rankNamesList)) :
                        specialResultsFileHandler.write(rankNamesList[j]
                                                        +' '+str(oneWayData[i][j])+os.linesep)
                    specialResultsFileHandler.write(os.linesep)
                if (len(oneWayData[0]) != 0):
                    oneWayData.append(directoryName.replace('results_',''))
                    listAllData.append(oneWayData) 
                    
        allResultFileHandler.close()
        specialResultsFileHandler.close()
                  
        if(calculateKendallTau):
            self.calculateKendallTau(listAllData,resultsFolderAddress)
        else:
            return listAllData   
    averageFileName = 'avarage.txt'
      
    def randomQrelSwapAndEvaluate(self,targetFolder,firstQrelsAddress,secondQrelsAddress,numberOFTime
                            ,randomDocLable=1,calculateKendallTau=False,log=False):
        from TChopper import TRECParser 
        import scipy.stats
        trecParcer = TRECParser()
        listOfDirectory = os.listdir(targetFolder)
        corpusAddress = str() 
        for directory in listOfDirectory:
            corpusAddress = os.path.join(targetFolder,directory)
            if directory !='results' and os.path.isdir(corpusAddress):
                break 
        resultsPath = os.path.join(targetFolder,'results')
        self.makeFolder(resultsPath)
        averageFileHandler = open(os.path.join(resultsPath,self.averageFileName),'a')
        if calculateKendallTau : 
            pAt10KendalTauFileHandler =    open(os.path.join(resultsPath,self.pAt10KendalTauFileName),'a')
            meanAveragePerKendalTauFileHandler = open(os.path.join(resultsPath,self.meanAveragePerKendalTauFileName),'a')
        if os.path.isdir(corpusAddress):      
            for i in range(randomDocLable,numberOFTime+randomDocLable):
                firstQerlsBucket,secondQerlsBuket = trecParcer.randomQrelsSwapper(firstQrelsAddress, secondQrelsAddress)
                relvanceAdrees = os.path.join(corpusAddress,'relevance','relevance')
                firstQerlsBucket.writeToFile(relvanceAdrees)
                fListAllData =self.terrierRoundRobinSpilitedData(targetFolder,log=log,calculateKendallTau=False,
                                                               keepOldIndex=True, withTopic=True,useOldIndex=True)
                subResultsPath = str() 
                for directory in os.listdir(resultsPath):
                    subResultsPath = os.path.join(resultsPath,directory)
                    if os.path.isdir(subResultsPath):
                        self.deletAllFileInsideDirectory(subResultsPath)
                
                relvanceAdrees = os.path.join(corpusAddress,'relevance','relevance')
                secondQerlsBuket.writeToFile(relvanceAdrees)
                sListAllData =self.terrierRoundRobinSpilitedData(targetFolder, log=log,calculateKendallTau=False,
                                                                 keepOldIndex=True, withTopic=True,useOldIndex=True)
                for directory in os.listdir(resultsPath):
                    subResultsPath = os.path.join(resultsPath,directory)
                    if os.path.isdir(subResultsPath):
                        self.deletAllFileInsideDirectory(subResultsPath)
                meanKendalTau = scipy.stats.kendalltau(fListAllData[0][0],sListAllData[0][0])
                pAt10KendalTau = scipy.stats.kendalltau(fListAllData[0][1],sListAllData[0][1])
                meanAveragePerKendalTauFileHandler.write(str(i)+' '+
                                        str(meanKendalTau[0])+' '+str(meanKendalTau[1])+os.linesep)
                
                pAt10KendalTauFileHandler.write(str(i)+' '+str(pAt10KendalTau[0])+' '+
                                                str(pAt10KendalTau[1])+os.linesep)
                acumulator = float()
                for num in fListAllData[0][0] :
                    acumulator += num
                avarage = acumulator/len(fListAllData[0][0])
                averageFileHandler.write(str(i)+'a'+' '+str(avarage)+os.linesep)
                acumulator = float()
                for num in sListAllData[0][0] :
                    acumulator += num
                avarage = acumulator/len(sListAllData[0][0])
                averageFileHandler.write(str(i)+'b'+' '+str(avarage)+os.linesep)
                pAt10KendalTauFileHandler.flush()
                meanAveragePerKendalTauFileHandler.flush()
                averageFileHandler.flush()
            averageFileHandler.flush()
            averageFileHandler.close()
            if calculateKendallTau:   
                pAt10KendalTauFileHandler.flush()
                pAt10KendalTauFileHandler.close()
                meanAveragePerKendalTauFileHandler.flush()
                meanAveragePerKendalTauFileHandler.close()
                
    def t_test(self,rootFolder,fileAddressResults,fileAddressPerQuery,label=''):
        
        fileAddressResults = os.path.join(rootFolder,fileAddressResults)
        fileAddressPerQuery = os.path.join(rootFolder,fileAddressPerQuery)
        fileHandler = open(fileAddressPerQuery,'r+')
        results = {}
        corpusName = None
        rankNames = None
        run= None
        rankModelRun = None
        
        line = fileHandler.readline()
        while(line):
            if line.count("#"):
                corpusName =fileHandler.readline().split("_")[1].strip()
                run = Run(corpusName)
                results[corpusName]= run
                
                fileHandler.readline()
            elif line.count("-"):
                rankNames = line.split("(")[1].split(")")[0].strip()
                rankModelRun = RankModelRun(rankNames)
                run.addRankModelRun(rankModelRun)
            else :
                averagePrecision = float(line.split(" ")[1].replace(os.linesep,""))
                queryNumber = int(line.split(" ")[0].strip())
                rankModelRun.addPerQueryEvaluate(queryNumber, averagePrecision)
            line = fileHandler.readline()
        fileHandler.close()
        self.allResultsParsser(fileAddressResults, results)
        
        allTTestResults = {}
        resultsAddress = os.path.join(rootFolder,label+'_TTest_results.txt')
        resultsFile = open(resultsAddress,'w+')
        for run in results.itervalues():
            resultsFile.write('**********************'+os.linesep)
            resultsFile.write('corpus name: '+str(run)+os.linesep)
            resultsFile.write('**********************'+os.linesep)
            TTestPairs = []
            allTTestResults[run.getCorpusName()] = TTestPairs
            n = 0 
            #sorted(run,key=lambda rankModel: rankModel.getRankModelName())
            for rankModelRun1 in run:
                n +=1
                insideRun = run.getRankModelRuns().values()
                insideRun = insideRun[n:]
                for rankModelRun2 in insideRun: 
                    ttestRun = TTestRun(run.getCorpusName(),rankModelRun1,rankModelRun2)
                    TTestPairs.append(ttestRun)
                    resultsFile.write(str(ttestRun)+os.linesep)       
        resultsFile.close()
        
        resultsInTableAddress = os.path.join(rootFolder,label+'_TTest_resultsInTabel.txt')
        fileHandlerResultTabel = open(resultsInTableAddress,'w+')
        ranksSignificant = {}
        header = 'RA & RB / Condition, RA>RB Significant TTest,RB>RA Significant TTest,'
        header += 'RA>RB Insignificant TTest,RB>RA Insignificant TTest'
        for key in allTTestResults.iterkeys():
            fileHandlerResultTabel.write(key+ os.linesep)
            # write header of of table 
            fileHandlerResultTabel.write(header+os.linesep)
            testList = allTTestResults[key]
            #sorted(testList, key=lambda tTestRun: tTestRun.getRankNamesUnderTest()) 
            #print testList
            for ttest in testList:
                rankNames  = ttest.getRankNamesUnderTest() 
                row = rankNames + ','
#                + \
#                '('+ str(ttest.getMAPAMAPB())+'_' +str(ttest.calculateTTest()[1])+')'
                rowsForSameRank = ranksSignificant.get(rankNames,[0,0,0,0])
                MAPA,MAPB = ttest.getMAPAMAPB()
                if ttest.testIsSignificant():
                    if MAPA>MAPB:
                        rowsForSameRank[0] = rowsForSameRank[0]+1
                        row +='yes,no,no,no' 
                    else:
                        rowsForSameRank[1] = rowsForSameRank[1]+1
                        row +='no,yes,no,no'
                else:
                    if MAPA>MAPB:
                        rowsForSameRank[2] = rowsForSameRank[2]+1
                        row += 'no,no,yes,no'
                    else:
                        rowsForSameRank[3] = rowsForSameRank[3]+1
                        row += 'no,no,no,yes'
                fileHandlerResultTabel.write(row+os.linesep)
                ranksSignificant[rankNames] = rowsForSameRank
               
        fileHandlerResultTabel.write('All results'+os.linesep)
        fileHandlerResultTabel.write(header+os.linesep)
        
        for testName in ranksSignificant.iterkeys():
            row = testName 
            rowNumber = ranksSignificant[testName]
            for numberOfTime in rowNumber:
                row+=','+str(numberOfTime) 
            fileHandlerResultTabel.write(row + os.linesep)
        fileHandlerResultTabel.close()
        return ranksSignificant
    
    def calculateAndCombineAllTTest(self,rootFolder,fileNamePairs):
        allRanksSignificant  = {} 
        for pair in fileNamePairs:
            fileAddressResults,fileAddressPerQuery = pair
            fileAddressResults = os.path.join(rootFolder,fileAddressResults)
            fileAddressPerQuery = os.path.join(rootFolder,fileAddressPerQuery)
            ranksSignificant = self.t_test(rootFolder,fileAddressResults, fileAddressPerQuery)
            for testName in ranksSignificant.iterkeys():
                allTestRow = allRanksSignificant.get(testName,[0,0,0,0])
                testRow = ranksSignificant.get(testName)
                i = 0
                for n in testRow:  
                    allTestRow[i] = allTestRow[i]+n
                    i = i+1
                allRanksSignificant[testName] =allTestRow
        resultAddress = os.path.join(rootFolder,'combineResults.txt')
        fileHandeler = open(resultAddress,'w+')
        
        header = 'RA & RB / Condition, RA>RB Significant TTest,RB>RA Significant TTest,'
        header += 'RA>RB Insignificant TTest,RB>RA Insignificant TTest'
        fileHandeler.write(header+os.linesep)
        for testName in allRanksSignificant.iterkeys():
            row = testName 
            rowNumber = allRanksSignificant[testName]
            for numberOfTime in rowNumber:
                row+=','+str(numberOfTime) 
            fileHandeler.write(row + os.linesep)
        fileHandeler.close()
          
        
    def allResultsParsser(self,fileAddressResults,results):
        fileHandler = open(fileAddressResults,'r+')
        corpusName = None
        rankName = None
        run= None
        rankModelRun = None
        line = fileHandler.readline()
        while(line):
            if line.count("#"):
                corpusName =fileHandler.readline().split("_")[1].strip()
                run =results[corpusName]         
                fileHandler.readline()
            elif line.count("-"):
                rankName = line.split("(")[1].split(")")[0].strip()
                rankModelRun = run.getRankModel(rankName)        
            elif line.count("Average Precision") :
                averagePrecision = float(line.split(":")[1].replace(os.linesep,""))
                rankModelRun.setAveragePrecision(averagePrecision)
            line = fileHandler.readline()
        fileHandler.close()
 
class RankModelRun():
    _rankModelName = None
    _perQueryEvaluate = None 
    _averagePrecision = None 
    
    def __init__(self,rankModelName):
        self._rankModelName = rankModelName.strip()
        self._perQueryEvaluate = {}
        self._perQueryEvaluate.values()
    
    def __str__(self):
        return self._rankModelName
    
    def __iter__(self):
        return iter(self._perQueryEvaluate.iteritems())
        
    def getAveragePrecision(self):
        return self._averagePrecision
    
    def setAveragePrecision(self,averagePrecision):
        self._averagePrecision = averagePrecision 
    
    def getRankModelName(self):
        return self._rankModelName
    
    def sameRankModelName(self,rankModelRun):
        if rankModelRun.getRankModelName() == self.getRankModelName():
            return True
        else:
            return False
    
    def getPerQueryEvalute(self):
        return self._perQueryEvaluate
    
    def getResultsValue(self):
        return self._perQueryEvaluate.values() 
    
    def addPerQueryEvaluate(self,queryNumber,averagePrecision):
        self._perQueryEvaluate[queryNumber] = averagePrecision
        
    def getMeanAveragPrecision(self):
        total = 0
        for meanAve in self._perQueryEvaluate.itervalues():
            total += meanAve
        return total/float(len(self._perQueryEvaluate))
        
class Run():
    _corpusName= None
    _rankModelRuns = None
    def __init__(self,corpusName):
        self._corpusName = corpusName.strip()
        self._rankModelRuns = {}
    
    def __str__(self):
        return self._corpusName
    
    def __iter__(self):
        return self._rankModelRuns.itervalues()
        
    def addRankModelRun(self,rankModelRun):
        self._rankModelRuns[rankModelRun.getRankModelName()]=rankModelRun
        
    def getRankModelRuns(self):
        return self._rankModelRuns
    
    def getRankModel(self,rankModelName):
        return self._rankModelRuns[rankModelName.strip()]
    
    def getCorpusName(self):
        return self._corpusName 
    
class TTestRun:
    import scipy.stats
    _corpusName = None
    _rankModelRunA = None
    _rankModelRunB = None
    _testTest = None 
    _significantThreshold = 0.05
    
    def __init__(self,corpusName,rankModelRunA,rankModelRunB):
        self._corpusName = corpusName
        if rankModelRunA.getRankModelName()< rankModelRunB.getRankModelName():
            rankModelRunA,rankModelRunB = rankModelRunB,rankModelRunA
            
        self._rankModelRunA = rankModelRunA
        self._rankModelRunB = rankModelRunB
        
    
    def __str__(self):
        ttest,pValue = self.calculateTTest()
        return (str(self._rankModelRunA)+ " " 
                + str(self._rankModelRunB) + " T test " 
                +str(ttest) +" Pvalue"+str(pValue) )
    def __repr__(self):
                return repr(self.getRankNamesUnderTest())

    def calculateTTest(self):
        if self._testTest == None:
            self._testTest =stats.ttest_rel(self._rankModelRunA.getResultsValue(),
                               self._rankModelRunB.getResultsValue())
        return self._testTest
        
    def getCorpusName(self):
        return self._corpusName
    
    def getTTestRanksName(self):
        return (self._rankModelRunA.getRankModelName()+ ' ' 
                + self._rankModelRunB.getRankModelName())
        
    def getTTestRankModelRuns(self):
        return [self._rankModelRunA,self._rankModelRunB]
    
    def onSameRankModels(self,tTest):
        passedRankModel1,passedRankModel2 = self.getTTestRankModelRuns()
        if ((passedRankModel1.sameRankModelName(self._rankModelRunA) and 
            passedRankModel2.sameRankModelName(self._rankModelRunB))or
            (passedRankModel1.sameRankModelName(self._rankModelRunB) and 
            passedRankModel2.sameRankModelName(self._rankModelRunA))):
            return True
        return False 
    
    def getMAPAMAPB(self):
        return [self._rankModelRunA.getAveragePrecision(),
                self._rankModelRunB.getAveragePrecision()]
        
    def testIsSignificant(self):
        ttest = self.calculateTTest()
        if ttest[1] > self._significantThreshold:
            return False
        return True 
    
    def getRankNamesUnderTest(self):
        firstRankName = self._rankModelRunA.getRankModelName()
        secondRankName = self._rankModelRunB.getRankModelName()
        return firstRankName+' & '+secondRankName
    
if __name__ == '__main__':
    #dividedCollectionsAddress = r'/home/mohsen/Research/WT2G_Top_Level_Domain'
    #dividedCollectionsAddress = r'/home/mohsen/Research/Earlier_TREC/DataForProcessing'
    collectionAddress = r'/home/mohsen/Research/Earlier_TREC/DataForProcessing'
    qrlsAddress = r'/home/mohsen/Research/relevance/qrels.401-450.trec8.small_web'
    #qrlsAddress = r'/home/mohsen/Research/relevance/qrels.401-450.trec8.small_web'
    #qrlsAddress = r'/home/mohsen/Research/relevance/qrels.151-200.trec3.adhoc'
    #topicsAddress = r'/home/mohsen/Research/topics/topics'
    terrierAddress = r'/home/mohsen/Research/terrier-3.5'
    #resultsAdress = os.path.join(terrierAddress,'var','results')
    #collectionSpec = CollectionSpecification(topicsAddress, qrlsAddress, collectionAddress,resultsAdress)
    #re = os.path.join(dividedCollectionsAddress,'results')
    glue = TerrierGlue(terrierAddress)
    #glue.terrierRoundRobin(collectionSpec,False)
    #withTopic = True
    #keepIndex = False
    #KandulTau = True
    #glue.terrierRoundRobinSpilitedData(collectionAddress,None,False,KandulTau,keepIndex,withTopic)
    #resultsAddress = dividedCollectionsAddress + os.sep + 'results'
    #glue.concatenateAllTheResults(re,True)
    
    numberOFTime = 1
    topicsAddress = r'/home/mohsen/Research/topics/topics.201-250'
    targetFolder = r'/home/mohsen/Research/Earlier_TREC/TREC'
    #targetFolder = r'/home/mohsen/Research/Earlier_TREC/DataForProcessing'
    relevanceFileAddresses =[r'/home/mohsen/Research/relevance/qrels.201-250.trec4.adhoc']
    randomDocLable= 100
    log = False 
    equalCorpus=False
    evaluatePerQuery = True          
    calculateKendallTau = False    
    equalEvaluatedDocument =True
    outPutTarget = r'/home/mohsen/Research/Earlier_TREC/test'
#    glue.randomSplitAndEvaluate(topicsAddress,numberOFTime,targetFolder,relevanceFileAddresses,
#                            outPutTarget,randomDocLable,calculateKendallTau,log)
#    glue.terrierRoundRobinSpilitedData(targetFolder, topicsAddress, log, calculateKendallTau =calculateKendallTau,
#                                        keepOldIndex=False, withTopic=False, 
#                                        useOldIndex=False,evaluatePerQuery=evaluatePerQuery)
#    glue.randomSplitAndEvaluateWithQrls(topicsAddress, numberOFTime, targetFolder, relevanceFileAddresses,

#                                 outPutTarget, randomDocLable ,log,equalCorpus,equalEvaluatedDocument)
    #######################
    ###  T test 
    #######################  
    rootFolder = r'/home/mohsen/Research/Earlier_TREC/TREC4_8_results_one_colleciton'
    label = 'TREC4'
    allResultFileAddressPerQuery = 'TREC4_ResultsP.txt'
    allResultFileAddressAVP = 'TREC4_Results.txt'
    glue.t_test(rootFolder,allResultFileAddressAVP,allResultFileAddressPerQuery,label)
    fileNamePairs = [['TREC8_Results.txt','TREC8_ResultsP.txt'],['TREC7_Results.txt','TREC7_ResultsP.txt'],
                     ['TREC6_Results.txt','TREC6_ResultsP.txt'],['TREC5_Results.txt','TREC5_ResultsP.txt'],
                     ['TREC4_Results.txt','TREC4_ResultsP.txt']]
#    glue.calculateAndCombineAllTTest(rootFolder, fileNamePairs)
    #######################
    ########################
    #### Query Swap
    ########################
#    numberOFTime = 1000
#    firstQrelsAddress = r'/home/mohsen/Research/Earlier_TREC/Relevance/relevance1'
#    secondQrelsAddress = r'/home/mohsen/Research/Earlier_TREC/Relevance/relevance2'
#    targetFolder = r'/home/mohsen/Research/Earlier_TREC/DataForProcessing'
#    relevanceFileAddresses =[r'/home/mohsen/Research/relevance/qrels.401-450.trec8.adhoc']
#    randomDocLable= 1
#    calculateKendallTau = True
#    log = False 
#    outPutTarget = r'/home/mohsen/Research/Earlier_TREC/randomFactory'
#    glue.randomQrelSwapAndEvaluate(targetFolder, firstQrelsAddress, secondQrelsAddress, 
#                                   numberOFTime, randomDocLable, calculateKendallTau, log)
    #########################
        