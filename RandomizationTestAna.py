from numpy.lib.scimath import sqrt

class RandomizationTest():
    _listOfNumber = []
    _avarage = None 
    
    def __init__(self,listOfNumber):
        self._listOfNumber = listOfNumber
        
    def calculateAverage(self):
        if self._avarage != None :
            return self._avarage
        totalNumberOfList = len(self._listOfNumber)
        average = None
        if totalNumberOfList != 0:
            average = sum(self._listOfNumber) / float(totalNumberOfList)
        self._avarage = average
        return average
    
    def _DistanceFromStartAndEnd(self,startNumber,endNumber):
        startCount = 0
        endCount = 0
#        print "start " + str(startNumber)
#        print "end " + str(endNumber)
        for number in self._listOfNumber:
            if number < startNumber :
                startCount += 1
            if number > endNumber :
                endCount += 1
        return [startCount,endCount]
        
    def RanodomiztionTest(self,testNumber):
        startNumber = 0
        endNumber = 0
        average = self.calculateAverage()
        onePairFromStart = False
        if testNumber > average :
            endNumber = testNumber
            distanceToAverage = endNumber - average
            startNumber = endNumber - 2* distanceToAverage
        else :
            onePairFromStart = True
            startNumber = testNumber
            distanceToAverage = average - startNumber
            endNumber = startNumber + 2* distanceToAverage
        startCount,endCount = self._DistanceFromStartAndEnd(startNumber, endNumber)
        listSize = float(len(self._listOfNumber))
        twoPair = (startCount+1)/(listSize+1) + (endCount+1)/(listSize+1)
        if onePairFromStart :
            onePair = (startCount+1)/(listSize+1)
        else :
            onePair = (endCount+1)/(listSize+1)
        return [onePairFromStart,listSize,startCount,endCount,onePair,twoPair]
    
    def NormalDisStarEndStanDiviation(self):
        average = self.calculateAverage()
        standardDiviation = float()
        for number in self._listOfNumber:
            standardDiviation += (average - number) **2
        standardDiviation = standardDiviation/len(self._listOfNumber)
#        print('average '+str(average))
#        print ('variance '+ str(standardDiviation))
        standardDiviation = sqrt(standardDiviation)
        return [standardDiviation,average-3*standardDiviation,average+3*standardDiviation]
import os
class IO():
    targetFolderAddress = None
    fileName = "MeanAveragePerKendalTau.txt" 
    resultFileName = "randomizationTestStatistics.txt"
    
    def __init__(self,targetFolderAddress):
        self.targetFolderAddress = targetFolderAddress
        
    def calculateRancomizationTest(self):
        resultFile = open(os.path.join(self.targetFolderAddress,self.resultFileName),"w")
        dirList = os.listdir(self.targetFolderAddress)
        resultFile.write("folderName testNumber average standardDiviation start end FromStart listSize startCount endCount onePair twoPair >0.01 >0.05"+os.linesep)
        for folderName in dirList:
            if not os.path.isdir(os.path.join(self.targetFolderAddress,folderName)):continue
            testNumber= float(raw_input("What is a test number for "+folderName+"?"+os.linesep))
            numbersList = []
            
            fileAddress = os.path.join(self.targetFolderAddress,
                                       folderName,self.fileName)
            
            fileHandler = open(fileAddress,"r")
            for line in fileHandler:
                numbersList.append(float(line.split()[1]))
            
            randomiztionTest = RandomizationTest(numbersList)
            average = randomiztionTest.calculateAverage()
                
            standardDiviation, start,end = randomiztionTest.NormalDisStarEndStanDiviation()
            onePairFromStart,listSize,startCount,endCount,onePair,twoPair = randomiztionTest.RanodomiztionTest(testNumber)
            
            onePair = round(float(onePair),5)
            twoPair = round(float(twoPair),5)
            bigerThan0Dot01 = str()
            bigerThan0Dot1 = str()
            
            if onePair< 0.01:
                bigerThan0Dot01 = 'Yes'
            else: 
                bigerThan0Dot01 = 'No'
                
            if onePair< 0.05:
                bigerThan0Dot1 = 'Yes'
            else:
                bigerThan0Dot1 = 'No'
            
            resultFile.write(folderName+" "+str(testNumber)+" "+str(average)+" "+str(standardDiviation)
                             +" "+str(start)+" "+str(end)+" "+str(onePairFromStart)+" "+str(listSize)+" "+str(startCount)
                             +" "+str(endCount)+" "+str(onePair)+" "+str(twoPair)+" "+
                             bigerThan0Dot01+" "+bigerThan0Dot1+" "+os.linesep)
            
            resultFile.flush()
            fileHandler.flush()
            fileHandler.close()
            
        resultFile.flush()
        resultFile.close() 
         
    def moreThan0Dot05(self,inputFileAddress,outputFileAddress):
        inputFileHandler = open(inputFileAddress,'r+')
        outputFileHandler = open(outputFileAddress,'w+')
        for line in inputFileHandler:
            lineList = []
            lineList = line.split()
            onePair = lineList[10]
            outPutOk = False
            try:
                onePairInt =round(float(onePair),5)
                onePair =str(onePairInt)
                if onePairInt <= 0.01:
                    lineList.append("Yes")
                else: 
                    lineList.append("No")
                if onePairInt <= 0.05:                    
                    lineList.append("Yes")
                    outPutOk = True
                else: 
                    lineList.append("No")
            except:
                lineList.append("<0.01")
                lineList.append("<0.05")
                pass
            lineList[10]= onePair
            outLine = str()
            for each in lineList:
                outLine = outLine+ each + " "
            if outPutOk :
                outputFileHandler.write(outLine+os.linesep)
            
    def roundAndCheckValue(self,inputFileAddress,outputFileAddress):
        inputFileHandler = open(inputFileAddress,'r+')
        outputFileHandler = open(outputFileAddress,'w+')
        for line in inputFileHandler:
            lineList = []
            lineList = line.split()
            onePair = lineList[10]
            twoPair = lineList[11]
            try:
                onePairInt =round(float(onePair),5)
                twoPairInt =round(float(twoPair),5)
                onePair =str(onePairInt)
                twoPair =str(twoPairInt)
                if onePairInt <= 0.01:
                    lineList.append("Yes")
                else: 
                    lineList.append("No")
                if onePairInt <= 0.05:
                    lineList.append("Yes")
                else: 
                    lineList.append("No")
            except:
                lineList.append("<0.01")
                lineList.append("<0.05")
                pass
            lineList[10]= onePair
            lineList[11]= twoPair
            outLine = str()
            for each in lineList:
                outLine = outLine+ each + " "
            print outLine 
            outputFileHandler.write(outLine+os.linesep)
            
if __name__ == '__main__':
    io = IO(r"/home/mohsen/Research/TREC6")
    io.calculateRancomizationTest()
#    inputFileAddress =r"/media/My Passport/Results/TREC 4-8 Randomization Test/TREC2004Robust/randomizationTestStatisticsb.txt"
#    outputFileAddress = r"/media/My Passport/Results/TREC 4-8 Randomization Test/TREC2004Robust/r.txt"
#    io.roundAndCheckValue(inputFileAddress, outputFileAddress)
#    io.moreThan0Dot05(inputFileAddress, outputFileAddress)