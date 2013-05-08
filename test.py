'''
Created on Aug 8, 2012

@author: mohsen
'''
from TChopper import RelevanceBucket
from TChopper import TRECParser
if __name__ == '__main__':
    targetFolder = r'/home/mohsen/Research/Earlier_TREC/RandomSplitToFour/1_1/data'
    tParser = TRECParser();
    print tParser.countNumberOfDocumentsInCollection(targetFolder, False)
    
