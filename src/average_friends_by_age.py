from mrjob.job import MRJob

class AvgFriendsByAge(MRJob):
    def mapper(self, key, line):
        (userId, name, age, numberOfFriends) = line.split(',')
        yield age, float(numberOfFriends)
        
    def reducer(self, age, numberOfFriends):
        totalCount = 0
        numElements = 0
        
        for each in numberOfFriends:
            totalCount += each
            numElements += 1 
        
        yield age, totalCount/numElements
         
if __name__ == "__main__":
    AvgFriendsByAge.run()