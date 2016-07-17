import re

from mrjob.job import MRJob
from mrjob.step import MRStep


WORD_REGEX = re.compile(r"[\w']+")

class WordFrequencySorted(MRJob):
    
    def steps(self):
        return[
               MRStep(mapper = self.mapper_get_words,
                      reducer = self.reduce_count_words),
               MRStep(mapper = self.mapper_make_counts_key,
                      reducer = self.reduce_output_words)
               ]
    
    def mapper_get_words(self, _, line):
        words = WORD_REGEX.findall(line)
        
        for word in words:
            word = word.encode('utf-8')      #avoids issues in mrjob 5.0
            yield word.lower(), 1
            
    def reduce_count_words(self, word, values):
        yield word, sum(values)
        
    def mapper_make_counts_key(self, key, count):
        yield '%04d'%int(count),key             #to append 4 0's before the count to make it a string
        
    def reduce_output_words(self, key, words):
        for word in words:
            yield key, word
        

if __name__ == "__main__":
    WordFrequencySorted.run()