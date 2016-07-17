
from mrjob.job import MRJob
import re

WORD_REGEX = re.compile(r"[\w']+")

class WordFrequency(MRJob):
    def mapper(self, _, line):
        words = WORD_REGEX.findall(line)
        for word in words:
            yield word.lower(),1

    
    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    WordFrequency.run()