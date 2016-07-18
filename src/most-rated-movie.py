from mrjob.job import MRJob
from mrjob.step import MRStep


class MostRatedMovie(MRJob):
    
    def steps(self):
        return [
                MRStep(mapper = self.mapper_movies_to_count,
                      reducer = self.reducer_movies_to_totalCount),
                MRStep(reducer = self.reducer_final_output)
                ]
    
    def mapper_movies_to_count(self, key, line):
        (userId,movieId,rating, timeStamp) = line.split('\t')
        yield movieId, 1
    
    def reducer_movies_to_totalCount(self, movieId, count):
        yield None, (sum(count),movieId)
        
    def reducer_final_output(self, _, tuples):
        yield max(tuples)


if __name__ == "__main__":
    MostRatedMovie.run()