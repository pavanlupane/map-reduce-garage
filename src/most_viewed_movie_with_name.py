from mrjob.job import MRJob
from mrjob.step import MRStep


class MostViwedMovieName(MRJob):
    
    def configure_options(self):
        super(MostViwedMovieName, self).configure_options()
        self.add_file_option('--items', help='../ml-100k/u.ITEM')
    
    def steps(self):
        return [
                MRStep(mapper = self.mapper_movies_to_count,
                       reducer_init = self.reducer_init,
                       reducer = self.reducer_movies_to_totalCount),
                MRStep(reducer = self.reducer_final_output)
                ]
    
    def reducer_init(self):
        self.movieNameMap = {}
        
        with open("u.ITEM",encoding = "ISO-8859-1") as f:
            for line in f:
                fields = line.split('|')
                self.movieNameMap[fields[0]] = fields[1]
    
    
    def mapper_movies_to_count(self, key, line):
        (userId,movieId,rating, timeStamp) = line.split('\t')
        yield movieId, 1
    
    def reducer_movies_to_totalCount(self, key, count):
        yield None, (sum(count),self.movieNameMap[key])
        
    def reducer_final_output(self, _, tuples):
        yield max(tuples)


if __name__ == "__main__":
    MostViwedMovieName.run()