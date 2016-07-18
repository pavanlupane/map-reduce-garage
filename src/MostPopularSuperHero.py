from mrjob.job import MRJob
from mrjob.step import MRStep



class MostPopularSuperHero(MRJob):
    
    def configure_options(self):
        super(MostPopularSuperHero,self).configure_options()
        self.add_file_option('--names', help='Path to Marvel-names.txt')
    
    
    def steps(self):
        return [
                MRStep(mapper = self.mapper_superhero_to_friends,
                        reducer = self.reducer_combine_friends_of_hero),
                MRStep(mapper = self.mapper_prep_for_sort,
                        mapper_init = self.prep_for_names_mapper,
                        reducer = self.reducer_find_max_friends)
                ]
    
    def mapper_superhero_to_friends(self, _, line):
        charIDs = line.split()
        superHero = charIDs[0]
        friendsCount = len(charIDs) - 1
        yield int(superHero), int(friendsCount)
        
    def reducer_combine_friends_of_hero(self, superHeroId, friendsCnt):
        yield superHeroId, sum(friendsCnt)
        
    def mapper_prep_for_sort(self, heroID, friendCounts):
        heroName = self.heroNames[heroID]
        yield None, (friendCounts, heroName)
        
    def reducer_find_max_friends(self, key, value):
        yield max(value)
        
    def prep_for_names_mapper(self):
        self.heroNames = {}
        
        with open("Marvel-names.txt",encoding = "ISO-8859-1") as f:
            for line in f:
                fields = line.split('"')
                heroID = int(fields[0])
                self.heroNames[heroID] = fields[1]

if __name__ == "__main__":
    MostPopularSuperHero.run()