from mrjob.job import MRJob

class MinTempFinder(MRJob):
    
    def getTemperatureInDegrees(self, data):
        celcius = float(data)/10.0
        fahrenheit = celcius * 1.8 + 32.0
        return fahrenheit
    
    def mapper(self, _, line):
        (location, year, category, data, a, s, d,f) = line.split(',')
        
        if(category == 'TMIN'):
            temperature_in_degrees = self.getTemperatureInDegrees(data)
            yield location,temperature_in_degrees
            
    def reducer(self, location, minTemp):
        yield location, min(minTemp)
        

if __name__ == "__main__":
    MinTempFinder.run()