from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class Node:
    def __init__(self):
        self.characterID = ''
        self.connections = []
        self.distance = 9999
        self.color = 'WHITE'
        
    def readLine(self,line): 
        fields = line.split('|')
        if(len(fields) == 4):
            self.characterID = fields[0]
            self.connections = fields[1].split(',')
            self.distance = int(fields[2])
            self.color = fields[3]
            
    def getLine(self):
        conn = ','.join(self.connections)
        return '|'.join( (self.characterID,conn,str(self.distance),self.color) )

class MRBFSDegreeOfSeparation(MRJob):
    
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def configure_options(self):
        super(MRBFSDegreeOfSeparation,self).configure_options()
        self.add_passthrough_option("--target", help="Character ID we are searching for")
        
    def mapper(self, _, line):
        node = Node()
        node.readLine(line)
        
        if(node.color == 'GREY'):
            for conn in node.connections:
                tnode = Node()
                tnode.characterID = conn
                tnode.distance = node.distance + 1
                tnode.color = 'GREY'
                if(conn == self.options.target):
                    counterName = ("Target ID "+ conn + " was found at a distance of ::" + str(tnode.distance))
                    self.increment_counter('Degree of Separation', counterName, 1)
                
                yield conn, tnode.getLine()
            
#             since we have processed this node
            node.color = 'BLACK'
        yield node.characterID,node.getLine()
    
    
    def reducer(self, key, values):
        edges = []
        distance = 9999
        color = 'WHITE'

        for value in values:
            node = Node()
            node.readLine(value)

            if (len(node.connections) > 0):
                edges.extend(node.connections)

            if (node.distance < distance):
                distance = node.distance

            if ( node.color == 'BLACK' ):
                color = 'BLACK'

            if ( node.color == 'GREY' and color == 'WHITE' ):
                color = 'GREY'

        node = Node()
        node.characterID = key
        node.distance = distance
        node.color = color
        node.connections = edges

        yield key, node.getLine()
        
if __name__ == "__main__":
    MRBFSDegreeOfSeparation.run()
    
    