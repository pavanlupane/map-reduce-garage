# Call this with one argument: the character ID you are starting from.
# For example, Spider Man is 5306, The Hulk is 2548. Refer to Marvel-names.txt
# for others.

from sys import argv

print('Creating BFS starting input for character ' + argv[1])

with open('../Data/BFS-iteration-0.txt','w') as out:
    with open('../Data/Marvel-Graph.txt') as f:
        
        for line in f:
            fields = line.split()
            heroID = fields[0]
            totalConn = len(fields) - 1
            connections = fields[-totalConn:]
            
            color = 'WHITE'
            distance = 9999
            
            if (heroID == argv[1]):
                color = "GREY"
                distance = 0
            
            if (heroID != ''):
                edges = ','.join(connections)
                outStr = '|'.join((heroID, edges, str(distance), color))
                out.write(outStr)
                out.write("\n")
                
    f.close()
out.close()         

print('Success! ')   