from mrjob.job import MRJob
from mrjob.step import MRStep


class sortedExpenseCust(MRJob):
    
    def steps(self):
        return [
                MRStep(mapper = self.mapper_cust_to_expense,
                       reducer = self.reducer_total_expense),
                MRStep(mapper = self.mapper_expense_to_cust,
                       reducer = self.reducer_sorted_result)
                ]
    
    def mapper_cust_to_expense(self, _, line):
        (custId, productId,amountSpent) = line.split(',')
        yield '%04d'%int(custId), float(amountSpent)
        
    def reducer_total_expense(self, custId, amountSpent):
        yield custId, sum(amountSpent)
        
    def mapper_expense_to_cust(self, custId, totalAmount):
        yield '%04.02f'%float(totalAmount), custId
        
    def reducer_sorted_result(self, totalAmount, custIds):
        for custId in custIds:
            yield totalAmount, custId
        

if __name__ == "__main__":
    sortedExpenseCust.run()