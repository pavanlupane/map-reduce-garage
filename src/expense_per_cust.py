from mrjob.job import MRJob

class ExpensePerCustCalc(MRJob):
    def mapper(self, _, line):
        (customer, item, order_amnt) = line.split(',')
        yield '%04d'%int(customer),float(order_amnt)
        
    def reducer(self, cust, expense):
        yield cust, sum(expense)

if __name__ == "__main__":
    ExpensePerCustCalc.run()