#1/usr/local/bin/python3
from behindthescenes import *
from tabulate import tabulate
"""
db = Database()
db.dbank_parser('debit-09-2015.csv')
db.cash_parser('cash-09-2015.csv')
print(tabulate(db.get_transactions()))
"""
ana = Analyzer('09-2015')
# bills = ana.get(category='bills')
output, total = ana.get(category='food')


# print(tabulate(bills[0]),bills[1])
print(tabulate(output), total)