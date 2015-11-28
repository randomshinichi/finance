#1/usr/local/bin/python3
from behindthescenes import *
from tabulate import tabulate
import os
rows, columns = os.popen('stty size', 'r').read().split()

def cap(s, l):
    return s if len(s) <= l else s[:l]

def build_output(answer):
	output = []
	for a in answer:
		o = list(a)
		o[2] = cap(o[2], int(0.5*int(columns)))
		output.append(o)
	return output

"""
db = Database()
db.dbank_parser('debit-09-2015.csv')
db.cash_parser('cash-09-2015.csv')
print(tabulate(db.get_transactions()))
"""
ana = Analyzer('11-2015')
# bills = ana.get(category='bills')
answer, total = ana.get(paymentMethod='debit')

output = build_output(answer)

print(tabulate(output))
