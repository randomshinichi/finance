#!/usr/local/bin/python3
import cmd
import pdb
import re
import sys
import os
from tabulate import tabulate

from behindthescenes import Analyzer


def cap(s, l):
    return s if len(s) <= l else s[:l]

def find_max_column_widths(answer):
    columns = list(zip(*answer))
    max_column_widths = []

    for c in columns:
        maximum = 0
        for x in c:
            maximum = max(len(str(x)), maximum)
        max_column_widths.append(maximum)

    return max_column_widths

def build_output(answer, filter_more):
    rows, columns = os.popen('stty size', 'r').read().split()
    output = []
    
    max_column_widths = find_max_column_widths(answer)
    details_width = int(columns)-max_column_widths[0]-max_column_widths[1]-max_column_widths[3]-max_column_widths[4]-max_column_widths[5]-(2*len(max_column_widths))

    useless27digits = re.compile('[0-9]{27}')
    uselesselv = re.compile('ELV[0-9]{8}')
    for a in answer:
        o = list(a)
        details = o[2]
        if filter_more:
            if details[0:3] == 'EC ':
                details = details[48:]
            elif details[0:5] == 'GA NR':
                details = details[27:]
            elif useless27digits.match(details):
                details = details[28:]
            elif uselesselv.match(details):
                details = details[12:]

        o[2] = cap(details, details_width)
        output.append(o)

    return output

class Interface(cmd.Cmd):
    prompt = 'finance: '

    def __init__(self, *args, **kwargs):
        super(Interface, self).__init__(*args, **kwargs)
        self.date = sys.argv[1]
        self.analyzer = Analyzer(self.date, True)
        self.cmdqueue = ['show']
        self.filter_more = True

    def do_show(self, arg):  # arg is category, paymentMethod
        """
        Usage: show [category/paymentMethod], show category paymentMethod
        Purpose: shows entries by category or paymentMethod, or filtered by both. Also calculates Expense and Net Income for convenience
        """
        args = arg.split(' ')
        if not args[0]:
            transactions, total = self.analyzer.get()
        elif len(args) == 1:
            category = None
            paymentMethod = None
            if args[0] in ['ccard', 'atm', 'debit', 'cash']:
                paymentMethod = args[0]
            else:
                category = args[0]
            transactions, total = self.analyzer.get(category=category, paymentMethod=paymentMethod)
        elif len(args) == 2:
            transactions, total = self.analyzer.get(category=args[0], paymentMethod=args[1])

        output = build_output(transactions, self.filter_more)

        print(tabulate(output))
        print(total)

    def do_analyze(self, arg):
        analysis = self.analyzer.analyze()
        output_for_tabulate = []
        grand_total = 0
        for a in sorted(analysis.keys()):
            output_for_tabulate.append([a, analysis[a]])
            grand_total += analysis[a]
        print(tabulate(output_for_tabulate))
        print("Total Spent: ", grand_total)


    def do_cat(self, arg):
        """
        Usage: cat id new_category
        Purpose: changes the category of a transaction
        """
        try:
            i, category = arg.split()
            i = int(i)
            self.analyzer.update_category(i, category)
        except ValueError:
            print("Give 2 arguments, the id and the new category")

    def do_type(self, arg):
        """
        Usage: type id new_paymentMethod
        Purpose: changes the payment type of a transaction, in case it was wrongly detected
        """
        try:
            i, paymentmethod = arg.split()
            if paymentmethod not in ['debit', 'atm', 'ccard']:
                raise ValueError(
                    "You can only classify payments as debit, atm or ccard")
            i = int(i)
            self.analyzer.update_type(i, paymentmethod)
            if paymentmethod == 'atm':
                self.analyzer.update_category(i, 'unaccounted')
        except ValueError as e:
            print(e)

    def do_recategorize(self, arg):
        self.analyzer.assign_category()

    def do_exit(self, arg):
        return -1

    def do_filter_more(self, arg):
        if self.filter_more:
            self.filter_more = False
            print("Displaying full details")
        else:
            self.filter_more = True
            print("Filtering excess numbers in debit card transactions")


if __name__ == "__main__":
    i = Interface()
    i.cmdloop()
