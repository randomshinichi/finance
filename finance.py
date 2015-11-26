#!/usr/local/bin/python3
import cmd
import pdb
import re
import sys
from tabulate import tabulate

from behindthescenes import Analyzer


def cap(s, l):
    return s if len(s) <= l else s[:l]


class Interface(cmd.Cmd):
    prompt = 'finance: '

    def __init__(self, *args, **kwargs):
        super(Interface, self).__init__(*args, **kwargs)
        self.date = sys.argv[1]
        self.analyzer = Analyzer(self.date, True)
        self.cmdqueue = ['analyze']
        self.cleaner_details = True

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

        if self.cleaner_details:
            transactions = self.sanitize_details(transactions)

        print(tabulate(transactions))
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

    def do_split(self, arg):
        if not arg:
            print("Please tell me which transaction id to split")
            return
        print("Splitting transaction %s" % arg)
        details = 'split from {0:s}: '.format(
            arg) + input("What did you buy that was contained within that transaction? ")
        category = input("How would you categorize it? ")
        expense = input("How much was it? ")

        self.analyzer.split_transaction(int(arg), details, category, expense)

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

    def sanitize_details(self, transactions):
        useless27digits = re.compile('[0-9]{27}')
        new_transactions = []
        for t in transactions:
            details = t[2]
            if details[0:3] == 'EC ':
                details = details[48:]
            elif details[0:5] == 'GA NR':
                details = details[28:]
            elif useless27digits.match(details):
                details = details[28:]
            new_transactions.append(
                (t[0], t[1], cap(details, 100), t[3], t[4], t[5], t[6], t[7]))
        return new_transactions

    def do_sanitize_details(self, arg):
        if self.cleaner_details:
            self.cleaner_details = False
            print("Displaying full details")
        else:
            self.cleaner_details = True
            print("Filtering excess numbers in debit card transactions")
            print("Capping details to 100 characters")

if __name__ == "__main__":
    i = Interface()
    i.cmdloop()
