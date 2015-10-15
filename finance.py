#!/usr/local/bin/python3
import cmd
import pdb
import re
import Controller
from tabulate import tabulate

from Month import Month


class Interface(cmd.Cmd):
    prompt = 'finance '

    def __init__(self, *args, **kwargs):
        super(Interface, self).__init__(*args, **kwargs)
        self.date = '09-2015'
        self.month = Month(self.date, False)
        self.cmdqueue = ['add bank', 'show']

    def do_add(self, arg):
        args = arg.split(' ')
        if 'bank' in args:
            filename = 'debit-' + self.month.month_year + '.csv'
            self.month.dbank_parser(filename)
            self.month.assign_type()
            self.month.assign_category()

        if 'cash' in args:
            filename = 'cash-' + self.month.month_year + '.csv'
            self.month.cash_parser(filename)
        if 'ccard' in args:
            filename = 'ccard-' + self.month.month_year + '.csv'
            self.month.ccard_parser(filename)

    def do_show(self, arg):  # arg is category, paymentMethod
        """
        Usage: show [category/paymentMethod], show category paymentMethod
        Purpose: shows entries by category or paymentMethod, or filtered by both. Also calculates Expense and Net Income for convenience
        """
        args = arg.split(' ')
        if not args[0]:
            bank_transactions, bank_total = self.month.db.get_all()
            cash_transactions, cash_total = self.month.db.get_cash()
        elif len(args) == 1:
            bank_transactions, bank_total = self.month.db.get(args[0])
        elif len(args) == 2:
            bank_transactions, bank_total = self.month.db.get(args[0], args[1])

        bank_transactions = self.sanitize_details(bank_transactions)

        print(tabulate(bank_transactions))
        print("Total:", bank_total)

        # if I type 'show', show cash and ccard imported transactions too
        if not args[0]:
            print(tabulate(cash_transactions))
            print("Spent via cash:", cash_total)

    def do_analyze(self, arg):
        analysis = Controller.analyze(self.month)
        print(tabulate(analysis))

    def do_split(self, arg):
        if not arg:
            print("Please tell me which transaction id to split")
            return
        print("Splitting transaction %s" % arg)
        details = 'split from {0:s}: '.format(
            arg) + input("What did you buy that was contained within that transaction? ")
        category = input("How would you categorize it? ")
        expense = input("How much was it? ")

        self.month.db.insert_split(int(arg), details, category, expense)

    def do_cat(self, arg):
        """
        Usage: cat id new_category
        Purpose: changes the category of a transaction
        """
        try:
            i, category = arg.split()
            i = int(i)
            self.month.db.update_category(i, category)
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
            self.month.db.update_type(i, paymentmethod)
            if paymentmethod == 'atm':
                self.month.db.update_category(i, 'unaccounted')
        except ValueError as e:
            print(e)

    def do_recategorize(self, arg):
        self.month.assign_category()

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
                (t[0], t[1], details, t[3], t[4], t[5], t[6], t[7]))
        return new_transactions

if __name__ == "__main__":
    i = Interface()
    i.cmdloop()
