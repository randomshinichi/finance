import csv
import pdb
import zlib
import sqlite3
from decimal import Decimal


def adapt_decimal(d):
    return str(d)


def convert_decimal(derp):
    return Decimal(derp.decode('utf-8'))


def cap(s, l):
    return s if len(s) <= l else s[:l]


def calctotal(transactions):
    sum_list = []
    for i in transactions:
        if i[4]:
            sum_list.append(i[4])
    return sum(sum_list)


class Database:
    transactions_fields = 'transactions.id,transactions.date,transactions.details,transactions.category,transactions.expense,transactions.income,transactions.paymentMethod,transactions.transactionsaccount'

    def __init__(self, arg=None):
        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter('decimal', convert_decimal)
        if arg:
            arg += '.db'
            self.con = sqlite3.connect(
                arg, detect_types=sqlite3.PARSE_DECLTYPES)
            print("Database: using file {0:s}".format(arg))
        else:
            self.con = sqlite3.connect(
                ':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
            print("Database: data stored in RAM")
        self.cur = self.con.cursor()
        self.cur.execute('''CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            date TEXT,
            details TEXT,
            category TEXT,
            expense decimal,
            income decimal,
            paymentMethod TEXT,
            transactionsAccount TEXT,
            hash INTEGER UNIQUE
            )''')
    def update_category(self, id, category):
        self.cur.execute(
            'UPDATE transactions SET category = ? WHERE ID = ?', (category, id))
        self.con.commit()

    def update_details(self, id, details):
        self.cur.execute(
            'UPDATE transactions SET details = ? WHERE ID = ?', (details, id))
        self.con.commit()

    def update_type(self, id, paymentMethod):
        self.cur.execute(
            'UPDATE transactions SET paymentMethod = ? WHERE ID = ?', (paymentMethod, id))
        self.con.commit()

    def get_transactions(self, category=None, paymentMethod=None):
        if category and paymentMethod:
            transactions = self.cur.execute(
                'SELECT %s FROM transactions WHERE category = ? AND paymentMethod = ?' % self.transactions_fields, (category, paymentMethod)).fetchall()
        elif category and not paymentMethod:
            transactions = self.cur.execute(
                'SELECT %s FROM transactions WHERE category = ?' % self.transactions_fields, (category,)).fetchall()
        elif paymentMethod and not category:
            transactions = self.cur.execute(
                'SELECT %s FROM transactions WHERE paymentMethod = ?' % self.transactions_fields, (paymentMethod,)).fetchall()
        else:
            transactions = self.cur.execute(
                'SELECT %s FROM transactions' % self.transactions_fields).fetchall()
        return transactions

    def get_nocash_noccard(self):
        transactions = self.cur.execute('SELECT %s FROM transactions WHERE paymentMethod IS NOT ?' % self.transactions_fields, ('cash',)).fetchall()
        return transactions

    def get_distinct_categories(self):
        categories = self.cur.execute(
            'SELECT DISTINCT category FROM transactions ORDER BY category').fetchall()
        answer = [c[0] for c in categories]
        return answer

    def split_transaction(self, id, details, category, expense):
        expense = Decimal(expense) * -1
        t = self.cur.execute(
            'SELECT * FROM transactions WHERE id = ?', (id,)).fetchone()
        split = (None, t[1], details, category, t[4], expense, t[6], t[7])

        try:
            self.cur.execute(
                'INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?)', split)
        except sqlite3.IntegrityError:
            print("Database: a split with the same details already exists")
            return
        t_newexpense = t[5] - expense
        self.cur.execute(
            'UPDATE transactions SET expense = ? WHERE id = ?', (t_newexpense, id))
        self.con.commit()

    def insert_multiple(self, transactions):
        try:
            self.cur.executemany(
                'INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?)',
                transactions)
        except sqlite3.IntegrityError as e:
            print("Database:", e)
            rows = len(self.cur.execute('SELECT * FROM transactions').fetchall())
            print("Database: Removed first %i lines, trying again" % rows)
            self.cur.executemany(
                'INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?)',
                transactions[
                    rows:])
        self.con.commit()

    def dbank_parser(self, filename):
        f = open(filename, 'r', newline='', encoding='latin_1')
        f.seek(f.read().find('Booking date'))
        reader = csv.DictReader(f, delimiter=';')
        transactions = []
        for row in reader:
            if row['Booking date'] == 'Account balance':
                break
            else:
                month, day, year = row['Booking date'].split('/')
                date = day + '-' + month + '-' + year
                transactionsAccount = "db"
                category = "uncategorized"

                if row['Beneficiary / Originator']:
                    details = row['Beneficiary / Originator'] + \
                        ' ' + row['Payment Details']
                else:
                    details = row['Payment Details']

                if row['Debit']:
                    income = None
                    # get rid of thousands separators
                    expense = Decimal(row['Debit'].replace(',', ''))
                else:
                    expense = None
                    # get rid of thousands separators
                    income = Decimal(row['Credit'].replace(',', ''))

                paymentMethod = 'unknown'

                uniquehash = zlib.crc32(
                    bytes(date + details + str(expense), 'utf-8'))

                transaction = (
                    None,
                    date,
                    details,
                    category,
                    expense,
                    income,
                    paymentMethod,
                    transactionsAccount,
                    uniquehash
                )
                transactions.append(transaction)
        self.insert_multiple(transactions)
        f.close()

    def cash_parser(self, filename):
        f = open(filename, 'r', newline='', encoding='utf-8')
        reader = csv.DictReader(f)
        transactions = []
        for row in reader:
            day, month, year = row['date'].split('-')
            date = day + '-' + month + '-' + year
            details = row['details']
            category = row['category']
            expense = Decimal(row['amount']) * -1
            uniquehash = zlib.crc32(
                bytes(date + details + str(expense), 'utf-8'))
            paymentMethod = 'cash'
            transaction = (None, date, details, category, expense, None, paymentMethod, None, uniquehash)
            transactions.append(transaction)
        self.insert_multiple(transactions)
        f.close()

    def ccard_parser(self, filename):
        f = open(filename, 'r', newline='', encoding='utf-8')
        reader = csv.DictReader(f)
        transactions = []
        for row in reader:
            day, month, year = row['date'].split('-')
            date = day + '-' + month + '-' + year
            details = row['details']
            category = row['category']
            expense = Decimal(row['amount']) * -1
            uniquehash = zlib.crc32(
                bytes(date + details + str(expense), 'utf-8'))
            paymentMethod = 'cash'
            transaction = (None, date, details, category, expense, None, paymentMethod, None, uniquehash)
            transactions.append(transaction)
        self.insert_multiple(transactions)
        f.close()


class Analyzer:

    def __init__(self, arg, makedbfile=False):
        self.month_year = arg
        if makedbfile:
            self.db = Database(self.month_year)
        else:
            self.db = Database()

        self.db.dbank_parser('debit-' + self.month_year + '.csv')
        self.db.cash_parser('cash-' + self.month_year + '.csv')

        self.assign_category()
        self.assign_type()

    def assign_category(self):
        with open('rules.txt', 'r', encoding='utf-8') as f:
            ruleslist = []
            reader = csv.reader(f)
            for r in reader:
                if r:
                    ruleslist.append(r)

        rulesdict = dict(ruleslist)
        transactions = self.db.get_transactions()

        for a in transactions:
            details = set(a[2].split(' '))
            results = details.intersection(rulesdict)
            if results:
                key = results.pop()
                self.db.update_category(a[0], rulesdict[key])

    def assign_type(self):
        transactions = self.db.get_transactions(paymentMethod='unknown')
        for t in transactions:
            t_id = t[0]
            details = t[2]
            if details.find('GA NR') > -1:
                self.db.update_type(t_id, 'atm')
                self.db.update_category(t_id, 'unaccounted')
            elif details.find('5401871613159050') > -1:
                self.db.update_type(t_id, 'ccard')
                self.db.update_category(t_id, 'unaccounted')
            else:
                self.db.update_type(t_id, 'debit')

    def analyze(self):
        # Get total expenditures for each category in transactions
        expense_areas = {}
        for c in self.db.get_distinct_categories():
            expense_areas[c]=calctotal(self.db.get_transactions(category=c))

        # ATM withdrawals are unaccounted, but once cash is spent, it is accounted for in the cash CSV
        cash = calctotal(self.db.get_transactions(paymentMethod='cash'))
        expense_areas['unaccounted']-=cash
        return expense_areas

    def update_category(self, id, category):
        self.db.update_category(id, category)

    def update_details(self, id, details):
        self.db.update_details(id, details)

    def update_type(self, id, paymentMethod):
        self.db.update_type(id, paymentMethod)

    def get(self, category=None, paymentMethod=None):
        transactions = self.db.get_transactions(category, paymentMethod)
        if not category and not paymentMethod:
            total = calctotal(self.db.get_nocash_noccard())
        else:
            total = calctotal(transactions)
        return transactions, total

    def split_transaction(self, id, details, category, expense):
        self.db.split_transaction(id, details, category, expense)

