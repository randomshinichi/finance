import csv
import pdb
import zlib
import sqlite3
from decimal import Decimal


def adapt_decimal(d):
    return str(d)


def convert_decimal(d):
    return Decimal(d.decode('utf-8'))


def cap(s, l):
    return s if len(s) <= l else s[:l]


class Database:

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
        self.cur.execute('''CREATE TABLE IF NOT EXISTS bank (
            id INTEGER PRIMARY KEY,
            date TEXT,
            details TEXT,
            category TEXT,
            paymentMethod TEXT,
            expense decimal,
            income decimal,
            bankAccount TEXT,
            hash INTEGER UNIQUE
            )''')
        self.cur.execute('''CREATE TABLE IF NOT EXISTS cash (
            id INTEGER PRIMARY KEY,
            date TEXT,
            details TEXT,
            category TEXT,
            expense decimal,
            hash INTEGER unique
            )''')
        self.cur.execute('''CREATE TABLE IF NOT EXISTS ccard (
            id INTEGER PRIMARY KEY,
            date TEXT,
            details TEXT,
            category TEXT,
            expense decimal,
            hash INTEGER unique
            )''')
        self.cur.execute('''CREATE TABLE IF NOT EXISTS analysis(
            category TEXT,
            expense decimal
            )''')

    def update_category(self, id, category):
        self.cur.execute(
            'UPDATE bank SET category = ? WHERE ID = ?', (category, id))
        self.con.commit()

    def update_details(self, id, details):
        self.cur.execute(
            'UPDATE bank SET details = ? WHERE ID = ?', (details, id))
        self.con.commit()

    def update_type(self, id, paymentMethod):
        self.cur.execute(
            'UPDATE bank SET paymentMethod = ? WHERE ID = ?', (paymentMethod, id))
        self.con.commit()

    def get_all(self):
        transactions = self.cur.execute(
            'SELECT id,date,details,category,paymentMethod,expense,income,bankaccount FROM bank').fetchall()
        total = self.get_total(transactions)
        return transactions, total

    def get_cash(self):
        transactions = self.cur.execute(
            'SELECT id,date,details,category,expense FROM cash').fetchall()
        total = self.get_total(transactions, 4)
        return transactions, total

    def get_ccard(self):
        transactions = self.cur.execute(
            'SELECT id,date,details,category,expense FROM ccard').fetchall()
        total = self.get_total(transactions, 4)
        return transactions, total

    def get(self, arg1, arg2=None):
        # detect if arg1 is a paymentMethod, if not then it's a category
        if not arg2:
            if arg1 in ['ccard', 'debit', 'atm']:
                ans = self.cur.execute(
                    'SELECT * FROM bank WHERE paymentMethod = ?', (arg1,))
            else:
                ans = self.cur.execute(
                    'SELECT * FROM bank WHERE category = ?', (arg1,))
        else:
            ans = self.cur.execute(
                'SELECT * FROM bank WHERE category = ? AND paymentMethod = ?', (arg1, arg2))

        transactions = ans.fetchall()
        total = self.get_total(transactions)
        return transactions, total

    def get_categories(self):
        answer = []
        categories = self.cur.execute(
            'SELECT DISTINCT category FROM bank ORDER BY category').fetchall()
        for c in categories:
            transactions = self.cur.execute(
                'SELECT * FROM bank WHERE category = ?', c).fetchall()
            total = self.get_total(transactions)
            answer.append((c[0], total))
        return answer

    def insert_split(self, id, details, category, expense):
        expense = Decimal(expense) * -1
        t = self.cur.execute(
            'SELECT * FROM bank WHERE id = ?', (id,)).fetchone()
        split = (None, t[1], details, category, t[4], expense, t[6], t[7])

        try:
            self.cur.execute(
                'INSERT INTO bank VALUES (?,?,?,?,?,?,?,?)', split)
        except sqlite3.IntegrityError:
            print("Database: a split with the same details already exists")
            return
        t_newexpense = t[5] - expense
        self.cur.execute(
            'UPDATE bank SET expense = ? WHERE id = ?', (t_newexpense, id))
        self.con.commit()

    def insert_multiple(self, transactions):
        try:
            self.cur.executemany(
                'INSERT INTO bank VALUES (?,?,?,?,?,?,?,?,?)',
                transactions)
        except sqlite3.IntegrityError as e:
            print("Database:", e)
            rows = len(self.cur.execute('SELECT * FROM bank').fetchall())
            print("Database: Removed first %i lines, trying again" % rows)
            self.cur.executemany(
                'INSERT INTO bank VALUES (?,?,?,?,?,?,?,?,?)',
                transactions[
                    rows:])
        self.con.commit()

    def cash_insert_multiple(self, transactions):
        try:
            self.cur.executemany(
                'INSERT INTO cash VALUES (?,?,?,?,?,?)',
                transactions)
        except sqlite3.IntegrityError as e:
            print("Database:", e)
            rows = len(self.cur.execute('SELECT * FROM cash').fetchall())
            print("Database: Removed first %i lines, trying again" % rows)
            self.cur.executemany(
                'INSERT INTO cash VALUES (?,?,?,?,?,?)',
                transactions[
                    rows:])
        self.con.commit()

    def get_total(self, transactions, column=5):
        j = [i[column] for i in transactions if i[column]]
        return sum(j)


class Month:

    def __init__(self, arg, makedbfile=False):
        self.month_year = arg
        if makedbfile:
            self.db = Database(self.month_year)
        else:
            self.db = Database()

    def assign_category(self):
        with open('rules.txt', 'r', encoding='utf-8') as f:
            ruleslist = []
            reader = csv.reader(f)
            for r in reader:
                if r:
                    ruleslist.append(r)

        rulesdict = dict(ruleslist)
        transactions = self.db.get_all()[0]

        for a in transactions:
            details = set(a[2].split(' '))
            results = details.intersection(rulesdict)
            if results:
                key = results.pop()
                self.db.update_category(a[0], rulesdict[key])

    def assign_type(self):
        # transactions, total = self.db.get_all()
        transactions = self.db.get_all()[0]
        for t in transactions:
            t_id = t[0]
            details = t[2]
            if details.find('GA NR') > -1 or details.find('//BERLIN/DE') > -1:
                self.db.update_type(t_id, 'atm')
                self.db.update_category(t_id, 'unaccounted')
            elif details.find('5401871613159050') > -1:
                self.db.update_type(t_id, 'ccard')
                self.db.update_category(t_id, 'unaccounted')
            else:
                self.db.update_type(t_id, 'debit')

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
                bankAccount = "db"
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
                    paymentMethod,
                    expense,
                    income,
                    bankAccount,
                    uniquehash
                )
                transactions.append(transaction)
        self.db.insert_multiple(transactions)
        f.close()

    def cash_parser(self, filename):
        f = open(filename, 'r', newline='', encoding='utf-8')
        reader = csv.DictReader(f)
        transactions = []
        for row in reader:
            day, month, year = row['date'].split('-')
            date = year + '-' + month + '-' + day
            details = row['details']
            category = row['category']
            expense = Decimal(row['amount']) * -1
            uniquehash = zlib.crc32(
                bytes(date + details + str(expense), 'utf-8'))

            transaction = (None, date, details, category, expense, uniquehash)
            transactions.append(transaction)
        self.db.cash_insert_multiple(transactions)
        f.close()

    def ccard_parser(self, filename):
        f = open(filename, 'r', newline='', encoding='utf-8')
        reader = csv.DictReader(f)
        transactions = []
        for row in reader:
            day, month, year = row['date'].split('-')
            date = year + '-' + month + '-' + day
            bankAccount = ""

            amount = Decimal(row['amount']) * -1
            category = row['category']
            paymentMethod = 'ccard_sub'
            details = row['details']
            transaction = (
                None,
                date,
                details,
                category,
                paymentMethod,
                amount,
                bankAccount)
            transactions.append(transaction)
        self.db.cash_insert_multiple(transactions)
        f.close()
