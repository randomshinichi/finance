from decimal import Decimal


def analyze(month):
    # Get total expenditures for each category in bank
    # a list of tuples(category, total)
    categories = dict(month.db.get_categories())
    # Assign each cash expenditure to a category
    cash_transactions = month.db.cur.execute(
        'SELECT category, expense FROM cash').fetchall()
    for c in cash_transactions:
        # c[0] category, c[1] expense
        # What if c[0] is a category that was not specified in categories (from debit)? Then I will
        # have to create it first and set its total to 0
        if not c[0] in categories:
            categories[c[0]] = Decimal(0)

        categories[c[0]] += c[1]  # classify the expense
        # the amount spent is no longer 'unaccounted for'
        categories['unaccounted'] -= c[1]

    return sorted(categories.items())
