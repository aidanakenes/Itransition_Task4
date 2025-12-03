import pandas as pd
import yaml
from datetime import datetime
from dateutil import parser


def clean_data(users):
    users[['name', 'address', 'phone', 'email']] = (
        users[['name', 'address', 'phone', 'email']].fillna('')
    )
    users = users.drop_duplicates()

    users = users[
        users['email'].str.contains('@') &
        users['email'].str.contains(r'\.')
    ]

def fix_year(year):
    try:
        year = int(year)
    except:
        return 0

    if year <= 0 or year > datetime.now().year:
        return 0

    return year


if __name__ == '__main__':
    folder = 'DATA3'
    users = pd.read_csv(f'../data/{folder}/users.csv')
    orders = pd.read_parquet(f'../data/{folder}/orders.parquet')
    books = []

    with open(f'../data/{folder}/books.yaml', 'r') as f:
        books = yaml.safe_load(f)


    # Clean users data
    users[['name', 'address', 'phone', 'email']] = (
        users[['name', 'address', 'phone', 'email']].fillna('')
    )
    users = users.drop_duplicates()

    users = users[
        users['email'].str.contains('@') &
        users['email'].str.contains(r'\.')
    ]


    # Clean books data
    books = pd.DataFrame(books)
    books[':year'] = books[':year'].apply(fix_year)
    books = books.drop_duplicates()

    # Simplify orders date field
    orders['timestamp'] = (
        orders['timestamp']
        .astype(str)
        .str.replace(r'A[\.\s]*M[\.\s]*', 'AM', regex=True)
        .str.replace(r'P[\.\s]*M[\.\s]*', 'PM', regex=True)
        .str.replace(r'[;,]', ' ', regex=True)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
        .apply(lambda x: parser.parse(x) if x else None)
    )

    # Convert and unify price field
    orders['unit_price_str'] = orders['unit_price'].apply(
        lambda v: ''.join(chr(i) for i in v) if isinstance(v, (list, tuple)) else str(v)
    )
    s = orders['unit_price_str'].astype(str).str.strip()

    is_eur = s.str.contains(r'€|\bEUR\b', case=False, regex=True)
    is_usd = s.str.contains(r'\$|\bUSD\b', case=False, regex=True)

    clean = (
        s
        .str.replace('¢', '.', regex=False)  # $71¢00 -> 71.00
        .str.replace(',', '.', regex=False)  # 1,234 -> 1.234
        .str.replace(r'(€|\$|\bEUR\b|\bUSD\b)', '', regex=True)
        .str.replace(r'[^\d\.\-]', '', regex=True)  # leave only digits, dot and minus
    )

    orders['unit_price_num'] = pd.to_numeric(clean, errors='coerce').fillna(0.0)
    orders['unit_price_usd'] = orders['unit_price_num']
    orders.loc[is_eur, 'unit_price_usd'] = orders.loc[is_eur, 'unit_price_num'] * 1.2
    orders['paid_price'] = orders['quantity'].astype(float) * orders['unit_price_usd']

    orders['date'] = orders['timestamp'].dt.date

    # - Compute daily revenue (sum of paid_price grouped by date) and find top 5 days by revenue.
    daily_rev = (
        orders.groupby('date')['paid_price']
        .sum()
        .sort_index()
    )

    top5_days = (
        daily_rev.sort_values(ascending=False)
        .head(5)
    )


    # - Find how many real unique users there are. Note that user can change address or change
    # phone or even provide alias instead of a real name; you need to reconciliate data.
    # You may assume that only one field is changed.
    import networkx as nx

    df = orders.merge(users, on='id', how='left')

    G = nx.Graph()

    for idx, row in df.iterrows():
        rid = f"row_{idx}"
        G.add_node(rid)

        for field in ['email', 'phone', 'address', 'name']:
            val = str(row.get(field, '')).strip().lower()
            if val and val != 'nan':
                key = f"{field}::{val}"
                G.add_edge(rid, key)

    real_users = []
    for comp in nx.connected_components(G):
        rows = [n for n in comp if n.startswith('row_')]
        if rows:
            real_users.append(rows)

    unique_real_users = len(real_users)


    # - Find how many unique sets of authors there are. For example, if John and Paul wrote
    # a book together and wrote several books separately, it means that there are 3 different sets.
    books[':authors_norm'] = (
        books[':author']
        .astype(str)
        .str.replace(',', ';')
        .str.split(';')
        .apply(lambda lst: ';'.join(sorted([a.strip() for a in lst if a.strip()])))
    )
    unique_author_sets = books[':authors_norm'].nunique()


    # - Find the most popular (by sold book count) author (or author set).
    ob = orders.merge(books[[':id', ':authors_norm']], left_on='book_id', right_on=':id', how='left')
    author_set_sales = (
        ob.groupby(':authors_norm')['quantity']
        .sum()
        .sort_values(ascending=False)
    )

    top_author_set = author_set_sales.index[0]

    counts = {}
    for _, r in ob.iterrows():
        authors = r[':authors_norm'].split(';')
        for a in authors:
            if a:
                counts[a] = counts.get(a, 0) + r['quantity']

    top_author = max(counts, key=counts.get)


    # - Identify the top customer by total spending (list all user_id values for the possible
    # different addresses, phones, e-mails, or aliases).
    group_spend = {}

    for g_idx, group in enumerate(real_users):
        # convert row_x → original index
        idxs = [int(r.split('_')[1]) for r in group]
        total = orders.loc[idxs, 'paid_price'].sum()
        group_spend[g_idx] = total

    best_group = max(group_spend, key=group_spend.get)
    best_group_rows = real_users[best_group]
    best_group_idxs = [int(r.split('_')[1]) for r in best_group_rows]

    top_customer_user_ids = orders.loc[best_group_idxs, 'user_id'].unique().tolist()


    # - Plot a simple line chart of daily revenue using matplotlib.
    import matplotlib.pyplot as plt

    print('TOP 5 DAYS:', top5_days)
    print('NUMBER OF UNIQUE USERS:', unique_real_users)
    print('NUMBER OF UNIQUE SETS OF AUTHORS:', unique_author_sets)
    print('NAME OF MOST POPULAR AUTHOR:', top_author)
    print('BEST BUYERS:', top_customer_user_ids)

    daily_rev.to_csv("daily_rev.csv")
    plt.figure(figsize=(10, 4))
    daily_rev.plot()
    plt.title("Daily Revenue")
    plt.xlabel("Date")
    plt.ylabel("Revenue (USD)")
    plt.tight_layout()
    plt.show()








