import sqlite3
import datetime
import traceback
import csv

from sys import exit
from argparse import ArgumentParser
from os.path import dirname, join
from os import mkdir


class Db:
    def __init__(self, path):
        self.__conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self.__conn.row_factory = sqlite3.Row

    def get_accounts(self):
        c = self.__conn.cursor()
        r = c.execute('''
            SELECT address FROM accounts
        ''')
        return list(map(lambda row: row['address'], c.fetchall()))

    def get_full_report(self, address):
        c = self.__conn.cursor()
        r = c.execute('''
            SELECT * FROM snapshots
            WHERE address = ?
            ORDER BY timestamp ASC
        ''', (address,))
        rows = r.fetchall()

        def add_income(i, row):
            row = dict(row)
            last_state = rows and (i-1 >= 0) and rows[i-1] or None

            if not last_state:
                income = None
            else:
                # to calculate income:
                #   today's balance - yesterday's balance +
                #   yesterday's bond - today's bond +
                #   today's pending rewards - yesterday's pending rewards -
                #   today's pending commission - yesterday's pending commission -
                #   net transaction flow since last snapshot
                income = row['balance'] - last_state['balance'] + \
                         row['bond'] - last_state['bond'] + \
                         row['pending_commission'] - last_state['pending_commission'] + \
                         row['pending_rewards'] - last_state['pending_rewards'] - \
                         row['net_tx']

            row['income'] = income
            return row

        return [add_income(i, row) for (i, row) in enumerate(rows)]


# parse command line arguments
parser = ArgumentParser(description="Generate CSV output for account(s) earnings")
parser.add_argument('--db-path', required=True, help="path to sqlite3 database with daily report snapshots")
parser.add_argument('--output-dir', default=join(dirname(__file__), 'csvs'), help="path to store csvs")
parser.add_argument('--denom', nargs='?', default='uatom', help="the denomination of balances/shares/etc")
args = parser.parse_args()


try: mkdir(args.output_dir)
except: pass
print(f"Generating CSV outputs in {args.output_dir}...")

db = Db(args.db_path)


for address in db.get_accounts():
    print(f"\t#{address} ", end='')

    lines = db.get_full_report(address)
    print(f"({len(lines)} lines) ")

    with open(join(args.output_dir, f"{address}.csv"), 'w', newline='') as csvfile:
        fields = [
            'timestamp', 'balance', 'bond',
            'pending_rewards', 'pending_commission', 'net_tx',
            'income'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fields, quoting=csv.QUOTE_ALL)

        for line in lines:
            writer.writerow(line)

    print("DONE")
