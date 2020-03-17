import json
import datetime
import traceback
import bech32
import logging

from sqlite3 import connect, Row, PARSE_DECLTYPES, PARSE_COLNAMES
from sys import exit
from re import sub
from argparse import ArgumentParser
from functools import reduce
from urllib.request import urlopen
from urllib.error import HTTPError
from http.client import RemoteDisconnected
from os.path import dirname, join


class Db:
    def __init__(self, path):
        self.__conn = connect(path, detect_types=PARSE_DECLTYPES|PARSE_COLNAMES)
        self.__conn.row_factory = Row
        self.__init_schema()

    def commit(self):
        self.__conn.commit()

    def is_empty(self):
        c = self.__conn.cursor()
        c.execute('''
            SELECT COUNT(1) FROM accounts
        ''')
        count = c.fetchone()[0]
        return count == 0

    def get_accounts(self):
        c = self.__conn.cursor()
        r = c.execute('''
            SELECT address FROM accounts
        ''')
        return list(map(lambda row: row['address'], c.fetchall()))

    def add_account(self, address):
        c = self.__conn.cursor()
        c.execute('''
            INSERT OR REPLACE INTO accounts (address)
            VALUES (?)
        ''', (address,))
        self.commit()

    def get_latest_report(self, address):
        c = self.__conn.cursor()
        r = c.execute('''
            SELECT * FROM snapshots
            WHERE address = ?
            ORDER BY timestamp DESC
            LIMIT 1
        ''', (address,))
        return r.fetchone()

    def insert_report(self, address, timestamp, height, values):
        self.__conn.execute('''
            INSERT INTO snapshots(timestamp, height, address, balance, bond,
                                  pending_rewards, pending_commission, net_tx)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            timestamp,
            height,
            address,
            values['balance'],
            values['bond'],
            values['pending_rewards'],
            values['pending_commission'],
            values['net_tx']
        ))

    def __init_schema(self):
        with self.__conn:
            self.__conn.execute('''
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY,
                    timestamp TIMESTAMP,
                    height TEXT,
                    address TEXT,
                    balance REAL,
                    bond REAL,
                    pending_rewards REAL,
                    pending_commission REAL,
                    net_tx REAL
                )
            ''')
            self.__conn.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    address TEXT PRIMARY KEY
                )
            ''')


class Transaction:
    def __init__(self, data):
        self.__data = data

    def amount(self):
        if not self.__data['logs'][0]['success']:
            return 0

        def reducer(acc, msg):
            # other message types can be ignored
            if msg['type'] != 'cosmos-sdk/MsgSend':
                return acc

            value = msg['value']
            amount = reduce(
                lambda acc, amount: acc + float(amount['amount']),
                filter(lambda amount: amount['denom'] == args.denom, value['amount']),
                0
            )

            return acc + (amount * (10 ** -args.scale))

        return reduce(reducer, self.__data['tx']['value']['msg'], 0)

    def fees(self):
        if not self.__data['logs'][0]['success']:
            return 0

        relevant_fee_amounts = list(filter(
            lambda amount: amount['denom'] == args.denom,
            self.__data['tx']['value']['fee']['amount'] or []
        ))

        if len(relevant_fee_amounts) > 0:
            return float(relevant_fee_amounts[0]['amount']) * (10 ** -args.scale)
        else:
            return 0.0


class Delegation:
    def __init__(self, data):
        self.__data = data

    def amount(self):
        try:
            amount = self.__data.get('shares') or \
                     sum([float(entry['balance']) for entry in self.__data['entries']])
        except:
            amount = 0

        return float(amount) * (10 ** -args.scale)


class AccountProcessor:
    def __init__(self, address):
        self.address = address

    def process_next(self, height, timestamp, prev_timestamp):
        if prev_timestamp is None:
            return self._get_genesis_state()
        else:
            return self._get_next_state(prev_timestamp), timestamp, height

    def _get_genesis_state(self):
        global genesis_cache
        if genesis_cache is None:
            response = urlopen(f"{RPC}/genesis").read()
            genesis_cache = json.loads(response.decode('utf-8'))['result']['genesis']

        genesis_time = sub('\.\d+Z$', "Z", genesis_cache['genesis_time'])
        genesis_timestamp = datetime.datetime.strptime(genesis_time, "%Y-%m-%dT%H:%M:%SZ")

        try:
            balance_at_genesis = int(list(filter(
                lambda account: account['address'] == self.address,
                genesis_cache['app_state']['accounts']
            ))[0]['coins'][0]['amount']) * (10 ** -args.scale)
        except:
            balance_at_genesis = 0

        bonded_at_genesis = int(sum(map(lambda delegation: float(delegation['shares']), filter(
            lambda delegation: delegation['delegator_address'] == self.address,
            genesis_cache['app_state']['staking']['delegations'] or []
        )))) * (10 ** -args.scale)

        # special case: add any self bond at genesis
        for gentx in (genesis_cache['app_state']['gentxs'] or []):
            for msg in gentx['value']['msg']:
                if msg['type'] != 'cosmos-sdk/MsgCreateValidator': continue
                if msg['value']['delegator_address'] == self.address and msg['value']['value']['denom'] == args.denom:
                    # the bond amount is also included in the genesis balance
                    # so in the case of creating validators only, we need to substract
                    # the bonded amount from balance ¯\_(ツ)_/¯
                    amount = int(msg['value']['value']['amount']) * (10 ** -args.scale)
                    bonded_at_genesis += amount
                    balance_at_genesis -= amount

        unbonding_at_genesis = int(sum(map(lambda delegation: sum(map(lambda entry: float(entry['balance']), delegation['entries'])), filter(
            lambda delegation: delegation['delegator_address'] == self.address,
            genesis_cache['app_state']['staking']['unbonding_delegations'] or []
        )))) * (10 ** -args.scale)

        print(f"\tGenesis baseline! Bal: {balance_at_genesis}, Bond: {bonded_at_genesis + unbonding_at_genesis}")
        genesis_state = {
            'balance': balance_at_genesis,
            'bond': bonded_at_genesis + unbonding_at_genesis,
            'pending_rewards': 0,
            'pending_commission': 0,
            'net_tx': 0
        }

        return genesis_state, genesis_timestamp, 0

    def _get_next_state(self, latest_report_time):
        balance = self._get_current_balance()
        bond = self._get_total_bond_balance()
        pending = self._get_current_pending_rewards()
        commission = self._get_current_pending_commission()
        net = self._get_net_transaction_flow(latest_report_time)

        print(f"\tBal: {balance}, Bond: {bond}, PRew: {pending}, PCom: {commission}, NetTx: {net}")
        return {
            'balance': balance,
            'bond': bond,
            'pending_rewards': pending,
            'pending_commission': commission,
            'net_tx': net
        }

    def _get_current_balance(self):
        try:
            response = urlopen(f"{LCD}/bank/balances/{self.address}").read().decode('utf-8')
            if len(response) == 0: return 0
            data = json.loads(response)
            if data is None: return 0
            relevant_balances = list(filter(lambda bal: bal['denom'] == args.denom, data))
            if len(relevant_balances) == 0: return 0
        except:
            logger.error(f"Could not retrieve balance for {self.address} at height {report_height}. Recorded `0`")
            return 0

        try:
            amount = float(relevant_balances[0]['amount']) * (10 ** -args.scale)
        except IndexError:
            print(f"No relevant balances found for {self.address} (in {args.denom}). Did you specify the correct `denom`?")
            exit(1)

        return round(amount, 3)

    def _get_current_pending_commission(self):
        operator = bech32.encode('cosmosvaloper', bech32.decode(self.address)[1])
        try:
            response = urlopen(f"{LCD}/distribution/validators/{operator}").read()
            data = json.loads(response.decode('utf-8'))
        except:
            # if it failed, it's (likely/hopefully) just because this address
            # is not a validator at all, so just set data to {} and move on
            data = {}

        if data.get('val_commission') is None: return 0.0

        relevant_commission = list(filter(lambda bal: bal['denom'] == args.denom, data['val_commission']))

        try:
            amount = float(relevant_commission[0]['amount']) * (10 ** -args.scale)
        except IndexError:
            print(f"No relevant commission balances found for {operator} (in {args.denom}). Did you specify the correct `denom`?")
            exit(1)

        return round(amount, 3)

    def _get_current_pending_rewards(self):
        try:
            response = urlopen(f"{LCD}/distribution/delegators/{self.address}/rewards").read()
        except HTTPError as e:
            # body = e.read().decode('utf-8')
            # print(f"Explosion requesting rewards: {LCD}/distribution/delegators/{self.address}/rewards\n\n{body}\n\n\n\n")
            # exit(1)
            logger.error(f"Could not retrieve pending rewards for {self.address} at height {report_height}. Recorded `0`")
            return 0

        data = json.loads(response.decode('utf-8')) or [{ 'amount': 0, 'denom': args.denom }]
        relevant_balances = list(filter(lambda bal: bal['denom'] == args.denom, data))

        try:
            amount = float(relevant_balances[0]['amount']) * (10 ** -args.scale)
        except IndexError:
            print(f"No relevant reward balances found for {self.address} (in {args.denom}). Did you specify the correct `denom`?")
            exit(1)

        return round(amount, 3)

    def _get_net_transaction_flow(self, cutoff):
        try:
            sends = urlopen(f"{LCD}/txs?action=send&sender={self.address}&limit=100").read()
            receives = urlopen(f"{LCD}/txs?action=send&recipient={self.address}&limit=100").read()
        except (HTTPError, RemoteDisconnected) as e:
            logger.error(f"Could not retrieve net transaction flow for {self.address} at height {report_height}. Recorded `0`")
            return 0

        sends_data = json.loads(sends.decode('utf-8')) or []
        receives_data = json.loads(receives.decode('utf-8')) or []

        sends_data = filter(lambda tx: datetime.datetime.strptime(tx['timestamp'], '%Y-%m-%dT%H:%M:%SZ') > cutoff, sends_data)
        receives_data = filter(lambda tx: datetime.datetime.strptime(tx['timestamp'], '%Y-%m-%dT%H:%M:%SZ') > cutoff, receives_data)

        sends_amount = reduce(
            lambda acc, tx: acc + tx.amount() + tx.fees(),
            map(lambda tx: Transaction(tx), sends_data),
            0
        )
        receives_amount = reduce(
            lambda acc, tx: acc + tx.amount() + tx.fees(),
            map(lambda tx: Transaction(tx), receives_data),
            0
        )

        return round(receives_amount - sends_amount, 3)

    def _get_total_bond_balance(self):
        bonded = urlopen(f"{LCD}/staking/delegators/{self.address}/delegations?limit=100").read()
        unbonding = urlopen(f"{LCD}/staking/delegators/{self.address}/unbonding_delegations?limit=100").read()

        bonded_data = json.loads(bonded.decode('utf-8')) or []
        unbonding_data = json.loads(unbonding.decode('utf-8')) or []

        bonded_amount = reduce(
            lambda acc, bond: acc + bond.amount(),
            map(lambda bond: Delegation(bond), bonded_data),
            0
        ) if bonded_data else 0
        unbonding_amount = reduce(
            lambda acc, bond: acc + bond.amount(),
            map(lambda bond: Delegation(bond), unbonding_data),
            0
        ) if unbonding_data else 0

        return round(bonded_amount + unbonding_amount, 3)


# parse command line arguments
parser = ArgumentParser(description="Report on an account's earnings")
parser.add_argument('--db-path', required=True, help="path to store sqlite3 database with reports")
parser.add_argument('--log-path', default=join(dirname(__file__), 'error.log'), help="path to error log")
parser.add_argument('--denom', nargs='?', default='uatom', help="the denomination of balances/shares/etc")
parser.add_argument('--scale', nargs='?', default=6, type=int, help="scale factor to real world denom from chain denom")
args = parser.parse_args()


RPC = 'http://localhost:26657'
LCD = 'http://localhost:1317'

rpc_status = json.loads(urlopen(f"{RPC}/status").read())
report_height = rpc_status['result']['sync_info']['latest_block_height']
chain = rpc_status['result']['node_info']['network']

print(f"Running report at block {report_height}...")


db = Db(args.db_path)


logging.basicConfig(filename=args.log_path, format='%(message)s', filemode='a')
logger = logging.getLogger()


# check that we can access LCD
lcd_status = urlopen(f"{LCD}/node_info").read()


# dont want to request genesis more than once
genesis_cache = None


# ensure genesis accounts exist
if db.is_empty():
    print("Ensure genesis accounts... ")
    response = urlopen(f"{RPC}/genesis").read()
    genesis_cache = json.loads(response.decode('utf-8'))['result']['genesis']

    for gentx in (genesis_cache['app_state']['gentxs'] or []):
        for msg in gentx['value']['msg']:
            if msg['type'] == 'cosmos-sdk/MsgCreateValidator':
                db.add_account(msg['value']['delegator_address'])


# get all delegation transactions and ensure we have accounts saved

# on hub1/hub2 there is no indication of the number of pages, so
# we have to just keep getting pages until the highest height in
# a page is the highest height we already have
tx_hwm = 0

# if we try to get all delegation transactions each day, the
# process will quickly reach 500+ pages of requests for every report day
# so store the highest page we've retrieved and start there (less one to be safe)
# on the next run through
try:
    page = int(open(f".detect-delegators-page-num-{chain}",'r').read()) - 1
except:
    page = 1

print("Adding new delegator accounts... ")
while True:
    txs_response = urlopen(f"{LCD}/txs?action=delegate&limit=100&page={page}").read()
    txs = json.loads(txs_response.decode('utf-8'))

    highest_tx = max(map(lambda tx: int(tx['height']), txs))
    if highest_tx <= tx_hwm: break
    tx_hwm = highest_tx

    for tx in txs:
        for msg in tx['tx']['value']['msg']:
            try:
                address = msg['value']['delegator_address']
                db.add_account(address)
            except:
                # a message without a delegator_address means it was
                # a different type, such as a withdraw rewards etc,
                # included in the same transaction
                pass

    page += 1
    open(f".detect-delegators-page-num-{chain}", 'w').write(str(page))


all_accounts = db.get_accounts()
print(f"Total accounts: {len(all_accounts)}")


latest_block_time = datetime.datetime.strptime(
    sub('\.\d+Z$', "Z", rpc_status['result']['sync_info']['latest_block_time']),
    '%Y-%m-%dT%H:%M:%SZ'
)

for address in all_accounts:
    print(f"Generating report for {address} at {latest_block_time}...")

    latest_report_time = dict(db.get_latest_report(address) or {}).get('timestamp', None)
    ap = AccountProcessor(address)

    report, timestamp, height = ap.process_next(report_height, latest_block_time, latest_report_time)
    db.insert_report(address, timestamp, height, report)

    # run again if we just did genesis
    if latest_report_time is None:
        report, timestamp, height = ap.process_next(report_height, latest_block_time, timestamp)
        db.insert_report(address, timestamp, height, report)

    db.commit()

print("DONE")
