import sqlite3
import requests
from bolt11 import decode

con = sqlite3.connect('wallet.db')
con.row_factory = sqlite3.Row
cur = con.cursor()

con.execute('''CREATE TABLE IF NOT EXISTS providers
               (url text PRIMARY KEY, hasLostFunds Boolean NOT NULL, fees number, name text, description text, websiteUrl text)''')

con.execute('''CREATE TABLE IF NOT EXISTS wallets
               (url text NOT NULL, balance number NOT NULL, walltID text NOT NULL, adminKey text NOT NULL, invoiceKey text NOT NULL)''')

con.execute('''CREATE TABLE IF NOT EXISTS payments
               (paymentHash text, amount number NOT NULL, walletId text NOT NULL, paymentRequest text NOT NULL, isPending Boolean NOT NULL)''')


class Wallet(object):
    def __init__(self, row):
        self.rowid = row["rowid"]
        self.url = row["url"]
        self.balance = row["balance"]
        self.adminKey = row["adminKey"]
        self.invoiceKey = row["invoiceKey"]

    def pay(self, paymentRequest: str):
        invoice = decode(paymentRequest)
        amount = int(invoice.amount_msat / 1000)
        paymentData = _pay(self, paymentRequest)
        cur.execute("INSERT INTO payments VALUES('{}',{},{},'{}',{})".format(paymentData["payment_hash"], -amount,
                                                                             self.rowid, paymentRequest, False))
        cur.execute("UPDATE wallets SET balance = balance - {} WHERE rowid={}".format(amount, self.rowid))
        self.balance = self.balance - amount
        con.commit()

    def createInvoice(self, amount: int) -> str:
        invoiceData = _createInvoice(self, amount)
        print("generating invoice for", amount, " sats for wallet", self.rowid)
        cur.execute(
            "INSERT INTO payments VALUES('{}',{},'{}','{}',{})".format(invoiceData["payment_hash"], amount,
                                                                       self.rowid,
                                                                       invoiceData["payment_request"], True))
        con.commit()
        return invoiceData["payment_request"]


"""
LNBITS IMPLEMENTATION
"""


def _pay(wallet: Wallet, paymentRequest: str) -> object:
    jsonBody = {
        "out": True,
        "bolt11": paymentRequest
    }
    headers = {
        "X-Api-Key": wallet.adminKey
    }
    r = requests.post(wallet.url + "/api/v1/payments", json=jsonBody, headers=headers)
    if r.status_code == 201:
        r = r.json()
        return {"payment_hash": r["payment_hash"]}
    else:
        r = r.json()
        raise Exception(r)


def _createInvoice(wallet: Wallet, amount):
    jsonBody = {
        "out": False,
        "amount": amount,
        "memo": "test"
    }
    headers = {
        "X-Api-Key": wallet.invoiceKey
    }
    r = requests.post(wallet.url + "/api/v1/payments", json=jsonBody, headers=headers)
    if r.status_code == 201:
        r = r.json()
        return {"payment_hash": r["payment_hash"], "payment_request": r["payment_request"]}
    else:
        raise Exception(r.json())


def _paymentIsPending(wallet, payment):
    headers = {
        "X-Api-Key": wallet["invoiceKey"]
    }
    r = requests.get(wallet["url"] + "/api/v1/payments/" + payment["paymentHash"], headers=headers)
    if r.status_code == 200:
        if r.json()["paid"]:
            return False
    else:
        raise Exception(r.json())
    return True


def getTotalBalance():
    cur.execute("SELECT SUM(balance) as balance FROM wallets")
    for row in cur:
        return row["balance"]


def getMaxBalance():
    cur.execute("SELECT MAX(balance) as balance FROM wallets ")
    for row in cur:
        if row["balance"] is None:
            raise Exception("No wallets")
        else:
            return row["balance"]


def getMinBalance():
    cur.execute("SELECT MIN(balance) as balance FROM wallets ")
    for row in cur:
        if row["balance"] is None:
            raise Exception("No wallets")
        else:
            return row["balance"]


def getWalletWithMinBalance():
    cur.execute("SELECT rowid,* FROM wallets where balance=( SELECT MIN(balance) FROM wallets)")
    for row in cur:
        return row


def getWalletWithMaxBalance():
    cur.execute("SELECT rowid,* FROM wallets where balance=( SELECT MAX(balance) FROM wallets)")
    for row in cur:
        return row


def getWalletByUrl(url):
    cur.execute("SELECT rowid,* FROM wallets where rowid='{}'".format(url))
    for row in cur:
        return row


def getWalletById(id):
    cur.execute("SELECT rowid,* FROM wallets where rowid={}".format(id))
    for row in cur:
        return row


def getAllPendingPayments():
    cur.execute("SELECT * FROM payments WHERE isPending={}".format(True))
    results = []
    for row in cur:
        results.append(row)
    return results


def getWalletWithSecondLargestBalance():
    cur.execute(
        "SELECT rowid,* FROM wallets EXCEPT SELECT rowid,* FROM wallets where balance=( SELECT MAX(balance) FROM wallets) ORDER BY balance DESC")
    for row in cur:
        return row


def paymentIsSendable(amount):
    return getMaxBalance() >= amount


MAX_BALANCE_PER_WALLET = 22


def checkRulesAfterReceive():
    if getMinBalance() > MAX_BALANCE_PER_WALLET:
        raise Exception("Must create a wallet with a new provider or raise MAX BALANCE PER WALLET")
    if getMaxBalance() > MAX_BALANCE_PER_WALLET:
        # tramsfer from max --> min
        walletWithMaxBalance = Wallet(getWalletWithMaxBalance())
        walletWithMinBalance = Wallet(getWalletWithMinBalance())
        amount = walletWithMaxBalance.balance - MAX_BALANCE_PER_WALLET
        print(amount)
        transfer(walletWithMaxBalance, walletWithMinBalance, amount)


def pay(paymentRequest):
    invoice = decode(paymentRequest)
    amount = int(invoice.amount_msat / 1000)
    if amount > getTotalBalance():
        raise Exception("No enough funds available")
    wallet = Wallet(getWalletWithMaxBalance())
    print(wallet.rowid)
    while amount > getMaxBalance():
        secondLargestWallet = Wallet(getWalletWithSecondLargestBalance())
        print(wallet.rowid, secondLargestWallet.rowid)

        transfer(secondLargestWallet, wallet, secondLargestWallet.balance)

    print("atempting payment from wallet", wallet.rowid)
    wallet.pay(paymentRequest)

# returns payment_hash, payment_invoice:
def createInvoice(amount):
    wallet = Wallet(getWalletWithMinBalance())
    return wallet.createInvoice(amount)


def transfer(sourceWallet: Wallet, destinationWallet: Wallet, amount):
    if amount > sourceWallet.balance:
        raise Exception("Insufficient balance on sourceWallet(rowid): " + str(sourceWallet.rowid))
    paymentRequest = destinationWallet.createInvoice(amount)
    sourceWallet.pay(paymentRequest)
    checkAllPendingPayments()


def paymentIsPending(payment, wallet):
    return _paymentIsPending(wallet, payment)


def checkAllPendingPayments():
    for payment in getAllPendingPayments():
        print("checking payments:", payment["paymentHash"])
        wallet = getWalletById(payment["walletId"])
        if paymentIsPending(payment, wallet) == False:
            print("marked payment as completed")
            cur.execute(
                "UPDATE payments SET isPending = {} WHERE paymentHash='{}'".format(False, payment["paymentHash"]))
            cur.execute(
                "UPDATE wallets SET balance = balance + {} WHERE rowid={}".format(payment["amount"], wallet["rowid"]))
            con.commit()


# print(paymentIsSendable(0))
# con.execute("INSERT INTO wallets VALUES('sfa',30,'gsag','gsag','gsa')")
# con.commit()

# con.execute("INSERT INTO providers VALUES('{}',{},NULL,NULL,NULL,NULL)".format("https://lnbits.com",False))
# con.execute("INSERT INTO wallets VALUES('https://lnbits.com',20,'248fa81f7bcd4cf9baffd7a986f67fce','bdee09465483457da9808a1b00c459ce','1c76c00951814de99837a1ff359de704')")
# con.execute("INSERT INTO wallets VALUES('https://lnbits.com',30,'4b1a4649473a4675a8141750d13d3b1a','aeffddb5fc344d5e9e27b02d02dee6eb','d9b83894310e4c7cb690f021cce1130b')")
# con.commit()


i = "lnbc350n1ps25j0spp5rxfa356xncv5tzflffjf3scz3m0lyxqfxe8q84kcdp789lkrsleqdq9wahhwxqyjw5qcqpjsp54uc8swswzvr0a786x5emvgxldvwep5s4hvez0ah5s23fdugf0fyqrzjqwac3nxyg3f5mfa4ke9577c4u8kvkx8pqtdsusqdfww0aymk823x6znwa5qqzyqqqyqqqqq2qqqqqsqq9q9qy9qsq3f3t2e8ej36pwrsshwj9a6hsn3q9uu5t74uen7s7dwek3p6dnyvha0fhvq0ym5l4tlnjxs2w25yt205fw3xtarcrwd49zqdrnqp7arqpvc33t4"

#pay(i)
# print(getWalletWithSecondLargestBalance()["rowid"])
# print(createInvoice(50))
checkRulesAfterReceive()
transfer(Wallet(getWalletById(2)),Wallet(getWalletById(1)),12)
# print(createInvoice(30))
print(checkAllPendingPayments())
