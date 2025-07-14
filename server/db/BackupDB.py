import os
import sqlite3
from dotenv import load_dotenv
from schemas import temp_transactions, temp_txs_last_sig, transfers, wallets, supported_projects, known_tokens

load_dotenv()


class BackupDB:

    def __init__(self, temp, distributor):

        # Connections
        self.temp_storage_connection = sqlite3.connect(f"backup/temp_storage/{distributor}.db")
        self.distributor_connection = sqlite3.connect(f"backup/{distributor}.db")
        self.config_connection = sqlite3.connect(f"backup/config.db")

        # Cursors
        self.config_cursor = self.config_connection.cursor()
        self.temp_storage_cursor = self.temp_storage_connection.cursor()
        self.distributor_cursor = self.distributor_connection.cursor()


    def create_tables(self, temp):
        self.distributor_cursor.execute(transfers)
        self.config_cursor.execute(wallets)
        self.config_cursor.execute(supported_projects)
        self.config_cursor.execute(known_tokens)
        self.temp_storage_cursor.execute(temp_transactions)
        self.temp_storage_cursor.execute(temp_txs_last_sig)

if __name__ == "__main__":
    b = BackupDB(True, "1")
    b.create_tables(True)