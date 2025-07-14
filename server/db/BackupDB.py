import os
import sqlite3
import json
from dotenv import load_dotenv
from schemas import temp_transactions, temp_txs_last_sigs, transfers, wallets, supported_projects, known_tokens

load_dotenv()


class BackupDB:
    """
    The BackupDB class is used to manage the backup data for the rewards token tracker. It creates a
    DB for every distributor which stores all of the transfers. There is a "config" DB that stores all
    of the wallet rewards data, supported projects and their most recent tx sigs, and known tokens.
    In addition there is also a temp DB that used for initializing new projects and provides a place
    to quickly store txs so they can be processed and added to production.
    """

    def __init__(self, temp, distributor):
        """ Initialize the BackupDB class by configuring DB's """
        self.temp = temp

        # Configure db collections
        self.config_connection = sqlite3.connect(f"backup/config.db")
        self.config_cursor = self.config_connection.cursor()

        if self.temp:
            self.temp_storage_connection = sqlite3.connect(f"backup/temp_storage/{distributor}.db")
            self.temp_storage_cursor = self.temp_storage_connection.cursor()
        else:
            self.distributor_connection = sqlite3.connect(f"backup/{distributor}.db")
            self.distributor_cursor = self.distributor_connection.cursor()


    def create_tables(self, temp):
        """ Creates the tables for the dbs """
        self.config_cursor.execute(wallets)
        self.config_cursor.execute(supported_projects)
        self.config_cursor.execute(known_tokens)

        # Only needed when initializing new projects
        if temp:
            self.temp_storage_cursor.execute(temp_transactions)
            self.temp_storage_cursor.execute(temp_txs_last_sigs)
            self.temp_storage_cursor.execute(transfers)
        else:
            self.distributor_cursor.execute(transfers)

    def create_indexes(self):
        pass

    ##########################################################
    #               Project Initializer Functions            #
    ##########################################################
    def insert_temp_txs_batch(self, batch, batch_size=1000):
        """
        Insert a batch of temporary transactions into the temp_transactions table
        """
        try:
            # Process in batches to avoid memory issues with large datasets
            for i in range(0, len(batch), batch_size):
                batch_chunk = batch[i:i + batch_size]

                # Prepare data for insertion
                data_to_insert = []
                for tx in batch_chunk:

                    # Serialize list data
                    token_transfers_json = json.dumps(tx.get('token_transfers', []))
                    native_transfers_json = json.dumps(tx.get('native_transfers', []))

                    data_to_insert.append((
                        tx.get('fee_payer', ''),
                        tx.get('signature', ''),
                        tx.get('slot', 0),
                        tx.get('timestamp', 0),
                        token_transfers_json,
                        native_transfers_json
                    ))

                # Insert batch
                self.temp_storage_cursor.executemany(
                    """INSERT INTO temp_transactions
                       (fee_payer, signature, slot, timestamp, token_transfers, native_transfers)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    data_to_insert
                )

            self.temp_storage_connection.commit()
            print(f"Successfully inserted {len(batch)} temporary transactions")

        except Exception as e:
            print(f"Error inserting temp transactions batch: {e}")
            self.temp_storage_connection.rollback()

    def update_temp_txs_before_sig(self, new_sig):
        """
        Update the 'before' signature in temp_txs_last_sigs table.
        """
        try:
            self.temp_storage_cursor.execute(
                """UPDATE temp_txs_last_sigs SET before = ? WHERE id = 1""",
                (new_sig,)
            )

            # If no rows were updated, insert a new record
            if self.temp_storage_cursor.rowcount == 0:
                self.temp_storage_cursor.execute(
                    """INSERT INTO temp_txs_last_sigs (before, last_sig) VALUES (?, '')""",
                    (new_sig,)
                )

            self.temp_storage_connection.commit()
            print(f"Updated temp_txs before signature to: {new_sig}")

        except Exception as e:
            print(f"Error updating temp_txs before signature: {e}")
            self.temp_storage_connection.rollback()

    def update_temp_txs_last_sig(self, new_sig):
        """
        Update the 'last_sig' signature in temp_txs_last_sigs table
        """
        try:
            self.temp_storage_cursor.execute(
                """UPDATE temp_txs_last_sigs SET last_sig = ? WHERE id = 1""",
                (new_sig,)
            )

            # If no rows were updated, insert a new record
            if self.temp_storage_cursor.rowcount == 0:
                self.temp_storage_cursor.execute(
                    """INSERT INTO temp_txs_last_sigs (before, last_sig) VALUES ('', ?)""",
                    (new_sig,)
                )

            self.temp_storage_connection.commit()
            print(f"Updated temp_txs last signature to: {new_sig}")

        except Exception as e:
            print(f"Error updating temp_txs last signature: {e}")
            self.temp_storage_connection.rollback()

    def get_temp_txs_last_sigs(self):
        """
        Get the last signatures from temp_txs_last_sigs table
        """
        try:
            self.temp_storage_cursor.execute(
                """SELECT before, last_sig FROM temp_txs_last_sigs ORDER BY id DESC LIMIT 1"""
            )
            result = self.temp_storage_cursor.fetchone()

            if result:
                return {
                    'before': result[0],
                    'last_sig': result[1]
                }
            return None

        except Exception as e:
            print(f"Error getting temp_txs last signatures: {e}")
            return None

    def get_temp_transactions(self, limit=None):
        """
        Retrieve temporary transactions from the temp_transactions table
        """
        try:
            if limit:
                query = """SELECT fee_payer, signature, slot, timestamp, token_transfers, native_transfers
                          FROM temp_transactions ORDER BY id DESC LIMIT ?"""
                self.temp_storage_cursor.execute(query, (limit,))
            else:
                query = """SELECT fee_payer, signature, slot, timestamp, token_transfers, native_transfers
                          FROM temp_transactions ORDER BY id DESC"""
                self.temp_storage_cursor.execute(query)

            results = self.temp_storage_cursor.fetchall()

            # Parse JSON strings back to Python objects
            transactions = []
            for row in results:
                tx = {
                    'fee_payer': row[0],
                    'signature': row[1],
                    'slot': row[2],
                    'timestamp': row[3],
                    'token_transfers': json.loads(row[4]) if row[4] else [],
                    'native_transfers': json.loads(row[5]) if row[5] else []
                }
                transactions.append(tx)

            return transactions

        except Exception as e:
            print(f"Error retrieving temp transactions: {e}")
            return []

    def get_temp_transactions_count(self):
        """
        Get the total count of temporary transactions in the temp_transactions table
        """
        try:
            self.temp_storage_cursor.execute("SELECT COUNT(*) FROM temp_transactions")
            result = self.temp_storage_cursor.fetchone()
            return result[0] if result else 0

        except Exception as e:
            print(f"Error getting temp transactions count: {e}")
            return 0

    def migrate_to_production(self):
        pass
    ##########################################################
    #               Production Backup Function               #
    ##########################################################
    def insert_transfer_batch(self, batch, batch_size=1000):
        """
        Insert a batch of transfers into the transfers table.

        Args:
            batch (list): List of transfer dictionaries with keys:
                         signature, slot, timestamp, amount, token, wallet_address, distributor
            batch_size (int): Size of each batch for insertion
        """
        try:
            # Process in batches to avoid memory issues with large datasets
            for i in range(0, len(batch), batch_size):
                batch_chunk = batch[i:i + batch_size]

                # Prepare data for insertion
                data_to_insert = []
                for transfer in batch_chunk:
                    data_to_insert.append((
                        transfer.get('signature', ''),
                        transfer.get('slot', 0),
                        transfer.get('timestamp', 0),
                        transfer.get('amount', 0.0),
                        transfer.get('token', ''),
                        transfer.get('wallet_address', ''),
                        transfer.get('distributor', '')
                    ))

                # Insert batch
                self.distributor_cursor.executemany(
                    """INSERT INTO transfers
                       (signature, slot, timestamp, amount, token, wallet_address, distributor)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    data_to_insert
                )

            self.distributor_connection.commit()
            print(f"Successfully inserted {len(batch)} transfers")

        except Exception as e:
            print(f"Error inserting transfer batch: {e}")
            self.distributor_connection.rollback()

    def insert_or_update_wallets(self, wallets_data):
        """
        Insert or update wallet information in the wallets table.

        Args:
            wallets_data (list): List of wallet dictionaries with keys:
                               wallet_address, distributors
        """
        try:
            for wallet in wallets_data:
                wallet_address = wallet.get('wallet_address', '')
                distributors = wallet.get('distributors', '')

                # Check if wallet already exists
                self.config_cursor.execute(
                    """SELECT id FROM wallets WHERE wallet_address = ?""",
                    (wallet_address,)
                )
                existing = self.config_cursor.fetchone()

                if existing:
                    # Update existing wallet
                    self.config_cursor.execute(
                        """UPDATE wallets SET distributors = ? WHERE wallet_address = ?""",
                        (distributors, wallet_address)
                    )
                else:
                    # Insert new wallet
                    self.config_cursor.execute(
                        """INSERT INTO wallets (wallet_address, distributors) VALUES (?, ?)""",
                        (wallet_address, distributors)
                    )

            self.config_connection.commit()
            print(f"Successfully processed {len(wallets_data)} wallets")

        except Exception as e:
            print(f"Error inserting/updating wallets: {e}")
            self.config_connection.rollback()

    def insert_known_token(self, known_token):
        """
        Insert a known token into the known_tokens table.

        Args:
            known_token (dict): Dictionary with keys: symbol, name, mint, decimals
        """
        try:
            self.config_cursor.execute(
                """INSERT INTO known_tokens (symbol, name, mint, decimals) VALUES (?, ?, ?, ?)""",
                (
                    known_token.get('symbol', ''),
                    known_token.get('name', ''),
                    known_token.get('mint', ''),
                    known_token.get('decimals', '')
                )
            )
            self.config_connection.commit()
            print(f"Successfully inserted known token: {known_token.get('symbol', 'Unknown')}")

        except Exception as e:
            print(f"Error inserting known token: {e}")
            self.config_connection.rollback()

    def insert_supported_project(self, supported_project):
        """
        Insert a supported project into the supported_projects table.

        Args:
            supported_project (dict): Dictionary with keys: name, distributor, token_mint, dev_wallet, last_sig
        """
        try:
            self.config_cursor.execute(
                """INSERT INTO supported_projects (name, distributor, token_mint, dev_wallet, last_sig)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    supported_project.get('name', ''),
                    supported_project.get('distributor', ''),
                    supported_project.get('token_mint', ''),
                    supported_project.get('dev_wallet', ''),
                    supported_project.get('last_sig', '')
                )
            )
            self.config_connection.commit()
            print(f"Successfully inserted supported project: {supported_project.get('name', 'Unknown')}")

        except Exception as e:
            print(f"Error inserting supported project: {e}")
            self.config_connection.rollback()

    def close_connections(self):
        """Close all database connections"""
        try:

            self.config_connection.close()

            if self.temp:
                self.temp_storage_connection.close()
            else:
                self.distributor_connection.close()

        except Exception as e:
            print(f"Error closing connections: {e}")

    def __del__(self):
        """Destructor to ensure connections are closed"""
        self.close_connections()


if __name__ == "__main__":
    b = BackupDB(True, "HHBkrmzwY7TbDG3G5C4D52LPPd8JEs5oiKWHaPxksqvd")
    #b.create_tables(True)

    # Example usage:
    # Insert some temp transactions
    # temp_txs = [
    #     {
    #         'fee_payer': 'wallet1',
    #         'signature': 'sig1',
    #         'slot': 12345,
    #         'timestamp': 1640995200,
    #         'token_transfers': [{"hello": "hello"}],
    #         'native_transfers': [{"hell1o": "hell1o"}]
    #     }
    # ]
    # b.insert_temp_txs_batch(temp_txs)

    print(b.get_temp_transactions_count())

    # Close connections when done
    b.close_connections()