import os
import sqlite3
import json
from dotenv import load_dotenv
from .schemas import (
    temp_transactions,
    temp_txs_last_sigs,
    transfers,
    wallets,
    supported_projects,
    known_tokens,
)

load_dotenv()

class SQLiteDB:
    """
    The SQLiteDB class is used to manage the backup data for the rewards token tracker. It creates a
    DB for every distributor which stores all of the transfers. There is a "config" DB that stores all
    of the wallet rewards data, supported projects and their most recent tx sigs, and known tokens.
    """

    def __init__(self, temp=True):
        """Initialize the BackupDB class by configuring DB's"""
        self.temp = temp

        # Configure db collections
        self.config_connection = sqlite3.connect(f"backup/config.db")
        self.config_cursor = self.config_connection.cursor()

        # This one is for the temp transfers
        self.temp_transfers_connection = sqlite3.connect("backup/temp_transfers")
        self.temp_transfers_cursor = self.temp_transfers_connection.cursor()

        # Create the tables if they haven't been already
        self.create_config_tables()
        self.create_config_indexes()

    def __del__(self):
        """Destructor to ensure connections are closed"""
        self.close_connections()

    ##########################################################
    #                         DB Tables                      #
    ##########################################################
    def create_config_tables(self):
        """Creates the tables for the dbs"""
        self.config_cursor.execute(wallets)
        self.config_cursor.execute(supported_projects)
        self.config_cursor.execute(known_tokens)

        # Temp transfers db
        self.temp_transfers_cursor.execute(transfers)

    def create_distributor_tables(self, distributor):
        """Creates the tables for the dbs"""
        connection, cursor = self.get_distributors_db(distributor)

        # Create the transfers table
        cursor.execute(transfers)

        # Only needed when initializing new projects
        if self.temp:
            cursor.execute(temp_transactions)
            cursor.execute(temp_txs_last_sigs)

    ##########################################################
    #                        DB Indexes                      #
    ##########################################################
    def create_config_indexes(self):
        """ Config indexes for better lookups """
        try:
            # Config database indexes
            config_indexes = [
                # Supported projects table indexes
                "CREATE INDEX IF NOT EXISTS idx_supported_projects_token_mint ON supported_projects(token_mint)",
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_supported_projects_distributor ON supported_projects(distributor)",
                "CREATE INDEX IF NOT EXISTS idx_supported_projects_last_sig ON supported_projects(last_sig)",

                # Wallets table indexes
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_wallets_wallet_address ON wallets(wallet_address)",

                # Known tokens table indexes
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_known_tokens_mint ON known_tokens(mint)",
            ]
            transfers_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_transfers_wallet_distributor ON transfers(wallet_address, distributor)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_signature ON transfers(signature)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_wallet_address ON transfers(wallet_address)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_distributor ON transfers(distributor)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_slot ON transfers(slot)",
            ]

            # Execute config database indexes
            for index_sql in config_indexes:
                self.config_cursor.execute(index_sql)

            for index_sql in transfers_indexes:
                self.temp_transfers_cursor.execute(index_sql)

            # Commit all changes
            self.config_connection.commit()
            self.temp_transfers_connection.commit()
            return True
        except Exception as e:
            print(f"Error when creating config indexes! {e}")
            return False

    def create_distributor_indexes(self, distributor):

        # Delete any duplicates before trying to make the indexes
        success = self.delete_duplicate_transfers(distributor)
        if success is not True:
            return False

        # Get the connection the the distributors db
        connection, cursor = self.get_distributors_db(distributor)

        try:
            # Transfers table indexes (for both temp and distributor databases)
            transfers_indexes = [
                # Composite unique index
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_transfers_unique ON transfers(wallet_address, distributor, signature, slot, timestamp, token, amount)",

                # Individual indexes
                "CREATE INDEX IF NOT EXISTS idx_transfers_wallet_distributor ON transfers(wallet_address, distributor)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_signature ON transfers(signature)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_wallet_address ON transfers(wallet_address)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_distributor ON transfers(distributor)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_transfers_slot ON transfers(slot)",
            ]

            # Execute the indexes
            for index_sql in transfers_indexes:
                cursor.execute(index_sql)

            # Commit all changes
            connection.commit()

            print("Database indexes created successfully")
            return True
        except Exception as e:
            print(f"Error when creating transfer indexes! {e}")
            return False

    ##########################################################
    #                DB Connection Management                #
    ##########################################################
    def get_distributors_db(self, distributor):
        connection = sqlite3.connect(
            f"backup/transfers/{distributor}.db"
        )
        cursor = connection.cursor()

        return connection, cursor

    def close_connections(self):
        """Close all database connections"""
        try:
            self.config_connection.close()
            self.temp_transfers_connection.close()

        except Exception as e:
            print(f"Error closing connections")

    def close_distributor_connection(self, distributor):
        connection, cursor = self.get_distributors_db(distributor)
        try:
            connection.close()
        except Exception as e:
            print(f"Error closing distributor: {distributor} connection: {e}")

    ##########################################################
    #                       DB Clean Up                      #
    ##########################################################
    def clean_and_remove_temp_data(self, distributor):
        """
        This removes any duplicates from processing the transactions and removes them. Then it creates the
        indexes for the transfer table within the distributors db. Finally it drops the temp tables
        """
        # Delete any duplicates
        success = self.create_distributor_indexes(distributor)

        if success is not True:
            return False

        # Drop the temp tables since we don't need them anymore
        sucess = self.drop_temp_tables(distributor)

        if sucess is not True:
            return False

        return True

    def drop_temp_tables(self, distributor):
        """Drop temporary tables used for transaction processing."""
        # Tables to drop
        temp_tables = [
            'temp_transactions',
            'temp_txs_last_sigs'
        ]

        connection, cursor = self.get_distributors_db(distributor)

        try:
            for table in temp_tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"Dropped table: {table}")

            connection.commit()
            return True
        except Exception as e:
            print(f"Error dropping table: {e}")
            return False

    ##########################################################
    #               Supported Projects Functions             #
    ##########################################################
    def get_supported_projects(self):
        """
        Get all supported projects from the supported_projects table
        """
        try:
            self.config_cursor.execute(
                """SELECT name, distributor, token_mint, dev_wallet, last_sig
                FROM supported_projects ORDER BY name"""
            )
            results = self.config_cursor.fetchall()

            # Convert to list of dictionaries for consistency with MongoDB
            projects = []
            for row in results:
                project = {
                    "name": row[0],
                    "distributor": row[1],
                    "token_mint": row[2],
                    "dev_wallet": row[3],
                    "last_sig": row[4]
                }
                projects.append(project)

            return projects

        except Exception as e:
            print(f"Error getting supported projects: {e}")
            raise

    def get_supported_project(self, distributor):
        """
        Get a specific supported project by distributor address
        """
        try:
            self.config_cursor.execute(
                """SELECT name, distributor, token_mint, dev_wallet, last_sig
                FROM supported_projects WHERE distributor = ?""",
                (distributor,)
            )
            result = self.config_cursor.fetchone()

            if result:
                project = {
                    "name": result[0],
                    "distributor": result[1],
                    "token_mint": result[2],
                    "dev_wallet": result[3],
                    "last_sig": result[4]
                }
                return project
            else:
                return None

        except Exception as e:
            print(f"Error getting supported project with distributor {distributor}: {e}")
            raise

    def get_supported_project_count(self):
        """
        Get the total count of supported projects
        """
        try:
            self.config_cursor.execute("SELECT COUNT(*) FROM supported_projects")
            result = self.config_cursor.fetchone()
            return result[0] if result else 0

        except Exception as e:
            print(f"Error getting supported projects count: {e}")
            raise

    def update_supported_project(self, updated_project):
        """
        Update an existing supported project by distributor
        """
        try:
            self.config_cursor.execute(
                """UPDATE supported_projects
                SET name = ?, token_mint = ?, dev_wallet = ?, last_sig = ?
                WHERE distributor = ?""",
                (
                    updated_project.get("name"),
                    updated_project.get("token_mint"),
                    updated_project.get("dev_wallet", ""),
                    updated_project.get("last_sig", ""),
                    updated_project.get("distributor")
                )
            )

            self.config_connection.commit()

            print(f"Successfully updated supported project: {updated_project.get('name')}")
            return True

        except Exception as e:
            print(f"Error updating supported project: {e}")
            self.config_connection.rollback()
            raise

    def insert_supported_project(self, project):
        """
        Insert a supported project into the supported_projects table
        """
        try:
            self.config_cursor.execute(
                """INSERT INTO supported_projects (name, distributor, token_mint, dev_wallet, last_sig)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    project.get("name"),
                    project.get("distributor"),
                    project.get("token_mint"),
                    project.get("dev_wallet", ""),
                    project.get("last_sig", ""),
                ),
            )
            self.config_connection.commit()
            print(
                f"Successfully inserted supported project: {project.get('name')}"
            )
            return True

        except Exception as e:
            print(f"Error inserting supported project: {e}")
            self.config_connection.rollback()
            raise

    def upsert_supported_project(self, project):
        """
        Insert or update a supported project (upsert operation)
        """
        try:
            # Check if project exists
            existing_project = self.get_supported_project(project.get("distributor"))

            if existing_project:
                # Update existing project
                return self.update_supported_project(project)
            else:
                # Insert new project
                return self.insert_supported_project(project)

        except Exception as e:
            print(f"Error upserting supported project: {e}")
            raise

    ##########################################################
    #                  Known Tokens Functions                #
    ##########################################################
    def get_known_tokens(self):
        """
        Gets all of the known tokens from the known_tokens table
        """
        try:
            self.config_cursor.execute(
                """SELECT symbol, name, mint, decimals
                FROM known_tokens ORDER BY name"""
            )
            results = self.config_cursor.fetchall()

            tokens = []
            for row in results:
                token = {
                    "symbol": row[0],
                    "name": row[1],
                    "mint": row[2],
                    "decimals": row[3]
                }
                tokens.append(project)

            return tokens

        except Exception as e:
            print(f"Error getting known tokens: {e}")
            raise

    def get_known_token(self, mint):
        """
        Gets a specific known token by the mint address from known_tokens table
        """
        try:
            self.config_cursor.execute(
                """SELECT symbol, name, mint, decimals
                FROM known_tokens WHERE mint = ?""",
                (mint,)
            )
            result = self.config_cursor.fetchone()

            if result:
                token = {
                    "symbol": result[0],
                    "name": result[1],
                    "mint": result[2],
                    "decimals": result[3]
                }
                return token
            else:
                return None

        except Exception as e:
            print(f"Error getting known token: {e}")
            raise

    def get_known_token_count(self):
        """
        Gets the total count of known tokens from known_tokens table
        """
        try:
            self.config_cursor.execute("SELECT COUNT(*) FROM known_tokens")
            result = self.config_cursor.fetchone()
            return result[0] if result else 0

        except Exception as e:
            print(f"Error getting known tokens count: {e}")
            raise

    def insert_known_token(self, token):
        """
        Insert a known token into the known_tokens table if it doesn't exsist already
        """
        # Check if the token is there already
        exsists = self.get_known_token(token.get("mint"))
        if exsists:
            print("Token is already in known tokens table. Skipping...")
            return True

        try:
            self.config_cursor.execute(
                """INSERT INTO known_tokens (symbol, name, mint, decimals) VALUES (?, ?, ?, ?)""",
                (
                    token.get("symbol"),
                    token.get("name"),
                    token.get("mint"),
                    token.get("decimals", ""),
                ),
            )
            self.config_connection.commit()
            print(
                f"Successfully inserted known token: {token.get('symbol')}"
            )
            return True

        except Exception as e:
            print(f"Error inserting known token: {e}")
            self.config_connection.rollback()
            raise

    ##########################################################
    #                 Transactions Functions                 #
    ##########################################################
    def get_transactions(self, distributor, offset, batch_size=1000):
        """
        Generator that yields batches with resume capability.
        """
        connection, cursor = self.get_distributors_db(distributor)
        try:
            current_offset = offset
            query = """SELECT fee_payer, signature, slot, timestamp, token_transfers, native_transfers
                        FROM temp_transactions
                        ORDER BY id ASC
                        LIMIT ? OFFSET ?"""

            while True:
                cursor.execute(query, (batch_size, current_offset))
                results = cursor.fetchall()

                if not results:
                    # No more data to process - successful completion
                    break

                # Parse JSON strings back to Python objects
                transactions = []
                for row in results:
                    tx = {
                        "fee_payer": row[0],
                        "signature": row[1],
                        "slot": row[2],
                        "timestamp": row[3],
                        "token_transfers": json.loads(row[4]) if row[4] else [],
                        "native_transfers": json.loads(row[5]) if row[5] else [],
                    }
                    transactions.append(tx)

                # Yield the batch and current offset
                yield transactions, current_offset

                current_offset += batch_size

        except Exception as e:
            print(f"Error retrieving temp transactions batch: {e}")
            return None, current_offset

    def get_transactions_count(self, distributor):
        """
        Get the total count of temporary transactions in the temp_transactions table
        """
        connection, cursor = self.get_distributors_db(distributor)
        try:
            cursor.execute("SELECT COUNT(*) FROM temp_transactions")
            result = cursor.fetchone()
            return result[0] if result else 0

        except Exception as e:
            print(f"Error getting temp transactions count: {e}")
            return 0

    def insert_transactions_batch(self, distributor, batch, batch_size=5000):
        """
        Insert a batch of temporary transactions into the temp_transactions table.
        Optimized for performance with larger batch sizes and better SQLite settings.
        """
        if not batch:
            return True

        connection, cursor = self.get_distributors_db(distributor)

        try:
            # Set SQLite performance optimizations
            cursor.execute("PRAGMA journal_mode = MEMORY")
            cursor.execute("PRAGMA cache_size = 10000")

            # Begin explicit transaction
            cursor.execute("BEGIN TRANSACTION")

            # Process in larger batches
            for i in range(0, len(batch), batch_size):
                batch_chunk = batch[i : i + batch_size]

                # Pre-allocate list and use list comprehension for speed
                data_to_insert = [
                    (
                        tx.get("fee_payer", ""),
                        tx.get("signature", ""),
                        tx.get("slot", 0),
                        tx.get("timestamp", 0),
                        json.dumps(tx.get("token_transfers", [])),
                        json.dumps(tx.get("native_transfers", [])),
                    )
                    for tx in batch_chunk
                ]

                # Insert batch
                cursor.executemany(
                    """INSERT INTO temp_transactions
                       (fee_payer, signature, slot, timestamp, token_transfers, native_transfers)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    data_to_insert,
                )

            # Commit the entire transaction at once
            self.distributor_connection.commit()
            # print(f"Successfully inserted {len(batch)} temporary transactions")
            return True

        except Exception as e:
            print(f"Error inserting temp transactions batch: {e}")
            self.distributor_connection.rollback()
            return False

    ##########################################################
    #                    Transfer Functions                  #
    ##########################################################
    def get_transfers(self, distributor, offset, batch_size=1000):
        """
        Generator that yields batches with resume capability.
        """
        connection, cursor = self.get_distributors_db(distributor)

        try:
            current_offset = offset
            query = """SELECT signature, slot, timestamp, amount, token, wallet_address, distributor
                        FROM transfers
                        ORDER BY id ASC
                        LIMIT ? OFFSET ?"""

            while True:
                cursor.execute(query, (batch_size, current_offset))
                results = cursor.fetchall()

                if not results:
                    # No more data to process - successful completion
                    break

                # Parse JSON strings back to Python objects
                transfers = []
                for row in results:
                    tx = {
                        "signature": row[0],
                        "slot": row[1],
                        "timestamp": row[2],
                        "amount": row[3],
                        "token": row[4],
                        "wallet_address": row[5],
                        "distributor": row[6]
                    }
                    transfers.append(tx)

                # Yield the batch and current offset
                yield transfers, current_offset

                current_offset += batch_size

        except Exception as e:
            print(f"Error retrieving temp transactions batch: {e}")
            return None, current_offset

    def get_transfers_count(self, distributor):
        """
        Get the total count of temporary transfers in the transfers table
        """
        connection, cursor = self.get_distributors_db(distributor)
        try:
            cursor.execute("SELECT COUNT(*) FROM transfers")
            result = cursor.fetchone()
            return result[0] if result else 0

        except Exception as e:
            print(f"Error getting transfers count for {distributor}: {e}")
            return 0

    def insert_transfer_batch(self, distributor, batch, batch_size=5000):
        """
        Insert a batch of transfers into the transfers table of the distributor db. This will
        be used to store the transfers by distributor from the transfers in the config db transfers table
        """
        connection, cursor = self.get_distributors_db(distributor)

        try:
            # Process in batches to avoid memory issues with large datasets
            for i in range(0, len(batch), batch_size):
                batch_chunk = batch[i : i + batch_size]

                # Prepare data for insertion
                data_to_insert = []
                for transfer in batch_chunk:
                    data_to_insert.append(
                        (
                            transfer.get("signature", ""),
                            transfer.get("slot", 0),
                            transfer.get("timestamp", 0),
                            transfer.get("amount", 0.0),
                            transfer.get("token", ""),
                            transfer.get("wallet_address", ""),
                            transfer.get("distributor", ""),
                        )
                    )

                # Insert batch
                cursor.executemany(
                    """INSERT INTO transfers
                       (signature, slot, timestamp, amount, token, wallet_address, distributor)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    data_to_insert,
                )

            connection.commit()
            # print(f"Successfully inserted {len(batch)} transfers")
            return True

        except Exception as e:
            print(f"Error inserting transfer batch: {e}")
            connection.rollback()
            return False

    def delete_duplicate_transfers(self, distributor):
        """
        Delete duplicate records from the transfers table based on the unique constraint
        (wallet_address, distributor, signature, slot, timestamp, token, amount)
        """
        connection, cursor = self.get_distributors_db(distributor)
        try:
            # First, let's check if there are duplicates
            cursor.execute("""
                SELECT wallet_address, distributor, signature, slot, timestamp, token, amount, COUNT(*) as count
                FROM transfers
                GROUP BY wallet_address, distributor, signature, slot, timestamp, token, amount
                HAVING COUNT(*) > 1
            """)

            duplicates = cursor.fetchall()

            if not duplicates:
                print("No duplicates found in transfers table")
                return True

            print(f"Found {len(duplicates)} groups of duplicate records")

            # Delete duplicates, keeping only the first occurrence (lowest id)
            cursor.execute("""
                DELETE FROM transfers
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM transfers
                    GROUP BY wallet_address, distributor, signature, slot, timestamp, token, amount
                )
            """)

            deleted_count = cursor.rowcount
            connection.commit()

            print(f"Successfully deleted {deleted_count} duplicate records from transfers table")
            return True

        except Exception as e:
            print(f"Error deleting duplicates: {e}")
            connection.rollback()
            return False

    ##########################################################
    #                 Temp Transfer Functions                #
    ##########################################################
    def get_temp_transfers(self, offset, batch_size=1000):
        """
        Generator that yields batches with resume capability.
        """
        try:
            current_offset = offset
            query = """SELECT signature, slot, timestamp, amount, token, wallet_address, distributor
                        FROM transfers
                        ORDER BY id ASC
                        LIMIT ? OFFSET ?"""

            while True:
                self.temp_transfers_cursor.execute(query, (batch_size, current_offset))
                results = self.temp_transfers_cursor.fetchall()

                if not results:
                    # No more data to process - successful completion
                    break

                # Parse JSON strings back to Python objects
                transfers = []
                for row in results:
                    tx = {
                        "signature": row[0],
                        "slot": row[1],
                        "timestamp": row[2],
                        "amount": row[3],
                        "token": row[4],
                        "wallet_address": row[5],
                        "distributor": row[6]
                    }
                    transfers.append(tx)

                # Yield the batch and current offset
                yield transfers, current_offset

                current_offset += batch_size

        except Exception as e:
            print(f"Error retrieving temp transactions batch: {e}")
            return None, current_offset

    def get_temp_transfers_count(self):
        """
        Get the total count of temporary transfers in the transfers table
        """
        try:
            self.temp_transfers_cursor.execute("SELECT COUNT(*) FROM transfers")
            result = self.temp_transfers_cursor.fetchone()
            return result[0] if result else 0

        except Exception as e:
            print(f"Error getting temp transactions count: {e}")
            return 0

    def insert_temp_transfers_batch(self, batch, batch_size=5000):
        """
        Insert a batch of temp transfers config db. Later this will be pulled and placed in
        the proper db on the local backup. This table will store many distributors
        """
        try:
            # Process in batches to avoid memory issues with large datasets
            for i in range(0, len(batch), batch_size):
                batch_chunk = batch[i : i + batch_size]

                # Prepare data for insertion
                data_to_insert = []
                for transfer in batch_chunk:
                    data_to_insert.append(
                        (
                            transfer.get("signature", ""),
                            transfer.get("slot", 0),
                            transfer.get("timestamp", 0),
                            transfer.get("amount", 0.0),
                            transfer.get("token", ""),
                            transfer.get("wallet_address", ""),
                            transfer.get("distributor", ""),
                        )
                    )

                # Insert batch
                self.temp_transfers_cursor_cursor.executemany(
                    """INSERT INTO transfers
                       (signature, slot, timestamp, amount, token, wallet_address, distributor)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    data_to_insert,
                )

            self.temp_transfers_connection.commit()
            # print(f"Successfully inserted {len(batch)} transfers")
            return True

        except Exception as e:
            print(f"Error inserting transfer batch: {e}")
            self.temp_transfers_connection.rollback()
            return False

    def delete_duplicate_temp_transfers(self):
        """
        Delete duplicate records from the transfers table based on the unique constraint
        (wallet_address, distributor, signature, slot, timestamp, token, amount)
        """
        try:
            # First, let's check if there are duplicates
            self.temp_transfers_cursor.execute("""
                SELECT wallet_address, distributor, signature, slot, timestamp, token, amount, COUNT(*) as count
                FROM transfers
                GROUP BY wallet_address, distributor, signature, slot, timestamp, token, amount
                HAVING COUNT(*) > 1
            """)

            duplicates = self.temp_transfers_cursor.fetchall()

            if not duplicates:
                print("No duplicates found in transfers table")
                return True

            print(f"Found {len(duplicates)} groups of duplicate records")

            # Delete duplicates, keeping only the first occurrence (lowest id)
            self.temp_transfers_cursor.execute("""
                DELETE FROM transfers
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM transfers
                    GROUP BY wallet_address, distributor, signature, slot, timestamp, token, amount
                )
            """)

            deleted_count = cursor.rowcount
            self.temp_transfers_connection.commit()

            print(f"Successfully deleted {deleted_count} duplicate records from transfers table")
            return True

        except Exception as e:
            print(f"Error deleting duplicates: {e}")
            self.temp_transfers_connection.rollback()
            return False

    def delete_all_transfers(self):
        """
        After all of the transfers have been copied to the backup db we can delete everything
        from the transfers table to free up space
        """
        pass

    ##########################################################
    #                     Wallets Functions                  #
    ##########################################################
    def get_wallets(self):
        pass

    def get_wallet_data(self, wallet_address):
        pass

    def get_wallets_count(self):
        pass

    def insert_wallet_batch(self, wallets, batch_size=1000):
        """
        Insert or update wallet information in the wallets table
        """
        try:
            for wallet in wallets_data:
                wallet_address = wallet.get("wallet_address", "")
                distributors = wallet.get("distributors", "")

                # Check if wallet already exists
                self.config_cursor.execute(
                    """SELECT id FROM wallets WHERE wallet_address = ?""",
                    (wallet_address,),
                )
                existing = self.config_cursor.fetchone()

                if existing:
                    # Update existing wallet
                    self.config_cursor.execute(
                        """UPDATE wallets SET distributors = ? WHERE wallet_address = ?""",
                        (distributors, wallet_address),
                    )
                else:
                    # Insert new wallet
                    self.config_cursor.execute(
                        """INSERT INTO wallets (wallet_address, distributors) VALUES (?, ?)""",
                        (wallet_address, distributors),
                    )

            self.config_connection.commit()
            print(f"Successfully processed {len(wallets_data)} wallets")

        except Exception as e:
            print(f"Error inserting/updating wallets: {e}")
            self.config_connection.rollback()

    ##########################################################
    #                 Last Signature Functions               #
    ##########################################################
    def get_temp_txs_last_sigs(self, distributor):
        """
        Get the last signatures from temp_txs_last_sigs table
        """
        connection, cursor = self.get_distributors_db(distributor)
        try:
            cursor.execute(
                """SELECT before, last_sig FROM temp_txs_last_sigs ORDER BY id DESC LIMIT 1"""
            )
            result = cursor.fetchone()

            if result:
                return result[0], result[1]
            return None, None

        except Exception as e:
            print(f"Error getting temp_txs last signatures: {e}")
            return None, None

    def update_temp_txs_before_sig(self, distributor, new_sig):
        """
        Update the 'before' signature in temp_txs_last_sigs table.
        """
        connection, cursor = self.get_distributors_db(distributor)
        try:
            cursor.execute(
                """UPDATE temp_txs_last_sigs SET before = ? WHERE id = 1""", (new_sig,)
            )

            # If no rows were updated, insert a new record
            if cursor.rowcount == 0:
                cursor.execute(
                    """INSERT INTO temp_txs_last_sigs (before, last_sig) VALUES (?, '')""",
                    (new_sig,),
                )

            connection.commit()
            return True
        except Exception as e:
            print(f"Error updating temp_txs before signature: {e}")
            connection.rollback()
            return False

    def update_temp_txs_last_sig(self, distributor, new_sig):
        """
        Update the 'last_sig' signature in temp_txs_last_sigs table
        """
        connection, cursor = self.get_distributors_db(distributor)
        try:
            cursor.execute(
                """UPDATE temp_txs_last_sigs SET last_sig = ? WHERE id = 1""",
                (new_sig,),
            )

            # If no rows were updated, insert a new record
            if cursor.rowcount == 0:
                cursor.execute(
                    """INSERT INTO temp_txs_last_sigs (before, last_sig) VALUES ('', ?)""",
                    (new_sig,),
                )

            connection.commit()
            return True
        except Exception as e:
            print(f"Error updating temp_txs last signature: {e}")
            connection.rollback()
            return False

if __name__ == "__main__":
    b = SQLiteDB("HHBkrmzwY7TbDG3G5C4D52LPPd8JEs5oiKWHaPxksqvd")

    success = b.create_transfer_indexes()
    print(success)


    b.close_connections()
