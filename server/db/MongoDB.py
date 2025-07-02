import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from pymongo.errors import DuplicateKeyError, BulkWriteError
from pymongo import UpdateOne

load_dotenv()


class MongoDB:
    """
    This class connects to the MongoDB cluster url from the .env file. It can be used
    to query read and write projects and transactions to the database
    """

    def __init__(self):
        """
        Create the connection to mongodb and get the target db
        """
        self._client = MongoClient(os.getenv("MONGO_URL"), server_api=ServerApi("1"))
        self._db = self._client.rewards_db

    def test_connection(self):
        """
        Send a ping to the client and return true if it was successful
        """
        try:
            self._client.admin.command("ping")
            return True
        except Exception as e:
            print(e)
            return False

    def insert_supported_project(self, project):
        """
        Inserts a project into the DB
        """
        try:
            # get the collection to write to
            collection = self._db.supported_projects

            # document structure
            document = {
                "name": project["name"],
                "distributor": project["distributor"],
                "token_mint": project["token_mint"],
                "dev_wallet": project["dev_wallet"],
                "last_sig": project["last_sig"]
            }

            # Insert into database
            result = collection.insert_one(document)

            return result.inserted_id is not None
        except DuplicateKeyError:
            print(f"Project '{project['name']}' already exists, skipping...")
            return True
        except Exception as e:
            print(f"Error adding project to supported project: {e}")
            return False

    def get_supported_projects(self):
        """
        Return the supported projects
        """
        try:
            collection = self._db.supported_projects
            projects = list(collection.find({}, {"_id": 0}))
            return projects
        except Exception as e:
            print(f"Error getting wallet transfers: {e}")
            return []

    def get_last_tx_signature_for_distributor(self, distributor):
        """
        Get the most recent signature for a transaction of a given distributor
        """
        collection = self._db.supported_projects

        # Find the project by the distributor then grab the last_sig value
        project = collection.find_one({"distributor": distributor})

        if project:
            last_sig = project.get("last_sig")
            return last_sig
        else:
            return None

    def update_last_tx_signature_for_distributor(self, distributor, new_sig):
        """
        Update the most recent signature for a transaction of a given distributor
        """
        collection = self._db.supported_projects

        # Update the last_sig value for the matching distributor
        result = collection.update_one(
            {"distributor": distributor},
            {"$set": {"last_sig": new_sig}}
        )
        # Return True if a document was modified, False otherwise
        return result.modified_count > 0

    def insert_known_token(self, token):
        """
        Insert a known token to the database
        """
        try:
            # get the collection to write to
            collection = self._db.known_tokens

            # document structure
            document = {
                "symbol": token["symbol"],
                "name": token["name"],
                "mint": token["mint"],
                "decimals": token["decimals"],
            }

            # Insert into database
            result = collection.insert_one(document)

            return result.inserted_id is not None
        except DuplicateKeyError:
            print(f"Token '{token['symbol']}' already exists, skipping...")
            return True
        except Exception as e:
            print(f"Error adding project to supported project: {e}")
            return False

    def get_known_tokens(self):
        """
        Gets a list of known token data
        """
        try:
            collection = self._db.known_tokens
            projects = list(collection.find({}, {"_id": 0}))
            return projects
        except Exception as e:
            print(f"Error getting wallet transfers: {e}")
            return

    # def insert_distributor_transactions_batch(
    #     self, transactions, distributor, batch_size=1000
    # ):
    #     """
    #     Inserts a batch of distributors transactions containing all of the transfers to wallets into the database
    #     """
    #     # Make sure we have batches
    #     if not transactions:
    #         print("No transactions to insert")
    #         return True

    #     # Get the collection to insert transactions to
    #     collection = self._get_distributor_collection(distributor)

    #     # Process transactions in chunks
    #     total_inserted = 0
    #     error_count = 0
    #     total_batches = (len(transactions) + batch_size - 1) // batch_size
    #     print(f"Inserting {total_batches} batches")

    #     # Loop through and add the batches
    #     for i in range(0, len(transactions), batch_size):
    #         batch = transactions[i : i + batch_size]
    #         batch_num = (i // batch_size) + 1

    #         try:
    #             # Insert the current batch
    #             result = collection.insert_many(batch, ordered=False)
    #             total_inserted += len(result.inserted_ids)

    #         except BulkWriteError as e:
    #             errors = e.details["writeErrors"]
    #             for error in errors:
    #                 if error["code"] != 11000:
    #                     print(
    #                         f"BulkWriteError when inserting into distributor transfers collection {e}"
    #                     )

    #         except Exception as e:
    #             print(
    #                 f"There an unkown error has occured when adding inserting into  distributor transfers  collection: {e}"
    #             )

    #     return total_inserted

    # def get_all_distributor_transactions(self, distributor):
    #     """
    #     Get all of the transactions from a distributors collection

    #     NOTE: This is resource intensive and some files have over 300 MB of data and
    #         will continue to grow in size as time goes on. This will need to be
    #         changed in the future to something more practical.
    #     """
    #     collection = self._get_distributor_collection(distributor)
    #     transfers = list(collection.find({}, {"_id": 0}))
    #     return transfers

    def insert_transfers_batch(self, transactions, batch_size=1000):
        """
        Inserts a wallet transfer into the transfers collection
        """
        # Make sure we have transactions
        if not transactions:
            print("No transactions to insert")
            return True

        # Get the collection to insert into
        collection = self._db.transfers

        # Process transactions in chunks
        total_inserted = 0
        error_count = 0
        total_batches = (len(transactions) + batch_size - 1) // batch_size

        for i in range(0, len(transactions), batch_size):
            batch = transactions[i : i + batch_size]
            batch_num = (i // batch_size) + 1

            try:
                # Insert the current batch
                result = collection.insert_many(batch, ordered=False)
                total_inserted += len(result.inserted_ids)

            except BulkWriteError as e:
                errors = e.details["writeErrors"]
                for error in errors:
                    if error["code"] != 11000:
                        print(
                            f"BulkWriteError when inserting into rewards wallets collection {e}"
                        )

            except Exception as e:
                print(
                    f"There an unkown error has occured when adding inserting into rewards wallets collection: {e}"
                )

        # Return the total amount of tokens inserted
        return total_inserted

    def update_wallets(self, wallets, batch_size=1000):
        """
        Bulk update wallet balances with multiple distributors per wallet
        """
        collection = self._db.wallets
        from pymongo import UpdateOne  # Add this import

        # Convert dict to list for slicing
        wallet_items = list(wallets.items())

        total_updated = 0
        total_batches = (len(wallet_items) + batch_size - 1) // batch_size

        for i in range(0, len(wallet_items), batch_size):
            # Get the current batch of wallets
            batch = wallet_items[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            # Build bulk operations for this batch only
            bulk_ops = []

            for wallet_address, wallet_data in batch:
                # Build the $inc operations for all distributor/token combinations
                inc_ops = {}

                for distributor, distributor_data in wallet_data['distributors'].items():
                    for token, token_data in distributor_data['tokens'].items():
                        # Use dot notation for nested path
                        path = f"distributors.{distributor}.tokens.{token}.total_amount"
                        inc_ops[path] = token_data['total_amount']

                # Create the update operation using UpdateOne class
                bulk_ops.append(
                    UpdateOne(
                        {"wallet_address": wallet_address},
                        {"$inc": inc_ops},
                        upsert=True
                    )
                )

            # Execute this batch
            if bulk_ops:
                try:
                    result = collection.bulk_write(bulk_ops, ordered=False)
                    total_updated += result.modified_count + result.upserted_count
                except Exception as e:
                    print(f"Error in batch {batch_num}: {e}")

        return total_updated

    def get_wallet(self, wallet_address):
        """
        Get a specific wallet with all its distributors and tokens
        """
        collection = self._db.wallets

        # Find the wallet by its address
        wallet = collection.find_one({"wallet_address": wallet_address})

        return wallet

    def get_transfers_with_wallet_address_and_distributor(
        self, wallet_address, distributor
    ):
        """
        Get all transfers for a wallet from a specific distributor
        """
        try:
            collection = self._db.transfers

            # Query for transfers
            query = {"wallet_address": wallet_address, "distributor": distributor}

            # Get transfers and convert to list, excluding MongoDB's _id field
            transfers = list(collection.find(query, {"_id": 0}))

            return transfers

        except Exception as e:
            print(f"Error getting wallet transfers: {e}")
            return []

    def get_distributors_for_wallet(self, wallet_address):
        """
        Get list of all distributors that have sent transfers to a specific wallet
        """
        try:
            collection = self._db.transfers

            # Get distinct distributors for this wallet
            distributors = collection.distinct(
                "distributor", {"wallet_address": wallet_address}
            )
            return distributors

        except Exception as e:
            print(f"Error getting distributors for wallet: {e}")
            return []

    def get_all_wallets(self):
        """
        Get list of all unique wallet addresses that have received transfers
        """
        try:
            collection = self._db.transfers

            # Get distinct wallet addresses
            wallets = collection.distinct("wallet_address")
            return wallets

        except Exception as e:
            print(f"Error getting wallets: {e}")
            return []

    def create_indexes(self):
        """
        Create database indexes for better performance
        """
        try:
            # Get the collections from DB
            transfers_collection = self._db.transfers
            wallets_collection = self._db.wallets
            supported_projects_collection = self._db.supported_projects
            known_tokens_collection = self._db.known_tokens

            # Supported projects collection indexes
            supported_projects_collection.create_index("token_mint", unique=True)
            supported_projects_collection.create_index("last_sig")

            # Wallets collection indexes
            wallets_collection.create_index("wallet_address", unique=True)

            # Known tokens collection indexes
            known_tokens_collection.create_index("mint", unique=True)

            # Rewards wallets collencion indexes
            transfers_collection.create_index(
                [
                    ("wallet_address", 1),
                    ("distributor", 1),
                    ("signature", 1),
                    ("slot", 1),
                    ("timestamp", 1),
                    ("token", 1),
                    ("amount", 1),
                ],
                unique=True,
            )
            transfers_collection.create_index(
                [("wallet_address", 1), ("distributor", 1)]
            )
            transfers_collection.create_index("signature")
            transfers_collection.create_index("wallet_address")
            transfers_collection.create_index("distributor")
            transfers_collection.create_index("timestamp")
            transfers_collection.create_index("slot")

            print("Database indexes created successfully")
            return True

        except Exception as e:
            print(f"Error creating indexes: {e}")
            return False
