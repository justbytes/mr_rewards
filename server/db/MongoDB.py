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
            return None

    def get_supported_projects(self):
        """
        Return the supported projects
        """
        try:
            collection = self._db.supported_projects
            projects = list(collection.find({}, {"_id": 0}))
            return projects
        except Exception as e:
            print(f"Error getting wallet transfers")
            return None

    def get_newest_tx_signature_for_distributor(self, distributor):
        """
        Get the most recent signature for a transaction of a given distributor
        """
        try:
            collection = self._db.supported_projects

            # Find the project by the distributor then grab the last_sig value
            project = collection.find_one({"distributor": distributor})

            if project:
                last_sig = project.get("last_sig")
                return last_sig
            else:
                return None
        except Exception as e:
            print(f"Error getting the last tx signature")
            return None

    def update_newest_tx_signature_for_distributor(self, distributor, new_sig):
        """
        Update the most recent signature for a transaction of a given distributor
        """
        try:
            collection = self._db.supported_projects

            # Update the last_sig value for the matching distributor
            result = collection.update_one(
                {"distributor": distributor},
                {"$set": {"last_sig": new_sig}}
            )
            # Return True if a document was modified, False otherwise
            return result.modified_count > 0
        except Exception as e:
            print(f"Error when updating tx signature in db")
            return None

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
            print(f"Error adding project to supported project")
            return None

    def get_known_tokens(self):
        """
        Gets a list of known token data
        """
        try:
            collection = self._db.known_tokens
            projects = list(collection.find({}, {"_id": 0}))
            return projects
        except Exception as e:
            print(f"Error getting wallet transfers")
            return None

    def insert_transfers_batch(self, transactions, batch_size=1000):
        """
        Inserts a wallet transfer into the transfers collection
        Returns the actual inserted documents and their count
        """
        if not transactions:
            print("No transactions to insert")
            return [], 0

        collection = self._db.transfers
        total_inserted = 0
        actually_inserted_docs = []

        for i in range(0, len(transactions), batch_size):
            batch = transactions[i : i + batch_size]
            batch_num = (i // batch_size) + 1

            try:
                result = collection.insert_many(batch, ordered=False)
                # These were successfully inserted
                inserted_count = len(result.inserted_ids)
                total_inserted += inserted_count

                # Track which documents were actually inserted
                actually_inserted_docs.extend(batch)

            except BulkWriteError as e:
                # Some succeeded, some failed (likely duplicates)
                errors = e.details["writeErrors"]
                successful_inserts = len(batch) - len(errors)
                total_inserted += successful_inserts

                # Figure out which ones were actually inserted
                error_indices = {error["index"] for error in errors if error["code"] == 11000}
                for idx, doc in enumerate(batch):
                    if idx not in error_indices:
                        actually_inserted_docs.append(doc)

                # Log non-duplicate errors
                for error in errors:
                    if error["code"] != 11000:
                        print(f"BulkWriteError other then duplicate error occured")

            except Exception as e:
                print(f"Unknown error inserting transfers")

        return actually_inserted_docs, total_inserted

    def get_all_rewards_wallets(self):
        """
        Get all wallet documents from the wallets collection
        """
        try:
            collection = self._db.wallets

            # Find all documents, exclude _id field
            wallets = list(collection.find({}, {"_id": 0}))
            return wallets

        except Exception as e:
            print(f"Error getting all wallets: {e}")
            return None

    def insert_wallet_rewards(self, wallets, batch_size=5000):
        """
        Bulk update wallet balances with multiple distributors per wallet
        """
        collection = self._db.wallets

        # Convert dict to list for slicing
        wallet_items = list(wallets.items())

        total_updated = 0
        total_batches = (len(wallet_items) + batch_size - 1) // batch_size

        for i in range(0, len(wallet_items), batch_size):
            # Get the current batch of wallets
            batch = wallet_items[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            # Build bulk operations for batch
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
                    print(f"Error inserting wallet rewards into db {batch_num}")

        return total_updated

    def get_wallet_rewards(self, wallet_address):
        """
        Get a specific wallet with all its distributors and tokens
        """
        try:
            collection = self._db.wallets

            # Find the wallet by its address
            wallet = collection.find_one({"wallet_address": wallet_address})

            return wallet
        except Exception as e:
            print(f"Error getting wallet rewards")
            return None

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
            print(f"Error getting wallet transfers with wallet address and distributor")
            return None

    def get_all_transfers_for_distributor(self, distributor):
        """
        Get all transfer documents for a specific distributor (without _id field)
        """
        try:
            collection = self._db.transfers

            # Find all documents where distributor matches, exclude _id field
            transfers = list(collection.find({"distributor": distributor}, {"_id": 0}))
            return transfers

        except Exception as e:
            print(f"Error getting transfers for distributor {distributor}: {e}")
            return None

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
            supported_projects_collection.create_index("token_mint", unique=False)
            supported_projects_collection.create_index("distributor", unique=True)
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
            print(f"Error creating indexes")
            return False
