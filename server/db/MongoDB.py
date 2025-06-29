import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from pymongo.errors import DuplicateKeyError

load_dotenv()

"""""
This class connects to the MongoDB cluster url from the .env file. It can be used
to query read and write projects and transactions to the database
"""""
class MongoDB:

    """""
    Create the connection to mongodb and get the target db
    """""
    def __init__(self):
        self._client = MongoClient(os.getenv("MONGO_URL"), server_api=ServerApi("1"))
        self._db = self._client.rewards_db

    """""
    Send a ping to the client and return true if it was successful
    """""
    def test_connection(self):
        try:
            self._client.admin.command("ping")
            return True
        except Exception as e:
            print(e)
            return False

    """""
    Inserts a project into the DB
    """""
    def insert_supported_project(self, project):
        try:
            # get the collection to write to
            collection = self._db.supported_projects

            # document structure
            document = {
                "name": project["name"],
                "distributor": project["distributor"],
                "token_mint": project["token_mint"],
                "dev_wallet": project["dev_wallet"],
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

    """""
    Return the supported projects
    """""
    def get_supported_projects(self):
        try:

            collection = self._db.supported_projects

            projects = list(collection.find({}, {"_id": 0}))


            return projects
        except Exception as e:
            print(f"Error getting wallet transfers: {e}")
            return []

    """""
    Insert a known token to the database
    """""
    def insert_known_token(self, token):
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

    """""
    Gets a list of known token data
    """""
    def get_known_tokens(self):
        try:
            collection = self._db.known_tokens
            projects = list(collection.find({}, {"_id": 0}))
            return projects
        except Exception as e:
            print(f"Error getting wallet transfers: {e}")
            return

    """""
    Inserts a wallet transfer into the rewards_wallets collection
    """""
    # def insert_rewards_wallet_batch(
    #     self, data, batch_size=500
    # ):
    #     # print(f"DATA FROM REWARDS WALLET {len(data)}")
    #     total_inserted = 0
    #     total_batches = (len(data) + batch_size - 1) // batch_size

    #     # get the collection
    #     collection = self._db.rewards_wallets

    #     for i in range(0, len(data), batch_size):
    #         batch = data[i:i + batch_size]
    #         batch_num = (i // batch_size) + 1

    #         # Insert into database
    #         try:
    #             result = collection.insert_many(data)
    #             total_inserted += len(result.inserted_ids)

    #             print(f"Batch {batch_num}/{total_batches}: Inserted {len(result.inserted_ids)} documents")
    #         except DuplicateKeyError:
    #             print(f"Transfer already exists, skipping...")
    #             return True
    #         except Exception as e:
    #             print(f"Error inserting transfer: {e}")
    #             return False

    #     print(f"Total inserted: {total_inserted}/{len(data)} transactions")
    #     return total_inserted == len(data)

    def insert_rewards_wallet_batch(self, transactions, batch_size=500):

        if not transactions:
            print("No transactions to insert")
            return True

        collection = self._db.rewards_wallets

        # Process transactions in chunks
        total_inserted = 0
        total_batches = (len(transactions) + batch_size - 1) // batch_size

        for i in range(0, len(transactions), batch_size):
            batch = transactions[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            try:
                # Insert the current batch
                result = collection.insert_many(batch)
                total_inserted += len(result.inserted_ids)

                print(f"Batch {batch_num}/{total_batches}: Inserted {len(result.inserted_ids)} documents")

            except DuplicateKeyError:
                print(f"Transfer already exists, skipping...")
                return True

            except Exception as e:
                print(f"Batch {batch_num}: Unexpected error inserting batch: {e}")
                return False

        print(f"Total inserted: {total_inserted}/{len(transactions)} transactions")
        return total_inserted == len(transactions)


    """""
    Inserts a batch of projects transfer data containing all of the transfers to wallets into the database
    """""
    def insert_batch_project_transfers(self, transactions, distributor, batch_size=500):

        if not transactions:
            print("No transactions to insert")
            return True

        collection = self._get_distributor_collection(distributor)

        # Process transactions in chunks
        total_inserted = 0
        total_batches = (len(transactions) + batch_size - 1) // batch_size

        for i in range(0, len(transactions), batch_size):
            batch = transactions[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            try:
                # Insert the current batch
                result = collection.insert_many(batch)
                total_inserted += len(result.inserted_ids)

                print(f"Batch {batch_num}/{total_batches}: Inserted {len(result.inserted_ids)} documents")

            except DuplicateKeyError:
                print(f"Transfer already exists, skipping...")
                return True

            except Exception as e:
                print(f"Batch {batch_num}: Unexpected error inserting batch: {e}")
                return False

        print(f"Total inserted: {total_inserted}/{len(transactions)} transactions")
        return total_inserted == len(transactions)

    """""
    Get the most recent signature for a transaction of a given distributor
    """""
    def get_last_signature_for_distributor(self, distributor):
        return

    """""
    Get all of the transfers for a distributor
    """""
    def get_project_transfers(self, distributor):
        collection = self._get_distributor_collection(distributor)
        transfers = list(collection.find({}, {"_id": 0}))
        return transfers

    """""
    Get all transfers for a wallet from a specific distributor
    """""
    def get_rewards_wallets_by_distributor(
        self, wallet_address, distributor
    ):
        try:
            collection = self._db.rewards_wallets

            # Query for transfers
            query = {"wallet_address": wallet_address, "distributor": distributor}

            # Get transfers and convert to list, excluding MongoDB's _id field
            transfers = list(collection.find(query, {"_id": 0}))

            # Convert to the format expected by token_aggregator.py
            formatted_transfers = []
            for transfer in transfers:
                formatted_transfers.append(
                    {
                        "signature": transfer["signature"],
                        "slot": transfer["slot"],
                        "timestamp": transfer["timestamp"],
                        "amount": transfer["amount"],
                        "token": transfer["token"],
                    }
                )

            return formatted_transfers

        except Exception as e:
            print(f"Error getting wallet transfers: {e}")
            return []

    """""
    Get list of all unique wallet addresses that have received transfers
    """""
    def get_all_wallets(self):
        try:
            collection = self._db.rewards_wallets

            # Get distinct wallet addresses
            wallets = collection.distinct("wallet_address")
            return wallets

        except Exception as e:
            print(f"Error getting wallets: {e}")
            return []

    """""
    Get list of all distributors that have sent transfers to a specific wallet
    """""
    def get_distributors_for_wallet(self, wallet_address):
        try:
            collection = self._db.rewards_wallets

            # Get distinct distributors for this wallet
            distributors = collection.distinct(
                "distributor", {"wallet_address": wallet_address}
            )
            return distributors

        except Exception as e:
            print(f"Error getting distributors for wallet: {e}")
            return []

    """""
    Check if a wallet has any transfers from a specific distributor
    """""
    def wallet_has_transfers_from_distributor(
        self, wallet_address, distributor
    ):
        try:

            collection = self._db.rewards_wallets

            # Check if any documents exist
            count = collection.count_documents(
                {"wallet_address": wallet_address, "distributor": distributor}
            )

            return count > 0

        except Exception as e:
            print(f"Error checking wallet transfers: {e}")
            return False


    """""
    Create database indexes for better performance
    """""
    def create_indexes(self):
        try:
            # Create indexes for the project_transfers collections
            projects = self.get_supported_projects()

            # Create the indexes for each supported project
            for p in projects:
                collection = self._get_distributor_collection(p.get("distributor"))
                collection.create_index("signature", unique=True)
                collection.create_index("slot")
                collection.create_index("timestamp")
                collection.create_index("native_transfers.toUserAccount")
                collection.create_index("native_transfers.amount")
                collection.create_index("token_transfers.toUserAccount")
                collection.create_index("token_transfers.tokenAmount")
                collection.create_index("token_transfers.mint")


            # Get the collections from DB
            rewards_wallets_collection = self._db.rewards_wallets
            supported_projects_collection = self._db.supported_projects
            known_tokens_collection = self._db.known_tokens

            # Supported projects collection indexes
            supported_projects_collection.create_index("token_mint", unique=True)

            # Known tokens collection indexes
            known_tokens_collection.create_index("mint", unique=True)

            # Rewards wallets collencion indexes
            rewards_wallets_collection.create_index([ ("wallet_address", 1), ("distributor", 1),("signature", 1),("slot", 1), ("timestamp", 1),("token", 1), ("amount",1)], unique=True)
            rewards_wallets_collection.create_index([("wallet_address", 1), ("distributor", 1)])
            rewards_wallets_collection.create_index("signature")
            rewards_wallets_collection.create_index("wallet_address")
            rewards_wallets_collection.create_index("distributor")
            rewards_wallets_collection.create_index("timestamp")
            rewards_wallets_collection.create_index("slot")

            print("Database indexes created successfully")
            return True

        except Exception as e:
            print(f"Error creating indexes: {e}")
            return False


    """""
    Gets the collection for a given distributor
    """""
    def _get_distributor_collection(self, distributor):
        print(distributor)
        match distributor:
            case "CvgM6wSDXWCZeCmZnKRQdnh4CSga3UuTXwrCXy9Ju6PC":
                return self._db.distribute_transfers
            case "GVLwP2iR4sqEX9Tos3cmQQRqAumzRumxKD42qyCbCyCC":
                return self._db.tnt_transfers
            case "D8gKfTxnwBG3XPTy4ZT6cGJbz1s13htKtv9j69qbhmv4":
                return self._db.iplr_transfers
            case "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ":
                return self._db.boon_transfers
            case "9uJbttvvowG1rVpPt6GMB3mL7BuktaHaNzFQbkACfiNN":
                return self._db.click_transfers
            case _:
                print("Unknown distributor address")
                return
