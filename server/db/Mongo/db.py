import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv

load_dotenv()

class MongoDB:
    """
    This class connects to the MongoDB cluster url from the .env file. It can be used
    to query read and write projects and transactions to the database
    """
    def __init__(self, test):
        """
        Create the connection to mongodb and get the target db
        """
        self._client = MongoClient(os.getenv("MONGO_URL"), server_api=ServerApi("1"))

        if test is True:
            self._db = self._client.test_rewards_db
        else:
            self._db = self._client.rewards_db

        self.create_indexes()

    def create_indexes(self):
        """
        Create database indexes for better performance
        """
        try:
            # Get the collections from DB
            wallets_collection = self._db.wallets
            supported_projects_collection = self._db.supported_projects

            # Supported projects collection indexes
            supported_projects_collection.create_index("token_mint", unique=False)
            supported_projects_collection.create_index("distributor", unique=True)

            # Wallets collection indexes
            wallets_collection.create_index("wallet_address", unique=True)

            print("Database indexes created successfully")
            return True

        except Exception as e:
            print(f"Error creating indexes")
            return False

    ##########################################################
    #                     Supported Projects                 #
    ##########################################################
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
            print(f"Error adding project to supported project")
            return None

    ##########################################################
    #                          Wallets                       #
    ##########################################################
    def get_all_wallets(self):
        """
        Get all wallet documents from the wallets collection
        """
        try:
            collection = self._db.wallets

            # Find all documents, exclude _id field
            wallets = list(collection.find({}, {"_id": 0}))
            return wallets

        except Exception as e:
            print(f"Error getting all wallets")
            return None

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

    def insert_wallets_batch(self, wallets, batch_size=5000):
        """
        Insert or update wallets in batches. If a wallet exists, it will be replaced.
        If it doesn't exist, it will be inserted.

        Args:
            wallets: List of wallet dictionaries to insert/update
            batch_size: Number of documents to process in each batch (default: 5000)

        Returns:
            dict: Summary of the operation with counts of inserted, updated, and failed documents
        """
        try:
            collection = self._db.wallets

            # Initialize counters
            total_processed = 0
            total_inserted = 0
            total_updated = 0
            total_failed = 0

            # Process wallets in batches
            for i in range(0, len(wallets), batch_size):
                batch = wallets[i:i + batch_size]
                batch_operations = []

                # Prepare bulk operations for this batch
                for wallet in batch:
                    try:
                        # Ensure wallet has required fields
                        if not wallet.get('wallet_address'):
                            print(f"Skipping wallet without wallet_address: {wallet}")
                            total_failed += 1
                            continue

                        # Create the document structure
                        document = {
                            "wallet_address": wallet["wallet_address"],
                            "distributors": wallet.get("distributors", {})
                        }

                        # Add _id if provided (for updates)
                        if "_id" in wallet:
                            document["_id"] = wallet["_id"]

                        # Create upsert operation - FIXED: Use ReplaceOne class instead of dict
                        from pymongo import ReplaceOne
                        operation = ReplaceOne(
                            {"wallet_address": wallet["wallet_address"]},
                            document,
                            upsert=True
                        )

                        batch_operations.append(operation)

                    except Exception as e:
                        print(f"Error preparing wallet operation: {e}")
                        total_failed += 1
                        continue

                # Execute batch operations if we have any
                if batch_operations:
                    try:
                        result = collection.bulk_write(batch_operations, ordered=False)

                        # Update counters based on bulk write result
                        total_inserted += result.upserted_count
                        total_updated += result.modified_count
                        total_processed += len(batch_operations)

                        print(f"Processed batch {i//batch_size + 1}: "
                            f"{len(batch_operations)} operations, "
                            f"{result.upserted_count} inserted, "
                            f"{result.modified_count} updated")

                    except Exception as e:
                        print(f"Error executing batch operations: {e}")
                        total_failed += len(batch_operations)

            # Return summary
            result_summary = {
                "total_processed": total_processed,
                "total_inserted": total_inserted,
                "total_updated": total_updated,
                "total_failed": total_failed,
                "success": total_failed == 0
            }

            print(f"Batch operation completed: {result_summary}")
            return result_summary

        except Exception as e:
            print(f"Error in insert_wallets_batch: {e}")
            return {
                "total_processed": 0,
                "total_inserted": 0,
                "total_updated": 0,
                "total_failed": len(wallets),
                "success": False,
                "error": str(e)
            }