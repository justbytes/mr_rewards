import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
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
        pass
