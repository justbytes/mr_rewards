import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

load_dotenv()

# Global client variable
_client = None
_db = None


# Get or create MongoDB connection
def get_db_connection():
    global _client, _db

    # Checks to see if we currently have a connection
    if _client is None:
        _client = MongoClient(os.getenv("MONGO_URL"), server_api=ServerApi("1"))
        _db = _client.rewards_db

    return _db


# Initialize and test MongoDB connection
def initialize_db_connection():
    try:
        # get the connection
        db = get_db_connection()

        # Send a ping to confirm a successful connection
        _client.admin.command("ping")
        return True
    except Exception as e:
        print(e)
        return False


# Add a project to the database
def insert_supported_project(project):
    try:
        # get the connection
        db = get_db_connection()

        # get the collection to write to
        collection = db.supported_projects

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
    except Exception as e:
        print(f"Error adding project to supported project: {e}")
        return False


# Add a transaction to the database
def insert_wallet_transfer(
    wallet_address: str, distributor: str, transfer_data: Dict[str, Any]
) -> bool:
    try:

        # get a connection
        db = get_db_connection()

        # get the collection
        collection = db.wallet_transfers

        # document structure
        document = {
            "wallet_address": wallet_address,
            "distributor": distributor,
            "signature": transfer_data["signature"],
            "slot": transfer_data["slot"],
            "timestamp": transfer_data["timestamp"],
            "amount": transfer_data["amount"],
            "token": transfer_data["token"],
        }

        # Insert into database
        result = collection.insert_one(document)
        return result.inserted_id is not None

    except Exception as e:
        print(f"Error inserting wallet transfer: {e}")
        return False


# Returns all of the supported projects
def get_supported_projects():
    try:

        # Get connection
        db = get_db_connection()

        # get collection
        collection = db.supported_projects

        projects = list(collection.find({}, {"_id": 0}))
        return projects

    except Exception as e:
        print(f"Error getting wallet transfers: {e}")
        return []


#  Get all transfers for a wallet from a specific distributor
def get_wallet_transfers_by_distributor(
    wallet_address: str, distributor: str
) -> List[Dict[str, Any]]:
    try:
        db = get_db_connection()
        collection = db.wallet_transfers

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


# Get list of all unique wallet addresses that have received transfers
def get_all_wallets() -> List[str]:

    try:
        db = get_db_connection()
        collection = db.wallet_transfers

        # Get distinct wallet addresses
        wallets = collection.distinct("wallet_address")
        return wallets

    except Exception as e:
        print(f"Error getting wallets: {e}")
        return []


# Get list of all distributors that have sent transfers to a specific wallet
def get_distributors_for_wallet(wallet_address: str) -> List[str]:
    try:
        db = get_db_connection()
        collection = db.wallet_transfers

        # Get distinct distributors for this wallet
        distributors = collection.distinct(
            "distributor", {"wallet_address": wallet_address}
        )
        return distributors

    except Exception as e:
        print(f"Error getting distributors for wallet: {e}")
        return []


# Check if a wallet has any transfers from a specific distributor
def wallet_has_transfers_from_distributor(
    wallet_address: str, distributor: str
) -> bool:
    try:
        db = get_db_connection()
        collection = db.wallet_transfers

        # Check if any documents exist
        count = collection.count_documents(
            {"wallet_address": wallet_address, "distributor": distributor}
        )

        return count > 0

    except Exception as e:
        print(f"Error checking wallet transfers: {e}")
        return False


# Create database indexes for better performance
# #Call this once after setting up the database
def create_indexes():

    try:
        db = get_db_connection()
        collection = db.wallet_transfers

        # Create compound index for wallet_address and distributor (most common query)
        collection.create_index([("wallet_address", 1), ("distributor", 1)])

        # Create individual indexes
        collection.create_index("wallet_address")
        collection.create_index("distributor")
        collection.create_index("timestamp")

        print("Database indexes created successfully")
        return True

    except Exception as e:
        print(f"Error creating indexes: {e}")
        return False
