import os
import json
from pathlib import Path
from dotenv import load_dotenv
from ..db.MongoDB import MongoDB
load_dotenv()

"""""
Creates an instance of the DB
"""""
def get_db_instance():
    try:
        db = MongoDB()
        return db
    except Exception as e:
        print(f"There was an error when trying to initialize DB: {e}")
        return None

"""""
Uploads the supported projects from file to DB
"""""
def add_projects_from_file():
    # Setup the file path
    data_dir = Path(os.getenv('PROJECTS_FILE_PATH'))
    projects_file =  data_dir / "projects.json"

    # Get DB connection
    db = get_db_instance()

    # Make sure we have an instance
    if db is None:
        return

    try:
        # Open the project file and load the data
        with open(projects_file, 'r') as f:
            projects = json.load(f)

        # Insert each project into the DB
        for project in projects:
            db.insert_supported_project(project)
    except:
        raise Exception("Could not insert projects into DB")


"""
Filter and uploads data to DB for distributor transfer txs in a json file
"""
def add_distributor_transactions_from_file(distributor):
    batch = []
    count = 0

    # Get DB connection
    db = get_db_instance()

    # Make sure we have an instance
    if db is None:
        return

    # Get the target distributor file
    data_dir = Path(os.getenv('PROJECTS_FILE_PATH'))
    distributor_file =  data_dir / f"project_transactions/{distributor}.json"

    # Open file and load data
    with open(distributor_file, 'r') as f:
        transfers = json.load(f)

        # Loop through txs and add filtered txs to the batch list
        for tx in transfers:
            batch.append(
                {
                    "fee_payer": tx.get("feePayer"),
                    "signature": tx.get("signature"),
                    "slot": tx.get("slot"),
                    "timestamp": tx.get("timestamp"),
                    "token_transfers": tx.get("tokenTransfers", []),
                    "native_transfers": tx.get("nativeTransfers", []),
                }
            )

        try:
            db.insert_distributor_transactions_batch(batch, distributor)
        except:
            raise Exception("Could not insert transfer batch into DB")


# def test_add_project_transfers_from_file():
#     batch = []

#     # Get the target distributor file
#     data_dir = Path(os.getenv('PROJECTS_FILE_PATH'))
#     distributor_file =  data_dir / "project_transactions/CvgM6wSDXWCZeCmZnKRQdnh4CSga3UuTXwrCXy9Ju6PC.json"

#     # Open file and load data
#     with open(distributor_file, 'r') as f:
#         transfers = json.load(f)
#         count = 0
#         # Loop through txs and add to the batch list
#         for tx in transfers:
#             fee_payer = tx.get("feePayer")
#             signature = tx.get("signature")
#             slot = tx.get("slot")
#             timestamp = tx.get("timestamp")
#             token_transfers = tx.get("tokenTransfers", [])
#             native_transfers = tx.get("nativeTransfers", [])
#             batch.append(
#                 {
#                     "fee_payer": fee_payer,
#                     "signature": signature,
#                     "slot": slot,
#                     "timestamp": timestamp,
#                     "token_transfers": token_transfers,
#                     "native_transfers": native_transfers,
#                 }
#             )


#         try:
#             save_batch_to_file(batch)
#             count += 1
#             batch = []
#             print(f"Inserted {count} batches to DB")
#         except:
#             raise Exception("Could not insert transfer batch into DB")


# # Save a batch of transactions to file, appending to existing data.
# def save_batch_to_file(batch_transactions):
#     try:
#         # Write updated data back to file
#         with open("/Users/xtox/Coding/mr_rewards/data/test_dist.json", "w") as f:
#             json.dump(batch_transactions, f, indent=2)

#         return True

#     except Exception as e:
#         print(f"Error saving batch {batch_number} to file: {e}")
#         return False
