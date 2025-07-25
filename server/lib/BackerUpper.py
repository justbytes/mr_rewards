# server/lib/BackerUpper.py
import os
import json
import sys
from datetime import datetime
from collections import defaultdict

# Add the server directory to the path for imports
if hasattr(sys, '_getframe'):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    server_dir = os.path.dirname(current_dir)
    if server_dir not in sys.path:
        sys.path.insert(0, server_dir)

from db.Mongo.db import MongoDB
from db.SQLite.db import SQLiteDB


class BackerUpper:
    """
    Used for backing up the data from the production db to local sqlite db
    """
    def __init__(self):
        try:
            self.sqlite = SQLiteDB(True, True)
            self.mongo = MongoDB(False)
        except Exception as e:
            print(f"There was an error when trying to initialize DB: {e}")
            raise

    ##########################################################
    #                Backup SQLiteDB Functions               #
    ##########################################################
    def backup_supported_projects(self):
        """
        Updates the local SQLiteDB supported projects with the incoming projects data from production
        """
        try:
            print("Starting backup of supported projects...")

            # Get supported projects from MongoDB (production)
            mongo_projects = self.mongo.get_supported_projects()

            if mongo_projects is None:
                print("No supported projects found in MongoDB or error occurred")
                return False

            print(f"Found {len(mongo_projects)} supported projects in MongoDB")

            # Process each project from MongoDB
            for mongo_project in mongo_projects:

                # TODO remove the current supported tokens from the table and replace them with the new
                success = self.sqlite.upsert_supported_project(mongo_project)

                if success is not True:
                    return False

            print(f"\nBackup Summary:")
            print(f"- MongoDB Supported Projects: {len(mongo_projects)}")
            print(f"- SQLiteDB Supported Projects: {self.sqlite.get_supported_project_count()}")

            return True

        except Exception as e:
            print(f"Could not backup supported projects: {e}")
            raise

    def backup_known_tokens(self):
        """
        Updates the local SQLiteDB known tokens with incoming token data from production
        """
        try:
            print("Starting backup of supported projects...")

            # Get supported projects from MongoDB (production)
            mongo_known_tokens = self.mongo.get_known_tokens()

            if mongo_known_tokens is None:
                print("No supported projects found in MongoDB or error occurred")
                return False

            print(f"Found {len(mongo_known_tokens)} supported projects in MongoDB")

            # Process each project from MongoDB
            for mongo_known_token in mongo_known_tokens:

                success = self.sqlite.insert_known_token(mongo_known_token)

                if success is not True:
                    return False

            print(f"\nBackup Summary:")
            print(f"- MongoDB Known Tokens: {len(mongo_known_tokens)}")
            print(f"- SQLiteDB Known Tokens: {self.sqlite.get_known_token_count()}")

            return True

        except Exception as e:
            print(f"Could not backup known tokens: {e}")
            raise

    def backup_all_distributor_transfers(self):
        """
        The AWS has a db soley to hold all of the transfers picked up from the updater. This functions goes through
        this list of transfers and updates each SQLiteDB distributors db with the new transfers
        """
        try:
            # Get the transfers from mongo
            mongo_temp_transfers = self.mongo.get_all_transfers()

            transfers_by_distributor = defaultdict(list)

            for transfer in mongo_temp_transfers:
                distributor = transfer['distributor']
                transfers_by_distributor[distributor].append(transfer)

            print(f"Found {len(transfers_by_distributor)} unique distributors")

            # Insert batch for each distributor
            for distributor, batch in transfers_by_distributor.items():
                print(f"Inserting {len(batch)} transfers for distributor: {distributor}")

                # Create the table and indexes if they don't exsist already
                if not self.sqlite.table_exists(distributor):
                    self.sqlite.create_distributor_tables(distributor)
                    self.sqlite.create_distributor_indexes(distributor)

                # Insert the transfers to the correct distributor
                self.sqlite.insert_transfer_batch(distributor, batch)

        except Exception as e:
            print(f"Could not backup all transfers: {e}")
            raise

    def backup_single_distributor_transfers(self, distributor, transfers):
        """
        This is used if you have a list of transfers for a distributor and want to back them up
        """
        try:
            # Create the table and indexes if they don't exsist already
            if not self.sqlite.table_exists(distributor):
                self.sqlite.create_distributor_tables(distributor)
                self.sqlite.create_distributor_indexes(distributor)

            # Insert the transfers to the correct distributor
            self.sqlite.insert_transfer_batch(distributor, transfers)

        except Exception as e:
            print(f"Could not backup single distributors transfers: {e}")
            raise

    def backup_wallets(self):
        """
        Updates the locla SQLiteDB wallets with incoming wallets data from production
        """
        try:
            mongo_wallets = self.mongo.get_all_wallets()
            print(f"SQLite wallets: {self.sqlite.get_wallets_count()}")
            print(f"Mongo wallets: {len(mongo_wallets)}")

            self.sqlite.insert_wallets_batch(mongo_wallets)
            print(f"SQLite wallets: {self.sqlite.get_wallets_count()}")

        except Exception as e:
            print(f"Could not backup wallets: {e}")
            return

if __name__ == "__main__":
    backup_wallets()