import os
import json
from pathlib import Path
from dotenv import load_dotenv
from ..db.MongoDB import MongoDB
from ..db.SQLiteDB import SQLiteDB
from datetime import datetime

load_dotenv()

class BackerUpper:
    """
    Used for backing up the data from the production db to local sqlite db
    """
    def __init__(self):
        try:
            self.sqlite = SQLiteDB(False)
            self.mongo = MongoDB()
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

    def backup_transfers(self, transfers):
        """
        The AWS has a db soley to hold all of the transfers picked up from the updater. This functions goes through
        this list of transfers and updates each SQLiteDB distributors db with the new transfers
        """
        try:
            pass
        except Exception as e:
            print(f"Could not backup transfers: {e}")
            raise

    def backup_wallets(self, wallets):
        """
        Updates the locla SQLiteDB wallets with incoming wallets data from production
        """
        try:
            pass
        except Exception as e:
            print(f"Could not backup wallets: {e}")
            return


if __name__ == "__main__":
    backup_wallets()