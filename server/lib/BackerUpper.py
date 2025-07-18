import os
import json
from pathlib import Path
from dotenv import load_dotenv
from db.MongoDB import MongoDB
from db.SQLiteDB import SQLiteDB
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
    def backup_supported_projects(self, projects):
        """
        Updates the local SQLiteDB supporeted projects with the incoming projects data from production
        """
        try:
            pass
        except Exception as e:
            print(f"Could not backup supported projects: {e}")
            raise

    def backup_known_tokens(self, tokens):
        """
        Updates the local SQLiteDB known tokens with incoming token data from production
        """
        try:
            pass
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