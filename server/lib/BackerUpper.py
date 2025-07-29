# server/lib/BackerUpper.py
import os
import json
import sys
import time
from datetime import datetime
from collections import defaultdict

# Add the server directory to the path for imports
if hasattr(sys, '_getframe'):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    server_dir = os.path.dirname(current_dir)
    if server_dir not in sys.path:
        sys.path.insert(0, server_dir)

from db.SQLite.db import SQLiteDB


class BackerUpper:
    """
    Used for backing up the data from the production db to local sqlite db
    """
    def __init__(self):
        try:
            self.sqlite = SQLiteDB(False, False)
        except Exception as e:
            print(f"There was an error when trying to initialize DB: {e}")
            raise

        self.transfers_offset = 0
        self.transfers_by_distributor = defaultdict(list)

    def backup_all_distributor_transfers(self, error_count=0):
        """
        The AWS has a db soley to hold all of the transfers picked up from the updater. This functions goes through
        this list of transfers and updates each SQLiteDB distributors db with the new transfers
        """

        if error_count >= 5:
            print(f"Maximum error count ({error_count}) reached. Stopping processing at offset {self.transfers_offset}.")
            self.transfers_offset = 0
            return False

        try:

            # Get the transfers from mongo
            for transfers, current_offset in self.sqlite.get_temp_transfers(self.transfers_offset):

                # Make sure we have transactions
                if transfers is None:
                    error_count += 1
                    print(f"Failed to get temp transfers from SQLite. Error count: {error_count}")
                    time.sleep(10)
                    return self.backup_all_distributor_transfers(error_count)

                for transfer in transfers:
                    distributor = transfer['distributor']
                    self.transfers_by_distributor[distributor].append(transfer)

                # Update the offset
                self.transfers_offset = current_offset + len(transfers)

                # Reset error count on successful processing
                error_count = 0

            print(f"Found {len(self.transfers_by_distributor)} unique distributors")

            # Insert batch for each distributor
            for distributor, batch in self.transfers_by_distributor.items():
                print(f"Inserting {len(batch)} transfers for distributor: {distributor}")

                # Create the table and indexes if they don't exsist already
                if not self.sqlite.table_exists(distributor):
                    self.sqlite.create_distributor_tables(distributor)
                    self.sqlite.create_distributor_indexes(distributor)

                # Insert the transfers to the correct distributor
                self.sqlite.insert_transfer_batch(distributor, batch)

            return True
        except Exception as e:
            print(f"Could not backup all transfers: {e}")
            return False
