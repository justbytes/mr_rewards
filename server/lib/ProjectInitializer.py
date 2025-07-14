import os
import json
from pathlib import Path
from dotenv import load_dotenv
from ..db.MongoDB import MongoDB
from ..db.BackupDB import BackupDB
from ..utils.helius import get_historical_transactions_for_distributor

load_dotenv()

class ProjectInitializer:

    def __init__(self, distributor):
        self.distributor = distributor

        # Get connection to MongoDB
        try:
            self.mongo_db = MongoDB()
        except:
            raise Exception("Error getting instance of MongoDB")


        # Get connection to BackupDB
        try:
            self.backup_db = BackupDB(True, distributor)
        except:
            raise Exception("Error getting instance of BackupDB")

        # Get known tokens
        self.known_tokens = self.mongo_db.get_known_tokens()

        # Create a dictionary for O(1) lookups
        self.known_tokens_dict = {
            str(token.get("mint")).lower(): token.get("symbol")
            for token in self.known_tokens
        }

        # Cache for unknown tokens to avoid duplicate API calls
        self.unknown_token_cache = {}

        # Counters
        self.finished_count = 0
        self.error_count = 0

        # Setup file paths
        self.backup_dir = Path(os.getenv('PROJECTS_FILE_PATH'))
        self.temp_storage_dir = self.backup_dir / "temp_storage"
        self.temp_data_file = self.temp_storage_dir / "temp_txs_storage.json"
        self.last_sig_file = self.temp_storage_dir / "temp_txs_last_sig.json"

        # Create directories if they don't exist
        self.temp_storage_dir.mkdir(parents=True, exist_ok=True)

    def get_initial_data(self, finished_count=0, error_count=0, batch_size=1000):
        """
        Get all historical transactions for a distributor and save them to file.
        Handles retries for both 404 errors and 'finished' false positives.
        """
        print(f"Starting to fetch transactions for distributor: {self.distributor}")
        batch_count = 0
        before, newest = self.get_last_sigs()

        if newest:
            updated_sig = True
        else:
            updated_sig = False

        # This will get all of the transfer txs for a given distributor
        for txs_batch in get_historical_transactions_for_distributor(self.distributor, before):
            batch_count += 1
            print(batch_count)
            # 404 is returned if the fetch fails in which case we will increment the error counter
            # if the error counter has 5 concurrent errors then something is wrong and we stop the program
            # otherwise if we get data again the error_counter is reset
            if txs_batch == 404:
                print(f"Error occurred, incrementing error count. Current count: {error_count + 1}")
                error_count += 1

                # Stop if we are over 5
                if error_count >= 5:
                    print(f"5 concurrent errors in a row! Quitting couldn't get data for project starting at {before}")
                    return

                # Call the function again starting at the last saved point
                self.get_initial_data(finished_count, error_count, batch_size)
                return

            # Finished comes back when helius can't find any more transactions. Sometimes this
            # is a false positive because it times out if the txs are from a long time ago. Here
            # we will ensure that we get 5 'finished' responses in a row before actually stopping
            if txs_batch.get("finished"):
                finished_count += 1
                print(f"Received 'finished' signal. Count: {finished_count}")

                if finished_count >= 5:
                    print("Received 5 'finished' signals in a row. All transactions fetched.")
                    return

                # Call the function again starting at the last saved point
                self.get_initial_data(finished_count, error_count, batch_size)
                return

            else:
                # Reset finished count if we get actual data
                finished_count = 0

            # Save transactions to file
            if txs_batch.get("txs"):
                success = self.save_transactions_to_file(txs_batch.get("txs"))

                # False means data didn't get saved and we should retry and increment the error counter
                if success is False:
                    error_count += 1
                    self.get_initial_data(self.distributor, finished_count, error_count, batch_size)
                    return

                # Save the new sig if we haven't already
                if not updated_sig:
                    # Update the projects last signature
                    self.set_last_sig(txs_batch.get('last_sig'))
                    updated_sig = True

                # Update the before sig in cache
                self.set_before_sig(txs_batch.get("before"))

                print(f"Saved {len(txs_batch.get('txs'))} transactions to file")

                # If we reach this point we need to reset the error counter because
                # we only want concurrent counts and if we get data it resets
                error_count = 0

    def get_last_sigs(self):
        """Get both before and last_sig values from file"""
        try:
            if self.last_sig_file.exists():
                with open(self.last_sig_file, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        before = data.get("before")
                        last_sig = data.get("last_sig")
                        return before, last_sig
                    else:
                        # Handle legacy format where it was just a string
                        return None, data if isinstance(data, str) else None
            return None, None
        except Exception as e:
            print(f"Error reading last signatures: {e}")
            return None, None

    def set_last_sig(self, new_sig):
        """Save the last processed signature to file (legacy method)"""
        try:
            # Try to preserve existing data structure
            existing_data = {"before": None, "last_sig": None}
            if self.last_sig_file.exists():
                with open(self.last_sig_file, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        existing_data = data

            # Update last_sig
            existing_data["last_sig"] = new_sig

            with open(self.last_sig_file, 'w') as f:
                json.dump(existing_data, f, indent=2, default=str)
        except Exception as e:
            print(f"Error saving last signature: {e}")

    def set_before_sig(self, new_sig):
        """Save the last processed signature to file"""
        try:
            # Try to preserve existing data structure
            existing_data = {"before": None, "last_sig": None}
            if self.last_sig_file.exists():
                with open(self.last_sig_file, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        existing_data = data

            # Update last_sig
            existing_data["before"] = new_sig

            with open(self.last_sig_file, 'w') as f:
                json.dump(existing_data, f, indent=2, default=str)
        except Exception as e:
            print(f"Error saving last signature: {e}")

    def save_transactions_to_file(self, transactions):
        """Save transactions to the temporary storage file"""
        try:
            # Load existing data if file exists
            existing_data = []
            if self.temp_data_file.exists():
                with open(self.temp_data_file, 'r') as f:
                    existing_data = json.load(f)

            # Append new transactions
            existing_data.extend(transactions)

            # Save back to file
            with open(self.temp_data_file, 'w') as f:
                json.dump(existing_data, f, indent=2, default=str)

        except Exception as e:
            print(f"Error saving transactions to file: {e}")
            raise

    def clear_temp_files(self):

        """Clear temporary storage files"""
        try:
            if self.temp_data_file.exists():
                self.temp_data_file.unlink()
            if self.last_sig_file.exists():
                self.last_sig_file.unlink()
            print("Temporary files cleared")
        except Exception as e:
            print(f"Error clearing temp files: {e}")
