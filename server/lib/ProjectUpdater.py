# server/lib/ProjectUpdater.py
import sys
import os

# Add the server directory to the path for imports
if hasattr(sys, '_getframe'):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    server_dir = os.path.dirname(current_dir)
    if server_dir not in sys.path:
        sys.path.insert(0, server_dir)

from utils.utils import process_distributor_transfers, aggregate_transfers, timer
from utils.helius import get_token_metadata, get_new_distributor_transactions

class ProjectUpdater:

    def __init__(self, controller):
        self.controller = controller
        self.begin_polling()
        self.updating = False

    def begin_polling(self):
        """
        Runs the update distributors function every five minutes(300 seconds) using the timer utility to check for new transactions
        """
        timer(self.update_distributors_transactions, 300)

    def update_distributors_transactions(self):
        """This will loop through each supported project and get any new transfers"""

        if self.updating is True:
            return

        projects = self.controller.sqlite.get_supported_projects()

        self.updating = True

        for project in projects:
            distributor = project.get("distributor")
            self.fetch_and_process_new_distributor_transactions(distributor)

        print("Update complete")
        self.updating = False

    def fetch_and_process_new_distributor_transactions(self, distributor):
        """
        Gets a list of transactions starting from last signature from the distributor_transfers collection
        """
        try:
            # Get the last tx signature so we can start from the at point
            last_sig = self.controller.sqlite.get_last_tx_signature(distributor)

            updated_sig = False

            # Get the all of the transactions starting from the last signature by calling the distributor_transfer_generator
            for transaction_batch in get_new_distributor_transactions(
                distributor, last_sig
            ):

                # Save the new sig if we haven't already
                if not updated_sig:
                    # Update the projects last signature
                    self.controller.sqlite.update_last_tx_signature(
                        distributor, transaction_batch.get("last_sig")
                    )
                    updated_sig = (
                        True  # Set to true so we don't keep updating the same value
                    )

                # Extract the transfers from the transactions and insert them into the db
                for transfer_batch in self.extract_transfers_from_distributor_transactions(
                    transaction_batch.get("txs"), distributor
                ):

                    # Update wallets with new rewards amounts
                    self.aggregate_rewards(transfer_batch)

        except:
            raise Exception("There was an error when fetching and processing new transactions in ProjectUpdater.")

    def extract_transfers_from_distributor_transactions(
        self, transactions, distributor, batch_size=1000
    ):
        """
        Extract transfers and insert into DB, yielding only successfully inserted transfers
        """
        total_docs = 0
        total_batches = (len(transactions) + batch_size - 1) // batch_size

        try:
            for i in range(0, len(transactions), batch_size):
                batch = transactions[i : i + batch_size]
                batch_num = (i // batch_size) + 1

                # Get the transfers
                processed_batch = process_distributor_transfers(self, batch, distributor)

                # Insert into database and get what was actually inserted
                success = self.controller.sqlite.insert_temp_transfers_batch(processed_batch)

                # Check if the insert was successfull
                if success is True:
                    total_docs += len(processed_batch)
                else:
                    raise Exception("Batch of temp transfers failed to insert into SQLite DB from ProjectUpdater")

                print(
                    f"Transfer Batch {batch_num}/{total_batches}. Total Docs: {total_docs}"
                )

                # Only yield the transfers that were actually inserted
                yield processed_batch
        except:
            raise Exception("There was an error when extracting transfers from distributor in ProjectUpdater.")

    def aggregate_rewards(self, transfers, batch_size=1000):
        """
        Given a list of project transfer transactions extract each transfer from native and token transfer lists and insert it into the DB
        """
        total_inserted = 0
        error_count = 0
        total_batches = (len(transfers) + batch_size - 1) // batch_size

        # Loop through transactions and get the transfers from the native and token transfer lists
        # then insert them into the DB
        try:
            for i in range(0, len(transfers), batch_size):
                batch = transfers[i : i + batch_size]
                batch_num = (i // batch_size) + 1

                aggregated_batch = aggregate_transfers(batch)
                self.controller.upsert_wallets(aggregated_batch)
        except:
            raise Exception("There was an error when aggregating rewards in the ProjectUpdater.")