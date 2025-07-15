import os
import json
from pathlib import Path
from dotenv import load_dotenv
from ..db.MongoDB import MongoDB
from ..db.BackupDB import BackupDB
from ..utils.helius import get_historical_transactions_for_distributor, get_token_metadata
from ..utils.utils import process_distributor_transfers

load_dotenv()

class ProjectInitializer:

    def __init__(self, distributor):
        self.distributor = distributor

        self.mongo_db, self.backup_db = self.get_db_connections()

        # Get known tokens
        self.known_tokens = self.mongo_db.get_known_tokens()
        print(f"Loaded {len(self.known_tokens)} known tokens.")
        # Create a dictionary for O(1) lookups
        self.known_tokens_dict = {
            str(token.get("mint")).lower(): token.get("symbol")
            for token in self.known_tokens
        }

        # Cache for unknown tokens to avoid duplicate API calls
        self.unknown_token_cache = {}

    def get_db_connections(self):
        """ Establish DB connections """
        try:
            # Get connection to MongoDB
            mongo_db = MongoDB()
        except:
            raise Exception("Error getting instance of MongoDB")

        try:
            # Get connection to BackupDB
            backup_db = BackupDB(True, self.distributor)
        except:
            raise Exception("Error getting instance of BackupDB")

        return mongo_db, backup_db

    def get_initial_txs(self, finished_count=0, error_count=0, batch_size=1000):
        """
        Get all historical transactions for a distributor and save them to file.
        Handles retries for both 404 errors and 'finished' false positives.
        """
        print(f"Starting to fetch transactions for distributor: {self.distributor}")
        batch_count = 0

        # Get the last sigs
        before, newest, offset = self.backup_db.get_temp_txs_last_sigs()

        # Check if we have already updated our the first unitl tx sig
        if newest:
            updated_sig = True
        else:
            updated_sig = False

        # Stop if we are over 5
        if error_count >= 5:
            print(f"5 concurrent errors in a row! Quitting couldn't get data for project starting at {before}")
            return False

        # Stop if we have hit the finished count
        if finished_count >= 5:
            print("Received 5 'finished' signals in a row. All transactions fetched.")
            return True

        # This will get all of the transfer txs for a given distributor
        for txs_batch in get_historical_transactions_for_distributor(self.distributor, before):
            batch_count += 1

            # 404 is returned if the fetch fails in which case we will increment the error counter
            # if the error counter has 5 concurrent errors then something is wrong and we stop the program
            # otherwise if we get data again the error_counter is reset
            if txs_batch == 404:
                print(f"Error occurred, incrementing error count. Current count: {error_count + 1}")
                error_count += 1

                # Call the function again starting at the last saved point
                return self.get_initial_txs(finished_count, error_count, batch_size)


            # Finished comes back when helius can't find any more transactions. Sometimes this
            # is a false positive because it times out if the txs are from a long time ago. Here
            # we will ensure that we get 5 'finished' responses in a row before actually stopping
            if txs_batch.get("finished"):
                finished_count += 1
                print(f"Received 'finished' signal. Count: {finished_count}")

                # Call the function again starting at the last saved point
                return self.get_initial_txs(finished_count, error_count, batch_size)

            else:
                # Reset finished count if we get actual data
                finished_count = 0

            # Save transactions to file
            if txs_batch.get("txs"):
                success = self.backup_db.insert_temp_txs_batch(txs_batch.get("txs"))

                # False means data didn't get saved and we should retry and increment the error counter
                if success is False:
                    error_count += 1
                    return self.get_initial_txs(finished_count, error_count, batch_size)

                # Save the new sig if we haven't already
                if not updated_sig:
                    # Update the projects last signature
                    self.backup_db.update_temp_txs_last_sig(txs_batch.get('last_sig'))
                    updated_sig = True

                # Update the before sig in cache
                self.backup_db.update_temp_txs_before_sig(txs_batch.get("before"))

                # If we reach this point we need to reset the error counter because
                # we only want concurrent counts and if we get data it resets
                error_count = 0

    def process_initial_txs(self, error_count=0, batch_size=1000):
        """
        Process initial transactions using batched approach with resume capability
        """
        # Stop if we hit max amount of concurrent errors
        if error_count >= 5:
            print(f"Maximum error count ({error_count}) reached. Stopping processing.")
            return False

        # Get starting offset
        before, newest, last_offset = self.backup_db.get_temp_txs_last_sigs()

        # Set it to zero if theres no value
        if last_offset is None:
            last_offset = 0

        # Get total count for progress tracking
        total_count = self.backup_db.get_temp_transactions_count()
        processed_count = last_offset

        print(f"Starting to process {total_count} transactions from offset {last_offset}")

        try:
            # Use the generator with the specified batch_size
            for transactions, current_offset in self.backup_db.get_temp_transactions(
                batch_size=batch_size, offset=last_offset
            ):

                # Process the batch
                processed_batch = process_distributor_transfers(self, transactions, self.distributor)

                # Insert the processed transfers to the local db
                success = self.backup_db.insert_transfer_batch(processed_batch)

                # Try again with the last offset if it didn't save successfully
                if success is False:
                    error_count += 1
                    print(f"Failed to insert batch. Error count: {error_count}")

                    # Recursive call with error count - but be careful about infinite recursion
                    return self.process_initial_txs(error_count, batch_size)

                # Update the offset in cache for resume capability
                last_offset = current_offset + len(transactions)

                # Cache the offset in case of crash
                self.backup_db.update_temp_offset(last_offset)

                # Reset error count on successful processing
                error_count = 0

                # Update progress
                processed_count += len(transactions)
                progress = (processed_count / total_count) * 100
                print(f"Progress: {processed_count}/{total_count} ({progress:.1f}%)")

            print("Successfully processed all transactions")
            return True

        except Exception as e:
            print(f"Error processing transactions: {e}")
            error_count += 1
            if error_count < 5:
                print(f"Retrying... Error count: {error_count}")
                return self.process_initial_txs(error_count, batch_size)
            else:
                print("Maximum errors reached. Stopping processing.")
                return False



    def get_and_add_token_metadata(self, mint_address):
        """
        Fetches token metadata, adds it to the list of known tokens in DB, and returns the symbol of the token added
        """
        try:
            # Make helius call to get the metadata
            token_document = get_token_metadata(mint_address)

            # Add it to the database
            self.mongo_db.insert_known_token(token_document)

            # Update our local caches
            symbol = token_document["symbol"]
            self.known_tokens_dict[mint_address.lower()] = symbol
            return symbol
        except Exception as e:
            print(f"Error when trying to get_and_add_token_metadata {e}")
            return token_document["symbol"]

    def get_token_symbol(self, mint_address):
        """
        Checks if the mint address is in the known list of tokens, if it isn't then we get the token metadata and instert it into the DB
        """
        # Normalize the mint
        mint_lower = mint_address.lower()

        # Check known tokens dictionary
        if mint_lower in self.known_tokens_dict:
            return self.known_tokens_dict[mint_lower]

        # Check unknown token cache to avoid duplicate API calls
        if mint_lower in self.unknown_token_cache:
            return self.unknown_token_cache[mint_lower]

        # Fetch from API and cache result
        symbol = self.get_and_add_token_metadata(mint_address)
        self.unknown_token_cache[mint_lower] = symbol

        return symbol
