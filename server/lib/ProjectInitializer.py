import os
import json
import time
from pathlib import Path
from dotenv import load_dotenv
from ..db.MongoDB import MongoDB
from ..db.SQLiteDB import SQLiteDB
from ..utils.helius import get_historical_transactions_for_distributor, get_token_metadata
from ..utils.utils import process_distributor_transfers, aggregate_transfers

load_dotenv()

class ProjectInitializer:
    """
    This class contains functions to initialize new projects by getting all of the transfer transactions
    and then processes them and saves them to their applicable storage locations
    """

    def __init__(self, project):
        self.project = project
        self.distributor = project.get("distributor")

        # Get DB instances
        self.mongo_db, self.sqlite_db = self.get_db_connections()

        # Get known tokens
        self.known_tokens = self.mongo_db.get_known_tokens()

        # Create a dictionary for O(1) lookups
        self.known_tokens_dict = {
            str(token.get("mint")).lower(): token.get("symbol")
            for token in self.known_tokens
        }

        # Cache for unknown tokens to avoid duplicate API calls
        self.unknown_token_cache = {}

        # Holds the offset value in case the funtion is get txs/transfers function is called recusivly
        self.txs_offset = 0
        self.transfers_offset = 0

    def initalize_new_project(self):
        """
        This is used to get all of the data for a new project. It runs through
        the entire process starting with getting all of the transfer transactions for
        a distributor. Then it processes those transactions removing unnessesary fields
        and combines the native and token transfers and then saves them to the transfers table.
        From there we remove any duplicates, create indexes for the db, and drop the temp tables, and inserts
        the project into the supported projects collection/db. After that the transfers are processed
        by the aggregator function which adds all of the amounts for each wallet and inserts the wallets rewards to MongoDB
        """

        # Get all of the projects transfer transactions
        # This can take hours depending on how long
        success = self.get_initial_txs()
        if success is False:
            return

        # Processes the transactions removing unnessesary fields and creates an object
        # for each transfer and saves it to the transfers table
        success = self.process_initial_txs()
        if success is False:
            return

        # Remove duplicate transfers, create indexes, drop temp tables,
        # and insert project into supported projects collection/db
        success = self.insert_and_clean_project()
        if success is False:
            return

        # Aggregates all of the rewards for each wallet in the transfers db and
        # inserts them to the MongoDB
        success = self.aggregate_rewards_from_transfers()
        if success is False:
            return

        print("New project successfully initialized")

    ##########################################################
    #            Functions For Getting Initial Data          #
    ##########################################################
    def get_initial_txs(self, finished_count=0, error_count=0):
        """
        Get all historical transactions for a distributor and save them to file.
        Handles retries for both 404 errors and 'finished' false positives.
        """
        print(f"Starting to fetch transactions for distributor: {self.distributor}")

        # Create the tables for the distributor
        self.sqlite_db.create_distributor_tables(self.distributor)

        # Get the last sigs
        before, newest = self.sqlite_db.get_temp_txs_last_sigs(self.distributor)

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

            # 404 is returned if the fetch fails in which case we will increment the error counter
            # if the error counter has 5 concurrent errors then something is wrong and we stop the program
            # otherwise if we get data again the error_counter is reset
            if txs_batch == 404:
                print(f"Error occurred, incrementing error count. Current count: {error_count + 1}")
                error_count += 1
                time.sleep(10)
                # Call the function again starting at the last saved point
                return self.get_initial_txs(finished_count, error_count)


            # Finished comes back when helius can't find any more transactions. Sometimes this
            # is a false positive because it times out if the txs are from a long time ago. Here
            # we will ensure that we get 5 'finished' responses in a row before actually stopping
            if txs_batch.get("finished"):
                finished_count += 1
                print(f"Received 'finished' signal. Count: {finished_count}")
                time.sleep(10)
                # Call the function again starting at the last saved point
                return self.get_initial_txs(finished_count, error_count)

            else:
                # Reset finished count if we get actual data
                finished_count = 0

            # Save transactions to file
            if txs_batch.get("txs"):
                success = self.sqlite_db.insert_transactions_batch(self.distributor, txs_batch.get("txs"))

                # False means data didn't get saved and we should retry and increment the error counter
                if success is False:
                    error_count += 1
                    time.sleep(10)
                    return self.get_initial_txs(finished_count, error_count)

                # Save the new sig if we haven't already
                if not updated_sig:
                    # Update the projects last signature
                    self.sqlite_db.update_temp_txs_last_sig(self.distributor, txs_batch.get('last_sig'))
                    updated_sig = True

                # Update the before sig in cache
                self.sqlite_db.update_temp_txs_before_sig(self.distributor, txs_batch.get("before"))

                # If we reach this point we need to reset the error counter because
                # we only want concurrent counts and if we get data it resets
                error_count = 0

        # Make sure we check if the finished count was hit
        if finished_count < 5:
            finished_count += 1
            time.sleep(10)
            return self.get_initial_txs(finished_count, error_count)

    def process_initial_txs(self, error_count=0):
        """
        Process initial transactions using batched approach with resume capability
        """
        # Stop if we hit max amount of concurrent errors
        if error_count >= 5:
            print(f"Maximum error count ({error_count}) reached. Stopping processing at offset {self.txs_offset}.")
            self.txs_offset = 0
            return False

        # Get total count for progress tracking
        total_count = self.sqlite_db.get_transactions_count(self.distributor)
        processed_count = self.txs_offset

        print(f"Starting to process {total_count} transactions from offset {self.txs_offset}")

        try:
            # Use the generator
            for transactions, current_offset in self.sqlite_db.get_transactions(self.distributor, self.txs_offset):

                if transactions is None:
                    error_count += 1
                    print(f"Failed to get temp transactions. Error count: {error_count}")
                    time.sleep(10)
                    return self.process_initial_txs(error_count)

                # Process the batch
                processed_batch = process_distributor_transfers(self, transactions, self.distributor)

                # Insert the processed transfers to the local db
                success = self.sqlite_db.insert_transfer_batch(self.distributor, processed_batch)

                # Try again with the last offset if it didn't save successfully
                if success is False:
                    error_count += 1
                    print(f"Failed to insert batch. Error count: {error_count}")
                    time.sleep(10)
                    # Recursive call with error count - but be careful about infinite recursion
                    return self.process_initial_txs(error_count)

                # Update the offset in cache for resume capability
                self.txs_offset = current_offset + len(transactions)

                # Reset error count on successful processing
                error_count = 0

                # Update progress
                processed_count += len(transactions)
                progress = (processed_count / total_count) * 100
                print(f"Progress: {processed_count}/{total_count} ({progress:.1f}%)")

            print("Successfully processed all transactions")
            self.txs_offset = 0
            return True

        except Exception as e:
            print(f"Error processing transactions: {e}")
            error_count += 1
            if error_count < 5:
                print(f"Retrying... Error count: {error_count}")
                time.sleep(10)
                return self.process_initial_txs(error_count)
            else:
                print(f"Maximum errors reached. Stopping processing at offset {self.txs_offset}.")
                self.txs_offset = 0
                return False

    def insert_and_clean_project(self):
        """
        This inserts the project into both the sqlite database and the mongodb then removes the temp tables
        """
        before, newest = self.sqlite_db.get_temp_txs_last_sigs(self.distributor)

        # Set the newest sig as last_sig for the unitl helius param
        self.project["last_sig"] = newest
        print(self.project)
        # Write the project to the local db
        sucess = self.sqlite_db.insert_supported_project(self.project)

        # Make sure the insert was successful
        if sucess is not True:
            return False

        # Write the project to the mongo db
        sucess = self.mongo_db.insert_supported_project(self.project)

        # Make sure the insert was successful
        if sucess is not True:
            return False

        # Next we should delete any duplicate transfers, create indexes, and drop the temp tables
        success = self.sqlite_db.clean_and_remove_temp_data(self.distributor)
        if sucess is not True:
            return False

        return True

    def aggregate_rewards_from_transfers(self, error_count=0):
        """
        Process for aggregating rewards from transfers and saves the results to the mongoDB
        """
        # Stop if we hit max amount of concurrent errors
        if error_count >= 5:
            print(f"Maximum error count ({error_count}) reached. Stopping processing at offset {self.transfers_offset}.")
            self.transfers_offset = 0
            return False

        # Get total count for progress tracking
        total_count = self.sqlite_db.get_transfers_count(self.distributor)
        processed_count = self.transfers_offset
        updated = 0

        print(f"Starting to process {total_count} transactions from offset {self.transfers_offset}")

        try:
            # Use the generator
            for transfers, current_offset in self.sqlite_db.get_transfers(self.distributor, self.transfers_offset):

                # Make sure we have transactions
                if transfers is None:
                    error_count += 1
                    print(f"Failed to get transfers from SQLite. Error count: {error_count}")
                    time.sleep(10)
                    return self.aggregate_rewards_from_transfers(error_count)

                # Add up the totals for each wallet address
                aggregated_transfers = aggregate_transfers(transfers)

                # Use the aggregated transfers to update the wallets collection on MongoDB
                updated = self.mongo_db.insert_wallet_rewards(aggregated_transfers)
                updated += updated

                # Update the offset
                self.transfers_offset = current_offset + len(aggregated_transfers)

                # Reset error count on successful processing
                error_count = 0

                # Update progress
                processed_count += len(transfers)
                progress = (processed_count / total_count) * 100
                print(f"Progress: {processed_count}/{total_count} Wallets Updated: {updated} ({progress:.1f}%)")

            print("Successfully aggregated rewards and inserted them into the local db")
            self.transfers_offset = 0
            return True

        except Exception as e:
            print(f"Error processing transactions: {e}")
            error_count += 1
            if error_count < 5:
                print(f"Retrying... Error count: {error_count}")
                time.sleep(10)
                return self.aggregate_rewards_from_transfers(error_count)
            else:
                print(f"Maximum errors reached. Stopping processing. {self.transfers_offset}")
                self.transfers_offset = 0
                return False

    ##########################################################
    #                          Helpers                       #
    ##########################################################
    def get_db_connections(self):
        """ Establish DB connections """
        try:
            # Get connection to MongoDB
            mongo_db = MongoDB()
        except:
            raise Exception("Error getting instance of MongoDB")

        try:
            # Get connection to BackupDB
            sqlite_db = SQLiteDB()
        except Exception as e:
            print(f"Error getting instance of BackupDB {e}")
            raise

        return mongo_db, sqlite_db

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
