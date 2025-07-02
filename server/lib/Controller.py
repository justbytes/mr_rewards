import asyncio
import requests
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from ..db.MongoDB import MongoDB
from ..utils.utils import process_distributor_transfers, aggregate_transfers
from ..utils.helius import get_token_metadata, get_distributor_transactions

load_dotenv()


class Controller:

    def __init__(self):
        """Initialize the FetchData class with db instance and known_tokens list"""
        self.db = self.get_db_instance()

        self.known_tokens = self.get_known_tokens_from_db()

        # Create a dictionary for O(1) lookups
        self.known_tokens_dict = {
            str(token.get("mint")).lower(): token.get("symbol")
            for token in self.known_tokens
        }

        # Cache for unknown tokens to avoid duplicate API calls
        self.unknown_token_cache = {}

        print(f"Loaded {len(self.known_tokens)} known tokens")

    def get_db_instance(self):
        """Get an instance of the DB"""
        try:
            db = MongoDB()

            if db is None:
                raise Exception(f"There was an error when trying to initialize DB: {e}")

            return db
        except Exception as e:
            raise Exception(f"There was an error when trying to initialize DB: {e}")

    def fetch_and_process_distributor_transactions(self, distributor):
        """
        Gets a list of transactions starting from last signature from the distributor_transfers collection
        """

        # Get the last tx signature so we can start from the at point
        last_sig = self.db.get_last_tx_signature_for_distributor(distributor)

        # Get the all of the transactions starting from the last signature by calling the distributor_transfer_generator
        for transaction_batch in get_distributor_transactions(distributor, last_sig):
            # Update the projects last signature
            self.db.update_last_tx_signature_for_distributor(distributor, transaction_batch.get('last_sig'))

            # Extract the transfers from the transactions and insert them into the db
            for transfer_batch in self.extract_transfers_from_distributor_transactions(transaction_batch.get("txs"), distributor):
                # Update wallets with new rewards amounts
                self.aggregate_rewards(transfer_batch, distributor)

    def extract_transfers_from_distributor_transactions(
        self, transactions, distributor, batch_size=1000
    ):
        """
        Given a list of project transfer transactions extract each transfer from native and token transfer lists and insert it into the DB
        """
        total_inserted = 0
        error_count = 0
        total_batches = (len(transactions) + batch_size - 1) // batch_size

        # Loop through each transaction and get the transfers from the native and token transfer lists
        # then insert them into the DB
        for i in range(0, len(transactions), batch_size):
            batch = transactions[i : i + batch_size]
            batch_num = (i // batch_size) + 1

            # Get the transfers
            processed_batch = process_distributor_transfers(self, batch, distributor)

            # Insert into the database
            inserted = self.db.insert_transfers_batch(processed_batch)
            total_inserted += inserted
            print(
                f"Extracted Transfer Batch: {batch_num}/{total_batches} Total inserted: {total_inserted} "
            )
            yield processed_batch

    def aggregate_rewards(self, transfers, distributor, batch_size=1000):
        """
        Given a list of project transfer transactions extract each transfer from native and token transfer lists and insert it into the DB
        """
        total_inserted = 0
        error_count = 0
        total_batches = (len(transfers) + batch_size - 1) // batch_size

        # Loop through each transaction and get the transfers from the native and token transfer lists
        # then insert them into the DB
        for i in range(0, len(transfers), batch_size):
            batch = transfers[i : i + batch_size]
            batch_num = (i // batch_size) + 1


            aggregated_batch = aggregate_transfers(batch)
            updated = self.db.update_wallets(aggregated_batch)
            total_inserted += updated
            print(
                f"Aggregated Rewards Batch: {batch_num}/{total_batches} Total updated: {total_inserted} "
            )

    def get_and_add_token_metadata(self, mint_address):
        """
        Fetches token metadata, adds it to the list of known tokens in DB, and returns the symbol of the token added
        """
        try:
            # Make helius call to get the metadata
            token_document = get_token_metadata(mint_address)

            # Add it to the database
            self.db.insert_known_token(token_document)

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


    ##################################
    #        GETTERS FOR DB          #
    ##################################
    def get_all_wallets_from_db(self):
        return self.db.get_all_wallets()

    def get_supported_projects_from_db(self):
        return self.db.get_supported_projects()

    def get_known_tokens_from_db(self):
        return self.db.get_known_tokens()

    def get_rewards_with_wallet_address_and_distributor_from_db(self, wallet_address, distributor):
        return self.db.get_rewards_with_wallet_address_and_distributor(wallet_address, distributor)

    def get_wallets_distributors_from_db(self, wallet_address):
        return self.db.get_wallets_distributors(wallet_address)