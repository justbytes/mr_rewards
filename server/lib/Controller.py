from ..db.Mongo.db import MongoDB
from ..db.SQLite.db import SQLiteDB

class Controller:

    def __init__(self):
        """Initialize the FetchData class with db instance and known_tokens list"""
        self.mongo = MongoDB()
        self.sqlite = SQLiteDB()

        self.known_tokens = self.sqlite.get_known_tokens()

        # Create a dictionary for O(1) lookups
        self.known_tokens_dict = {
            str(token.get("mint")).lower(): token.get("symbol")
            for token in self.known_tokens
        }

        # Cache for unknown tokens to avoid duplicate API calls
        self.unknown_token_cache = {}

        print(f"Loaded {len(self.known_tokens)} known tokens")

    def upsert_wallets(self, wallets):
        """
        This function gets all of the wallets that need to be updated and then adds the new values to the wallets
        and then updates the wallets in the database
        """
        if not wallets:
            return 0

        try:
            # Extract wallet addresses from the input wallets
            wallet_addresses = list(wallets.keys())

            # Get existing wallets from SQLite DB
            existing_wallets = self.sqlite.get_wallets_by_addresses(wallet_addresses)

            update_wallets = []
            insert_wallets = []

            # Process each wallet
            for wallet_address, new_wallet_data in wallets.items():
                if wallet_address in existing_wallets:
                    # Wallet exists - merge the distributor data
                    existing_wallet = existing_wallets[wallet_address]
                    merged_wallet = self._merge_wallet_distributors(
                        existing_wallet,
                        {"wallet_address": wallet_address, "distributors": new_wallet_data["distributors"]}
                    )
                    update_wallets.append(merged_wallet)
                else:
                    # New wallet - add to insert list
                    insert_wallets.append({
                        "wallet_address": wallet_address,
                        "distributors": new_wallet_data["distributors"]
                    })

            # Perform batch operations
            success_count = 0

            if insert_wallets:
                success = self.sqlite.insert_wallets_batch(insert_wallets)
                if success:
                    success_count += len(insert_wallets)
                    print(f"Inserted {len(insert_wallets)} new wallets")

            if update_wallets:
                success = self.sqlite.update_wallets_batch(update_wallets)
                if success:
                    success_count += len(update_wallets)
                    print(f"Updated {len(update_wallets)} existing wallets")

            # TODO: Update MongoDB collections if needed
            # self._update_mongodb_wallets(insert_wallets + update_wallets)

            return success_count

        except Exception as e:
            print(f"Error in upsert_wallets: {e}")
            return 0

    def _merge_wallet_distributors(self, existing_wallet, new_wallet_data):
        """
        Merge new distributor data with existing wallet data
        """
        merged_wallet = {
            "wallet_address": existing_wallet["wallet_address"],
            "distributors": existing_wallet["distributors"].copy()
        }

        # Merge distributors
        for distributor, distributor_data in new_wallet_data["distributors"].items():
            if distributor in merged_wallet["distributors"]:
                # Distributor exists - merge tokens
                existing_distributor = merged_wallet["distributors"][distributor]

                if "tokens" not in existing_distributor:
                    existing_distributor["tokens"] = {}

                # Merge tokens
                for token, token_data in distributor_data["tokens"].items():
                    if token in existing_distributor["tokens"]:
                        # Token exists - add amounts
                        existing_amount = existing_distributor["tokens"][token]["total_amount"]
                        new_amount = token_data["total_amount"]
                        existing_distributor["tokens"][token]["total_amount"] = existing_amount + new_amount
                    else:
                        # New token - add it
                        existing_distributor["tokens"][token] = token_data.copy()
            else:
                # New distributor - add it
                merged_wallet["distributors"][distributor] = distributor_data.copy()

        return merged_wallet

    def get_and_add_token_metadata(self, mint_address):
        """
        Fetches token metadata, adds it to the list of known tokens in DB, and returns the symbol of the token added
        """
        try:
            # Make helius call to get the metadata
            token_document = get_token_metadata(mint_address)

            # Add it to the database
            self.sqlite.insert_known_token(token_document)

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