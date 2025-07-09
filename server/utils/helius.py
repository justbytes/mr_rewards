import requests
import os
from .utils import process_distributor_transactions
from dotenv import load_dotenv
load_dotenv()

def get_historical_transactions_for_distributor(
    distributor, batch_size=1000
):
    """ Gets all of the transactions for a distributor """
    batch = []
    batch_count = 0
    total_count = 0
    real_count = 0
    before = None
    last_sig = None

    # URL for call to Helius
    url = f"https://api.helius.xyz/v0/addresses/{distributor}/transactions"

    while True:
        # Parameters for the API call
        params = {
            "api-key": os.getenv("HELIUS_API_KEY"),
            "commitment": "finalized",
            "type": "TRANSFER",
            "limit": "100",  # Max out API limit requests
        }

        # Add the 'before' parameter for pagination
        if before:
            params["before"] = before

        try:
            # Make the request
            response = requests.get(url, params=params)
            response.raise_for_status()

            # Parse the response transactions as JSON
            txs = response.json()

            # If no transactions returned, we've reached the end
            if not txs:
                print("No more transactions!")
                break

            # Add transactions to current batch
            batch.extend(process_distributor_transactions(txs))

            if last_sig is None:
                last_sig = txs[0]["signature"]

            # Set the before parameter to the signature of the last transaction
            before = txs[-1]["signature"]

        except Exception as e:
            print(f"Error when fetching distributor transactions from helius: {e}")
            return None

        # Check if batch is full
        if len(batch) >= batch_size:
            batch_count += 1

            total_count += len(batch)
            print(
                f"Yielding distributor transactions batch {batch_count}. Transaction count: {total_count}"
            )

            yield { "txs": batch, "last_sig": last_sig}

            # Clear batch for next iteration
            batch = []

    # Yield any remaining transactions in the final batch
    if batch:
        batch_count += 1
        total_count += len(batch)
        print(f"Yielding final batch. Transaction count: {total_count}")
        yield { "txs": batch, "last_sig": last_sig}


def get_new_distributor_transactions(
    distributor, until, batch_size=1000
):
    """ Get all of the latest transactions base of the newest (until) signature """
    batch = []
    batch_count = 0
    total_count = 0
    before = None
    newest_sig = None

    # URL for call to Helius
    url = f"https://api.helius.xyz/v0/addresses/{distributor}/transactions"

    while True:
        # Parameters for the API call
        params = {
            "api-key": os.getenv("HELIUS_API_KEY"),
            "commitment": "finalized",
            "type": "TRANSFER",
            "limit": "100",  # Max out API limit requests
            "until": until
        }

        # Add the 'before' parameter for pagination
        if before:
            params["before"] = before

        try:
            # Make the request
            response = requests.get(url, params=params)
            response.raise_for_status()

            # Parse the response transactions as JSON
            txs = response.json()

            # If no transactions returned, we've reached the end
            if not txs:
                print("No more transactions!")
                break

            # Holds txs that have been checked to ensure they don't contain the until sig
            checked_txs = []
            found_cutoff = False

            # Check if any of the transactions are the cut off point
            for tx in txs:
                if tx.get("signature") == until:
                    print("All new transactions have been fetched")
                    found_cutoff = True
                    break  # Stop processing more transactions

                # Add tx to the checked list
                checked_txs.append(tx)

            # Process and add transactions to current batch
            if checked_txs:
                processed_txs = process_distributor_transactions(checked_txs)
                batch.extend(processed_txs)

            # Set the newest tx as the last signature for future fetching
            if newest_sig is None and txs:
                newest_sig = txs[0]["signature"]

            # Set the before parameter to the signature of the last transaction
            before = txs[-1]["signature"]

            # If we found the cutoff signature, stop the main loop
            if found_cutoff:
                break

        except Exception as e:
            print(f"Error when fetching distributor transactions from helius: {e}")
            return  # Changed from 'return None' to just 'return' since this is a generator

        # Check if batch is full
        if len(batch) >= batch_size:
            batch_count += 1
            total_count += len(batch)
            print(
                f"Yielding distributor transactions batch {batch_count}. Transaction count: {total_count}"
            )

            yield {"txs": batch, "last_sig": newest_sig}

            # Clear batch for next iteration
            batch = []

    # Yield any remaining transactions in the final batch
    if batch:
        batch_count += 1
        total_count += len(batch)
        print(f"Yielding final batch. Transaction count: {total_count}")
        yield {"txs": batch, "last_sig": newest_sig}


def get_token_metadata(mint_address):
    """""
    Use the Helius rpc url endpoint of getAsset to fetch a tokens metadata
    """""
     # Payload for the helius getAssets endpoint
    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getAsset",
        "params": {
            "id": mint_address,
            "options": {
                "showInscription": False,
                "showFungible": False,
                "showCollectionMetadata": False,
                "showUnverifiedCollections": False
            }
        }
    }

    # request headers
    headers = {"Content-Type": "application/json"}

    try:
        # Helius request
        response = requests.post(os.getenv('HELIUS_RPC_URL'), json=payload, headers=headers)
        response.raise_for_status()  # Raise an error if it failed

        # Parse the response transactions as JSON
        data = response.json()

        # If no result returned
        if not data.get('result'):
            return mint_address  # Return mint as fallback

        # Get the metadata field
        content = data['result'].get('content', {})
        metadata = content.get('metadata', {})

        # Create the token document
        token_document = {
            "symbol": metadata.get("symbol", mint_address[:8]),  # Use truncated mint as fallback
            "name": metadata.get("name", "Unknown Token"),
            "mint": mint_address,
            "decimals": "unknown"
        }
        return token_document
    except Exception as e:
        print(f"There was an error when fetching token metadata from helius: {e}")
        return mint_address