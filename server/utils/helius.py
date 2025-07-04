import requests
import os
from .utils import process_distributor_transactions
from dotenv import load_dotenv
load_dotenv()

def get_distributor_transactions(
    distributor, before, batch_size=500
):
    batch = []
    batch_count = 0
    total_count = 0
    real_count = 0

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

            yield { "txs": batch, "last_sig": before}

            # Clear batch for next iteration
            batch = []

    # Yield any remaining transactions in the final batch
    if batch:
        batch_count += 1
        total_count += len(batch)
        print(f"Yielding final batch. Transaction count: {total_count}")
        yield { "txs": batch, "last_sig": before}


"""""
Use the Helius rpc url endpoint of getAsset to fetch a tokens metadata
"""""
def get_token_metadata(mint_address):
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