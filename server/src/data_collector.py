import asyncio
import requests
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load in env vars
load_dotenv()

# This will be used to get all the transactions for the distribute protocol
def get_distributor_transactions(distributor):
    project_transactions_dir = Path("./data/project_transactions")

    project_transactions_file = project_transactions_dir / f"{distributor}.json"

    # URL for the api call to Helius
    url = f"https://api.helius.xyz/v0/addresses/{distributor}/transactions"

    # Parameters for the api call
    params = {
        "api-key": os.getenv('HELIUS_API_KEY'),
        "commitment": "finalized",
        "type": "TRANSFER",
        "limit": "100"
    }

    try:
        # Make the request
        response = requests.request("GET", url, params=params)
        response.raise_for_status()

        # Parse the response transactions as JSON
        txs = json.loads(response.text)

    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"Error with getting distributor transactions: {e}")
        return None


    # Here we try to save our data to file
    try:
         # Update existing file or create new one
        if project_transactions_file.exists():
            # Load existing data
            with open(project_transactions_file, 'r') as f:
                existing_data = json.load(f)

            # Append new transfer
            if isinstance(existing_data, list):
                existing_data.append(txs)
            else:
                # Handle case where file exists but isn't a list
                existing_data = [existing_data, txs]

            # Write updated data
            with open(project_transactions_file, 'w') as f:
                json.dump(existing_data, f, indent=2)
        else:
            # Create new file with transfer data as a list
            with open(project_transactions_file, 'w') as f:
                json.dump([txs], f, indent=2)

    except (KeyError, IndexError) as e:
        print(f"Error when saving data to file {e}")
        return None

    # Now we send it over to organize the data by recipent address
    # and save it to the wallets dir
    organize_transfers(txs, distributor)


# Organizes txs creating seperate dirs and files by recipient wallets
def organize_transfers(transaction_data, distributor):
    wallets_dir = Path("./data/wallets")

    # Loop through each transaction
    for transaction in transaction_data:
        fee_payer = transaction.get('feePayer')
        signature = transaction.get('signature')
        slot = transaction.get('slot')
        timestamp = transaction.get('timestamp')

        # Check if fee payer is one of our targeted distributors
        if fee_payer != distributor:
            continue

        # Process all transfers in the transaction
        native_transfers = transaction.get('nativeTransfers', [])
        token_transfers = transaction.get('tokenTransfers', [])

         # Check which transfers to process
        if native_transfers and token_transfers:
            # If we have both of these it was mostlikly a token swap not the
            # distributor wallet giving out rewards
            continue
        elif native_transfers:
            # These transfers are sol rewards being issued out to holders
            transfers = native_transfers
            token = "sol"
            decimals = 9
        elif token_transfers:
            # Sometimes distributors give out spl tokens instead of sol
            transfers = token_transfers
            token = "spl"
            # decimals = ? we will have to handle this later since each token
            # can have different decimals
        else:
            print(f"Transaction {signature} has no transfers. Skipping.")
            print("")
            continue

        # Loop through the transfers
        for transfer in transfers:
            # Get the recipient and amount recieved
            to_account = transfer.get('toUserAccount')
            amount = transfer.get('amount')

            # Create recipient directory if it doesn't exist
            recipient_dir = wallets_dir / to_account
            if not recipient_dir.exists():
                recipient_dir.mkdir()

            # Define file path for the feePayer JSON
            fee_payer_file = recipient_dir / f"{fee_payer}.json"

            # Prepare transfer data
            transfer_data = {
                'signature': signature,
                'slot': slot,
                'timestamp': timestamp,
                'amount': amount,
                'token': token
            }

            # Update existing file or create new one
            if fee_payer_file.exists():
                # Load existing data
                with open(fee_payer_file, 'r') as f:
                    existing_data = json.load(f)

                # Append new transfer
                if isinstance(existing_data, list):
                    existing_data.append(transfer_data)
                else:
                    # Handle case where file exists but isn't a list
                    existing_data = [existing_data, transfer_data]

                # Write updated data
                with open(fee_payer_file, 'w') as f:
                    json.dump(existing_data, f, indent=2)
            else:
                # Create new file with transfer data as a list
                with open(fee_payer_file, 'w') as f:
                    json.dump([transfer_data], f, indent=2)

    print(f"Data organization complete. Check the './data/wallets' directory for results.")


# NOTE: Still testing this one
# Currently gets a the top 100 transactions. This will soon be changed to get the last transaction
# based on the latest cached transactions from either the json file or the db
def get_transactions_for_wallet(wallet_address, max_transactions=None):
    # initialize vars
    all_transactions = []
    before = None

    # For right now we will work with just one distributor then scale it up
    distributor = "CvgM6wSDXWCZeCmZnKRQdnh4CSga3UuTXwrCXy9Ju6PC"

    # URL for the api call to Helius
    url = f"https://api.helius.xyz/v0/addresses/{distributor}/transactions"

    while True:
        # Parameters for the api call
        params = {
            "api-key": os.getenv('HELIUS_API_KEY'),
            "commitment": "finalized",
            "type": "TRANSFER",
            "limit": "100"
        }

        # Add the 'before' parameter for pagination
        if before:
            params["before"] = before

        try:
            # Make the request
            response = requests.request("GET", url, params=params)
            response.raise_for_status()

            # Parse the response transactions as JSON
            txs = json.loads(response.text)

            # If no transactions returned, we've reached the end
            if not txs:
                break

            # Add transactions to our collection
            all_transactions.extend(txs)

            # Check if we've hit our max limit
            if max_transactions and len(all_transactions) >= max_transactions:
                all_transactions = all_transactions[:max_transactions]
                break

            # Set the before parameter to the signature of the last transaction
            # This will fetch the next batch of older transactions
            before = txs[-1]["signature"]

            # Small sleep between requests
            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")
            return None
        except (KeyError, IndexError) as e:
            print(f"Error accessing transaction signature: {e}")
            break

    # Sort all transactions by the project distributor mint
    return sort_transactions_by_distributor(all_transactions, wallet_address)

# NOTE: In the future we will loop throught the projects.json file to get the target
#       distributor wallets. Getting every transaction is very resource intensive and
#       could take a few hours to process the transactions. This the MVP and only gets the
#       first 100 transactions of a single distributor.
distributor = "CvgM6wSDXWCZeCmZnKRQdnh4CSga3UuTXwrCXy9Ju6PC"

# Main function to start the data collection of the transactions for a distributor wallet
def main():
    # Fetch distributor transaction data, filter it, and save it
    data = get_distributor_transactions(distributor)


# Run the program
if __name__ == "__main__":
    main()