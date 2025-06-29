import asyncio
import requests
import os
import json
import time
from pathlib import Path
from dotenv import load_dotenv
from server.db.MongoDB import MongoDB

# Load in env vars
load_dotenv()


# Fetch all transactions for a distributor in batches, saving data incrementally.
def get_distributor_transactions(
    distributor, batch_size=1000
):
    # Setup file paths
    project_transactions_dir = Path("./data/project_transactions")
    project_transactions_dir.mkdir(parents=True, exist_ok=True)
    project_transactions_file = project_transactions_dir / f"{distributor}.json"

    # Initialize tracking variables
    total_processed = 0
    batch_transactions = []
    batch_count = 0
    before = None

    # URL for the API call to Helius
    url = f"https://api.helius.xyz/v0/addresses/{distributor}/transactions"

    print(f"Starting transaction fetch for distributor: {distributor}")
    print(
        f"Batch size: {batch_size}, Max transactions: {max_transactions or 'unlimited'}"
    )

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
                print("No more transactions found.")
                break

            # Add transactions to current batch
            batch_transactions.extend(txs)

            print(
                f"Fetched {len(txs)} transactions. Batch total: {len(batch_transactions)}"
            )

            # Check if batch is full or we've hit max limit
            should_save_batch = len(batch_transactions) >= batch_size or (
                max_transactions
                and total_processed + len(batch_transactions) >= max_transactions
            )

            if should_save_batch:
                # Trim batch if we're over max_transactions
                if max_transactions:
                    remaining = max_transactions - total_processed
                    if len(batch_transactions) > remaining:
                        batch_transactions = batch_transactions[:remaining]

                # Save current batch
                batch_count += 1
                print(
                    f"Saving batch {batch_count} with {len(batch_transactions)} transactions..."
                )

                success = save_batch_to_file(
                    batch_transactions, project_transactions_file, batch_count
                )
                if success:
                    # Process the batch through organize_transfers
                    organize_transfers_to_file(batch_transactions, distributor)
                    total_processed += len(batch_transactions)
                    print(
                        f"Batch {batch_count} saved successfully. Total processed: {total_processed}"
                    )
                else:
                    print(f"Failed to save batch {batch_count}")
                    return total_processed

                # Clear batch for next iteration
                batch_transactions = []

                # Check if we've reached max transactions
                if max_transactions and total_processed >= max_transactions:
                    print(f"Reached maximum transaction limit: {max_transactions}")
                    break

            # Set the before parameter to the signature of the last transaction
            before = txs[-1]["signature"]

            # Small sleep between requests
            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            # Save current batch before returning on error
            if batch_transactions:
                save_batch_to_file(
                    batch_transactions, project_transactions_file, batch_count + 1
                )
                organize_transfers_to_file(batch_transactions, distributor)
                total_processed += len(batch_transactions)
            return total_processed

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")
            return total_processed

        except (KeyError, IndexError) as e:
            print(f"Error accessing transaction data: {e}")
            break

    # Save any remaining transactions in the final batch
    if batch_transactions:
        batch_count += 1
        print(
            f"Saving final batch {batch_count} with {len(batch_transactions)} transactions..."
        )
        success = save_batch_to_file(
            batch_transactions, project_transactions_file, batch_count
        )
        if success:
            organize_transfers_to_file(batch_transactions, distributor)
            total_processed += len(batch_transactions)
            print(f"Final batch saved successfully.")

    print(
        f"Transaction fetch complete. Total transactions processed: {total_processed}"
    )
    return total_processed


# Save a batch of transactions to file, appending to existing data.
def save_batch_to_file(batch_transactions, file_path, batch_number):
    try:
        # Load existing data if file exists
        if file_path.exists():
            with open(file_path, "r") as f:
                existing_data = json.load(f)

            # Ensure existing_data is a list
            if not isinstance(existing_data, list):
                existing_data = [existing_data] if existing_data else []
        else:
            existing_data = []

        # Append new batch to existing data
        existing_data.extend(batch_transactions)

        # Write updated data back to file
        with open(file_path, "w") as f:
            json.dump(existing_data, f, indent=2)

        return True

    except Exception as e:
        print(f"Error saving batch {batch_number} to file: {e}")
        return False


# Creates individual wallet files with their transfer data.
def organize_transfers_by_wallet(transfers, distributor):
    transfer_count = 0
    error_count = 0

    # Loop through each transaction
    for tf in transfers:
        signature = tf.get("signature")
        slot = tf.get("slot")
        timestamp = tf.get("timestamp")
        native_transfers = tf.get("nativeTransfers", [])
        token_transfers = tf.get("tokenTransfers", [])

        total_transfers = []

        if len(native_transfers) > 0:
            for t in native_transfers:
                to_user_account = t.get("toUserAccount")
                amount = Number(t.get("amount")) * 1e9
                token = "sol"

                total_transfers.append({
                    "signature": signature,
                    "slot": slot,
                    "timestamp": timestamp,
                    "amount": amount,
                    "token": token,
                    "wallet_address": to_account,
                    "distributor": distributor,
                })


        if len(token_transfers) > 0:
            for t in native_transfers:
                to_user_account = t.get("toUserAccount")
                amount = t.get("tokenAmount")
                token = get_token_symbol(t.get('mint'))

                total_transfers.append({
                    "signature": signature,
                    "slot": slot,
                    "timestamp": timestamp,
                    "amount": amount,
                    "token": token,
                    "wallet_address": to_account,
                    "distributor": distributor,
                })

        # Check which transfers to process
        if native_transfers and token_transfers:
            # If we have both, it was most likely a token swap
            continue
        elif native_transfers:
            # These transfers are SOL rewards
            transfers = native_transfers
            token = "sol"
        elif token_transfers:
            # SPL token transfers
            transfers = token_transfers
            token = "spl"
        else:
            continue

        # Loop through the transfers
        for transfer in transfers:
            # Get the recipient and amount received
            to_account = transfer.get("toUserAccount")
            amount = transfer.get("amount")

            if not to_account:
                continue

            # Prepare transfer data
            transfer_data = {
                "signature": signature,
                "slot": slot,
                "timestamp": timestamp,
                "amount": amount,
                "token": token,
                "wallet_address": to_account,
                "distributor": distributor,
            }



            # Save to individual wallet file
            success = db.insert_wallet_transfer(transfer_data)

            if success:
                transfer_count += 1
            else:
                error_count += 1

    if transfer_count > 0:
        print(f"Organized {transfer_count} transfers to wallet files.")
    if error_count > 0:
        print(f"Encountered {error_count} errors during organization.")


# Save transfer data to individual wallet file.
def save_wallet_transfer_to_file(wallet_address, transfer_data, wallets_dir):

    try:
        # Get the wallet file
        wallet_file = wallets_dir / f"{wallet_address}.json"

        # Load existing wallet data if file exists
        if wallet_file.exists():
            with open(wallet_file, "r") as f:
                existing_transfers = json.load(f)

            # Ensure it's a list
            if not isinstance(existing_transfers, list):
                existing_transfers = [existing_transfers] if existing_transfers else []
        else:
            existing_transfers = []

        # Check for duplicate signatures to avoid duplicates
        existing_signatures = {
            t.get("signature") for t in existing_transfers if t.get("signature")
        }

        if transfer_data["signature"] not in existing_signatures:
            existing_transfers.append(transfer_data)

            # Save updated data
            with open(wallet_file, "w") as f:
                json.dump(existing_transfers, f, indent=2)

        return True

    except Exception as e:
        print(f"Error saving transfer for wallet {wallet_address}: {e}")
        return False


# Example usage
if __name__ == "__main__":
    distributor = "9uJbttvvowG1rVpPt6GMB3mL7BuktaHaNzFQbkACfiNN"

    # Fetch all transactions in batches of 1000, saving as we go
    total = get_distributor_transactions(
        distributor=distributor,
        batch_size=1000,  # Save every 1000 transactions
        max_transactions=None,  # Set to a number to limit, or None for all
        before=None,
    )
