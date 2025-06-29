import asyncio
import requests
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from ..db.MongoDB import MongoDB
load_dotenv()


# Creates individual wallet files with their transfer data.
def organize_transfers_by_wallet(transfers, distributor):

    # print(len(transfers))
    transfer_count = 0
    error_count = 0

    # Get a connection to DB
    db = _get_db_instance()

    # Rais an error if we failed to connect
    if db is None:
        raise Exception("Couldn't connect to DB")

    known_tokens = db.get_known_tokens()
    total_transfers = []

    # Loop through each transaction
    for tf in transfers:
        signature = tf.get("signature")
        slot = tf.get("slot")
        timestamp = tf.get("timestamp")
        native_transfers = tf.get("native_transfers")
        token_transfers = tf.get("token_transfers")

        # Loop throught the token two lists and


        # Native sol transfers list
        if len(native_transfers) > 0:
            for t in native_transfers:
                to_user_account = t.get("toUserAccount")
                amount = t.get("amount") * 1e9
                token = "sol"

                total_transfers.append({
                    "signature": signature,
                    "slot": slot,
                    "timestamp": timestamp,
                    "amount": amount,
                    "token": token,
                    "wallet_address": to_user_account,
                    "distributor": distributor,
                })

        # SPL transfers list
        if len(token_transfers) > 0:
            for t in token_transfers:
                to_user_account = t.get("toUserAccount")
                amount = t.get("tokenAmount")
                token = get_token_symbol(t.get('mint'), known_tokens)

                total_transfers.append({
                    "signature": signature,
                    "slot": slot,
                    "timestamp": timestamp,
                    "amount": amount,
                    "token": token,
                    "wallet_address": to_user_account,
                    "distributor": distributor,
                })

    print(len(total_transfers))    # Save to individual wallet file
    db.insert_rewards_wallet_batch(total_transfers)

"""""
Gets an instance of the MongoDB
"""""
def _get_db_instance():
    try:
        db = MongoDB()
        return db
    except Exception as e:
        print(f"There was an error when trying to initialize DB: {e}")
        return None

"""""
Fetches token metadata, adds it to the list of known tokens in DB, and returns the symbol of the token added
"""""
def get_and_add_token_metadata(mint_address):
    # Get a connection to DB
    db = _get_db_instance()

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

    # Helius request
    response = requests.request("POST", os.getenv('HELIUS_RPC_URL'), json=payload, headers=headers)
    response.raise_for_status() # Raise and error if it failed

    # Parse the response transactions as JSON
    data = json.loads(response.text)

    # If no transactions returned, we've reached the end
    if not data:
        return None

    # Get the metadata field
    metadata = data['result'].get('content').get('metadata')

    # Create the token document
    token_document = {
        "symbol": metadata.get("symbol"),
        "name": metadata.get("name"),
        "mint": mint_address,
        "decimals": "unknown"
    }

    # Save the token to the known_tokens collection
    try:
        db.insert_known_token(token_document)
        return metadata.get("symbol")
    except Exception as e:
        print(f"Could not add token to known tokens: {e}")
        return metadata.get("symbol")

"""""
Checks if the token is known and if it isn't then we add it to the DB
"""""
def get_token_symbol(mint_address, known_tokens):
    for t in known_tokens:
        if mint_address.lower() == str(t.get("mint")).lower():
            return t.get("symbol")

    return get_and_add_token_metadata(mint_address)



# Fetch all transactions for a distributor in batches, saving data incrementally.
# def get_distributor_transactions(distributor, batch_size=1000):
#     # Initialize tracking variables
#     total_processed = 0
#     batch_transactions = []
#     batch_count = 0

#     # URL for the API call to Helius
#     url = f"https://api.helius.xyz/v0/addresses/{distributor}/transactions"

#     print(f"Starting transaction fetch for distributor: {distributor}")
#     print(
#         f"Batch size: {batch_size}, Max transactions: {max_transactions or 'unlimited'}"
#     )

#     db = MongoDB()

#     before = db.get_last_signature_for_distributor(distributor)

#     while True:
#         # Parameters for the API call
#         params = {
#             "api-key": os.getenv("HELIUS_API_KEY"),
#             "commitment": "finalized",
#             "type": "TRANSFER",
#             "limit": "100",  # Max out API limit requests
#         }

#         # Add the 'before' parameter for pagination
#         if before:
#             params["before"] = before

#         try:
#             # Make the request
#             response = requests.get(url, params=params)
#             response.raise_for_status()  # Raise an error if the call failed

#             # Parse the response transactions as JSON
#             txs = response.json()

#             # If no transactions returned, we've reached the end
#             if not txs:
#                 print("No more transactions found.")
#                 break

#             # Holds the
#             filtered_txs = []

#             # Loop through the transactions and only get the relevent data
#             for tx in txs:
#                 fee_payer = tx.get("feePayer")
#                 signature = tx.get("signature")
#                 slot = tx.get("slot")
#                 timestamp = tx.get("timestamp")
#                 token_transfers = tx.get("tokenTransfers", [])
#                 native_transfers = tx.get("nativeTransfers", [])
#                 filtered_txs.append(
#                     {
#                         fee_payer,
#                         signature,
#                         slot,
#                         timestamp,
#                         token_transfers,
#                         native_transfers,
#                     }
#                 )

#             # Add transactions to current batch
#             batch_transactions.extend(filtered_txs)

#             print(
#                 f"Fetched {len(txs)} transactions. Batch total: {len(batch_transactions)}"
#             )

#             # Check if batch is full or we've hit max limit
#             if len(batch_transactions) >= batch_size:
#                 # Save current batch
#                 batch_count += 1
#                 print(
#                     f"Saving batch {batch_count} with {len(batch_transactions)} transactions..."
#                 )

#                 success = insert_batch_rewards_wallet_transfer(
#                     batch_transactions, known_tokens
#                 )

#                 if success:
#                     # Process the batch through organize_transfers
#                     update_wallet_transfers(batch_transactions, distributor)
#                     total_processed += len(batch_transactions)
#                     print(
#                         f"Batch {batch_count} saved successfully. Total processed: {total_processed}"
#                     )
#                 else:
#                     raise (f"Failed to save batch {batch_count}")

#                 # Clear batch for next iteration
#                 batch_transactions = []

#                 # Check if we've reached max transactions
#                 if max_transactions and total_processed >= max_transactions:
#                     print(f"Reached maximum transaction limit: {max_transactions}")
#                     break

#             # Set the before parameter to the signature of the last transaction
#             before = txs[-1]["signature"]

#             # Small sleep between requests
#             time.sleep(0.1)

#         except requests.exceptions.RequestException as e:
#             print(f"Error making API request: {e}")
#             # Save current batch before returning on error
#             if batch_transactions:
#                 save_batch_to_file(
#                     batch_transactions, project_transactions_file, batch_count + 1
#                 )
#                 organize_transfers_to_file(batch_transactions, distributor)
#                 total_processed += len(batch_transactions)
#             return total_processed

#         except json.JSONDecodeError as e:
#             print(f"Error decoding JSON response: {e}")
#             return total_processed

#         except (KeyError, IndexError) as e:
#             print(f"Error accessing transaction data: {e}")
#             break

#     # Save any remaining transactions in the final batch
#     if batch_transactions:
#         batch_count += 1
#         print(
#             f"Saving final batch {batch_count} with {len(batch_transactions)} transactions..."
#         )
#         success = save_batch_to_file(
#             batch_transactions, project_transactions_file, batch_count
#         )
#         if success:
#             organize_transfers_to_file(batch_transactions, distributor)
#             total_processed += len(batch_transactions)
#             print(f"Final batch saved successfully.")

#     print(
#         f"Transaction fetch complete. Total transactions processed: {total_processed}"
#     )
#     return total_processed


# # This will be used to get all the transactions for the distribute protocol
# def get_distributor_transactions(distributor):
#     project_transactions_dir = Path("./data/project_transactions")

#     project_transactions_file = project_transactions_dir / f"{distributor}.json"

#     # URL for the api call to Helius
#     url = f"https://api.helius.xyz/v0/addresses/{distributor}/transactions"

#     # Parameters for the api call
#     params = {
#         "api-key": os.getenv("HELIUS_API_KEY"),
#         "commitment": "finalized",
#         "type": "TRANSFER",
#         "limit": "100",
#     }

#     try:
#         # Make the request
#         response = requests.request("GET", url, params=params)
#         response.raise_for_status()

#         # Parse the response transactions as JSON
#         txs = json.loads(response.text)

#     except requests.exceptions.RequestException as e:
#         print(f"Error making API request: {e}")
#         return None
#     except json.JSONDecodeError as e:
#         print(f"Error decoding JSON response: {e}")
#         return None
#     except (KeyError, IndexError) as e:
#         print(f"Error with getting distributor transactions: {e}")
#         return None

#     # Here we try to save our data to file
#     try:
#         # Update existing file or create new one
#         if project_transactions_file.exists():
#             # Load existing data
#             with open(project_transactions_file, "r") as f:
#                 existing_data = json.load(f)

#             # Append new transfer
#             if isinstance(existing_data, list):
#                 existing_data.append(txs)
#             else:
#                 # Handle case where file exists but isn't a list
#                 existing_data = [existing_data, txs]

#             # Write updated data
#             with open(project_transactions_file, "w") as f:
#                 json.dump(existing_data, f, indent=2)
#         else:
#             # Create new file with transfer data as a list
#             with open(project_transactions_file, "w") as f:
#                 json.dump([txs], f, indent=2)

#     except (KeyError, IndexError) as e:
#         print(f"Error when saving data to file {e}")
#         return None

#     # Now we send it over to organize the data by recipent address
#     # and save it to the wallets dir
#     organize_transfers(txs, distributor)


# # Organizes txs and saves to MongoDB
# def organize_transfers(transaction_data, distributor):
#     transfer_count = 0
#     error_count = 0

#     # Loop through each transaction
#     for transaction in transaction_data:
#         fee_payer = transaction.get("feePayer")
#         signature = transaction.get("signature")
#         slot = transaction.get("slot")
#         timestamp = transaction.get("timestamp")

#         # Check if fee payer is one of our targeted distributors
#         if fee_payer != distributor:
#             continue

#         # Process all transfers in the transaction
#         native_transfers = transaction.get("nativeTransfers", [])
#         token_transfers = transaction.get("tokenTransfers", [])

#         # Check which transfers to process
#         if native_transfers and token_transfers:
#             # If we have both of these it was mostlikly a token swap not the
#             # distributor wallet giving out rewards
#             continue
#         elif native_transfers:
#             # These transfers are sol rewards being issued out to holders
#             transfers = native_transfers
#             token = "sol"
#             decimals = 9
#         elif token_transfers:
#             # Sometimes distributors give out spl tokens instead of sol
#             transfers = token_transfers
#             token = "spl"
#             # decimals = ? we will have to handle this later since each token
#             # can have different decimals
#         else:
#             print(f"Transaction {signature} has no transfers. Skipping.")
#             print("")
#             continue

#         # Loop through the transfers
#         for transfer in transfers:
#             # Get the recipient and amount received
#             to_account = transfer.get("toUserAccount")
#             amount = transfer.get("amount")

#             # Prepare transfer data
#             transfer_data = {
#                 "signature": signature,
#                 "slot": slot,
#                 "timestamp": timestamp,
#                 "amount": amount,
#                 "token": token,
#             }

#             # Insert into MongoDB
#             success = insert_wallet_transfer(to_account, fee_payer, transfer_data)

#             if success:
#                 transfer_count += 1
#             else:
#                 error_count += 1
#                 print(
#                     f"Failed to insert transfer for wallet {to_account} from transaction {signature}"
#                 )

#     print(
#         f"Data organization complete. Inserted {transfer_count} transfers to MongoDB."
#     )
#     if error_count > 0:
#         print(f"Encountered {error_count} errors during insertion.")


# # NOTE: Still testing this one
# # Currently gets a the top 100 transactions. This will soon be changed to get the last transaction
# # based on the latest cached transactions from either the json file or the db
# def get_transactions_for_wallet(wallet_address, max_transactions=None):
#     # initialize vars
#     all_transactions = []
#     before = None

#     # For right now we will work with just one distributor then scale it up
#     distributor = "CvgM6wSDXWCZeCmZnKRQdnh4CSga3UuTXwrCXy9Ju6PC"

#     # URL for the api call to Helius
#     url = f"https://api.helius.xyz/v0/addresses/{distributor}/transactions"

#     while True:
#         # Parameters for the api call
#         params = {
#             "api-key": os.getenv("HELIUS_API_KEY"),
#             "commitment": "finalized",
#             "type": "TRANSFER",
#             "limit": "100",
#         }

#         # Add the 'before' parameter for pagination
#         if before:
#             params["before"] = before

#         try:
#             # Make the request
#             response = requests.request("GET", url, params=params)
#             response.raise_for_status()

#             # Parse the response transactions as JSON
#             txs = json.loads(response.text)

#             # If no transactions returned, we've reached the end
#             if not txs:
#                 break

#             # Add transactions to our collection
#             all_transactions.extend(txs)

#             # Check if we've hit our max limit
#             if max_transactions and len(all_transactions) >= max_transactions:
#                 all_transactions = all_transactions[:max_transactions]
#                 break

#             # Set the before parameter to the signature of the last transaction
#             # This will fetch the next batch of older transactions
#             before = txs[-1]["signature"]

#             # Small sleep between requests
#             time.sleep(0.1)

#         except requests.exceptions.RequestException as e:
#             print(f"Error making API request: {e}")
#             return None
#         except json.JSONDecodeError as e:
#             print(f"Error decoding JSON response: {e}")
#             return None
#         except (KeyError, IndexError) as e:
#             print(f"Error accessing transaction signature: {e}")
#             break

#     # Sort all transactions by the project distributor mint
#     return sort_transactions_by_distributor(all_transactions, wallet_address)
