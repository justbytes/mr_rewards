def process_distributor_transactions(transactions):
    filtered_txs = []
    # Loop through txs and add filtered txs to the batch list
    for tx in transactions:
        token_transfers = tx.get("tokenTransfers", [])
        native_transfers = tx.get("nativeTransfers", [])

        # Skip transactions with no transfers
        # if len(native_transfers) < 2 or len(token_transfers) < 2:
        #     continue

        filtered_txs.append(
            {
                "fee_payer": tx.get("feePayer"),
                "signature": tx.get("signature"),
                "slot": tx.get("slot"),
                "timestamp": tx.get("timestamp"),
                "token_transfers": token_transfers,
                "native_transfers": native_transfers,
            }
        )
    return filtered_txs


def process_distributor_transfers(app, transactions, distributor):
    total_transfers = []

    # Loop through each transaction
    for tx in transactions:
        signature = tx.get("signature")
        slot = tx.get("slot")
        timestamp = tx.get("timestamp")
        native_transfers = tx.get("native_transfers", [])
        token_transfers = tx.get("token_transfers", [])

        # Native sol transfers list
        for tf in native_transfers:
            # Get the user wallet
            to_user_account = tf.get("toUserAccount")

            # Skip if None or empty
            if to_user_account:
                # Normailize price
                amount = tf.get("amount", 0) / 1e9

                # Add to transfer to total_transfers list
                total_transfers.append({
                    "signature": signature,
                    "slot": slot,
                    "timestamp": timestamp,
                    "amount": amount,
                    "token": "sol",
                    "wallet_address": to_user_account,
                    "distributor": distributor,
                })

        # SPL transfers list
        for tf in token_transfers:
            # Get user wallet and token mint
            to_user_account = tf.get("toUserAccount")
            mint = tf.get('mint')

             # Skip if either is None or empty
            if to_user_account and mint:
                amount = tf.get("tokenAmount", 0)

                # Gets the the token symbol from the known tokens or adds the token
                # to the known tokens
                token = app.get_token_symbol(mint)

                # Add to transfer to total_transfers list
                total_transfers.append({
                    "signature": signature,
                    "slot": slot,
                    "timestamp": timestamp,
                    "amount": amount,
                    "token": token,
                    "wallet_address": to_user_account,
                    "distributor": distributor,
                })

    return total_transfers


def aggregate_transfers(transfers):
    # Dictionary to track totals by token type
    wallets = {}

    # Loop through the wallet's transactions
    for transfer in transfers:
        wallet_address = transfer.get("wallet_address")
        distributor = transfer.get("distributor")
        token = transfer.get('token')
        amount = transfer.get('amount')

        if wallet_address in wallets:
            # Check if this distributor exists for this wallet
            if distributor in wallets[wallet_address]['distributors']:
                # Check if this token exists for this distributor
                if token in wallets[wallet_address]['distributors'][distributor]['tokens']:
                    wallets[wallet_address]['distributors'][distributor]['tokens'][token]['total_amount'] += amount
                else:
                    wallets[wallet_address]['distributors'][distributor]['tokens'][token] = {'total_amount': amount}
            else:
                # Add new distributor for this wallet
                wallets[wallet_address]['distributors'][distributor] = {
                    'tokens': {
                        token: {'total_amount': amount}
                    }
                }
        else:
            # Create new wallet entry
            wallets[wallet_address] = {
                'distributors': {
                    distributor: {
                        'tokens': {
                            token: {'total_amount': amount}
                        }
                    }
                }
            }

    return wallets