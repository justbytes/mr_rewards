import os
from pathlib import Path
import json
from typing import List, Dict, Any, Optional

# Gets the total rewards for given wallet address and distributor address
def get_total_rewards(target_wallet: str, distributor_address: str) -> Dict[str, Any]:

    # Initialize result dictionary
    result = {
        'found': False,
        'total_amounts': [],
        'transfer_count': 0,
        'error': None
    }

    # NOTE: Some of this will need to be changed when we hook up the db

    # Path to wallets
    wallets_dir = Path(f"./data/wallets")

    try:
        # Check if wallets directory exists
        if not wallets_dir.exists():
            result['error'] = "Wallets directory does not exist"
            return result

        # Check if target wallet directory exists
        wallet_dir = wallets_dir / target_wallet
        if not wallet_dir.exists():
            result['error'] = f"Wallet directory for {target_wallet} not found"
            return result

        # Check if distributor file exists for this wallet
        distributor_file = wallet_dir / f"{distributor_address}.json"
        if not distributor_file.exists():
            result['error'] = f"No rewards found from distributor {distributor_address} for wallet {target_wallet}"
            return result

        # Read and parse the distributor file
        with open(distributor_file, 'r') as f:
            transfer_data = json.load(f)

        # Dictionary to track totals by token type
        token_totals = {}

        # Loop through the wallets transactions
        for transfer in transfer_data:
            # Get amount and token type
            amount = transfer.get('amount', 0)
            token = transfer.get('token', 'unknown')

            # If the token is sol then we can just add the decimals otherwise
            # we need to search for what they are
            if token == 'sol':
                # Convert lamports to SOL (9 decimals)
                sol_amount = amount / (10 ** 9)
                if 'sol' not in token_totals:
                    token_totals['sol'] = {
                        'token': 'SOL',
                        'total_amount': 0,
                        'raw_amount': 0,
                        'decimals': 9
                    }

                token_totals['sol']['total_amount'] += sol_amount
                token_totals['sol']['raw_amount'] += amount
            else:
                # We should make another helius call using the sig to get tx detals
                # From the sig we can get the token account data and get the decimals
                # and the ticker for the token which will then be used to get the total
                #
                # NOTE: This should probably just be done during the intial data fetch
                #       and the the decimals and ticker should be saved to file as well
                print("SPL tokens not yet supported!")
                continue

        # Add token_totals dictionary to a list
        total_amounts_list = []
        for token_key, token_data in token_totals.items():
            total_amounts_list.append({
                'token': token_data['token'],
                'total_amount': token_data['total_amount'],
                'raw_amount': token_data['raw_amount'],
                'decimals': token_data['decimals']
            })

        # Update result
        result.update({
            'found': True,
            'total_amounts': total_amounts_list,
            'transfer_count': len(transfer_data)
        })

        return result

    except json.JSONDecodeError as e:
        result['error'] = f"Error decoding JSON file: {e}"
        return result
    except Exception as e:
        result['error'] = f"Unexpected error: {e}"
        return result
