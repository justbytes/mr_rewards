# server/routes/wallet_rewards.py
from fastapi import APIRouter, HTTPException, Depends, Request
from typing import List
from lib.Controller import Controller
from .dependency import get_controller
from .models import WalletsRewardsResponse
from .auth import api_key_auth  # Import Bearer token auth
from limiter import limiter

# Initialize the router
router = APIRouter()

@router.get("/{wallet_address}", response_model=WalletsRewardsResponse | None)
@limiter.limit("10/minute")
async def get_wallets_rewards(
    request: Request,
    wallet_address: str,
    controller: Controller = Depends(get_controller),
    api_key: str = Depends(api_key_auth)  # Requires Bearer token
):
    """Gets the total rewards amounts for a given wallet address (requires Bearer token authentication)"""
    # Validate address
    wallet_address = wallet_address.strip()

    # Solana address should be 32-44 characters long
    if len(wallet_address) < 32 or len(wallet_address) > 44:
        raise HTTPException(
            status_code=400,
            detail="Incorrect address format"
        )

    # Fetch the data from SQLite
    try:
        # Access SQLite database through controller
        wallet_data = controller.sqlite.get_wallet(wallet_address)

        if not wallet_data:
            return None

        # Convert to match your Pydantic model
        wallet_response = WalletsRewardsResponse(
            _id=wallet_data.get("wallet_address", ""),
            wallet_address=wallet_data.get("wallet_address", ""),
            distributors=wallet_data.get("distributors", {})
        )

        return wallet_response

    except Exception as e:
        print(f"Error in get_wallets_rewards: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error getting rewards for wallet and distributor"
        )