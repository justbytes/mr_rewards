from fastapi import APIRouter, HTTPException, Depends, Request
from typing import List
from lib.Controller import Controller
from .dependency import get_controller
from .models import  WalletsRewardsResponse
from limiter import limiter

# Initialize the router
router = APIRouter()

@router.get("/{wallet_address}", response_model=WalletsRewardsResponse | None)
@limiter.limit("10/minute")
async def get_wallets_rewards(request: Request, wallet_address: str, controller: Controller = Depends(get_controller)):
    """Gets the total rewards amounts for a given wallet address"""
    # Validate address
    wallet_address = wallet_address.strip()

    # Solana address should be 32-44 characters long
    if len(wallet_address) < 32 or len(wallet_address) > 44:
        raise HTTPException(
            status_code=400,
            detail="Incorrect address format"
        )

    # Fetch the data
    try:
        return controller.get_rewards_with_wallet_address_from_db(wallet_address)
    except:
        raise HTTPException(
            status_code=500, detail="Error getting rewards for wallet and distributor"
        )