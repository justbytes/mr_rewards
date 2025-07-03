from fastapi import APIRouter, HTTPException, Depends
from typing import List
from lib.Controller import Controller
from .dependency import get_controller
from .models import  WalletsRewardsResponse

router = APIRouter()

@router.get("/{wallet_address}", response_model=WalletsRewardsResponse)
async def get_wallets_rewards(wallet_address: str, controller: Controller = Depends(get_controller)):
    try:
        return controller.get_rewards_with_wallet_address_from_db(wallet_address)
    except:
        raise HTTPException(
            status_code=500, detail="Error getting rewards for wallet and distributor"
        )