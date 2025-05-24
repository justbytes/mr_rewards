from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pathlib import Path
import json
from typing import List, Dict, Any, Optional
import uvicorn
from token_aggregator import get_total_rewards


"""
    This file contains a basic server using the FastAPI library and includes 4 routes:
    /health, /rewards/{wallet}/{distributor}, /wallets/, /wallets/{wallet_address}/distributors
"""

# Initialize the app
app = FastAPI(
    title="Wallet Rewards Aggergator API",
    description="API to retrieve aggregated rewards recieved from distribution wallets",
    version="1.0.0",
)


# A model for how the total_amounts data
class RewardAmount(BaseModel):
    token: str
    total_amount: float
    raw_amount: int
    decimals: int


# A model for the response of the get_total_rewards function
class RewardsResponse(BaseModel):
    found: bool
    total_amounts: List[RewardAmount]
    transfer_count: int
    error: Optional[str] = None


# Model for the health route response
class HealthResponse(BaseModel):
    status: str
    message: str


# Model for the wallets route response
class WalletsResponse(BaseModel):
    wallets: List[str]
    count: int


# Model for the distributors route
class DistributorsResponse(BaseModel):
    wallet: str
    distributors: List[str]
    count: int


# Model for the root response
class RootResponse(BaseModel):
    message: str
    version: str
    endpoints: Dict[str, str]


# API Routes
@app.get("/")
# Root endpoint with API information
async def root():
    return {
        "message": "Wallet Rewards API",
        "version": "1.0.0",
        "endpoints": {
            "status": "/health",
            "get_rewards": "/rewards/{wallet_address}/{distributor_address}",
            "wallets": "/wallets",
            "wallets_distributors": "/wallets/{wallet_address}/distributors",
            "docs": "/docs",
            "openapi": "/openapi.json",
        },
    }


# Sever helper function that checks if everything is working
@app.get("/health", response_model=HealthResponse)
async def health_check():
    return {"status": "healthy", "message": "API is running"}


# Route for getting total rewards for a specific wallet from a specific distributor
@app.get(
    "/rewards/{wallet_address}/{distributor_address}", response_model=RewardsResponse
)
async def get_wallet_rewards(wallet_address: str, distributor_address: str):
    try:
        # Call the get_total_rewards function
        result = get_total_rewards(wallet_address, distributor_address)

        # Put the results into the rewards response
        return RewardsResponse(**result)

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Handle any unexpected exceptions
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# A route that lists available wallets / wallets that have recieved rewards
# NOTE: This will change whe we add the db and not result.get("found")
@app.get("/wallets")
async def list_wallets():
    try:
        # Ensure directory is there
        wallets_dir = Path("./data/wallets")
        if not wallets_dir.exists():
            raise HTTPException(
                status_code=404, detail="Wallets directory does not exist"
            )

        # Get the a list of all the wallets in the wallets dir
        wallets = [d.name for d in wallets_dir.iterdir() if d.is_dir()]
        return {"wallets": wallets, "count": len(wallets)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing wallets: {str(e)}")


# This route gets a list of distribotrs for a given wallet
# NOTE: This will change whe we add the db
@app.get("/wallets/{wallet_address}/distributors")
async def list_distributors_for_wallet(wallet_address: str):
    try:
        # Ensure directory is there
        wallet_dir = Path(f"./data/wallets/{wallet_address}")
        if not wallet_dir.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Wallet directory for {wallet_address} not found",
            )

        # Get all of the json files in wallet dir
        distributors = [f.stem for f in wallet_dir.glob("*.json")]
        return {
            "wallet": wallet_address,
            "distributors": distributors,
            "count": len(distributors),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error listing distributors: {str(e)}"
        )


# Starts the server
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
