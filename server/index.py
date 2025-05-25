from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sys
import os
from pathlib import Path
import json
from typing import List, Dict, Any, Optional
import uvicorn
from contextlib import asynccontextmanager
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from token_aggregator import get_total_rewards
from db import get_all_wallets, get_distributors_for_wallet, initialize_db_connection, get_supported_projects


"""
    This file contains a basic server using the FastAPI library and includes 4 routes:
    /health, /rewards/{wallet}/{distributor}, /wallets/, /wallets/{wallet_address}/distributors
"""

# Lifespan event handle
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize the db connection
    print("Starting up the API...")
    if initialize_db_connection():
        print("Database connected successfully")
    else:
        print("Warning: Database connection failed")

    yield

    print("Shutting down the API...")


# Initialize the app
app = FastAPI(
    title="Wallet Rewards Aggergator API",
    description="API to retrieve aggregated rewards recieved from distribution wallets",
    version="1.0.0",
    lifespan=lifespan
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

        return get_all_wallets()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing wallets: {str(e)}")


# This route gets a list of distribotrs for a given wallet
# NOTE: This will change whe we add the db
@app.get("/wallets/{wallet_address}/distributors")
async def list_distributors_for_wallet(wallet_address: str):
    try:
        return get_distributors_for_wallet(wallet_address)

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error listing distributors: {str(e)}"
        )

# Gets the list of supported projects
@app.get("/projects")
async def list_supported_projects():
    try:
        return get_supported_projects()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error listing supported projects: {str(e)}"
        )


# Starts the server
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
