
import sys
import os
import json
import uvicorn
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
from utils.token_aggregator import get_total_rewards
from utils.models import *
from lib.Controller import Controller



"""
This file contains a basic server using the FastAPI library and includes 4 routes:
/health, /rewards/{wallet}/{distributor}, /wallets/, /wallets/{wallet_address}/distributors
"""


# Global MongoDB instance
controller = None

# Initialize the connection to the MongoDB and asign it the global variable
def initialize_controller():
    """Initialize the global database connection"""
    global controller
    try:
        controller = Controller()
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize the db connection
    print("Starting up the API...")
    if initialize_controller():
        print("Database connected successfully")
    else:
        print("Warning: Database connection failed")

    # Initialize Telegram bot
    # TODO initalize telegram bot here
    yield

    print("Shutting down the API...")
    # Optionally close the database connection here
    global controller
    if controller:
        controller = None


# Initialize the app
app = FastAPI(
    title="Mr. Rewards | Solana Rewards Token Tracker",
    description="API to retrieve aggregated rewards received from distribution wallets",
    version="1.0.0",
    lifespan=lifespan
)

# API Routes
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Wallet Rewards API",
        "version": "1.0.0",
        "endpoints": {
            "status": "/health",
            "get_rewards": "/rewards/{wallet_address}/{distributor_address}",
            "wallets": "/wallets",
            "wallets_distributors": "/wallets/{wallet_address}/distributors",
            "supported_projects": "/supported_projects",
            "known_tokens": "/known_tokens",
            "docs": "/docs",
        },
    }

"""""
Route for getting total rewards for a specific wallet from a specific distributor
"""""
@app.get("/rewards/{wallet_address}/{distributor}", response_model=RewardsResponse)
async def get_wallet_rewards(wallet_address: str, distributor: str):
    try:
        # Get all of the transfers for the wallet_address and distributor
        transfers = controller.get_rewards_with_wallet_address_and_distributor_from_db(wallet_address, distributor)

        # Aggregate the rewards
        result = get_total_rewards(transfers)

        # Put the results into the rewards response
        return RewardsResponse(**result)

    except Exception as e:
        # Handle any unexpected exceptions
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/wallets/{wallet_address}/distributors", response_model=DistributorsResponse)
async def list_distributors_for_wallet(wallet_address: str):
    """This route gets a list of distributors for a given wallet"""
    try:
        return controller.get_wallets_distributors_from_db(wallet_address)

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error listing distributors: {str(e)}"
        )

@app.get("/wallets", response_model=WalletsResponse)
async def list_wallets():
    """A route that lists available wallets / wallets that have received rewards"""
    try:
        return controller.get_all_wallets_from_db()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing wallets: {str(e)}")

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Server helper function that checks if everything is working"""
    return {"status": "healthy", "message": "API is running"}

@app.get("/supported_projects")
async def get_supported_projects():
    """Gets the list of supported projects"""
    try:
        return controller.get_supported_projects_from_db()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error listing supported projects: {str(e)}"
        )

@app.get("/known_tokens")
async def get_known_tokens():
    """Gets the list of supported projects"""
    try:
        return controller.get_known_tokens_from_db()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error getting known tokens: {str(e)}"
        )

# Starts the server
if __name__ == "__main__":
    # Start the FastAPI
    uvicorn.run(app, host="0.0.0.0", port=8000)