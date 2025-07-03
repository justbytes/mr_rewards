import os
import uvicorn
from pathlib import Path
from fastapi import FastAPI
from contextlib import asynccontextmanager
from routes import system_config, wallet_rewards
from routes.models import RootResponse
from lib.Controller import Controller
from routes.dependency import set_controller, remove_controller
from dotenv import load_dotenv
load_dotenv()

# Initialize the connection to the MongoDB and asign it the global variable
def initialize_controller():
    """Initialize the global database connection"""
    try:
        # Get and instance of the Controller which is used to read from DB
        controller = Controller()

        # TODO have the controller poll every 5 minutes for new transactions to insert

        # Add controller to dependencies
        set_controller(controller)
    except:
        raise Exception("An error has occured when initializing the contoller")


# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):

    print("Starting up the API...")

    # Initialize the db connection
    initialize_controller()
    print("Controller connected successfully")

    yield
    print("Shutting down the API...")
    remove_controller()




# Initialize the app
app = FastAPI(
    title="Mr. Rewards | Solana Rewards Token Tracker",
    description="API to retrieve aggregated rewards received from distribution wallets",
    version="1.0.0",
    lifespan=lifespan
)


# Add the routes to the app
app.include_router(wallet_rewards.router, prefix="/rewards", tags=["rewards"])
app.include_router(system_config.router, tags=["system"])


@app.get("/", response_model=RootResponse)
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Wallet Rewards API",
        "version": "1.0.0",
        "endpoints": {
            "status": "/health",
            "supported_projects": "/supported_projects",
            "wallet_rewards": "/wallets/rewards/{wallet_address}",
            "docs": "/docs",
        },
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
