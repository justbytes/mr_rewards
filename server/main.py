import os
import uvicorn
from pathlib import Path
from fastapi import FastAPI
from contextlib import asynccontextmanager
from routes import system_config, wallet_rewards
from routes.models import RootResponse
from lib.Controller import Controller
from apps.TelegramBot import TelegramBot
from routes.dependency import set_controller, remove_controller
from utils.rate_limiter import cleanup_rate_limiter
from dotenv import load_dotenv
load_dotenv()

async def periodic_cleanup():
    """Cleanup rate limiter memory every 10 minutes"""
    while True:
        await asyncio.sleep(600)  # 10 minutes
        cleanup_rate_limiter()

# Initialize the connection to the MongoDB and asign it the global variable
def initialize_program():
    """Initialize the global database connection"""
    try:
        # Get and instance of the Controller which is used to read from DB
        controller = Controller()
        cleanup_task = asyncio.create_task(periodic_cleanup())
        # TODO have the controller poll every 5 minutes for new transactions to insert

        # Add controller to dependencies
        set_controller(controller)
    except:
        raise Exception("An error has occured when initializing the contoller")

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):

    print("Starting up the API...")

    # Initialize the controller and telegram bot
    initialize_program()


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
            "wallet_rewards": "/rewards/{wallet_address}",
            "docs": "/docs",
        },
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
    try:
        telegram_bot = TelegramBot()
    except Exception as e:
        print(f"Error with tele bot {e}")
    print("Controller connected successfully")
