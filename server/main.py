import os
import uvicorn
from pathlib import Path
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from routes import system_config, wallet_rewards
from routes.models import RootResponse
from lib.Controller import Controller
from limiter import limiter
from routes.dependency import set_controller, remove_controller
from dotenv import load_dotenv
load_dotenv()

# Initialize the connection to the MongoDB and asign it the global variable
def initialize_program():
    """Initialize the global database connection"""
    try:
        # Get and instance of the Controller which is used to read from DB
        controller = Controller()
        controller.begin_polling()

        # Add controller to dependencies
        set_controller(controller)
    except Exception as e:
        raise Exception(f"An error has occured when initializing the contoller {e}")

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up the API...")

    # Initialize the controller and telegram bot
    initialize_program()

    yield
    print("Shutting down the API...")

    # Unset the dependency variable
    remove_controller()

# Initialize the app
app = FastAPI(
    title="Mr. Rewards | Solana Rewards Token Tracker",
    description="API to retrieve aggregated rewards received from rewards token projects",
    version="1.0.0",
    lifespan=lifespan
)

# Setup the Redis rate limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# Add the routes to the app
app.include_router(wallet_rewards.router, prefix="/rewards", tags=["rewards"])
app.include_router(system_config.router, tags=["system"])

@app.get("/", response_model=RootResponse)
@limiter.limit("30/minute")
async def root(request: Request):
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
