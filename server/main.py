import uvicorn
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
import asyncio

from routes import system_config, wallet_rewards
from routes.models import RootResponse
from lib.Controller import Controller
from lib.ProjectUpdater import ProjectUpdater
from limiter import limiter
from routes.dependency import set_controller, remove_controller

# Global variables
project_updater = None
update_task = None

async def periodic_update():
    """Simple periodic update that runs in the main event loop"""
    while True:
        try:
            await asyncio.sleep(25)  # Wait 5 minutes (300 seconds)

            if project_updater and not project_updater.updating:
                print("Running scheduled database update...")
                # Call the sync method directly (no threads!)
                project_updater.update_distributors_transactions()

        except Exception as e:
            print(f"Error in periodic update: {e}")
            # Continue the loop even if there's an error

def initialize_program():
    """Initialize the global database connection"""
    global project_updater

    try:
        # Get an instance of the Controller
        controller = Controller(False, False)
        project_updater = ProjectUpdater(controller)

        # Add controller to dependencies
        set_controller(controller)

        print("ProjectUpdater initialized - periodic updates will start after server startup")

    except Exception as e:
        raise Exception(f"An error has occurred when initializing the controller {e}")

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    global update_task

    print("Starting up the API...")

    # Initialize the controller
    initialize_program()

    # Start the periodic update task
    update_task = asyncio.create_task(periodic_update())
    print("Periodic update task started - will update every 5 minutes")

    yield

    print("Shutting down the API...")

    # Cancel the update task
    if update_task:
        update_task.cancel()
        try:
            await update_task
        except asyncio.CancelledError:
            print("Update task cancelled")

    # Unset the dependency variable
    remove_controller()

# Initialize the app
app = FastAPI(
    title="Mr. Rewards | Solana Rewards Token Tracker",
    description="API to retrieve aggregated rewards received from rewards token projects. Authentication required for most endpoints.",
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
            "status": "/health (public)",
            "supported_projects": "/supported_projects (requires Bearer token)",
            "wallet_rewards": "/rewards/{wallet_address} (requires Bearer token)",
            "docs": "/docs (public)",
        },
    }

# Optional: Manual trigger endpoint for testing
@app.post("/admin/trigger-update")
async def trigger_update():
    """Manually trigger a database update (for testing)"""
    if project_updater and not project_updater.updating:
        project_updater.update_distributors_transactions()
        return {"message": "Update triggered successfully"}
    else:
        return {"message": "Update already in progress or updater not available"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)