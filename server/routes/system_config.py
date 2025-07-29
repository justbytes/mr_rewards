# server/routes/system_config.py
from fastapi import APIRouter, HTTPException, Depends, Request
from typing import List
from lib.Controller import Controller
from .dependency import get_controller
from .models import HealthResponse, SupportedProject
from .auth import api_key_auth
from limiter import limiter

# Initialize the router
router = APIRouter()

@router.get("/supported_projects", response_model=List[SupportedProject])
@limiter.limit("10/minute")
async def get_supported_projects(
    request: Request,
    controller: Controller = Depends(get_controller),
    api_key: str = Depends(api_key_auth)  # Requires Bearer token
):
    """Gets the list of supported projects (requires Bearer token authentication)"""
    try:
        # Access SQLite database through controller
        projects = controller.sqlite.get_supported_projects()

        # Convert to match your Pydantic model
        formatted_projects = []
        for project in projects:
            formatted_project = SupportedProject(
                _id=str(project.get("distributor", "")),  # Use distributor as _id
                name=project.get("name", ""),
                distributor=project.get("distributor", ""),
                token_mint=project.get("token_mint", ""),
                dev_wallet=project.get("dev_wallet"),
                last_sig=project.get("last_sig")
            )
            formatted_projects.append(formatted_project)

        return formatted_projects
    except Exception as e:
        print(f"Error in get_supported_projects: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error getting supported projects"
        )

@router.get("/health", response_model=HealthResponse)
@limiter.limit("15/minute")
async def health_check(request: Request):
    """Server health check (no authentication required)"""
    return {"status": "healthy", "message": "API is running"}