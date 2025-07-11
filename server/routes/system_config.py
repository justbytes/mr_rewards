
from fastapi import APIRouter, HTTPException, Depends, Request
from typing import List
from lib.Controller import Controller
from .dependency import get_controller
from .models import HealthResponse, SupportedProject
from limiter import limiter

# Initialize the router
router = APIRouter()

@router.get("/supported_projects", response_model=List[SupportedProject])
@limiter.limit("10/minute")
async def get_supported_projects(request: Request, controller: Controller = Depends(get_controller)):
    """Gets the list of supported projects"""
    try:
        return controller.get_supported_projects_from_db()
    except:
        raise HTTPException(
            status_code=500, detail=f"Error getting supported projects"
        )

@router.get("/health", response_model=HealthResponse)
@limiter.limit("15/minute")
async def health_check(request: Request):
    """Server helper function that checks if everything is working"""
    return {"status": "healthy", "message": "API is running"}
