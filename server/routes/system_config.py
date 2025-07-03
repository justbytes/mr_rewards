
from fastapi import APIRouter, HTTPException, Depends
from typing import List
from lib.Controller import Controller
from .dependency import get_controller
from .models import HealthResponse, SupportedProject

router = APIRouter()

@router.get("/supported_projects", response_model=List[SupportedProject])
async def get_supported_projects(controller: Controller = Depends(get_controller)):
    """Gets the list of supported projects"""
    try:
        return controller.get_supported_projects_from_db()
    except:
        raise HTTPException(
            status_code=500, detail=f"Error getting supported projects"
        )

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Server helper function that checks if everything is working"""
    return {"status": "healthy", "message": "API is running"}
