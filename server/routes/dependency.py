from fastapi import HTTPException, Depends
from lib.Controller import Controller

# Global controller instance
_controller_instance = None

def set_controller(controller: Controller):
    """Set the global controller instance"""
    global _controller_instance
    _controller_instance = controller

def get_controller() -> Controller:
    """Dependency to get the controller instance"""
    if _controller_instance is None:
        raise HTTPException(status_code=500, detail="Controller connection not available")
    return _controller_instance

def remove_controller():
    """Dependency to remove the controller instance"""
    global _controller_instance
    _controller_instance = None