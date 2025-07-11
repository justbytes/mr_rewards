from fastapi import HTTPException, Depends
from lib.Controller import Controller

# Global controller instance
_controller_instance = None
_limiter_instance = None

# Set the controllers global variable
def set_controller(controller: Controller):
    """Set the global controller instance"""
    global _controller_instance
    _controller_instance = controller

# Check if we have a controller instance
def get_controller() -> Controller:
    """Dependency to get the controller instance"""
    if _controller_instance is None:
        raise HTTPException(status_code=500, detail="Controller connection not available")
    return _controller_instance

# Remove the conroller instance variable
def remove_controller():
    """Dependency to remove the controller instance"""
    global _controller_instance
    _controller_instance = None