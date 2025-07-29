from fastapi import HTTPException, Security, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from lib.Controller import Controller
from .dependency import get_controller
from typing import Optional

# Security scheme for Bearer token
security = HTTPBearer()

class APIKeyAuth:
    def __init__(self):
        self.security = HTTPBearer()

    async def __call__(self,
                       request: Request,
                       credentials: HTTPAuthorizationCredentials = Security(security),
                       controller: Controller = Depends(get_controller)) -> str:
        """
        Validates API key from Authorization header using Bearer token
        Expected format: Authorization: Bearer <api_key>
        """
        if not credentials:
            raise HTTPException(
                status_code=401,
                detail="Authorization header required",
                headers={"WWW-Authenticate": "Bearer"}
            )

        api_key = credentials.credentials

        # Validate API key format (adjust based on your key format)
        if len(api_key) < 32:
            raise HTTPException(
                status_code=401,
                detail="Invalid API key format",
                headers={"WWW-Authenticate": "Bearer"}
            )

        # Check if API key exists in database
        try:
            is_valid = self.validate_api_key(controller, api_key)
            if not is_valid:
                raise HTTPException(
                    status_code=401,
                    detail="Invalid or inactive API key",
                    headers={"WWW-Authenticate": "Bearer"}
                )

            # Log API key usage for analytics/monitoring
            await self.log_api_key_usage(controller, api_key, request)

            return api_key

        except HTTPException:
            # Re-raise HTTP exceptions (401, etc.)
            raise
        except Exception as e:
            # Log the actual error for debugging but don't expose it
            print(f"API key validation error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Error validating API key"
            )

    def validate_api_key(self, controller: Controller, api_key: str) -> bool:
        """
        Validate API key against SQLite database

        Args:
            controller: Database controller instance
            api_key: The API key to validate

        Returns:
            bool: True if valid and active, False otherwise
        """
        # Access SQLite database through controller
        return controller.sqlite.validate_api_key(api_key)

    async def log_api_key_usage(self, controller: Controller, api_key: str, request: Request):
        """
        Log API key usage for analytics and monitoring

        Args:
            controller: Database controller instance
            api_key: The API key that was used
            request: The FastAPI request object
        """
        try:
            # Extract useful info for logging
            endpoint = request.url.path
            method = request.method
            user_agent = request.headers.get("user-agent", "Unknown")
            ip_address = request.client.host if request.client else "Unknown"

            controller.sqlite.log_api_key_usage(
                api_key=api_key,
                endpoint=endpoint,
                method=method,
                user_agent=user_agent,
                ip_address=ip_address
            )
        except Exception as e:
            # Don't fail the request if logging fails
            print(f"Failed to log API key usage: {str(e)}")

# Create global instance
api_key_auth = APIKeyAuth()