from pydantic import BaseModel
from typing import List, Dict, Any, Optional

# A model for how the total_amounts data
class RewardAmount(BaseModel):
    token: str
    total_amount: float


# A model for the response of the get_total_rewards function
class RewardsResponse(BaseModel):
    total_amounts: List[RewardAmount]


# Model for the health route response
class HealthResponse(BaseModel):
    status: str
    message: str

# Model for the wallets route response
class WalletsResponse(BaseModel):
    wallets: List[str]
    count: int

# Model for the distributors route
class DistributorsResponse(BaseModel):
    wallet: str
    distributors: List[str]
    count: int

# Model for the root response
class RootResponse(BaseModel):
    message: str
    version: str
    endpoints: Dict[str, str]
