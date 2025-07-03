from pydantic import BaseModel
from typing import List, Dict, Optional

# Model for the root response
class RootResponse(BaseModel):
    message: str
    version: str
    endpoints: Dict[str, str]

# Model for the health route response
class HealthResponse(BaseModel):
    status: str
    message: str

class SupportedProject(BaseModel):
    _id: str
    name: str
    distributor: str
    token_mint: str
    dev_wallet: Optional[str] = None
    last_sig: Optional[str] = None

class TokenAmount(BaseModel):
    total_amount: float

class DistributorTokens(BaseModel):
    tokens: Dict[str, TokenAmount]

class WalletsRewardsResponse(BaseModel):
    _id: str
    wallet_address: str
    distributors: Dict[str, DistributorTokens]

class WalletTransfer(BaseModel):
    _id: str
    signature: str
    slot: int
    timestamp: int
    amount: float
    token: str
    wallet_address: str
    distributor: str
