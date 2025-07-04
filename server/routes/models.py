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

# Model for the supported project document
class SupportedProject(BaseModel):
    _id: str
    name: str
    distributor: str
    token_mint: str
    dev_wallet: Optional[str] = None
    last_sig: Optional[str] = None

# Goes inside a distributors tokens model
class TokenAmount(BaseModel):
    total_amount: float

# A model for a tokens and the total amount goes inside the WalletRewardsResponse
class DistributorTokens(BaseModel):
    tokens: Dict[str, TokenAmount]

# A model for a document in the wallets collection
class WalletsRewardsResponse(BaseModel):
    _id: str
    wallet_address: str
    distributors: Dict[str, DistributorTokens]

# A model for the a document inside the transfers collection
class WalletTransfer(BaseModel):
    _id: str
    signature: str
    slot: int
    timestamp: int
    amount: float
    token: str
    wallet_address: str
    distributor: str
