# server/db/SQLite/schemas.py
temp_transactions = """
CREATE TABLE IF NOT EXISTS temp_transactions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fee_payer TEXT,
    signature TEXT,
    slot INTEGER,
    timestamp INTEGER,
    token_transfers TEXT,
    native_transfers TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

temp_txs_last_sigs = """
CREATE TABLE IF NOT EXISTS temp_txs_last_sigs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    before TEXT,
    last_sig TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

transfers = """
CREATE TABLE IF NOT EXISTS transfers(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signature TEXT,
    slot INTEGER,
    timestamp INTEGER,
    amount REAL,
    token TEXT,
    wallet_address TEXT,
    distributor TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

wallets = """
CREATE TABLE IF NOT EXISTS wallets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT NOT NULL,
    distributors TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

supported_projects = """
CREATE TABLE IF NOT EXISTS supported_projects(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    distributor TEXT,
    token_mint TEXT,
    dev_wallet TEXT,
    last_sig TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""
known_tokens = """
CREATE TABLE IF NOT EXISTS known_tokens(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    name TEXT,
    mint TEXT,
    decimals TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

api_keys = """
CREATE TABLE IF NOT EXISTS api_keys(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT UNIQUE NOT NULL,
    name TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_used TEXT,
    usage_count INTEGER DEFAULT 0,
    rate_limit INTEGER
)
"""

api_usage_logs = """
CREATE TABLE IF NOT EXISTS api_usage_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    api_key TEXT NOT NULL,
    endpoint TEXT,
    method TEXT,
    user_agent TEXT,
    ip_address TEXT,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (api_key) REFERENCES api_keys(key)
)
"""
