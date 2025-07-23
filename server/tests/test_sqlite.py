import pytest
import sys
import json
import time
from pathlib import Path
from dotenv import load_dotenv

# Add the server directory to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
SERVER_DIR = PROJECT_ROOT / "server"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SERVER_DIR))

load_dotenv()

# Import your real classes
from db.SQLite.db import SQLiteDB
from .mock_data import TRANSACTIONS, PROJECT, WALLET, KNOWN_TOKENS


class TestSQLite:
    def setup_method(self):
        self.sqlite = SQLiteDB(True, True)


    ##########################################################
    #                 Supported Projects Tests               #
    ##########################################################
    def test_sqlite_inserts_supported_project(self):
        # Insert a project into the temp db
        result = self.sqlite.insert_supported_project(PROJECT)
        assert result is True

        result = self.sqlite.get_supported_project(PROJECT["distributor"])
        result.get("token_mint") == PROJECT["token_mint"]
        print(f"✅ SQLite inserts a supported project!")

    def test_sqlite_does_not_create_duplicate_supported_projects(self):
        # Try to add the same project which shouldn't be allowed
        result = self.sqlite.insert_supported_project(PROJECT)
        assert result is True

        # Get the count
        result = self.sqlite.get_supported_project_count()
        assert result is 1
        print(f"✅ SQLite doesn't create duplicate supported projects!")

    def test_sqlite_updates_supported_project_last_sig_field(self):
        result = self.sqlite.get_last_tx_signature(PROJECT["distributor"])
        assert result is None

        result = self.sqlite.update_last_tx_signature(PROJECT["distributor"], "new_last_sig")
        assert result is True

        result = self.sqlite.get_last_tx_signature(PROJECT["distributor"])
        assert result == "new_last_sig"
        print(f"✅ SQLite updates the last sig field of a supported projects!")

    ##########################################################
    #                    Known Tokens Tests                  #
    ##########################################################
    def test_sqlite_inserts_known_token(self):
        result = self.sqlite.insert_known_token(KNOWN_TOKENS)
        assert result is True

        result = self.sqlite.get_known_token(KNOWN_TOKENS["mint"])
        assert result.get("mint") == KNOWN_TOKENS["mint"]
        print(f"✅ SQLite inserts known tokens!")

    def test_sqlite_does_not_create_duplicate_known_tokens(self):
        # Try to add the same project which shouldn't be allowed
        result = self.sqlite.insert_known_token(KNOWN_TOKENS)
        assert result is True

        # Get the count
        result = self.sqlite.get_known_tokens_count()
        assert result is 1
        print(f"✅ SQLite doesn't create duplicate known tokens!")

    ##########################################################
    #                   Transactions Tests                   #
    ##########################################################
    def test_sqlite_inserts_transaction_batch(self):
        distributor = "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE"

        self.sqlite.create_distributor_tables(distributor)

        result = self.sqlite.insert_transactions_batch(distributor, TRANSACTIONS)
        assert result is True

        for txs, offset in self.sqlite.get_transactions(distributor, 0):
            assert txs[0]["fee_payer"] == TRANSACTIONS[0]["feePayer"]

        print(f"✅ SQLite inserts temp transaction!")
