import pytest
import sys
import json
import time
import shutil
import os
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
        """Setup method run before each test"""
        # Clean up test data BEFORE creating SQLiteDB instance
        self.cleanup_test_data()

        # Ensure test directories exist with correct permissions
        self.ensure_test_directories()

        # Now create the SQLiteDB instance
        self.sqlite = SQLiteDB(True, True)

    def teardown_method(self):
        """Cleanup method run after each test"""
        # Close connections before cleanup
        try:
            self.sqlite.close_connections()
        except:
            pass

        # Clean up test data after each test
        self.cleanup_test_data()

    def ensure_test_directories(self):
        """Ensure test directories exist with correct permissions"""
        try:
            test_backup_path = Path("test_backup")
            transfers_path = test_backup_path / "transfers"

            # Create directories if they don't exist
            test_backup_path.mkdir(exist_ok=True)
            transfers_path.mkdir(exist_ok=True)

            # Set correct permissions (readable and writable)
            os.chmod(test_backup_path, 0o755)
            os.chmod(transfers_path, 0o755)

            print("Test directories created with correct permissions")

        except Exception as e:
            print(f"Error creating test directories: {e}")

    def cleanup_test_data(self):
        """Remove all test data from test_backup directory but keep transfers directory structure"""
        try:
            test_backup_path = Path("test_backup")

            if test_backup_path.exists():
                # Get all items in test_backup directory
                for item in test_backup_path.iterdir():
                    # Skip the transfers directory but clean its contents
                    if item.is_dir() and item.name == "transfers":
                        # Clean contents of transfers directory but keep the directory
                        for transfer_file in item.iterdir():
                            if transfer_file.is_file():
                                transfer_file.unlink()
                                print(f"Removed transfer file: {transfer_file}")
                        continue

                    # Remove files and other directories
                    if item.is_file():
                        item.unlink()
                        print(f"Removed file: {item}")
                    elif item.is_dir():
                        shutil.rmtree(item)
                        print(f"Removed directory: {item}")

                print("SQLite test data cleaned up (transfers directory preserved)")
            else:
                print("test_backup directory does not exist, nothing to clean")

        except Exception as e:
            print(f"Error cleaning up SQLite test data: {e}")

    ##########################################################
    #                 Supported Projects Tests               #
    ##########################################################
    def test_sqlite_inserts_supported_project(self):
        # Insert a project into the temp db
        result = self.sqlite.insert_supported_project(PROJECT)
        assert result is True

        result = self.sqlite.get_supported_project(PROJECT["distributor"])
        assert result.get("token_mint") == PROJECT["token_mint"]
        print(f"✅ SQLite inserts a supported project!")

    def test_sqlite_does_not_create_duplicate_supported_projects(self):
        # Try to add the same project which shouldn't be allowed
        result = self.sqlite.insert_supported_project(PROJECT)
        assert result is True

        # Insert again - should be ignored due to INSERT OR IGNORE
        result = self.sqlite.insert_supported_project(PROJECT)
        assert result is True

        # Get the count
        result = self.sqlite.get_supported_project_count()
        assert result == 1
        print(f"✅ SQLite doesn't create duplicate supported projects!")

    def test_sqlite_updates_supported_project_last_sig_field(self):
        # First insert a project
        result = self.sqlite.insert_supported_project(PROJECT)
        assert result is True

        result = self.sqlite.get_last_tx_signature(PROJECT["distributor"])
        assert result is None

        result = self.sqlite.update_last_tx_signature(PROJECT["distributor"], "new_last_sig")
        assert result is True

        result = self.sqlite.get_last_tx_signature(PROJECT["distributor"])
        assert result == "new_last_sig"
        print(f"✅ SQLite updates the last sig field of a supported projects!")

    def test_sqlite_gets_all_supported_projects(self):
        # Insert a project
        result = self.sqlite.insert_supported_project(PROJECT)
        assert result is True

        # Get all projects
        projects = self.sqlite.get_supported_projects()
        assert len(projects) >= 1
        assert projects[0]["distributor"] == PROJECT["distributor"]
        print(f"✅ SQLite gets all supported projects!")

    def test_sqlite_upserts_supported_project(self):
        # First upsert (should insert)
        result = self.sqlite.upsert_supported_project(PROJECT)
        assert result is True

        # Second upsert with updated data (should update)
        updated_project = PROJECT.copy()
        updated_project["last_sig"] = "updated_signature"
        result = self.sqlite.upsert_supported_project(updated_project)
        assert result is True

        # Verify the update
        project = self.sqlite.get_supported_project(PROJECT["distributor"])
        assert project["last_sig"] == "updated_signature"
        print(f"✅ SQLite upserts supported projects!")

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
        # Try to add the same token which shouldn't be allowed
        result = self.sqlite.insert_known_token(KNOWN_TOKENS)
        assert result is True

        # Insert again - should be skipped
        result = self.sqlite.insert_known_token(KNOWN_TOKENS)
        assert result is True

        # Get the count
        result = self.sqlite.get_known_tokens_count()
        assert result == 1
        print(f"✅ SQLite doesn't create duplicate known tokens!")

    def test_sqlite_gets_all_known_tokens(self):
        # Insert a token
        result = self.sqlite.insert_known_token(KNOWN_TOKENS)
        assert result is True

        # Get all tokens
        tokens = self.sqlite.get_known_tokens()
        assert len(tokens) >= 1
        assert tokens[0]["mint"] == KNOWN_TOKENS["mint"]
        print(f"✅ SQLite gets all known tokens!")

    def test_sqlite_gets_known_token_by_mint(self):
        # Insert a token
        result = self.sqlite.insert_known_token(KNOWN_TOKENS)
        assert result is True

        # Get specific token
        token = self.sqlite.get_known_token(KNOWN_TOKENS["mint"])
        assert token is not None
        assert token["symbol"] == KNOWN_TOKENS["symbol"]
        assert token["name"] == KNOWN_TOKENS["name"]
        print(f"✅ SQLite gets known token by mint!")

    def test_sqlite_returns_none_for_nonexistent_known_token(self):
        result = self.sqlite.get_known_token("nonexistent_mint")
        assert result is None
        print(f"✅ SQLite returns None for nonexistent known token!")

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

    def test_sqlite_gets_transactions_count(self):
        distributor = "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE"

        self.sqlite.create_distributor_tables(distributor)

        # Get initial count
        current_count = self.sqlite.get_transactions_count(distributor)

        # Insert transaction
        result = self.sqlite.insert_transactions_batch(distributor, TRANSACTIONS)
        assert result is True

        # Get count after insertion
        count = self.sqlite.get_transactions_count(distributor)
        assert count == current_count + 1
        print(f"✅ SQLite gets transactions count!")

    ##########################################################
    #                   Transfer Tests                       #
    ##########################################################
    def test_sqlite_inserts_transfer_batch(self):
        distributor = "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE"
        transfers = [
            {
                "signature": "test_sig_1",
                "slot": 100,
                "timestamp": 1234567890,
                "amount": 1000.0,
                "token": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "wallet_address": "wallet1",
                "distributor": distributor
            },
            {
                "signature": "test_sig_2",
                "slot": 101,
                "timestamp": 1234567891,
                "amount": 2000.0,
                "token": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "wallet_address": "wallet2",
                "distributor": distributor
            }
        ]

        self.sqlite.create_distributor_tables(distributor)
        result = self.sqlite.insert_transfer_batch(distributor, transfers)
        assert result is True

        # Verify transfers were inserted
        count = self.sqlite.get_transfers_count(distributor)
        assert count == len(transfers)
        print(f"✅ SQLite inserts transfer batch!")

    def test_sqlite_gets_transfers_count(self):
        distributor = "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE"

        self.sqlite.create_distributor_tables(distributor)

        # Initial count should be 0
        current_count = self.sqlite.get_transfers_count(distributor)

        transfers = [
            {
                "signature": "test_sig_1",
                "slot": 100,
                "timestamp": 1234567890,
                "amount": 1000.0,
                "token": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "wallet_address": "wallet1",
                "distributor": distributor
            }
        ]

        result = self.sqlite.insert_transfer_batch(distributor, transfers)
        assert result is True

        count = self.sqlite.get_transfers_count(distributor)
        assert count == current_count + 1
        print(f"✅ SQLite gets transfers count!")

    ##########################################################
    #                   Temp Transfer Tests                  #
    ##########################################################
    def test_sqlite_inserts_temp_transfers_batch(self):
        transfers = [
            {
                "signature": "temp_sig_1",
                "slot": 100,
                "timestamp": 1234567890,
                "amount": 1000.0,
                "token": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "wallet_address": "wallet1",
                "distributor": "distributor1"
            },
            {
                "signature": "temp_sig_2",
                "slot": 101,
                "timestamp": 1234567891,
                "amount": 2000.0,
                "token": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "wallet_address": "wallet2",
                "distributor": "distributor2"
            }
        ]

        result = self.sqlite.insert_temp_transfers_batch(transfers)
        assert result is True

        count = self.sqlite.get_temp_transfers_count()
        assert count == len(transfers)
        print(f"✅ SQLite inserts temp transfers batch!")

    ##########################################################
    #                     Wallet Tests                       #
    ##########################################################
    def test_sqlite_inserts_wallets_batch(self):
        wallets = [
            {
                "wallet_address": "wallet1",
                "distributors": {"dist1": {"amount": 1000, "token": "USDC"}}
            },
            {
                "wallet_address": "wallet2",
                "distributors": {"dist2": {"amount": 2000, "token": "SOL"}}
            }
        ]

        result = self.sqlite.insert_wallets_batch(wallets)
        assert result is True

        count = self.sqlite.get_wallets_count()
        assert count == len(wallets)
        print(f"✅ SQLite inserts wallets batch!")

    def test_sqlite_gets_wallet_by_address(self):
        wallet_data = {
            "wallet_address": "test_wallet_123",
            "distributors": {"dist1": {"amount": 1000, "token": "USDC"}}
        }

        result = self.sqlite.insert_wallets_batch([wallet_data])
        assert result is True

        wallet = self.sqlite.get_wallet("test_wallet_123")
        assert wallet is not None
        assert wallet["wallet_address"] == "test_wallet_123"
        assert "dist1" in wallet["distributors"]
        print(f"✅ SQLite gets wallet by address!")

    def test_sqlite_gets_wallets_by_addresses(self):
        wallets = [
            {
                "wallet_address": "batch_wallet1",
                "distributors": {"dist1": {"amount": 1000, "token": "USDC"}}
            },
            {
                "wallet_address": "batch_wallet2",
                "distributors": {"dist2": {"amount": 2000, "token": "SOL"}}
            }
        ]

        result = self.sqlite.insert_wallets_batch(wallets)
        assert result is True

        addresses = ["batch_wallet1", "batch_wallet2", "nonexistent_wallet"]
        wallets_dict = self.sqlite.get_wallets_by_addresses(addresses)

        assert len(wallets_dict) == 2  # Only existing wallets should be returned
        assert "batch_wallet1" in wallets_dict
        assert "batch_wallet2" in wallets_dict
        assert "nonexistent_wallet" not in wallets_dict
        print(f"✅ SQLite gets wallets by addresses!")

    def test_sqlite_updates_wallets_batch(self):
        # Insert initial wallet
        wallet_data = {
            "wallet_address": "update_wallet",
            "distributors": {"dist1": {"amount": 1000, "token": "USDC"}}
        }

        result = self.sqlite.insert_wallets_batch([wallet_data])
        assert result is True

        # Update wallet
        updated_wallet = {
            "wallet_address": "update_wallet",
            "distributors": {"dist1": {"amount": 2000, "token": "USDC"}, "dist2": {"amount": 500, "token": "SOL"}}
        }

        result = self.sqlite.update_wallets_batch([updated_wallet])
        assert result is True

        # Verify update
        wallet = self.sqlite.get_wallet("update_wallet")
        assert wallet["distributors"]["dist1"]["amount"] == 2000
        assert "dist2" in wallet["distributors"]
        print(f"✅ SQLite updates wallets batch!")

    def test_sqlite_gets_all_wallets(self):
        wallets = [
            {
                "wallet_address": "all_wallet1",
                "distributors": {"dist1": {"amount": 1000, "token": "USDC"}}
            },
            {
                "wallet_address": "all_wallet2",
                "distributors": {"dist2": {"amount": 2000, "token": "SOL"}}
            }
        ]

        result = self.sqlite.insert_wallets_batch(wallets)
        assert result is True

        all_wallets = self.sqlite.get_all_wallets()
        assert len(all_wallets) >= len(wallets)

        # Check that our test wallets are in the results
        wallet_addresses = [w["wallet_address"] for w in all_wallets]
        assert "all_wallet1" in wallet_addresses
        assert "all_wallet2" in wallet_addresses
        print(f"✅ SQLite gets all wallets!")

    ##########################################################
    #                Last Signature Tests                    #
    ##########################################################
    def test_sqlite_updates_temp_txs_before_sig(self):
        distributor = "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE"

        self.sqlite.create_distributor_tables(distributor)

        # Initially should return None, None
        before, last = self.sqlite.get_temp_txs_last_sigs(distributor)
        assert before is None
        assert last is None

        # Update before signature
        result = self.sqlite.update_temp_txs_before_sig(distributor, "before_sig_123")
        assert result is True

        # Verify update
        before, last = self.sqlite.get_temp_txs_last_sigs(distributor)
        assert before == "before_sig_123"
        print(f"✅ SQLite updates temp txs before signature!")

    def test_sqlite_updates_temp_txs_last_sig(self):
        distributor = "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE"

        self.sqlite.create_distributor_tables(distributor)

        # Update last signature
        result = self.sqlite.update_temp_txs_last_sig(distributor, "last_sig_456")
        assert result is True

        # Verify update
        before, last = self.sqlite.get_temp_txs_last_sigs(distributor)
        assert last == "last_sig_456"
        print(f"✅ SQLite updates temp txs last signature!")

    def test_sqlite_gets_temp_txs_last_sigs(self):
        distributor = "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE"

        self.sqlite.create_distributor_tables(distributor)

        # Set both signatures
        self.sqlite.update_temp_txs_before_sig(distributor, "before_sig_789")
        self.sqlite.update_temp_txs_last_sig(distributor, "last_sig_999")

        # Get both signatures
        before, last = self.sqlite.get_temp_txs_last_sigs(distributor)
        assert before == "before_sig_789"
        assert last == "last_sig_999"
        print(f"✅ SQLite gets temp txs last signatures!")

    ##########################################################
    #                  Table Management Tests                #
    ##########################################################
    def test_sqlite_creates_distributor_tables(self):
        distributor = "new_distributor_123"

        # Table should not exist initially
        exists = self.sqlite.table_exists(distributor, "transfers")
        assert exists is False

        # Create tables
        self.sqlite.create_distributor_tables(distributor)

        # Table should now exist
        exists = self.sqlite.table_exists(distributor, "transfers")
        assert exists is True
        print(f"✅ SQLite creates distributor tables!")

    def test_sqlite_creates_distributor_indexes(self):
        distributor = "index_distributor_456"

        self.sqlite.create_distributor_tables(distributor)

        # Insert some test data first
        transfers = [
            {
                "signature": "index_sig_1",
                "slot": 100,
                "timestamp": 1234567890,
                "amount": 1000.0,
                "token": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "wallet_address": "index_wallet1",
                "distributor": distributor
            }
        ]
        self.sqlite.insert_transfer_batch(distributor, transfers)

        # Create indexes
        result = self.sqlite.create_distributor_indexes(distributor)
        assert result is True
        print(f"✅ SQLite creates distributor indexes!")

    def test_sqlite_drops_temp_tables(self):
        distributor = "temp_table_distributor"

        self.sqlite.create_distributor_tables(distributor)

        # Verify temp tables exist
        connection, cursor = self.sqlite.get_distributors_db(distributor)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='temp_transactions'")
        assert cursor.fetchone() is not None

        # Drop temp tables
        result = self.sqlite.drop_temp_tables(distributor)
        assert result is True

        # Verify temp tables are dropped
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='temp_transactions'")
        assert cursor.fetchone() is None
        print(f"✅ SQLite drops temp tables!")

    def test_sqlite_cleans_and_removes_temp_data(self):
        distributor = "cleanup_distributor"

        self.sqlite.create_distributor_tables(distributor)

        # Insert some test data
        transfers = [
            {
                "signature": "cleanup_sig_1",
                "slot": 100,
                "timestamp": 1234567890,
                "amount": 1000.0,
                "token": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "wallet_address": "cleanup_wallet1",
                "distributor": distributor
            }
        ]
        self.sqlite.insert_transfer_batch(distributor, transfers)

        # Clean and remove temp data
        result = self.sqlite.clean_and_remove_temp_data(distributor)
        assert result is True

        # Verify temp tables are gone
        connection, cursor = self.sqlite.get_distributors_db(distributor)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='temp_transactions'")
        assert cursor.fetchone() is None
        print(f"✅ SQLite cleans and removes temp data!")

    ##########################################################
    #                  Connection Tests                      #
    ##########################################################
    def test_sqlite_gets_distributors_db_connection(self):
        distributor = "connection_test_distributor"

        connection, cursor = self.sqlite.get_distributors_db(distributor)

        assert connection is not None
        assert cursor is not None

        # Test that we can execute a simple query
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1

        connection.close()
        print(f"✅ SQLite gets distributor database connection!")

    def test_sqlite_closes_connections(self):
        # This test verifies the close_connections method works without error
        # We can't easily test if connections are actually closed, but we can test it doesn't throw
        result = self.sqlite.close_connections()
        # The method doesn't return anything, so we just check it runs without exception
        print(f"✅ SQLite closes connections!")

    ##########################################################
    #                    API Key Tests                       #
    ##########################################################

    def test_sqlite_validates_api_key(self):
        """Test API key validation"""
        test_api_key = "sk_live_test123456789012345678901234567890123456789012345678"

        # Initially should return False for non-existent key
        result = self.sqlite.validate_api_key(test_api_key)
        assert result is False

        # Insert API key
        insert_result = self.sqlite.insert_api_key(test_api_key, "Test Key", 1000)
        assert insert_result is True

        # Now should return True
        result = self.sqlite.validate_api_key(test_api_key)
        assert result is True
        print(f"✅ SQLite validates API keys!")

    def test_sqlite_validates_inactive_api_key(self):
        """Test that inactive API keys are not validated"""
        test_api_key = "sk_live_inactive123456789012345678901234567890123456789012"

        # Insert and then deactivate API key
        self.sqlite.insert_api_key(test_api_key, "Inactive Key", 500)
        self.sqlite.update_api_key(test_api_key, is_active=False)

        # Should return False for inactive key
        result = self.sqlite.validate_api_key(test_api_key)
        assert result is False
        print(f"✅ SQLite rejects inactive API keys!")

    def test_sqlite_inserts_api_key(self):
        """Test API key insertion"""
        test_api_key = "sk_live_insert123456789012345678901234567890123456789012"
        test_name = "Insert Test Key"
        test_rate_limit = 2000

        result = self.sqlite.insert_api_key(test_api_key, test_name, test_rate_limit)
        assert result is True

        # Verify the key was inserted correctly
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data is not None
        assert api_key_data["key"] == test_api_key
        assert api_key_data["name"] == test_name
        assert api_key_data["rate_limit"] == test_rate_limit
        assert api_key_data["is_active"] is True
        assert api_key_data["usage_count"] == 0
        print(f"✅ SQLite inserts API keys!")

    def test_sqlite_prevents_duplicate_api_keys(self):
        """Test that duplicate API keys are prevented"""
        test_api_key = "sk_live_duplicate12345678901234567890123456789012345678901"

        # Insert first time - should succeed
        result1 = self.sqlite.insert_api_key(test_api_key, "First Key", 1000)
        assert result1 is True

        # Insert again - should fail due to unique constraint
        result2 = self.sqlite.insert_api_key(test_api_key, "Duplicate Key", 2000)
        assert result2 is False
        print(f"✅ SQLite prevents duplicate API keys!")

    def test_sqlite_gets_api_key(self):
        """Test getting a specific API key"""
        test_api_key = "sk_live_getkey123456789012345678901234567890123456789012"
        test_name = "Get Key Test"
        test_rate_limit = 1500

        # Insert API key
        self.sqlite.insert_api_key(test_api_key, test_name, test_rate_limit)

        # Get API key
        api_key_data = self.sqlite.get_api_key(test_api_key)

        assert api_key_data is not None
        assert api_key_data["key"] == test_api_key
        assert api_key_data["name"] == test_name
        assert api_key_data["rate_limit"] == test_rate_limit
        assert api_key_data["is_active"] is True
        assert "created_at" in api_key_data
        assert api_key_data["last_used"] is None
        print(f"✅ SQLite gets API key details!")

    def test_sqlite_returns_none_for_nonexistent_api_key(self):
        """Test that None is returned for non-existent API key"""
        result = self.sqlite.get_api_key("sk_live_nonexistent123456789012345678901234567890123")
        assert result is None
        print(f"✅ SQLite returns None for non-existent API key!")

    def test_sqlite_gets_all_api_keys(self):
        """Test getting all API keys"""
        # Insert multiple API keys
        test_keys = [
            ("sk_live_all1234567890123456789012345678901234567890123456", "All Test 1", 1000),
            ("sk_live_all2345678901234567890123456789012345678901234567", "All Test 2", 2000),
            ("sk_live_all3456789012345678901234567890123456789012345678", "All Test 3", None)
        ]

        for key, name, rate_limit in test_keys:
            self.sqlite.insert_api_key(key, name, rate_limit)

        # Get all API keys
        all_keys = self.sqlite.get_all_api_keys()

        assert len(all_keys) >= len(test_keys)

        # Check that our test keys are in the results
        test_key_strings = [key for key, _, _ in test_keys]
        found_keys = [api_key["key"] for api_key in all_keys if api_key["key"] in test_key_strings]

        assert len(found_keys) == len(test_keys)
        print(f"✅ SQLite gets all API keys!")

    def test_sqlite_updates_api_key_name(self):
        """Test updating API key name"""
        test_api_key = "sk_live_updatename123456789012345678901234567890123456789"
        original_name = "Original Name"
        updated_name = "Updated Name"

        # Insert API key
        self.sqlite.insert_api_key(test_api_key, original_name, 1000)

        # Update name
        result = self.sqlite.update_api_key(test_api_key, name=updated_name)
        assert result is True

        # Verify update
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data["name"] == updated_name
        print(f"✅ SQLite updates API key name!")

    def test_sqlite_updates_api_key_active_status(self):
        """Test updating API key active status"""
        test_api_key = "sk_live_updateactive123456789012345678901234567890123456"

        # Insert API key (active by default)
        self.sqlite.insert_api_key(test_api_key, "Status Test", 1000)

        # Verify it's active
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data["is_active"] is True

        # Deactivate
        result = self.sqlite.update_api_key(test_api_key, is_active=False)
        assert result is True

        # Verify deactivation
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data["is_active"] is False

        # Reactivate
        result = self.sqlite.update_api_key(test_api_key, is_active=True)
        assert result is True

        # Verify reactivation
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data["is_active"] is True
        print(f"✅ SQLite updates API key active status!")

    def test_sqlite_updates_api_key_rate_limit(self):
        """Test updating API key rate limit"""
        test_api_key = "sk_live_updateratelimit123456789012345678901234567890123"
        original_rate_limit = 1000
        updated_rate_limit = 5000

        # Insert API key
        self.sqlite.insert_api_key(test_api_key, "Rate Limit Test", original_rate_limit)

        # Update rate limit
        result = self.sqlite.update_api_key(test_api_key, rate_limit=updated_rate_limit)
        assert result is True

        # Verify update
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data["rate_limit"] == updated_rate_limit
        print(f"✅ SQLite updates API key rate limit!")

    def test_sqlite_updates_multiple_api_key_fields(self):
        """Test updating multiple API key fields at once"""
        test_api_key = "sk_live_updatemulti123456789012345678901234567890123456"

        # Insert API key
        self.sqlite.insert_api_key(test_api_key, "Multi Test", 1000)

        # Update multiple fields
        result = self.sqlite.update_api_key(
            test_api_key,
            name="Updated Multi Test",
            is_active=False,
            rate_limit=3000
        )
        assert result is True

        # Verify all updates
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data["name"] == "Updated Multi Test"
        assert api_key_data["is_active"] is False
        assert api_key_data["rate_limit"] == 3000
        print(f"✅ SQLite updates multiple API key fields!")

    def test_sqlite_update_nonexistent_api_key_returns_false(self):
        """Test that updating non-existent API key returns False"""
        result = self.sqlite.update_api_key(
            "sk_live_nonexistent123456789012345678901234567890123456789",
            name="Should Fail"
        )
        assert result is False
        print(f"✅ SQLite returns False when updating non-existent API key!")

    def test_sqlite_deletes_api_key(self):
        """Test API key deletion"""
        test_api_key = "sk_live_delete123456789012345678901234567890123456789012"

        # Insert API key
        self.sqlite.insert_api_key(test_api_key, "Delete Test", 1000)

        # Verify it exists
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data is not None

        # Delete API key
        result = self.sqlite.delete_api_key(test_api_key)
        assert result is True

        # Verify it's gone
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data is None
        print(f"✅ SQLite deletes API keys!")

    def test_sqlite_delete_nonexistent_api_key_returns_false(self):
        """Test that deleting non-existent API key returns False"""
        result = self.sqlite.delete_api_key("sk_live_nonexistent123456789012345678901234567890123")
        assert result is False
        print(f"✅ SQLite returns False when deleting non-existent API key!")

    def test_sqlite_logs_api_key_usage(self):
        """Test API key usage logging"""
        test_api_key = "sk_live_usage123456789012345678901234567890123456789012"
        test_endpoint = "/test/endpoint"
        test_method = "GET"
        test_user_agent = "Test User Agent"
        test_ip = "192.168.1.100"

        # Insert API key
        self.sqlite.insert_api_key(test_api_key, "Usage Test", 1000)

        # Get initial usage count
        initial_data = self.sqlite.get_api_key(test_api_key)
        initial_usage_count = initial_data["usage_count"]
        initial_last_used = initial_data["last_used"]

        # Log usage
        result = self.sqlite.log_api_key_usage(
            test_api_key, test_endpoint, test_method, test_user_agent, test_ip
        )
        assert result is True

        # Verify usage count increased
        updated_data = self.sqlite.get_api_key(test_api_key)
        assert updated_data["usage_count"] == initial_usage_count + 1
        assert updated_data["last_used"] != initial_last_used
        assert updated_data["last_used"] is not None

        # Verify usage log was created
        usage_logs = self.sqlite.get_api_key_usage_stats(test_api_key, 10)
        assert len(usage_logs) >= 1

        # Find our log entry
        our_log = None
        for log in usage_logs:
            if (log["api_key"] == test_api_key and
                log["endpoint"] == test_endpoint and
                log["method"] == test_method):
                our_log = log
                break

        assert our_log is not None
        assert our_log["user_agent"] == test_user_agent
        assert our_log["ip_address"] == test_ip
        print(f"✅ SQLite logs API key usage!")

    def test_sqlite_logs_multiple_api_key_usages(self):
        """Test logging multiple API key usages"""
        test_api_key = "sk_live_multiusage123456789012345678901234567890123456789"

        # Insert API key
        self.sqlite.insert_api_key(test_api_key, "Multi Usage Test", 1000)

        # Log multiple usages
        usage_data = [
            ("/endpoint1", "GET", "Agent1", "192.168.1.1"),
            ("/endpoint2", "POST", "Agent2", "192.168.1.2"),
            ("/endpoint3", "PUT", "Agent3", "192.168.1.3")
        ]

        for endpoint, method, user_agent, ip in usage_data:
            result = self.sqlite.log_api_key_usage(test_api_key, endpoint, method, user_agent, ip)
            assert result is True

        # Verify usage count
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data["usage_count"] == len(usage_data)

        # Verify all logs were created
        usage_logs = self.sqlite.get_api_key_usage_stats(test_api_key, 10)
        assert len(usage_logs) >= len(usage_data)
        print(f"✅ SQLite logs multiple API key usages!")

    def test_sqlite_gets_api_key_usage_stats_for_specific_key(self):
        """Test getting usage stats for a specific API key"""
        test_api_key = "sk_live_stats123456789012345678901234567890123456789012"
        other_api_key = "sk_live_other123456789012345678901234567890123456789012"

        # Insert both API keys
        self.sqlite.insert_api_key(test_api_key, "Stats Test", 1000)
        self.sqlite.insert_api_key(other_api_key, "Other Test", 1000)

        # Log usage for both keys
        self.sqlite.log_api_key_usage(test_api_key, "/test1", "GET", "Agent1", "192.168.1.1")
        self.sqlite.log_api_key_usage(test_api_key, "/test2", "POST", "Agent2", "192.168.1.2")
        self.sqlite.log_api_key_usage(other_api_key, "/other", "GET", "Agent3", "192.168.1.3")

        # Get stats for specific key
        stats = self.sqlite.get_api_key_usage_stats(test_api_key, 10)

        # Should only get logs for our test key
        for log in stats:
            assert log["api_key"] == test_api_key

        # Should have at least 2 logs for our key
        test_key_logs = [log for log in stats if log["api_key"] == test_api_key]
        assert len(test_key_logs) >= 2
        print(f"✅ SQLite gets usage stats for specific API key!")

    def test_sqlite_gets_all_api_key_usage_stats(self):
        """Test getting usage stats for all API keys"""
        test_api_key1 = "sk_live_allstats1234567890123456789012345678901234567890"
        test_api_key2 = "sk_live_allstats2345678901234567890123456789012345678901"

        # Insert API keys
        self.sqlite.insert_api_key(test_api_key1, "All Stats Test 1", 1000)
        self.sqlite.insert_api_key(test_api_key2, "All Stats Test 2", 1000)

        # Log usage for both keys
        self.sqlite.log_api_key_usage(test_api_key1, "/all1", "GET", "Agent1", "192.168.1.1")
        self.sqlite.log_api_key_usage(test_api_key2, "/all2", "POST", "Agent2", "192.168.1.2")

        # Get all stats
        all_stats = self.sqlite.get_api_key_usage_stats(None, 100)

        # Should include logs from both keys
        key1_logs = [log for log in all_stats if log["api_key"] == test_api_key1]
        key2_logs = [log for log in all_stats if log["api_key"] == test_api_key2]

        assert len(key1_logs) >= 1
        assert len(key2_logs) >= 1
        print(f"✅ SQLite gets all API key usage stats!")

    def test_sqlite_respects_usage_stats_limit(self):
        """Test that usage stats limit is respected"""
        test_api_key = "sk_live_limit123456789012345678901234567890123456789012"

        # Insert API key
        self.sqlite.insert_api_key(test_api_key, "Limit Test", 1000)

        # Log multiple usages
        for i in range(10):
            self.sqlite.log_api_key_usage(test_api_key, f"/endpoint{i}", "GET", f"Agent{i}", f"192.168.1.{i}")

        # Get limited stats
        limited_stats = self.sqlite.get_api_key_usage_stats(test_api_key, 5)

        # Should respect the limit
        test_key_logs = [log for log in limited_stats if log["api_key"] == test_api_key]
        assert len(test_key_logs) <= 5
        print(f"✅ SQLite respects usage stats limit!")

    def test_sqlite_handles_api_key_with_none_values(self):
        """Test handling API keys with None values for optional fields"""
        test_api_key = "sk_live_none123456789012345678901234567890123456789012"

        # Insert API key with None values
        result = self.sqlite.insert_api_key(test_api_key, None, None)
        assert result is True

        # Verify None values are handled correctly
        api_key_data = self.sqlite.get_api_key(test_api_key)
        assert api_key_data["name"] is None
        assert api_key_data["rate_limit"] is None
        assert api_key_data["is_active"] is True  # Should default to True
        print(f"✅ SQLite handles API keys with None values!")

    def test_sqlite_api_key_usage_logging_failure_doesnt_break_validation(self):
        """Test that usage logging failure doesn't break API key validation"""
        test_api_key = "sk_live_logfail123456789012345678901234567890123456789012"

        # Insert API key
        self.sqlite.insert_api_key(test_api_key, "Log Fail Test", 1000)

        # Verify validation works
        assert self.sqlite.validate_api_key(test_api_key) is True

        # Try to log usage with invalid data (this might fail internally but shouldn't crash)
        try:
            # This should handle the error gracefully
            result = self.sqlite.log_api_key_usage(test_api_key, None, None, None, None)
            # Even if logging fails, it should return False but not crash
            assert result in [True, False]
        except Exception:
            # If it does throw an exception, that's also acceptable for this test
            pass

        # Validation should still work
        assert self.sqlite.validate_api_key(test_api_key) is True
        print(f"✅ SQLite handles API key usage logging failures gracefully!")

    # Add this import to the top of your test file if it's not already there
    def test_sqlite_api_key_table_creation(self):
        """Test that API key tables are created properly"""
        # This is implicitly tested by the __init__ method, but let's verify the tables exist

        # Check api_keys table exists
        self.sqlite.config_cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='api_keys'
        """)
        assert self.sqlite.config_cursor.fetchone() is not None

        # Check api_usage_logs table exists
        self.sqlite.config_cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='api_usage_logs'
        """)
        assert self.sqlite.config_cursor.fetchone() is not None

        # Check that the unique index on api_keys.key exists
        self.sqlite.config_cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='index' AND name='idx_api_keys_key'
        """)
        assert self.sqlite.config_cursor.fetchone() is not None

        print(f"✅ SQLite creates API key tables and indexes!")