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