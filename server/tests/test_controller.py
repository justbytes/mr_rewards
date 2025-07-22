import pytest
import json
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from .mock_data import *

# Import your classes (adjust paths as needed)
from ..lib.Controller import Controller
from ..lib.ProjectInitializer import ProjectInitializer
from ..lib.ProjectUpdater import ProjectUpdater
from ..db.SQLite.db import SQLiteDB
from ..db.Mongo.db import MongoDB

class TestController:
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test databases"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_sqlite_db(self, temp_dir):
        """Create mock SQLite database"""
        with patch('server.lib.Controller.SQLiteDB') as mock_sqlite:
            mock_instance = Mock()
            mock_instance.get_known_tokens.return_value = list(MOCK_TOKEN_METADATA.values())
            mock_sqlite.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_mongo_db(self):
        """Create mock MongoDB"""
        with patch('server.lib.Controller.MongoDB') as mock_mongo:
            mock_instance = Mock()
            mock_mongo.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def controller(self, mock_sqlite_db, mock_mongo_db):
        """Create Controller instance with mocked dependencies"""
        with patch('server.lib.Controller.SQLiteDB', return_value=mock_sqlite_db), \
             patch('server.lib.Controller.MongoDB', return_value=mock_mongo_db):
            return Controller()

    def test_controller_initialization(self, controller, mock_sqlite_db):
        """Test that Controller initializes correctly"""
        assert controller.sqlite == mock_sqlite_db
        assert controller.mongo is not None
        assert len(controller.known_tokens) == 3
        assert len(controller.known_tokens_dict) == 3

    def test_get_token_symbol_known_token(self, controller):
        """Test getting symbol for known token"""
        mint = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
        symbol = controller.get_token_symbol(mint)
        assert symbol == "USDT"

    @patch('server.lib.Controller.get_token_metadata')
    def test_get_token_symbol_unknown_token(self, mock_get_metadata, controller):
        """Test getting symbol for unknown token"""
        unknown_mint = "UnknownMint123456789"
        mock_token_data = {
            "symbol": "UNK",
            "name": "Unknown Token",
            "mint": unknown_mint,
            "decimals": "9"
        }
        mock_get_metadata.return_value = mock_token_data
        controller.sqlite.insert_known_token.return_value = True

        symbol = controller.get_token_symbol(unknown_mint)
        assert symbol == "UNK"
        controller.sqlite.insert_known_token.assert_called_once()

    def test_upsert_wallets_new_wallet(self, controller):
        """Test upserting new wallets"""
        test_wallets = {
            "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": {
                "distributors": {
                    MOCK_DISTRIBUTOR: {
                        "tokens": {
                            "USDT": {"total_amount": 1000.0},
                            "sol": {"total_amount": 2.0}
                        }
                    }
                }
            }
        }

        controller.sqlite.get_wallets_by_addresses.return_value = {}
        controller.sqlite.insert_wallets_batch.return_value = True

        result = controller.upsert_wallets(test_wallets)
        assert result == 1
        controller.sqlite.insert_wallets_batch.assert_called_once()

    def test_upsert_wallets_existing_wallet(self, controller):
        """Test upserting existing wallets with merge"""
        wallet_address = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"

        existing_wallets = {
            wallet_address: {
                "wallet_address": wallet_address,
                "distributors": {
                    MOCK_DISTRIBUTOR: {
                        "tokens": {
                            "USDT": {"total_amount": 500.0}
                        }
                    }
                }
            }
        }

        new_wallet_data = {
            wallet_address: {
                "distributors": {
                    MOCK_DISTRIBUTOR: {
                        "tokens": {
                            "USDT": {"total_amount": 250.0},
                            "sol": {"total_amount": 1.0}
                        }
                    }
                }
            }
        }

        controller.sqlite.get_wallets_by_addresses.return_value = existing_wallets
        controller.sqlite.update_wallets_batch.return_value = True

        result = controller.upsert_wallets(new_wallet_data)
        assert result == 1

        # Verify the merge happened correctly
        call_args = controller.sqlite.update_wallets_batch.call_args[0][0]
        merged_wallet = call_args[0]
        assert merged_wallet["distributors"][MOCK_DISTRIBUTOR]["tokens"]["USDT"]["total_amount"] == 750.0
        assert merged_wallet["distributors"][MOCK_DISTRIBUTOR]["tokens"]["sol"]["total_amount"] == 1.0

class TestProjectInitializer:
    @pytest.fixture
    def mock_controller(self):
        """Create mock controller for ProjectInitializer"""
        controller = Mock()
        controller.sqlite = Mock()
        controller.mongo = Mock()

        # Mock known tokens
        controller.get_token_symbol.side_effect = lambda mint: MOCK_TOKEN_METADATA.get(
            mint, {"symbol": mint[:8]}
        )["symbol"] if mint in MOCK_TOKEN_METADATA else mint[:8]

        return controller

    @pytest.fixture
    def project_initializer(self, mock_controller):
        """Create ProjectInitializer instance"""
        return ProjectInitializer(mock_controller, MOCK_PROJECT)

    @patch('server.lib.ProjectInitializer.get_historical_transactions_for_distributor')
    def test_get_initial_txs_success(self, mock_get_txs, project_initializer):
        """Test successful transaction fetching"""
        # Mock the generator to yield our test transactions
        mock_get_txs.return_value = [
            {
                "txs": MOCK_TRANSACTIONS[:5],
                "before": "last_sig_1",
                "last_sig": "newest_sig_1",
                "finished": False
            },
            {
                "txs": MOCK_TRANSACTIONS[5:],
                "before": "last_sig_2",
                "last_sig": "newest_sig_2",
                "finished": True
            }
        ]

        # Mock database methods
        project_initializer.controller.sqlite.create_distributor_tables.return_value = None
        project_initializer.controller.sqlite.get_temp_txs_last_sigs.return_value = (None, None)
        project_initializer.controller.sqlite.insert_transactions_batch.return_value = True
        project_initializer.controller.sqlite.update_temp_txs_last_sig.return_value = True
        project_initializer.controller.sqlite.update_temp_txs_before_sig.return_value = True

        result = project_initializer.get_initial_txs()
        assert result is True
        assert project_initializer.controller.sqlite.insert_transactions_batch.call_count == 2

    def test_process_initial_txs(self, project_initializer):
        """Test transaction processing"""
        # Mock database methods
        project_initializer.controller.sqlite.get_transactions_count.return_value = len(MOCK_TRANSACTIONS)
        project_initializer.controller.sqlite.get_transactions.return_value = iter([
            (MOCK_TRANSACTIONS, 0)
        ])
        project_initializer.controller.sqlite.insert_transfer_batch.return_value = True

        # Mock the processing function
        with patch('server.lib.ProjectInitializer.process_distributor_transfers') as mock_process:
            mock_process.return_value = [
                {
                    "signature": tx["signature"],
                    "slot": tx["slot"],
                    "timestamp": tx["timestamp"],
                    "amount": 100.0,
                    "token": "USDT",
                    "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                    "distributor": MOCK_DISTRIBUTOR
                } for tx in MOCK_TRANSACTIONS
            ]

            result = project_initializer.process_initial_txs()
            assert result is True
            project_initializer.controller.sqlite.insert_transfer_batch.assert_called_once()

    def test_insert_and_clean_project(self, project_initializer):
        """Test project insertion and cleanup"""
        project_initializer.controller.sqlite.get_temp_txs_last_sigs.return_value = (
            "before_sig", "newest_sig"
        )
        project_initializer.controller.sqlite.insert_supported_project.return_value = True
        project_initializer.controller.sqlite.clean_and_remove_temp_data.return_value = True

        result = project_initializer.insert_and_clean_project()
        assert result is True

        # Verify project was updated with last_sig
        assert project_initializer.project["last_sig"] == "newest_sig"

        # Verify both SQLite and Mongo inserts were called
        assert project_initializer.controller.sqlite.insert_supported_project.call_count == 2

class TestProjectUpdater:
    @pytest.fixture
    def mock_controller(self):
        """Create mock controller for ProjectUpdater"""
        controller = Mock()
        controller.sqlite = Mock()
        controller.mongo = Mock()

        # Mock supported projects
        controller.sqlite.get_supported_projects.return_value = [MOCK_PROJECT]
        controller.sqlite.get_last_tx_signature.return_value = "last_signature"
        controller.sqlite.update_last_tx_signature.return_value = True
        controller.sqlite.insert_temp_transfers_batch.return_value = True
        controller.upsert_wallets.return_value = 5  # 5 wallets updated

        return controller

    @pytest.fixture
    def project_updater(self, mock_controller):
        """Create ProjectUpdater instance"""
        with patch('server.lib.ProjectUpdater.timer'):  # Prevent actual timer from starting
            return ProjectUpdater(mock_controller)

    @patch('server.lib.ProjectUpdater.get_new_distributor_transactions')
    def test_fetch_and_process_new_transactions(self, mock_get_new_txs, project_updater):
        """Test fetching and processing new transactions"""
        # Mock new transactions
        mock_get_new_txs.return_value = [
            {
                "txs": MOCK_TRANSACTIONS[:3],
                "last_sig": "new_last_sig"
            }
        ]

        # Mock transfer processing
        with patch.object(project_updater, 'extract_transfers_from_distributor_transactions') as mock_extract:
            mock_extract.return_value = iter([
                [  # Mock transfers batch
                    {
                        "signature": "sig1",
                        "slot": 250000001,
                        "timestamp": 1708000000,
                        "amount": 1000.0,
                        "token": "USDT",
                        "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                        "distributor": MOCK_DISTRIBUTOR
                    }
                ]
            ])

            with patch.object(project_updater, 'aggregate_rewards') as mock_aggregate:
                project_updater.fetch_and_process_new_distributor_transactions(MOCK_DISTRIBUTOR)

                # Verify calls
                mock_get_new_txs.assert_called_once()
                mock_extract.assert_called_once()
                mock_aggregate.assert_called_once()
                project_updater.controller.sqlite.update_last_tx_signature.assert_called_once()

    def test_extract_transfers_from_distributor_transactions(self, project_updater):
        """Test transfer extraction"""
        with patch('server.lib.ProjectUpdater.process_distributor_transfers') as mock_process:
            mock_transfers = [
                {
                    "signature": tx["signature"],
                    "slot": tx["slot"],
                    "timestamp": tx["timestamp"],
                    "amount": 100.0,
                    "token": "USDT",
                    "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                    "distributor": MOCK_DISTRIBUTOR
                } for tx in MOCK_TRANSACTIONS[:3]
            ]
            mock_process.return_value = mock_transfers

            transfers_generator = project_updater.extract_transfers_from_distributor_transactions(
                MOCK_TRANSACTIONS[:3], MOCK_DISTRIBUTOR
            )

            transfers_list = list(transfers_generator)
            assert len(transfers_list) == 1  # One batch
            assert len(transfers_list[0]) == 3  # Three transfers in batch

    def test_aggregate_rewards(self, project_updater):
        """Test reward aggregation"""
        test_transfers = [
            {
                "signature": "sig1",
                "slot": 250000001,
                "timestamp": 1708000000,
                "amount": 1000.0,
                "token": "USDT",
                "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "distributor": MOCK_DISTRIBUTOR
            },
            {
                "signature": "sig2",
                "slot": 250000002,
                "timestamp": 1708000010,
                "amount": 2.0,
                "token": "sol",
                "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "distributor": MOCK_DISTRIBUTOR
            }
        ]

        with patch('server.lib.ProjectUpdater.aggregate_transfers') as mock_aggregate_func:
            mock_aggregate_func.return_value = {
                "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": {
                    "distributors": {
                        MOCK_DISTRIBUTOR: {
                            "tokens": {
                                "USDT": {"total_amount": 1000.0},
                                "sol": {"total_amount": 2.0}
                            }
                        }
                    }
                }
            }

            project_updater.aggregate_rewards(test_transfers)

            mock_aggregate_func.assert_called_once_with(test_transfers)
            project_updater.controller.upsert_wallets.assert_called_once()

class TestIntegration:
    """Integration tests using all components together"""

    @pytest.fixture
    def temp_db_dir(self):
        """Create temporary directory for integration test databases"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @patch('server.lib.Controller.SQLiteDB')
    @patch('server.lib.Controller.MongoDB')
    def test_full_project_initialization_flow(self, mock_mongo_class, mock_sqlite_class, temp_db_dir):
        """Test complete project initialization flow"""
        # Setup mocks
        mock_sqlite = Mock()
        mock_mongo = Mock()
        mock_sqlite_class.return_value = mock_sqlite
        mock_mongo_class.return_value = mock_mongo

        # Mock SQLite responses
        mock_sqlite.get_known_tokens.return_value = list(MOCK_TOKEN_METADATA.values())
        mock_sqlite.create_distributor_tables.return_value = None
        mock_sqlite.get_temp_txs_last_sigs.return_value = (None, None)
        mock_sqlite.insert_transactions_batch.return_value = True
        mock_sqlite.update_temp_txs_last_sig.return_value = True
        mock_sqlite.update_temp_txs_before_sig.return_value = True
        mock_sqlite.get_transactions_count.return_value = len(MOCK_TRANSACTIONS)
        mock_sqlite.get_transactions.return_value = iter([(MOCK_TRANSACTIONS, 0)])
        mock_sqlite.insert_transfer_batch.return_value = True
        mock_sqlite.insert_supported_project.return_value = True
        mock_sqlite.clean_and_remove_temp_data.return_value = True
        mock_sqlite.get_transfers_count.return_value = 10
        mock_sqlite.get_transfers.return_value = iter([
            ([  # Mock transfers
                {
                    "signature": "sig1",
                    "slot": 250000001,
                    "timestamp": 1708000000,
                    "amount": 1000.0,
                    "token": "USDT",
                    "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                    "distributor": MOCK_DISTRIBUTOR
                }
            ], 0)
        ])
        mock_sqlite.get_wallets_by_addresses.return_value = {}
        mock_sqlite.insert_wallets_batch.return_value = True

        # Create controller
        controller = Controller()

        # Mock external API calls
        with patch('server.lib.ProjectInitializer.get_historical_transactions_for_distributor') as mock_get_txs, \
             patch('server.lib.ProjectInitializer.process_distributor_transfers') as mock_process, \
             patch('server.lib.ProjectInitializer.aggregate_transfers') as mock_aggregate:

            # Setup mock responses
            mock_get_txs.return_value = [
                {
                    "txs": MOCK_TRANSACTIONS,
                    "before": "last_sig",
                    "last_sig": "newest_sig",
                    "finished": True
                }
            ]

            mock_process.return_value = [
                {
                    "signature": tx["signature"],
                    "slot": tx["slot"],
                    "timestamp": tx["timestamp"],
                    "amount": 100.0,
                    "token": "USDT",
                    "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                    "distributor": MOCK_DISTRIBUTOR
                } for tx in MOCK_TRANSACTIONS
            ]

            mock_aggregate.return_value = {
                "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": {
                    "distributors": {
                        MOCK_DISTRIBUTOR: {
                            "tokens": {
                                "USDT": {"total_amount": 1000.0}
                            }
                        }
                    }
                }
            }

            # Initialize project
            initializer = ProjectInitializer(controller, MOCK_PROJECT.copy())

            # Test the full flow
            result = initializer.get_initial_txs()
            assert result is True

            result = initializer.process_initial_txs()
            assert result is True

            result = initializer.insert_and_clean_project()
            assert result is True

            result = initializer.aggregate_rewards_from_transfers()
            assert result is True

    @patch('server.lib.Controller.SQLiteDB')
    @patch('server.lib.Controller.MongoDB')
    def test_project_update_flow(self, mock_mongo_class, mock_sqlite_class):
        """Test project update flow"""
        # Setup mocks similar to initialization test
        mock_sqlite = Mock()
        mock_mongo = Mock()
        mock_sqlite_class.return_value = mock_sqlite
        mock_mongo_class.return_value = mock_mongo

        mock_sqlite.get_known_tokens.return_value = list(MOCK_TOKEN_METADATA.values())
        mock_sqlite.get_supported_projects.return_value = [MOCK_PROJECT]
        mock_sqlite.get_last_tx_signature.return_value = "last_signature"
        mock_sqlite.update_last_tx_signature.return_value = True
        mock_sqlite.insert_temp_transfers_batch.return_value = True
        mock_sqlite.get_wallets_by_addresses.return_value = {}
        mock_sqlite.insert_wallets_batch.return_value = True

        controller = Controller()

        with patch('server.lib.ProjectUpdater.timer'), \
             patch('server.lib.ProjectUpdater.get_new_distributor_transactions') as mock_get_new, \
             patch('server.lib.ProjectUpdater.process_distributor_transfers') as mock_process, \
             patch('server.lib.ProjectUpdater.aggregate_transfers') as mock_aggregate:

            # Setup mock responses for updates
            mock_get_new.return_value = [
                {
                    "txs": MOCK_TRANSACTIONS[:3],
                    "last_sig": "new_last_sig"
                }
            ]

            mock_process.return_value = [
                {
                    "signature": "new_sig",
                    "slot": 250000011,
                    "timestamp": 1708000100,
                    "amount": 500.0,
                    "token": "USDC",
                    "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                    "distributor": MOCK_DISTRIBUTOR
                }
            ]

            mock_aggregate.return_value = {
                "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": {
                    "distributors": {
                        MOCK_DISTRIBUTOR: {
                            "tokens": {
                                "USDC": {"total_amount": 500.0}
                            }
                        }
                    }
                }
            }

            # Create updater and test update flow
            updater = ProjectUpdater(controller)

            # Test single distributor update
            updater.fetch_and_process_new_distributor_transactions(MOCK_DISTRIBUTOR)

            # Verify calls were made
            mock_get_new.assert_called_once()
            mock_sqlite.update_last_tx_signature.assert_called_once()

# Utility functions for running specific test scenarios
def run_controller_tests():
    """Run only Controller tests"""
    pytest.main(["-v", "test_controller.py::TestController"])

def run_initializer_tests():
    """Run only ProjectInitializer tests"""
    pytest.main(["-v", "test_controller.py::TestProjectInitializer"])

def run_updater_tests():
    """Run only ProjectUpdater tests"""
    pytest.main(["-v", "test_controller.py::TestProjectUpdater"])

def run_integration_tests():
    """Run only Integration tests"""
    pytest.main(["-v", "test_controller.py::TestIntegration"])

def run_all_tests():
    """Run all tests with coverage"""
    pytest.main(["-v", "--cov=server", "test_controller.py"])


if __name__ == "__main__":
    # You can run individual test suites or all tests
    print("Available test functions:")
    print("1. run_controller_tests()")
    print("2. run_initializer_tests()")
    print("3. run_updater_tests()")
    print("4. run_integration_tests()")
    print("5. run_all_tests()")

    # Uncomment to run specific tests
    # run_all_tests()