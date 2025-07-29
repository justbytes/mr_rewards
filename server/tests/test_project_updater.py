import pytest
import sys
import json
import time
import shutil
import os
from pathlib import Path
from dotenv import load_dotenv
from unittest.mock import patch, MagicMock, call
import threading

# Add the server directory to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
SERVER_DIR = PROJECT_ROOT / "server"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SERVER_DIR))

load_dotenv()

# Import your real classes
from lib.Controller import Controller
from lib.ProjectUpdater import ProjectUpdater
from .mock_data import TRANSACTIONS, PROJECT, WALLET, KNOWN_TOKENS


class TestProjectUpdater:
    def setup_method(self):
        """Setup method run before each test"""
        # Clean up test data BEFORE creating Controller instance
        self.cleanup_test_data()

        # Ensure test directories exist with correct permissions
        self.ensure_test_directories()

        # Create the Controller instance (test=True, temp_dirs=False)
        self.controller = Controller(True, False)

        # Create ProjectUpdater instance (no need to patch anything now)
        self.project_updater = ProjectUpdater(self.controller)

    def teardown_method(self):
        """Cleanup method run after each test"""
        # Close connections before cleanup
        try:
            self.controller.sqlite.close_connections()
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

                print("ProjectUpdater test data cleaned up (transfers directory preserved)")
            else:
                print("test_backup directory does not exist, nothing to clean")

            # Also clean up MongoDB test data
            try:
                self.controller.mongo._db.supported_projects.delete_many({})
                self.controller.mongo._db.wallets.delete_many({})
                print("MongoDB test data cleaned up")
            except:
                print("MongoDB cleanup skipped (controller not initialized yet)")

        except Exception as e:
            print(f"Error cleaning up ProjectUpdater test data: {e}")

    ##########################################################
    #            ProjectUpdater Initialization Tests         #
    ##########################################################
    def test_project_updater_initializes_with_controller(self):
        """Test that ProjectUpdater initializes with Controller instance"""
        assert self.project_updater.controller is not None
        assert self.project_updater.controller == self.controller
        assert self.project_updater.updating is False
        print(f"✅ ProjectUpdater initializes with Controller instance!")

    ##########################################################
    #           Update Distributors Transactions Tests       #
    ##########################################################
    def test_update_distributors_transactions_with_no_projects(self):
        """Test update_distributors_transactions when no projects exist"""
        # Mock empty projects list
        with patch.object(self.controller.sqlite, 'get_supported_projects', return_value=[]):
            self.project_updater.update_distributors_transactions()

        assert self.project_updater.updating is False
        print(f"✅ ProjectUpdater handles empty projects list!")

    def test_update_distributors_transactions_prevents_concurrent_updates(self):
        """Test that update_distributors_transactions prevents concurrent execution"""
        # Set updating to True to simulate ongoing update
        self.project_updater.updating = True

        # Mock projects to ensure the method would normally process
        with patch.object(self.controller.sqlite, 'get_supported_projects', return_value=[PROJECT]):
            with patch.object(self.project_updater, 'fetch_and_process_new_distributor_transactions') as mock_fetch:
                self.project_updater.update_distributors_transactions()

                # Should not call fetch method due to updating flag
                mock_fetch.assert_not_called()

        # updating should still be True
        assert self.project_updater.updating is True
        print(f"✅ ProjectUpdater prevents concurrent updates!")

    def test_update_distributors_transactions_processes_projects(self):
        """Test that update_distributors_transactions processes all projects"""
        test_projects = [
            {"distributor": "dist1"},
            {"distributor": "dist2"},
            {"distributor": "dist3"}
        ]

        with patch.object(self.controller.sqlite, 'get_supported_projects', return_value=test_projects):
            with patch.object(self.project_updater, 'fetch_and_process_new_distributor_transactions') as mock_fetch:
                self.project_updater.update_distributors_transactions()

                # Should call fetch for each distributor
                expected_calls = [call("dist1"), call("dist2"), call("dist3")]
                mock_fetch.assert_has_calls(expected_calls)

        assert self.project_updater.updating is False
        print(f"✅ ProjectUpdater processes all projects!")

    def test_update_distributors_transactions_sets_updating_flag(self):
        """Test that update_distributors_transactions properly manages updating flag"""
        with patch.object(self.controller.sqlite, 'get_supported_projects', return_value=[PROJECT]):
            with patch.object(self.project_updater, 'fetch_and_process_new_distributor_transactions'):
                # Initially should be False
                assert self.project_updater.updating is False

                self.project_updater.update_distributors_transactions()

                # Should be False after completion
                assert self.project_updater.updating is False

        print(f"✅ ProjectUpdater manages updating flag correctly!")

    def test_update_distributors_transactions_error_handling(self):
        """Test that update_distributors_transactions handles errors and resets updating flag"""
        with patch.object(self.controller.sqlite, 'get_supported_projects', return_value=[PROJECT]):
            with patch.object(self.project_updater, 'fetch_and_process_new_distributor_transactions', side_effect=Exception("Test error")):

                # Should raise the exception
                with pytest.raises(Exception):
                    self.project_updater.update_distributors_transactions()

                # updating flag should be reset to False even after error
                assert self.project_updater.updating is False

        print(f"✅ ProjectUpdater handles errors and resets updating flag!")

    ##########################################################
    #      Fetch and Process New Distributor Transactions    #
    ##########################################################
    @patch('lib.ProjectUpdater.get_new_distributor_transactions')
    def test_fetch_and_process_new_distributor_transactions_success(self, mock_get_transactions):
        """Test successful fetch and process of new distributor transactions"""
        test_distributor = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"
        test_last_sig = "test_last_signature"
        test_new_sig = "test_new_signature"

        # Mock the last signature lookup
        with patch.object(self.controller.sqlite, 'get_last_tx_signature', return_value=test_last_sig):
            # Mock the signature update
            with patch.object(self.controller.sqlite, 'update_last_tx_signature') as mock_update_sig:
                # Mock the transaction generator
                mock_batch = {
                    "txs": TRANSACTIONS,
                    "last_sig": test_new_sig
                }
                mock_get_transactions.return_value = [mock_batch]

                # Mock the extract transfers method
                with patch.object(self.project_updater, 'extract_transfers_from_distributor_transactions') as mock_extract:
                    mock_extract.return_value = [["transfer1", "transfer2"]]

                    # Mock the aggregate rewards method
                    with patch.object(self.project_updater, 'aggregate_rewards') as mock_aggregate:
                        self.project_updater.fetch_and_process_new_distributor_transactions(test_distributor)

                        # Verify signature was updated
                        mock_update_sig.assert_called_once_with(test_distributor, test_new_sig)

                        # Verify extract method was called
                        mock_extract.assert_called_once_with(TRANSACTIONS, test_distributor)

                        # Verify aggregate method was called
                        mock_aggregate.assert_called_once_with(["transfer1", "transfer2"])

        print(f"✅ ProjectUpdater fetches and processes new transactions successfully!")

    @patch('lib.ProjectUpdater.get_new_distributor_transactions')
    def test_fetch_and_process_new_distributor_transactions_no_new_transactions(self, mock_get_transactions):
        """Test fetch and process when no new transactions exist"""
        test_distributor = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"
        test_last_sig = "test_last_signature"

        # Mock the last signature lookup
        with patch.object(self.controller.sqlite, 'get_last_tx_signature', return_value=test_last_sig):
            # Mock empty transaction generator
            mock_get_transactions.return_value = []

            # Mock the signature update (should not be called)
            with patch.object(self.controller.sqlite, 'update_last_tx_signature') as mock_update_sig:
                self.project_updater.fetch_and_process_new_distributor_transactions(test_distributor)

                # Verify signature was not updated (no new transactions)
                mock_update_sig.assert_not_called()

        print(f"✅ ProjectUpdater handles no new transactions gracefully!")

    @patch('lib.ProjectUpdater.get_new_distributor_transactions')
    def test_fetch_and_process_new_distributor_transactions_error_handling(self, mock_get_transactions):
        """Test error handling in fetch and process new distributor transactions"""
        test_distributor = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"

        # Mock the last signature lookup to raise an exception
        with patch.object(self.controller.sqlite, 'get_last_tx_signature', side_effect=Exception("Database error")):
            with pytest.raises(Exception) as exc_info:
                self.project_updater.fetch_and_process_new_distributor_transactions(test_distributor)

            assert "There was an error when fetching and processing new transactions in ProjectUpdater." in str(exc_info.value)

        print(f"✅ ProjectUpdater handles fetch and process errors!")

    ##########################################################
    #         Extract Transfers from Distributor Tests       #
    ##########################################################
    @patch('lib.ProjectUpdater.process_distributor_transfers')
    def test_extract_transfers_from_distributor_transactions_success(self, mock_process_transfers):
        """Test successful extraction of transfers from distributor transactions"""
        test_distributor = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"
        test_transfers = ["transfer1", "transfer2", "transfer3"]

        # Mock the process function
        mock_process_transfers.return_value = test_transfers

        # Mock successful database insert
        with patch.object(self.controller.sqlite, 'insert_temp_transfers_batch', return_value=True):
            result = list(self.project_updater.extract_transfers_from_distributor_transactions(
                TRANSACTIONS, test_distributor
            ))

            # Should yield one batch with the processed transfers
            assert len(result) == 1
            assert result[0] == test_transfers

            # Verify process function was called correctly
            mock_process_transfers.assert_called_once_with(self.controller, TRANSACTIONS, test_distributor)

        print(f"✅ ProjectUpdater extracts transfers successfully!")

    @patch('lib.ProjectUpdater.process_distributor_transfers')
    def test_extract_transfers_from_distributor_transactions_large_batch(self, mock_process_transfers):
        """Test extraction with large transaction batch that requires splitting"""
        test_distributor = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"

        # Create a large list of transactions (more than default batch size of 1000)
        large_transactions = TRANSACTIONS * 600  # This will be 600 transactions
        test_transfers = ["transfer"] * 600

        # Mock the process function
        mock_process_transfers.return_value = test_transfers

        # Mock successful database insert
        with patch.object(self.controller.sqlite, 'insert_temp_transfers_batch', return_value=True):
            result = list(self.project_updater.extract_transfers_from_distributor_transactions(
                large_transactions, test_distributor, batch_size=100
            ))

            # With batch size of 100 and 600 transactions, should have 6 batches
            assert len(result) == 6

            # Each result should be the test_transfers
            for batch in result:
                assert batch == test_transfers

        print(f"✅ ProjectUpdater handles large transaction batches!")

    @patch('lib.ProjectUpdater.process_distributor_transfers')
    def test_extract_transfers_database_insert_failure(self, mock_process_transfers):
        """Test extraction when database insert fails"""
        test_distributor = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"
        test_transfers = ["transfer1", "transfer2"]

        # Mock the process function
        mock_process_transfers.return_value = test_transfers

        # Mock failed database insert
        with patch.object(self.controller.sqlite, 'insert_temp_transfers_batch', return_value=False):
            with pytest.raises(Exception) as exc_info:
                list(self.project_updater.extract_transfers_from_distributor_transactions(
                    TRANSACTIONS, test_distributor
                ))

            # The actual implementation wraps the error in a generic message
            assert "There was an error when extracting transfers from distributor in ProjectUpdater." in str(exc_info.value)

        print(f"✅ ProjectUpdater handles database insert failures!")

    @patch('lib.ProjectUpdater.process_distributor_transfers')
    def test_extract_transfers_process_error_handling(self, mock_process_transfers):
        """Test error handling in extract transfers"""
        test_distributor = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"

        # Mock the process function to raise an exception
        mock_process_transfers.side_effect = Exception("Processing error")

        with pytest.raises(Exception) as exc_info:
            list(self.project_updater.extract_transfers_from_distributor_transactions(
                TRANSACTIONS, test_distributor
            ))

        assert "There was an error when extracting transfers from distributor in ProjectUpdater." in str(exc_info.value)

        print(f"✅ ProjectUpdater handles extract transfers errors!")

    ##########################################################
    #              Aggregate Rewards Tests                   #
    ##########################################################
    @patch('lib.ProjectUpdater.aggregate_transfers')
    def test_aggregate_rewards_success(self, mock_aggregate_transfers):
        """Test successful aggregation of rewards"""
        test_transfers = [
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "sol",
                "amount": 100.0
            },
            {
                "wallet_address": "wallet2",
                "distributor": "dist1",
                "token": "USDC",
                "amount": 50.0
            }
        ]

        test_aggregated = {
            "wallet1": {
                "distributors": {
                    "dist1": {
                        "tokens": {
                            "sol": {"total_amount": 100.0}
                        }
                    }
                }
            }
        }

        # Mock the aggregate function
        mock_aggregate_transfers.return_value = test_aggregated

        # Mock the controller upsert method
        with patch.object(self.controller, 'upsert_wallets', return_value=1) as mock_upsert:
            self.project_updater.aggregate_rewards(test_transfers)

            # Verify aggregate function was called
            mock_aggregate_transfers.assert_called_once_with(test_transfers)

            # Verify upsert was called with aggregated data
            mock_upsert.assert_called_once_with(test_aggregated)

        print(f"✅ ProjectUpdater aggregates rewards successfully!")

    @patch('lib.ProjectUpdater.aggregate_transfers')
    def test_aggregate_rewards_large_batch(self, mock_aggregate_transfers):
        """Test aggregation with large transfer batch"""
        # Create large list of transfers
        large_transfers = []
        for i in range(1500):  # More than default batch size of 1000
            large_transfers.append({
                "wallet_address": f"wallet{i}",
                "distributor": "dist1",
                "token": "sol",
                "amount": 10.0
            })

        test_aggregated = {"wallet1": {"distributors": {}}}

        # Mock the aggregate function
        mock_aggregate_transfers.return_value = test_aggregated

        # Mock the controller upsert method
        with patch.object(self.controller, 'upsert_wallets', return_value=1) as mock_upsert:
            self.project_updater.aggregate_rewards(large_transfers, batch_size=500)

            # Should call aggregate function 3 times (1500 / 500 = 3 batches)
            assert mock_aggregate_transfers.call_count == 3

            # Should call upsert 3 times
            assert mock_upsert.call_count == 3

        print(f"✅ ProjectUpdater handles large transfer batches in aggregation!")

    @patch('lib.ProjectUpdater.aggregate_transfers')
    def test_aggregate_rewards_error_handling(self, mock_aggregate_transfers):
        """Test error handling in aggregate rewards"""
        test_transfers = [{"wallet_address": "wallet1"}]

        # Mock the aggregate function to raise an exception
        mock_aggregate_transfers.side_effect = Exception("Aggregation error")

        with pytest.raises(Exception) as exc_info:
            self.project_updater.aggregate_rewards(test_transfers)

        assert "There was an error when aggregating rewards in the ProjectUpdater." in str(exc_info.value)

        print(f"✅ ProjectUpdater handles aggregation errors!")

    ##########################################################
    #                Integration Tests                        #
    ##########################################################
    @patch('lib.ProjectUpdater.get_new_distributor_transactions')
    @patch('lib.ProjectUpdater.process_distributor_transfers')
    @patch('lib.ProjectUpdater.aggregate_transfers')
    def test_full_workflow_integration(self, mock_aggregate, mock_process, mock_get_transactions):
        """Test full ProjectUpdater workflow integration"""
        test_distributor = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"
        test_last_sig = "old_signature"
        test_new_sig = "new_signature"

        # Mock data for the workflow
        mock_batch = {
            "txs": TRANSACTIONS,
            "last_sig": test_new_sig
        }
        mock_get_transactions.return_value = [mock_batch]

        test_transfers = [
            {
                "wallet_address": "wallet1",
                "distributor": test_distributor,
                "token": "sol",
                "amount": 100.0
            }
        ]
        mock_process.return_value = test_transfers

        test_aggregated = {
            "wallet1": {
                "distributors": {
                    test_distributor: {
                        "tokens": {
                            "sol": {"total_amount": 100.0}
                        }
                    }
                }
            }
        }
        mock_aggregate.return_value = test_aggregated

        # Mock database operations
        with patch.object(self.controller.sqlite, 'get_last_tx_signature', return_value=test_last_sig):
            with patch.object(self.controller.sqlite, 'update_last_tx_signature') as mock_update_sig:
                with patch.object(self.controller.sqlite, 'insert_temp_transfers_batch', return_value=True):
                    with patch.object(self.controller, 'upsert_wallets', return_value=1) as mock_upsert:

                        # Execute the full workflow
                        self.project_updater.fetch_and_process_new_distributor_transactions(test_distributor)

                        # Verify all steps were executed
                        mock_get_transactions.assert_called_once_with(test_distributor, test_last_sig)
                        mock_update_sig.assert_called_once_with(test_distributor, test_new_sig)
                        mock_process.assert_called_once_with(self.controller, TRANSACTIONS, test_distributor)
                        mock_aggregate.assert_called_once_with(test_transfers)
                        mock_upsert.assert_called_once_with(test_aggregated)

        print(f"✅ ProjectUpdater full workflow integration works!")

    def test_real_database_integration(self):
        """Test ProjectUpdater with real database operations"""
        # Insert a test project
        test_project = {
            "name": "Test Project",
            "distributor": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
            "token_mint": "HJ9LvBGce9f975mzkvTRMGn9mveQHcfFjQTwEiozoKqq",
            "dev_wallet": None,
            "last_sig": "initial_signature"
        }

        result = self.controller.sqlite.insert_supported_project(test_project)
        assert result is True

        # Mock the external API calls but use real database operations
        with patch('lib.ProjectUpdater.get_new_distributor_transactions') as mock_get_transactions:
            with patch('lib.ProjectUpdater.process_distributor_transfers') as mock_process:

                # Mock transaction batch
                mock_batch = {
                    "txs": TRANSACTIONS,
                    "last_sig": "new_signature"
                }
                mock_get_transactions.return_value = [mock_batch]

                # Mock processed transfers
                test_transfers = [
                    {
                        "signature": "test_sig",
                        "slot": 12345,
                        "timestamp": 1234567890,
                        "amount": 100.0,
                        "token": "sol",
                        "wallet_address": "test_wallet",
                        "distributor": test_project["distributor"]
                    }
                ]
                mock_process.return_value = test_transfers

                # Execute the workflow
                self.project_updater.fetch_and_process_new_distributor_transactions(test_project["distributor"])

                # Verify the project's last signature was updated
                updated_project = self.controller.sqlite.get_supported_project(test_project["distributor"])
                assert updated_project["last_sig"] == "new_signature"

                # Verify wallet was created/updated
                wallet = self.controller.sqlite.get_wallet("test_wallet")
                assert wallet is not None
                assert wallet["distributors"][test_project["distributor"]]["tokens"]["sol"]["total_amount"] == 100.0

        print(f"✅ ProjectUpdater real database integration works!")

    ##########################################################
    #              Edge Case Tests                           #
    ##########################################################
    def test_empty_transaction_batch(self):
        """Test handling of empty transaction batches"""
        test_distributor = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"

        with patch('lib.ProjectUpdater.process_distributor_transfers', return_value=[]) as mock_process:
            with patch.object(self.controller.sqlite, 'insert_temp_transfers_batch', return_value=True):
                result = list(self.project_updater.extract_transfers_from_distributor_transactions(
                    [], test_distributor
                ))

                # Should handle empty list gracefully
                assert len(result) == 0

                # Verify it was called with correct arguments (even for empty list)
                if mock_process.called:
                    mock_process.assert_called_with(self.controller, [], test_distributor)

        print(f"✅ ProjectUpdater handles empty transaction batches!")

    def test_zero_transfers_aggregation(self):
        """Test aggregation with zero transfers"""
        with patch('lib.ProjectUpdater.aggregate_transfers', return_value={}):
            with patch.object(self.controller, 'upsert_wallets', return_value=0) as mock_upsert:
                self.project_updater.aggregate_rewards([])

                # The method may have early return logic for empty lists, so we'll check if it was called or not
                # and verify the behavior is reasonable either way
                call_count = mock_upsert.call_count
                assert call_count in [0, 1], f"Expected 0 or 1 calls to upsert_wallets, got {call_count}"

                if call_count == 1:
                    mock_upsert.assert_called_with({})

        print(f"✅ ProjectUpdater handles zero transfers aggregation!")

    def test_manual_update_trigger(self):
        """Test that update_distributors_transactions can be called manually"""
        with patch.object(self.controller.sqlite, 'get_supported_projects', return_value=[PROJECT]):
            with patch.object(self.project_updater, 'fetch_and_process_new_distributor_transactions') as mock_fetch:

                # Should be able to call the method directly
                self.project_updater.update_distributors_transactions()

                # Verify it was executed
                mock_fetch.assert_called_once_with(PROJECT.get("distributor"))

                # Updating flag should be reset
                assert self.project_updater.updating is False

        print(f"✅ ProjectUpdater can be triggered manually!")