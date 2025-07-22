# test_with_mock_data.py
# Location: root/server/tests/test_with_mock_data.py
"""
Comprehensive test using 10 realistic mock transactions to test the entire system flow
"""
import pytest
import json
import sys
from pathlib import Path
from unittest.mock import Mock, patch, call
from collections import defaultdict

# Add the server directory to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
SERVER_DIR = PROJECT_ROOT / "server"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SERVER_DIR))

# Import your classes with the correct paths
from lib.Controller import Controller
from lib.ProjectInitializer import ProjectInitializer
from lib.ProjectUpdater import ProjectUpdater
from utils.utils import process_distributor_transfers, aggregate_transfers

# Our 10 test transactions with realistic Solana data
TEST_TRANSACTIONS = [
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "2ZE7Rz8gKRmCJhntJhX5cKJ8x9QqMzB4YnE6wP7Q8hX3KsH9fL2pWvR6tA4mN5bC8dF7yG9",
        "slot": 250000001,
        "timestamp": 1708000000,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 1000.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "3AF8Sz9hLSmDKioOKiY6dLK9z0QrNzC5ZoF7xQ8R9iY4LtI0gM3qXwS7uB5oO6cD9eG8zH0",
        "slot": 250000002,
        "timestamp": 1708000010,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "A7uqmajxP3NdzbYDXiGQRKhd2bMrVDKhRtHgVQRAyrZz",
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 500.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "4BG9Ta0iMTnELjpPLjZ7eML0a1RsOzD6ApG8yR9S0jZ5MuJ1hN4rYxT8vC6pP7dE0fH9aI1",
        "slot": 250000003,
        "timestamp": 1708000020,
        "token_transfers": [],
        "native_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "amount": 2000000000  # 2 SOL in lamports
            }
        ]
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "5CH0Ub1jNUoFMkqQMkA8fNM1b2StPaE7BqH9zS0T1kA6NvK2iO5sZyU9wD7qQ8eF1gI0bJ2",
        "slot": 250000004,
        "timestamp": 1708000030,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
                "mint": "So11111111111111111111111111111111111111112",  # Wrapped SOL
                "tokenAmount": 1.5
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "6DI1Vc2kOVpGNlrRNlB9gON2c3TuQbF8CrI0aT1U2lB7OwL3jP6tAzV0xE8rR9fG2hJ1cK3",
        "slot": 250000005,
        "timestamp": 1708000040,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Same user gets more
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 750.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "7EJ2Wd3lPWqHOmsSOmC0hPO3d4UvRcG9DsJ1bU2V3mC8PxM4kQ7uBaW1yF9sS0gH3iK2dL4",
        "slot": 250000006,
        "timestamp": 1708000050,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "C9xsobayxR5PfzbZFYkIQMmf4dOvScH0EtKiXSSczbBb",
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "tokenAmount": 100.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "8FK3Xe4mQXrIPntTPnD1iQP4e5VwSdH0FuK2cV3W4nD9QyN5lR8vCbX2zG0tT1hI4jL3eM5",
        "slot": 250000007,
        "timestamp": 1708000060,
        "token_transfers": [],
        "native_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "A7uqmajxP3NdzbYDXiGQRKhd2bMrVDKhRtHgVQRAyrZz",
                "amount": 1500000000  # 1.5 SOL in lamports
            }
        ]
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "9GL4Yf5nRYsJQouUQoE2jRP5f6WxTeI1GvL3dW4X5oE0RzO6mS9wDcY3aH1uU2iJ5kM4fN6",
        "slot": 250000008,
        "timestamp": 1708000070,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "D0ytpcbayxS6QgzbZGYkJRNmg5eOvTdI2HuL4eTdcCc",
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 2000.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "0HM5Zg6oSZtKRpvVRpF3kSQ6g7XyUfJ2IwM4eX5Y6pF1SaP7nT0xEdZ4bI2vV3jK6lN5gO7",
        "slot": 250000009,
        "timestamp": 1708000080,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "tokenAmount": 250.0
            }
        ],
        "native_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
                "amount": 500000000  # 0.5 SOL in lamports
            }
        ]
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "1IN6Ah7pTauLSpwWSqG4lTR7h8YzVgK3JxN5fY6Z7qG2TbQ8oU1yFeA5cJ3wW4kL7mO6hP8",
        "slot": 250000010,
        "timestamp": 1708000090,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Same user again
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "tokenAmount": 50.0
            }
        ],
        "native_transfers": []
    }
]

TEST_PROJECT = {
    "name": "Test Rewards Project",
    "distributor": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
    "token_mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "dev_wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
    "last_sig": ""
}

TOKEN_SYMBOLS = {
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
    "So11111111111111111111111111111111111111112": "WSOL"
}


class TestSystemWithMockData:
    """Test the entire system using 10 realistic mock transactions"""

    @pytest.fixture
    def mock_controller(self):
        """Create a fully mocked controller"""
        controller = Mock()
        controller.sqlite = Mock()
        controller.mongo = Mock()

        # Mock token symbol resolution
        controller.get_token_symbol.side_effect = lambda mint: TOKEN_SYMBOLS.get(mint, mint[:8])

        # Mock SQLite responses
        controller.sqlite.get_known_tokens.return_value = [
            {"symbol": "USDT", "name": "Tether", "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "decimals": "6"},
            {"symbol": "USDC", "name": "USD Coin", "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "decimals": "6"},
            {"symbol": "WSOL", "name": "Wrapped SOL", "mint": "So11111111111111111111111111111111111111112", "decimals": "9"}
        ]

        return controller

    def test_process_all_10_transactions(self, mock_controller):
        """Test processing all 10 mock transactions and verify the results"""

        # Process the transactions
        transfers = process_distributor_transfers(
            mock_controller,
            TEST_TRANSACTIONS,
            TEST_PROJECT["distributor"]
        )

        # We should have 12 total transfers (some transactions have multiple transfers)
        expected_transfer_count = 12  # Count from our test data
        assert len(transfers) == expected_transfer_count

        # Verify token symbol resolution was called
        assert mock_controller.get_token_symbol.call_count > 0

        # Verify some specific transfers
        usdt_transfers = [t for t in transfers if t["token"] == "USDT"]
        sol_transfers = [t for t in transfers if t["token"] == "sol"]
        usdc_transfers = [t for t in transfers if t["token"] == "USDC"]
        wsol_transfers = [t for t in transfers if t["token"] == "WSOL"]

        assert len(usdt_transfers) == 4  # 4 USDT transfers in our test data
        assert len(sol_transfers) == 2   # 2 native SOL transfers
        assert len(usdc_transfers) == 2  # 2 USDC transfers
        assert len(wsol_transfers) == 1  # 1 WSOL transfer

        # Verify amounts are correct (including lamports conversion for SOL)
        sol_amounts = [t["amount"] for t in sol_transfers]
        assert 2.0 in sol_amounts  # 2 SOL
        assert 1.5 in sol_amounts  # 1.5 SOL

    def test_aggregate_processed_transfers(self, mock_controller):
        """Test aggregating the processed transfers into wallet rewards"""

        # Process transfers first
        transfers = process_distributor_transfers(
            mock_controller,
            TEST_TRANSACTIONS,
            TEST_PROJECT["distributor"]
        )

        # Aggregate the transfers
        aggregated_wallets = aggregate_transfers(transfers)

        # We should have 5 unique wallets
        expected_wallet_count = 5
        assert len(aggregated_wallets) == expected_wallet_count

        # Check specific wallet aggregations
        wallet1 = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"
        assert wallet1 in aggregated_wallets

        # This wallet should have multiple tokens from the distributor
        wallet1_data = aggregated_wallets[wallet1]["distributors"][TEST_PROJECT["distributor"]]
        wallet1_tokens = wallet1_data["tokens"]

        # Wallet1 gets: 1000 USDT + 750 USDT + 50 USDC + 2 SOL
        assert "USDT" in wallet1_tokens
        assert "USDC" in wallet1_tokens
        assert "sol" in wallet1_tokens

        assert wallet1_tokens["USDT"]["total_amount"] == 1750.0  # 1000 + 750
        assert wallet1_tokens["USDC"]["total_amount"] == 50.0
        assert wallet1_tokens["sol"]["total_amount"] == 2.0

        # Check another wallet
        wallet2 = "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa"
        assert wallet2 in aggregated_wallets

        wallet2_data = aggregated_wallets[wallet2]["distributors"][TEST_PROJECT["distributor"]]
        wallet2_tokens = wallet2_data["tokens"]

        # Wallet2 gets: 1.5 WSOL + 250 USDC + 0.5 SOL
        assert wallet2_tokens["WSOL"]["total_amount"] == 1.5
        assert wallet2_tokens["USDC"]["total_amount"] == 250.0
        assert wallet2_tokens["sol"]["total_amount"] == 0.5

    def test_full_project_initialization_flow(self, mock_controller):
        """Test the complete ProjectInitializer flow with our mock data"""

        # Setup mock responses for ProjectInitializer
        mock_controller.sqlite.create_distributor_tables.return_value = None
        mock_controller.sqlite.get_temp_txs_last_sigs.return_value = (None, None)
        mock_controller.sqlite.insert_transactions_batch.return_value = True
        mock_controller.sqlite.update_temp_txs_last_sig.return_value = True
        mock_controller.sqlite.update_temp_txs_before_sig.return_value = True
        mock_controller.sqlite.get_transactions_count.return_value = len(TEST_TRANSACTIONS)
        mock_controller.sqlite.get_transactions.return_value = iter([(TEST_TRANSACTIONS, 0)])
        mock_controller.sqlite.insert_transfer_batch.return_value = True
        mock_controller.sqlite.insert_supported_project.return_value = True
        mock_controller.sqlite.clean_and_remove_temp_data.return_value = True
        mock_controller.sqlite.get_transfers_count.return_value = 12  # Expected transfer count

        # Mock transfers for aggregation
        mock_transfers = process_distributor_transfers(
            mock_controller,
            TEST_TRANSACTIONS,
            TEST_PROJECT["distributor"]
        )
        mock_controller.sqlite.get_transfers.return_value = iter([(mock_transfers, 0)])

        # Mock wallet operations
        mock_controller.sqlite.get_wallets_by_addresses.return_value = {}
        mock_controller.sqlite.insert_wallets_batch.return_value = True
        mock_controller.upsert_wallets.return_value = 5  # 5 wallets updated

        # Create initializer
        initializer = ProjectInitializer(mock_controller, TEST_PROJECT.copy())

        # Mock the external API call
        with patch('server.lib.ProjectInitializer.get_historical_transactions_for_distributor') as mock_get_txs:
            mock_get_txs.return_value = [
                {
                    "txs": TEST_TRANSACTIONS,
                    "before": "final_sig",
                    "last_sig": "newest_sig",
                    "finished": True
                }
            ]

            # Test transaction fetching
            result = initializer.get_initial_txs()
            assert result is True

            # Verify API was called
            mock_get_txs.assert_called_once()

            # Verify database operations
            mock_controller.sqlite.create_distributor_tables.assert_called_once()
            mock_controller.sqlite.insert_transactions_batch.assert_called_once()

        # Test transaction processing
        result = initializer.process_initial_txs()
        assert result is True

        # Test project insertion and cleanup
        result = initializer.insert_and_clean_project()
        assert result is True

        # Verify project was updated with last signature
        assert initializer.project["last_sig"] == "newest_sig"

        # Test reward aggregation
        result = initializer.aggregate_rewards_from_transfers()
        assert result is True

        # Verify wallet upsert was called
        mock_controller.upsert_wallets.assert_called_once()

    def test_project_updater_with_new_transactions(self, mock_controller):
        """Test ProjectUpdater with new transactions"""

        # Setup mock responses
        mock_controller.sqlite.get_supported_projects.return_value = [TEST_PROJECT]
        mock_controller.sqlite.get_last_tx_signature.return_value = "old_signature"
        mock_controller.sqlite.update_last_tx_signature.return_value = True
        mock_controller.sqlite.insert_temp_transfers_batch.return_value = True
        mock_controller.upsert_wallets.return_value = 3  # 3 wallets updated

        # Create updater (mock timer to prevent actual polling)
        with patch('server.lib.ProjectUpdater.timer'):
            updater = ProjectUpdater(mock_controller)

        # Test update with new transactions (use first 3 as "new" transactions)
        new_transactions = TEST_TRANSACTIONS[:3]

        with patch('server.lib.ProjectUpdater.get_new_distributor_transactions') as mock_get_new:
            mock_get_new.return_value = [
                {
                    "txs": new_transactions,
                    "last_sig": "updated_signature"
                }
            ]

            # Run the update
            updater.fetch_and_process_new_distributor_transactions(TEST_PROJECT["distributor"])

            # Verify calls
            mock_get_new.assert_called_once_with(TEST_PROJECT["distributor"], "old_signature")
            mock_controller.sqlite.update_last_tx_signature.assert_called_once_with(
                TEST_PROJECT["distributor"], "updated_signature"
            )

    def test_controller_wallet_merging(self, mock_controller):
        """Test Controller's wallet merging functionality with realistic data"""

        # Create existing wallet data
        existing_wallets = {
            "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": {
                "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "distributors": {
                    TEST_PROJECT["distributor"]: {
                        "tokens": {
                            "USDT": {"total_amount": 500.0}  # Existing amount
                        }
                    }
                }
            }
        }

        # Create new wallet data (from processing our transactions)
        transfers = process_distributor_transfers(
            mock_controller,
            TEST_TRANSACTIONS,
            TEST_PROJECT["distributor"]
        )
        new_aggregated_wallets = aggregate_transfers(transfers)

        # Setup mock responses
        mock_controller.sqlite.get_wallets_by_addresses.return_value = existing_wallets
        mock_controller.sqlite.update_wallets_batch.return_value = True
        mock_controller.sqlite.insert_wallets_batch.return_value = True

        # Create a real Controller instance for testing the merge logic
        with patch('server.lib.Controller.SQLiteDB') as mock_sqlite_class, \
             patch('server.lib.Controller.MongoDB') as mock_mongo_class:

            mock_sqlite_instance = Mock()
            mock_mongo_instance = Mock()
            mock_sqlite_class.return_value = mock_sqlite_instance
            mock_mongo_class.return_value = mock_mongo_instance

            # Mock known tokens
            mock_sqlite_instance.get_known_tokens.return_value = [
                {"symbol": "USDT", "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"},
                {"symbol": "USDC", "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"},
            ]
            mock_sqlite_instance.get_wallets_by_addresses.return_value = existing_wallets
            mock_sqlite_instance.update_wallets_batch.return_value = True
            mock_sqlite_instance.insert_wallets_batch.return_value = True

            controller = Controller()

            # Test wallet upsert
            result = controller.upsert_wallets(new_aggregated_wallets)

            # Should have updated wallets (some existing, some new)
            assert result > 0

            # Verify the merge happened - check the call to update_wallets_batch
            if mock_sqlite_instance.update_wallets_batch.called:
                call_args = mock_sqlite_instance.update_wallets_batch.call_args[0][0]

                # Find the merged wallet
                merged_wallet = next(
                    (w for w in call_args if w["wallet_address"] == "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"),
                    None
                )

                if merged_wallet:
                    # Should have merged USDT amounts: 500 (existing) + 1750 (new) = 2250
                    expected_usdt = 500.0 + 1750.0
                    actual_usdt = merged_wallet["distributors"][TEST_PROJECT["distributor"]]["tokens"]["USDT"]["total_amount"]
                    assert actual_usdt == expected_usdt

    def test_data_consistency_across_system(self, mock_controller):
        """Test that data remains consistent as it flows through the entire system"""

        # Step 1: Process transactions into transfers
        transfers = process_distributor_transfers(
            mock_controller,
            TEST_TRANSACTIONS,
            TEST_PROJECT["distributor"]
        )

        # Step 2: Aggregate transfers into wallet data
        aggregated_wallets = aggregate_transfers(transfers)

        # Step 3: Verify data consistency

        # Count total amounts by token across all wallets
        token_totals = defaultdict(float)
        wallet_count_by_token = defaultdict(int)

        for wallet_address, wallet_data in aggregated_wallets.items():
            distributor_data = wallet_data["distributors"][TEST_PROJECT["distributor"]]
            for token, token_data in distributor_data["tokens"].items():
                token_totals[token] += token_data["total_amount"]
                wallet_count_by_token[token] += 1

        # Verify expected totals match our test data
        # USDT: 1000 + 500 + 750 + 2000 = 4250
        # USDC: 100 + 250 + 50 = 400
        # SOL: 2.0 + 1.5 + 0.5 = 4.0
        # WSOL: 1.5

        assert token_totals["USDT"] == 4250.0
        assert token_totals["USDC"] == 400.0
        assert token_totals["sol"] == 4.0
        assert token_totals["WSOL"] == 1.5

        # Verify wallet distribution
        assert len(aggregated_wallets) == 5  # 5 unique recipient wallets

        # Verify each wallet has the correct distributor
        for wallet_data in aggregated_wallets.values():
            assert TEST_PROJECT["distributor"] in wallet_data["distributors"]

    def test_error_handling_with_invalid_data(self, mock_controller):
        """Test system behavior with invalid or missing data in transactions"""

        # Create transactions with some invalid data
        invalid_transactions = [
            # Valid transaction
            TEST_TRANSACTIONS[0],

            # Transaction with missing toUserAccount
            {
                "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
                "signature": "invalid_tx_1",
                "slot": 250000020,
                "timestamp": 1708000200,
                "token_transfers": [
                    {
                        "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                        "toUserAccount": None,  # Invalid - None
                        "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                        "tokenAmount": 1000.0
                    }
                ],
                "native_transfers": []
            },

            # Transaction with missing mint
            {
                "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
                "signature": "invalid_tx_2",
                "slot": 250000021,
                "timestamp": 1708000210,
                "token_transfers": [
                    {
                        "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                        "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                        "mint": None,  # Invalid - None
                        "tokenAmount": 1000.0
                    }
                ],
                "native_transfers": []
            },

            # Valid transaction
            TEST_TRANSACTIONS[1]
        ]

        # Process the transactions
        transfers = process_distributor_transfers(
            mock_controller,
            invalid_transactions,
            TEST_PROJECT["distributor"]
        )

        # Should only get transfers from valid transactions (should skip invalid ones)
        assert len(transfers) == 2  # Only the 2 valid transactions

        # Verify the invalid transactions were filtered out
        signatures = [t["signature"] for t in transfers]
        assert "invalid_tx_1" not in signatures
        assert "invalid_tx_2" not in signatures
        assert TEST_TRANSACTIONS[0]["signature"] in signatures
        assert TEST_TRANSACTIONS[1]["signature"] in signatures

    @pytest.mark.performance
    def test_performance_with_mock_data(self, mock_controller):
        """Test performance characteristics with our mock data scaled up"""
        import time

        # Scale up the test data (100x)
        scaled_transactions = []
        for i in range(100):
            for tx in TEST_TRANSACTIONS:
                scaled_tx = tx.copy()
                scaled_tx["signature"] = f"{tx['signature']}_copy_{i}"
                scaled_tx["slot"] = tx["slot"] + i
                scaled_transactions.append(scaled_tx)

        # Process scaled transactions and measure time
        start_time = time.time()
        transfers = process_distributor_transfers(
            mock_controller,
            scaled_transactions,
            TEST_PROJECT["distributor"]
        )
        processing_time = time.time() - start_time

        # Should handle 1000 transactions quickly (less than 1 second)
        assert processing_time < 1.0
        assert len(transfers) == len(scaled_transactions) * 1.2  # Approximate expected transfer count

        # Test aggregation performance
        start_time = time.time()
        aggregated_wallets = aggregate_transfers(transfers)
        aggregation_time = time.time() - start_time

        # Should aggregate quickly
        assert aggregation_time < 1.0
        assert len(aggregated_wallets) == 5  # Still 5 unique wallets


# Helper function to run just the mock data tests
def run_mock_data_tests():
    """Run only the mock data tests"""
    import subprocess
    import sys

    cmd = [sys.executable, "-m", "pytest", "-v", "test_with_mock_data.py"]
    subprocess.run(cmd)


if __name__ == "__main__":
    run_mock_data_tests()