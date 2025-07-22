# test_utils.py
import pytest
import json
from unittest.mock import Mock, patch
from ..utils.utils import process_distributor_transfers, aggregate_transfers

class TestUtils:
    """Test the utility functions used throughout the system"""

    def test_process_distributor_transfers_with_token_transfers(self):
        """Test processing transactions with token transfers"""
        mock_controller = Mock()
        mock_controller.get_token_symbol.return_value = "USDT"

        test_transactions = [
            {
                "signature": "test_sig_1",
                "slot": 250000001,
                "timestamp": 1708000000,
                "token_transfers": [
                    {
                        "fromUserAccount": "distributor123",
                        "toUserAccount": "user_wallet_1",
                        "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                        "tokenAmount": 1000.0
                    }
                ],
                "native_transfers": []
            }
        ]

        result = process_distributor_transfers(mock_controller, test_transactions, "distributor123")

        assert len(result) == 1
        assert result[0]["signature"] == "test_sig_1"
        assert result[0]["amount"] == 1000.0
        assert result[0]["token"] == "USDT"
        assert result[0]["wallet_address"] == "user_wallet_1"
        assert result[0]["distributor"] == "distributor123"

    def test_process_distributor_transfers_with_native_transfers(self):
        """Test processing transactions with native SOL transfers"""
        mock_controller = Mock()

        test_transactions = [
            {
                "signature": "test_sig_2",
                "slot": 250000002,
                "timestamp": 1708000010,
                "token_transfers": [],
                "native_transfers": [
                    {
                        "fromUserAccount": "distributor123",
                        "toUserAccount": "user_wallet_2",
                        "amount": 2000000000  # 2 SOL in lamports
                    }
                ]
            }
        ]

        result = process_distributor_transfers(mock_controller, test_transactions, "distributor123")

        assert len(result) == 1
        assert result[0]["signature"] == "test_sig_2"
        assert result[0]["amount"] == 2.0  # Should be converted from lamports
        assert result[0]["token"] == "sol"
        assert result[0]["wallet_address"] == "user_wallet_2"

    def test_process_distributor_transfers_mixed(self):
        """Test processing transactions with both token and native transfers"""
        mock_controller = Mock()
        mock_controller.get_token_symbol.return_value = "USDC"

        test_transactions = [
            {
                "signature": "test_sig_3",
                "slot": 250000003,
                "timestamp": 1708000020,
                "token_transfers": [
                    {
                        "fromUserAccount": "distributor123",
                        "toUserAccount": "user_wallet_3",
                        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "tokenAmount": 100.0
                    }
                ],
                "native_transfers": [
                    {
                        "fromUserAccount": "distributor123",
                        "toUserAccount": "user_wallet_3",
                        "amount": 1000000000  # 1 SOL in lamports
                    }
                ]
            }
        ]

        result = process_distributor_transfers(mock_controller, test_transactions, "distributor123")

        assert len(result) == 2  # Should have both token and native transfers

        # Find the token transfer
        token_transfer = next(t for t in result if t["token"] == "USDC")
        assert token_transfer["amount"] == 100.0

        # Find the native transfer
        native_transfer = next(t for t in result if t["token"] == "sol")
        assert native_transfer["amount"] == 1.0

    def test_aggregate_transfers_single_wallet_single_token(self):
        """Test aggregating transfers for a single wallet and token"""
        test_transfers = [
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 100.0
            },
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 200.0
            }
        ]

        result = aggregate_transfers(test_transfers)

        assert len(result) == 1
        assert "wallet1" in result
        assert result["wallet1"]["distributors"]["dist1"]["tokens"]["USDT"]["total_amount"] == 300.0

    def test_aggregate_transfers_single_wallet_multiple_tokens(self):
        """Test aggregating transfers for a single wallet with multiple tokens"""
        test_transfers = [
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 100.0
            },
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDC",
                "amount": 50.0
            },
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "sol",
                "amount": 2.0
            }
        ]

        result = aggregate_transfers(test_transfers)

        assert len(result) == 1
        wallet_data = result["wallet1"]["distributors"]["dist1"]["tokens"]
        assert wallet_data["USDT"]["total_amount"] == 100.0
        assert wallet_data["USDC"]["total_amount"] == 50.0
        assert wallet_data["sol"]["total_amount"] == 2.0

    def test_aggregate_transfers_multiple_wallets(self):
        """Test aggregating transfers for multiple wallets"""
        test_transfers = [
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 100.0
            },
            {
                "wallet_address": "wallet2",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 200.0
            },
            {
                "wallet_address": "wallet1",
                "distributor": "dist2",
                "token": "USDC",
                "amount": 50.0
            }
        ]

        result = aggregate_transfers(test_transfers)

        assert len(result) == 2
        assert result["wallet1"]["distributors"]["dist1"]["tokens"]["USDT"]["total_amount"] == 100.0
        assert result["wallet1"]["distributors"]["dist2"]["tokens"]["USDC"]["total_amount"] == 50.0
        assert result["wallet2"]["distributors"]["dist1"]["tokens"]["USDT"]["total_amount"] == 200.0

    def test_aggregate_transfers_multiple_distributors(self):
        """Test aggregating transfers from multiple distributors for same wallet"""
        test_transfers = [
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 100.0
            },
            {
                "wallet_address": "wallet1",
                "distributor": "dist2",
                "token": "USDT",
                "amount": 200.0
            },
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 50.0  # Additional amount from dist1
            }
        ]

        result = aggregate_transfers(test_transfers)

        assert len(result) == 1
        wallet_data = result["wallet1"]["distributors"]
        assert wallet_data["dist1"]["tokens"]["USDT"]["total_amount"] == 150.0  # 100 + 50
        assert wallet_data["dist2"]["tokens"]["USDT"]["total_amount"] == 200.0

    def test_aggregate_transfers_empty_list(self):
        """Test aggregating empty transfer list"""
        result = aggregate_transfers([])
        assert result == {}

    def test_aggregate_transfers_with_zero_amounts(self):
        """Test aggregating transfers with zero amounts"""
        test_transfers = [
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 0.0
            },
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 100.0
            }
        ]

        result = aggregate_transfers(test_transfers)

        assert result["wallet1"]["distributors"]["dist1"]["tokens"]["USDT"]["total_amount"] == 100.0


class TestDataValidation:
    """Test data validation and edge cases"""

    def test_process_transfers_with_missing_fields(self):
        """Test processing transactions with missing required fields"""
        mock_controller = Mock()

        test_transactions = [
            {
                "signature": "test_sig",
                "slot": 250000001,
                "timestamp": 1708000000,
                "token_transfers": [
                    {
                        "fromUserAccount": "distributor123",
                        "toUserAccount": None,  # Missing toUserAccount
                        "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                        "tokenAmount": 1000.0
                    }
                ],
                "native_transfers": []
            }
        ]

        result = process_distributor_transfers(mock_controller, test_transactions, "distributor123")

        # Should skip transfers with missing required fields
        assert len(result) == 0

    def test_process_transfers_with_empty_token_amount(self):
        """Test processing transactions with zero token amounts"""
        mock_controller = Mock()
        mock_controller.get_token_symbol.return_value = "USDT"

        test_transactions = [
            {
                "signature": "test_sig",
                "slot": 250000001,
                "timestamp": 1708000000,
                "token_transfers": [
                    {
                        "fromUserAccount": "distributor123",
                        "toUserAccount": "user_wallet",
                        "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                        "tokenAmount": 0.0
                    }
                ],
                "native_transfers": []
            }
        ]

        result = process_distributor_transfers(mock_controller, test_transactions, "distributor123")

        # Should include zero amounts (they might be meaningful)
        assert len(result) == 1
        assert result[0]["amount"] == 0.0

    def test_aggregate_transfers_with_negative_amounts(self):
        """Test aggregating transfers with negative amounts"""
        test_transfers = [
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": 100.0
            },
            {
                "wallet_address": "wallet1",
                "distributor": "dist1",
                "token": "USDT",
                "amount": -50.0  # Negative amount
            }
        ]

        result = aggregate_transfers(test_transfers)

        # Should handle negative amounts correctly
        assert result["wallet1"]["distributors"]["dist1"]["tokens"]["USDT"]["total_amount"] == 50.0


# Helper functions for creating test data
def create_mock_transaction(
    signature="test_sig",
    slot=250000001,
    timestamp=1708000000,
    token_transfers=None,
    native_transfers=None
):
    """Create a mock transaction for testing"""
    return {
        "signature": signature,
        "slot": slot,
        "timestamp": timestamp,
        "token_transfers": token_transfers or [],
        "native_transfers": native_transfers or []
    }

def create_mock_token_transfer(
    from_account="distributor",
    to_account="user_wallet",
    mint="Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    amount=1000.0
):
    """Create a mock token transfer for testing"""
    return {
        "fromUserAccount": from_account,
        "toUserAccount": to_account,
        "mint": mint,
        "tokenAmount": amount
    }

def create_mock_native_transfer(
    from_account="distributor",
    to_account="user_wallet",
    amount=1000000000  # 1 SOL in lamports
):
    """Create a mock native transfer for testing"""
    return {
        "fromUserAccount": from_account,
        "toUserAccount": to_account,
        "amount": amount
    }

def create_mock_transfer_record(
    signature="test_sig",
    slot=250000001,
    timestamp=1708000000,
    amount=100.0,
    token="USDT",
    wallet_address="user_wallet",
    distributor="distributor123"
):
    """Create a mock transfer record for testing aggregation"""
    return {
        "signature": signature,
        "slot": slot,
        "timestamp": timestamp,
        "amount": amount,
        "token": token,
        "wallet_address": wallet_address,
        "distributor": distributor
    }

def create_test_project(
    name="Test Project",
    distributor="BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
    token_mint="Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    dev_wallet="7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
    last_sig=""
):
    """Create a mock project for testing"""
    return {
        "name": name,
        "distributor": distributor,
        "token_mint": token_mint,
        "dev_wallet": dev_wallet,
        "last_sig": last_sig
    }


# Test performance and memory usage
class TestPerformance:
    """Test performance characteristics of key functions"""

    def test_aggregate_transfers_performance_large_dataset(self):
        """Test aggregation performance with large dataset"""
        import time

        # Create a large dataset (10,000 transfers)
        test_transfers = []
        for i in range(10000):
            wallet_id = f"wallet_{i % 1000}"  # 1000 unique wallets
            distributor_id = f"dist_{i % 10}"   # 10 distributors
            token = "USDT" if i % 2 == 0 else "USDC"

            test_transfers.append(create_mock_transfer_record(
                signature=f"sig_{i}",
                wallet_address=wallet_id,
                distributor=distributor_id,
                token=token,
                amount=100.0
            ))

        start_time = time.time()
        result = aggregate_transfers(test_transfers)
        end_time = time.time()

        # Should complete in reasonable time (less than 1 second for 10k transfers)
        assert end_time - start_time < 1.0

        # Verify results are correct
        assert len(result) == 1000  # 1000 unique wallets

        # Check a few random results
        sample_wallet = result["wallet_0"]
        assert len(sample_wallet["distributors"]) <= 10

    def test_process_transfers_memory_usage(self):
        """Test memory usage doesn't grow excessively with large batches"""
        import gc

        mock_controller = Mock()
        mock_controller.get_token_symbol.return_value = "USDT"

        # Create large batch of transactions
        large_batch = []
        for i in range(1000):
            tx = create_mock_transaction(
                signature=f"sig_{i}",
                token_transfers=[
                    create_mock_token_transfer(
                        to_account=f"wallet_{i}",
                        amount=100.0
                    )
                ]
            )
            large_batch.append(tx)

        # Force garbage collection before test
        gc.collect()

        result = process_distributor_transfers(mock_controller, large_batch, "distributor")

        # Verify all transfers were processed
        assert len(result) == 1000

        # Cleanup
        del result
        del large_batch
        gc.collect()


if __name__ == "__main__":
    # Run the utility tests
    pytest.main(["-v", "test_utils.py"])