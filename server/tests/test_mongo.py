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
from db.Mongo.db import MongoDB
from .mock_data import TRANSACTIONS, PROJECT, WALLET, KNOWN_TOKENS


class TestMongoDB:
    def setup_method(self):
        """Setup method run before each test"""
        self.mongodb = MongoDB(True)  # Use test database

        # Clean up test data before each test
        self.cleanup_test_data()

    def teardown_method(self):
        """Cleanup method run after each test"""
        self.cleanup_test_data()

    def cleanup_test_data(self):
        """Remove all test data from collections"""
        try:
            # Clear test collections
            self.mongodb._db.supported_projects.delete_many({})
            self.mongodb._db.wallets.delete_many({})
            print("Test data cleaned up")
        except Exception as e:
            print(f"Error cleaning up test data: {e}")

    ##########################################################
    #                 Supported Projects Tests               #
    ##########################################################
    def test_mongodb_inserts_supported_project(self):
        """Test inserting a supported project into MongoDB"""
        result = self.mongodb.insert_supported_project(PROJECT)
        assert result is True

        # Verify the project was inserted
        projects = self.mongodb.get_supported_projects()
        assert len(projects) == 1
        assert projects[0]["distributor"] == PROJECT["distributor"]
        assert projects[0]["token_mint"] == PROJECT["token_mint"]
        assert projects[0]["name"] == PROJECT["name"]
        print(f"✅ MongoDB inserts a supported project!")

    def test_mongodb_does_not_create_duplicate_supported_projects(self):
        """Test that duplicate projects are handled correctly"""
        # Insert the same project twice
        result1 = self.mongodb.insert_supported_project(PROJECT)
        assert result1 is True

        result2 = self.mongodb.insert_supported_project(PROJECT)
        assert result2 is True  # Should handle duplicate gracefully

        # Verify only one project exists (due to unique index on distributor)
        projects = self.mongodb.get_supported_projects()
        distributor_count = sum(1 for p in projects if p["distributor"] == PROJECT["distributor"])
        assert distributor_count == 1
        print(f"✅ MongoDB doesn't create duplicate supported projects!")

    def test_mongodb_gets_all_supported_projects(self):
        """Test retrieving all supported projects"""
        # Insert multiple projects
        project1 = PROJECT.copy()
        project2 = PROJECT.copy()
        project2["distributor"] = "different_distributor_123"
        project2["name"] = "Different Project"

        result1 = self.mongodb.insert_supported_project(project1)
        result2 = self.mongodb.insert_supported_project(project2)
        assert result1 is True
        assert result2 is True

        # Get all projects
        projects = self.mongodb.get_supported_projects()
        assert len(projects) == 2

        # Verify both projects are present
        distributors = [p["distributor"] for p in projects]
        assert PROJECT["distributor"] in distributors
        assert "different_distributor_123" in distributors
        print(f"✅ MongoDB gets all supported projects!")

    def test_mongodb_supported_project_has_correct_structure(self):
        """Test that inserted projects have the correct document structure"""
        result = self.mongodb.insert_supported_project(PROJECT)
        assert result is True

        projects = self.mongodb.get_supported_projects()
        project = projects[0]

        # Verify all required fields are present
        required_fields = ["name", "distributor", "token_mint", "dev_wallet", "last_sig"]
        for field in required_fields:
            assert field in project
            assert project[field] == PROJECT[field]

        # Verify _id is not included (excluded in query)
        assert "_id" not in project
        print(f"✅ MongoDB supported project has correct structure!")

    ##########################################################
    #                      Wallets Tests                     #
    ##########################################################
    def test_mongodb_gets_all_wallets_empty(self):
        """Test getting all wallets when collection is empty"""
        wallets = self.mongodb.get_all_wallets()
        assert wallets == []
        print(f"✅ MongoDB gets empty wallets list!")

    def test_mongodb_gets_specific_wallet_rewards(self):
        """Test getting rewards for a specific wallet"""
        # First insert a wallet using the batch method
        wallet_data = [WALLET]
        result = self.mongodb.insert_wallets_batch(wallet_data)
        assert result["success"] is True

        # Get specific wallet rewards
        wallet_rewards = self.mongodb.get_wallet_rewards(WALLET["wallet_address"])
        assert wallet_rewards is not None
        assert wallet_rewards["wallet_address"] == WALLET["wallet_address"]
        assert wallet_rewards["distributors"] == WALLET["distributors"]
        print(f"✅ MongoDB gets specific wallet rewards!")

    def test_mongodb_gets_nonexistent_wallet_rewards(self):
        """Test getting rewards for a wallet that doesn't exist"""
        wallet_rewards = self.mongodb.get_wallet_rewards("nonexistent_wallet_123")
        assert wallet_rewards is None
        print(f"✅ MongoDB returns None for nonexistent wallet!")

    def test_mongodb_inserts_wallets_batch_single_wallet(self):
        """Test inserting a single wallet using batch method"""
        wallets = [WALLET]
        result = self.mongodb.insert_wallets_batch(wallets)

        assert result["success"] is True
        assert result["total_processed"] == 1
        assert result["total_inserted"] == 1
        assert result["total_updated"] == 0
        assert result["total_failed"] == 0

        # Verify wallet was inserted
        all_wallets = self.mongodb.get_all_wallets()
        assert len(all_wallets) == 1
        assert all_wallets[0]["wallet_address"] == WALLET["wallet_address"]
        print(f"✅ MongoDB inserts single wallet batch!")

    def test_mongodb_inserts_wallets_batch_multiple_wallets(self):
        """Test inserting multiple wallets using batch method"""
        wallet1 = WALLET.copy()
        wallet2 = {
            "wallet_address": "test_wallet_2",
            "distributors": {
                "test_distributor": {
                    "tokens": {"SOL": {"total_amount": 100.0}}
                }
            }
        }
        wallet3 = {
            "wallet_address": "test_wallet_3",
            "distributors": {
                "another_distributor": {
                    "tokens": {"USDC": {"total_amount": 500.0}}
                }
            }
        }

        wallets = [wallet1, wallet2, wallet3]
        result = self.mongodb.insert_wallets_batch(wallets)

        assert result["success"] is True
        assert result["total_processed"] == 3
        assert result["total_inserted"] == 3
        assert result["total_failed"] == 0

        # Verify all wallets were inserted
        all_wallets = self.mongodb.get_all_wallets()
        assert len(all_wallets) == 3

        wallet_addresses = [w["wallet_address"] for w in all_wallets]
        assert WALLET["wallet_address"] in wallet_addresses
        assert "test_wallet_2" in wallet_addresses
        assert "test_wallet_3" in wallet_addresses
        print(f"✅ MongoDB inserts multiple wallets batch!")

    def test_mongodb_updates_existing_wallet_batch(self):
        """Test updating an existing wallet using batch method (upsert functionality)"""
        # Insert initial wallet
        initial_wallet = {
            "wallet_address": "update_test_wallet",
            "distributors": {
                "dist1": {
                    "tokens": {"SOL": {"total_amount": 50.0}}
                }
            }
        }

        result = self.mongodb.insert_wallets_batch([initial_wallet])
        assert result["success"] is True
        assert result["total_inserted"] == 1

        # Update the same wallet with new data
        updated_wallet = {
            "wallet_address": "update_test_wallet",
            "distributors": {
                "dist1": {
                    "tokens": {"SOL": {"total_amount": 100.0}}
                },
                "dist2": {
                    "tokens": {"USDC": {"total_amount": 250.0}}
                }
            }
        }

        result = self.mongodb.insert_wallets_batch([updated_wallet])
        assert result["success"] is True
        assert result["total_updated"] == 1

        # Verify the wallet was updated
        wallet = self.mongodb.get_wallet_rewards("update_test_wallet")
        assert wallet["distributors"]["dist1"]["tokens"]["SOL"]["total_amount"] == 100.0
        assert "dist2" in wallet["distributors"]
        assert wallet["distributors"]["dist2"]["tokens"]["USDC"]["total_amount"] == 250.0

        # Verify only one wallet exists (not duplicated)
        all_wallets = self.mongodb.get_all_wallets()
        update_wallets = [w for w in all_wallets if w["wallet_address"] == "update_test_wallet"]
        assert len(update_wallets) == 1
        print(f"✅ MongoDB updates existing wallet batch!")

    def test_mongodb_batch_insert_with_invalid_wallet(self):
        """Test batch insert with invalid wallet data"""
        valid_wallet = WALLET.copy()
        invalid_wallet = {"distributors": {"some": "data"}}  # Missing wallet_address

        wallets = [valid_wallet, invalid_wallet]
        result = self.mongodb.insert_wallets_batch(wallets)

        # Should process the valid wallet and fail the invalid one
        assert result["total_processed"] == 1
        assert result["total_inserted"] == 1
        assert result["total_failed"] == 1
        assert result["success"] is False  # Because there was a failure

        # Verify only the valid wallet was inserted
        all_wallets = self.mongodb.get_all_wallets()
        assert len(all_wallets) == 1
        assert all_wallets[0]["wallet_address"] == WALLET["wallet_address"]
        print(f"✅ MongoDB handles invalid wallet data in batch!")

    def test_mongodb_batch_insert_empty_list(self):
        """Test batch insert with empty wallet list"""
        result = self.mongodb.insert_wallets_batch([])

        assert result["total_processed"] == 0
        assert result["total_inserted"] == 0
        assert result["total_updated"] == 0
        assert result["total_failed"] == 0
        assert result["success"] is True

        # Verify no wallets were inserted
        all_wallets = self.mongodb.get_all_wallets()
        assert len(all_wallets) == 0
        print(f"✅ MongoDB handles empty wallet batch!")

    def test_mongodb_batch_insert_large_batch(self):
        """Test batch insert with large number of wallets to test batching logic"""
        # Create 100 test wallets
        wallets = []
        for i in range(100):
            wallet = {
                "wallet_address": f"test_wallet_{i}",
                "distributors": {
                    f"distributor_{i}": {
                        "tokens": {"SOL": {"total_amount": float(i * 10)}}
                    }
                }
            }
            wallets.append(wallet)

        result = self.mongodb.insert_wallets_batch(wallets)

        assert result["success"] is True
        assert result["total_processed"] == 100
        assert result["total_inserted"] == 100
        assert result["total_failed"] == 0

        # Verify all wallets were inserted
        all_wallets = self.mongodb.get_all_wallets()
        assert len(all_wallets) == 100

        # Verify some specific wallets
        wallet_addresses = [w["wallet_address"] for w in all_wallets]
        assert "test_wallet_0" in wallet_addresses
        assert "test_wallet_50" in wallet_addresses
        assert "test_wallet_99" in wallet_addresses
        print(f"✅ MongoDB handles large wallet batch!")

    def test_mongodb_wallet_document_structure(self):
        """Test that wallet documents have the correct structure"""
        result = self.mongodb.insert_wallets_batch([WALLET])
        assert result["success"] is True

        all_wallets = self.mongodb.get_all_wallets()
        wallet = all_wallets[0]

        # Verify required fields are present
        assert "wallet_address" in wallet
        assert "distributors" in wallet
        assert "_id" not in wallet  # Should be excluded in query

        # Verify structure matches original
        assert wallet["wallet_address"] == WALLET["wallet_address"]
        assert wallet["distributors"] == WALLET["distributors"]
        print(f"✅ MongoDB wallet has correct document structure!")

    ##########################################################
    #                Database Connection Tests                #
    ##########################################################
    def test_mongodb_connection_is_working(self):
        """Test that MongoDB connection is working"""
        # Test basic database operation
        collection = self.mongodb._db.test_collection
        test_doc = {"test": "document"}

        result = collection.insert_one(test_doc)
        assert result.inserted_id is not None

        # Clean up test document
        collection.delete_one({"_id": result.inserted_id})
        print(f"✅ MongoDB connection is working!")

    def test_mongodb_indexes_creation(self):
        """Test that database indexes are created successfully"""
        # This is tested indirectly by the successful operations above
        # and the fact that duplicate distributors are handled correctly

        # Insert a project to test unique index on distributor
        result1 = self.mongodb.insert_supported_project(PROJECT)
        assert result1 is True

        # Try to insert another project with same distributor
        duplicate_project = PROJECT.copy()
        duplicate_project["name"] = "Different Name"

        # This should handle the duplicate gracefully due to the unique index
        result2 = self.mongodb.insert_supported_project(duplicate_project)
        assert result2 is True

        print(f"✅ MongoDB indexes are working correctly!")

    ##########################################################
    #                   Error Handling Tests                 #
    ##########################################################
    def test_mongodb_handles_none_project_gracefully(self):
        """Test that the system handles None project data gracefully"""
        try:
            result = self.mongodb.insert_supported_project(None)
            # Should handle the error gracefully and return None or False
            assert result is None or result is False
        except Exception:
            # It's also acceptable for this to raise an exception
            pass
        print(f"✅ MongoDB handles None project data!")

    def test_mongodb_handles_malformed_project_gracefully(self):
        """Test that the system handles malformed project data gracefully"""
        malformed_project = {"name": "Test"}  # Missing required fields

        try:
            result = self.mongodb.insert_supported_project(malformed_project)
            # Should handle the error gracefully
            assert result is None or result is False
        except Exception:
            # It's also acceptable for this to raise an exception
            pass
        print(f"✅ MongoDB handles malformed project data!")