# server/tests/test_real_integration.py
"""
Real Integration Tests using actual database connections and real transaction data
This tests the complete system end-to-end with your test databases
"""
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

# Load environment variables (will use your test database settings)
load_dotenv()

# Import your real classes
from lib.Controller import Controller
from lib.ProjectInitializer import ProjectInitializer
from lib.ProjectUpdater import ProjectUpdater
from utils.utils import process_distributor_transfers, aggregate_transfers
from mock_data import TRANSACTIONS, PROJECT, WALLET, KNOWN_TOKENS


class TestRealIntegration:
    """Integration tests using real database connections and actual transaction data"""
    def __init__(self):
        self.controller = Controller(testing=True)

    def insert_known_tokens(self):
        try:
            pass
        except:
            raise Exception("There was an error when testing insert known tokens test")


    def get_known_tokens(self):
        """Create a real Controller instance with actual database connections"""
        print("\n🔌 Connecting to real test databases...")
        controller = Controller()
        print(f"✅ Connected! Loaded {len(controller.known_tokens)} known tokens")
        return controller


    def test_real_controller_initialization(self, real_controller):
        """Test that the real Controller initializes with actual databases"""
        # Test SQLite connection
        assert real_controller.sqlite is not None
        assert hasattr(real_controller.sqlite, 'get_known_tokens')

        # Test MongoDB connection
        assert real_controller.mongo is not None
        assert hasattr(real_controller.mongo, 'get_supported_projects')

        # Test known tokens were loaded
        assert len(real_controller.known_tokens) >= 0
        assert isinstance(real_controller.known_tokens_dict, dict)

        print(f"✅ Controller initialized with {len(real_controller.known_tokens)} known tokens")

    def test_real_transaction_processing_validation(self, real_controller):
        """Test processing real transactions and validate against expected results"""
        if not REAL_TRANSACTIONS:
            pytest.skip("No real transaction data provided yet")

        print(f"\n🔄 Processing and validating {len(REAL_TRANSACTIONS)} real transactions...")
        print(f"📊 Expected Results: {EXPECTED_RESULTS['total_transfers']} transfers, {EXPECTED_RESULTS['unique_recipients']} recipients")

        # Process the real transactions
        transfers = process_distributor_transfers(
            real_controller,
            REAL_TRANSACTIONS,
            TEST_PROJECT["distributor"]
        )

        # Validate against expected results
        assert len(transfers) == EXPECTED_RESULTS["total_transfers"], f"Expected {EXPECTED_RESULTS['total_transfers']} transfers, got {len(transfers)}"

        # Verify all transfers are SOL (since your data is all native transfers)
        sol_transfers = [t for t in transfers if t["token"] == "sol"]
        assert len(sol_transfers) == len(transfers), "All transfers should be SOL"

        # Verify total amounts
        total_amount = sum(t["amount"] for t in transfers)
        expected_sol = EXPECTED_RESULTS["total_sol_distributed"]
        assert abs(total_amount - expected_sol) < 0.000001, f"Expected {expected_sol} SOL, got {total_amount} SOL"

        print(f"✅ Validated: {len(transfers)} transfers totaling {total_amount:.9f} SOL")

        # Test aggregation and validate unique recipients
        aggregated_wallets = aggregate_transfers(transfers)
        assert len(aggregated_wallets) == EXPECTED_RESULTS["unique_recipients"], f"Expected {EXPECTED_RESULTS['unique_recipients']} recipients, got {len(aggregated_wallets)}"

        print(f"✅ Validated: {len(aggregated_wallets)} unique recipient wallets")

        return transfers, aggregated_wallets

    def test_real_database_operations(self, real_controller, clean_test_environment):
        """Test actual database operations with real connections"""
        distributor = TEST_PROJECT["distributor"]

        print(f"\n💾 Testing real database operations for {distributor}")

        # Test creating distributor tables
        real_controller.sqlite.create_distributor_tables(distributor)
        assert real_controller.sqlite.table_exists(distributor), "Should create distributor tables"
        print("✅ Created distributor tables in SQLite")

        # Test inserting a test project
        success = real_controller.sqlite.insert_supported_project(TEST_PROJECT)
        assert success == True, "Should insert project successfully"
        print("✅ Inserted test project to SQLite")

        # Test retrieving the project
        projects = real_controller.sqlite.get_supported_projects()
        test_project_found = any(p["distributor"] == distributor for p in projects)
        assert test_project_found, "Should find the test project"
        print("✅ Retrieved test project from SQLite")

        # Test MongoDB operations
        mongo_success = real_controller.mongo.insert_supported_project(TEST_PROJECT)
        assert mongo_success == True, "Should insert project to MongoDB"
        print("✅ Inserted test project to MongoDB")

    def test_real_project_initializer(self, real_controller, clean_test_environment):
        """Test ProjectInitializer with real database connections"""
        if not REAL_TRANSACTIONS:
            pytest.skip("No real transaction data provided yet")

        print(f"\n🚀 Testing ProjectInitializer with real data...")

        # Create initializer with real controller and project
        initializer = ProjectInitializer(real_controller, TEST_PROJECT.copy())

        # Test initialization
        assert initializer.controller == real_controller
        assert initializer.distributor == TEST_PROJECT["distributor"]
        print("✅ ProjectInitializer initialized")

        # Simulate inserting transactions (instead of fetching from API)
        print("📥 Simulating transaction insertion...")
        distributor = TEST_PROJECT["distributor"]

        # Create tables
        real_controller.sqlite.create_distributor_tables(distributor)

        # Insert our real transaction data
        success = real_controller.sqlite.insert_transactions_batch(distributor, REAL_TRANSACTIONS)
        assert success == True, "Should insert real transactions"
        print(f"✅ Inserted {len(REAL_TRANSACTIONS)} real transactions")

        # Test processing transactions
        result = initializer.process_initial_txs()
        assert result == True, "Should process transactions successfully"
        print("✅ Processed transactions into transfers")

        # Verify transfers were created
        transfer_count = real_controller.sqlite.get_transfers_count(distributor)
        assert transfer_count > 0, "Should have created transfers"
        print(f"✅ Created {transfer_count} transfers in database")

    def test_real_project_updater(self, real_controller, clean_test_environment):
        """Test ProjectUpdater with real database connections"""
        print(f"\n🔄 Testing ProjectUpdater with real connections...")

        # Ensure we have a test project in the database
        real_controller.sqlite.insert_supported_project(TEST_PROJECT)

        # Create updater (mock timer to prevent actual polling)
        with pytest.MonkeyPatch().context() as m:
            m.setattr('lib.ProjectUpdater.timer', lambda func, interval: None)
            updater = ProjectUpdater(real_controller)
            updater.updating = False  # Ensure it's not marked as updating

        # Test getting supported projects
        projects = real_controller.sqlite.get_supported_projects()
        test_project_exists = any(p["distributor"] == TEST_PROJECT["distributor"] for p in projects)
        assert test_project_exists, "Test project should exist in database"
        print("✅ Found test project in database")

        # Test the updating mechanism (without actual API calls)
        print("🔄 Testing update mechanism...")

        # We can't easily test the full update without mocking API calls,
        # but we can test the database interaction parts
        last_sig = real_controller.sqlite.get_last_tx_signature(TEST_PROJECT["distributor"])
        print(f"✅ Retrieved last signature: {last_sig or 'None'}")

    def test_real_wallet_operations(self, real_controller):
        """Test wallet operations with real database connections"""
        print(f"\n👛 Testing real wallet operations...")

        if not REAL_TRANSACTIONS:
            pytest.skip("No real transaction data provided yet")

        # Process transactions to get wallet data
        transfers = process_distributor_transfers(
            real_controller,
            REAL_TRANSACTIONS,
            TEST_PROJECT["distributor"]
        )

        aggregated_wallets = aggregate_transfers(transfers)

        if not aggregated_wallets:
            pytest.skip("No wallet data to test with")

        # Test upserting wallets
        print(f"💾 Upserting {len(aggregated_wallets)} wallets...")
        result = real_controller.upsert_wallets(aggregated_wallets)
        assert result > 0, "Should update/insert wallets"
        print(f"✅ Upserted {result} wallets")

        # Test retrieving wallets
        wallet_addresses = list(aggregated_wallets.keys())
        retrieved_wallets = real_controller.sqlite.get_wallets_by_addresses(wallet_addresses[:3])
        assert len(retrieved_wallets) > 0, "Should retrieve wallets"
        print(f"✅ Retrieved {len(retrieved_wallets)} wallets from database")

    def test_real_end_to_end_flow(self, real_controller, clean_test_environment):
        """Test the complete end-to-end flow with real data and databases"""
        if not REAL_TRANSACTIONS:
            pytest.skip("No real transaction data provided yet")

        print(f"\n🎯 Testing complete end-to-end flow with real data...")

        distributor = TEST_PROJECT["distributor"]

        # Step 1: Setup project infrastructure
        print("1️⃣ Setting up project infrastructure...")
        real_controller.sqlite.create_distributor_tables(distributor)
        real_controller.sqlite.insert_supported_project(TEST_PROJECT)
        real_controller.mongo.insert_supported_project(TEST_PROJECT)

        # Step 2: Insert transaction data
        print("2️⃣ Inserting real transaction data...")
        success = real_controller.sqlite.insert_transactions_batch(distributor, REAL_TRANSACTIONS)
        assert success == True

        # Step 3: Process transactions into transfers
        print("3️⃣ Processing transactions into transfers...")
        transfers = process_distributor_transfers(real_controller, REAL_TRANSACTIONS, distributor)
        success = real_controller.sqlite.insert_transfer_batch(distributor, transfers)
        assert success == True

        # Step 4: Aggregate rewards
        print("4️⃣ Aggregating wallet rewards...")
        aggregated_wallets = aggregate_transfers(transfers)
        wallet_count = real_controller.upsert_wallets(aggregated_wallets)
        assert wallet_count > 0

        # Step 5: Verify final state
        print("5️⃣ Verifying final database state...")

        # Check transactions were stored
        tx_count = real_controller.sqlite.get_transactions_count(distributor)
        assert tx_count == len(REAL_TRANSACTIONS)

        # Check transfers were stored
        transfer_count = real_controller.sqlite.get_transfers_count(distributor)
        assert transfer_count == len(transfers)

        # Check wallets were stored
        wallet_addresses = list(aggregated_wallets.keys())
        stored_wallets = real_controller.sqlite.get_wallets_by_addresses(wallet_addresses)
        assert len(stored_wallets) > 0

        print(f"✅ End-to-end test complete!")
        print(f"   📊 Processed: {tx_count} transactions → {transfer_count} transfers → {len(stored_wallets)} wallets")
