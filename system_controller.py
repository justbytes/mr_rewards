import sys
import subprocess
import os
import json
from pathlib import Path
from server.lib.BackerUpper import BackerUpper

"""
Interactive System Menu
Provides a menu to run SQLite tests, MongoDB tests, Controller tests, ProjectUpdater tests,
ProjectInitializer tests, initialize new projects, get database counts, manage API keys, or all tests.
"""

# Setup paths
PROJECT_ROOT = Path(__file__).parent
SERVER_DIR = PROJECT_ROOT / "server"
TESTS_DIR = SERVER_DIR / "tests"

def setup_python_path():
    """Add necessary paths to Python path"""
    sys.path.insert(0, str(PROJECT_ROOT))
    sys.path.insert(0, str(SERVER_DIR))

def get_user_input(prompt, required=True, default=None):
    """Get user input with optional validation"""
    while True:
        try:
            if default:
                user_input = input(f"{prompt} (default: {default}): ").strip()
                if not user_input:
                    return default
            else:
                user_input = input(f"{prompt}: ").strip()

            if required and not user_input:
                print("❌ This field is required. Please enter a value.")
                continue

            return user_input if user_input else None

        except KeyboardInterrupt:
            print("\n\n👋 Operation cancelled!")
            return None

##########################################################
#                      Data Management                   #
##########################################################

def get_sqlite_counts():
    """Get and display SQLite database counts"""
    print("📊 Getting SQLite database counts...")
    print("=" * 40)

    try:
        # Setup paths and import after path setup
        setup_python_path()
        from server.lib.Controller import Controller

        controller = Controller(False, False)
        transfers = controller.sqlite.get_temp_transfers_count()
        projects = controller.sqlite.get_supported_project_count()
        known_tokens = controller.sqlite.get_known_tokens_count()
        wallets = controller.sqlite.get_wallets_count()

        print(f"📋 Temp transfer count: {transfers}")
        print(f"📋 Projects count: {projects}")
        print(f"📋 Known tokens count: {known_tokens}")
        print(f"📋 Wallets count: {wallets}")
        print()

        print("📊 Project-specific transfer counts:")
        print("-" * 30)
        for project in controller.sqlite.get_supported_projects():
            count = controller.sqlite.get_transfers_count(project.get("distributor"))
            print(f"📋 Transfers for {project.get('distributor')}: {count}")

        return True

    except Exception as e:
        print(f"❌ Error getting SQLite counts: {e}")
        return False

def initialize_project():
    """Initialize a new project with user input"""
    print("🏗️  Initializing New Project...")
    print("=" * 40)
    print("Please provide the following information:")
    print()

    try:
        # Get project information from user
        name = get_user_input("Project name", required=True)
        if name is None:
            return False

        distributor = get_user_input("Distributor address", required=True)
        if distributor is None:
            return False

        token_mint = get_user_input("Token mint address", required=True)
        if token_mint is None:
            return False

        dev_wallet = get_user_input("Dev wallet address (optional)", required=False)

        last_sig = get_user_input("Last tx signature (optional)", required=False)

        # Project dictionary
        project = {
            "name": name,
            "distributor": distributor,
            "token_mint": token_mint,
            "dev_wallet": dev_wallet,
            "last_sig": last_sig
        }

        print("\n📋 Project Summary:")
        print("-" * 20)
        print(f"Name: {project['name']}")
        print(f"Distributor: {project['distributor']}")
        print(f"Token Mint: {project['token_mint']}")
        print(f"Dev Wallet: {project['dev_wallet'] or 'Not specified'}")
        print(f"Last Signature: {project['last_sig'] or 'None'}")
        print()

        # Confirm before proceeding
        confirm = input("Do you want to initialize this project? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Project initialization cancelled.")
            return False

        # Setup paths and import after path setup
        setup_python_path()
        from server.lib.ProjectInitializer import ProjectInitializer
        from server.lib.Controller import Controller

        # Create Controller instance
        controller = Controller(False, False)

        # Create instance of initializer
        initializer = ProjectInitializer(controller, project)

        # Initialize the new project
        print("\n🚀 Initializing project...")
        initializer.initalize_project()

        print("✅ Project initialized successfully!")
        return True

    except Exception as e:
        print(f"❌ Error initializing project: {e}")
        return False

def backup_temp_transfers():
    """Initialize a new project with user input"""
    print("🏗️  Backing up temp transfers...")
    print("=" * 40)

    try:
        backup = BackerUpper()
        success = backup.backup_all_distributor_transfers()

        if success is not True:
            return False

        return True

    except Exception as e:
        print(f"❌ Error backing up temp transfers: {e}")
        return False

##########################################################
#                   API Key Management                   #
##########################################################

def create_api_key():
    """Create a new API key with user input"""
    print("🔑 Creating New API Key...")
    print("=" * 30)
    print()

    try:
        # Get API key information from user
        name = get_user_input("API Key name", required=False, default="System Generated Key")
        if name is None:
            return False

        rate_limit_input = get_user_input("Rate limit (requests/minute)", required=False, default="1000")
        if rate_limit_input is None:
            return False

        # Parse rate limit
        try:
            rate_limit = int(rate_limit_input) if rate_limit_input.isdigit() else 1000
        except ValueError:
            rate_limit = 1000

        print(f"\n📋 API Key Summary:")
        print("-" * 20)
        print(f"Name: {name}")
        print(f"Rate Limit: {rate_limit} requests/minute")
        print()

        # Confirm before proceeding
        confirm = input("Do you want to create this API key? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ API key creation cancelled.")
            return False

        # Setup paths and import after path setup
        setup_python_path()
        from server.db.SQLite.db import SQLiteDB
        from server.utils.api_key_management import generate_api_key

        # Create SQLite instance
        sqlite_db = SQLiteDB(test=False, temp=False)

        # Generate a new API key
        api_key = generate_api_key()

        # Insert into database
        success = sqlite_db.insert_api_key(api_key, name, rate_limit)

        if success:
            print(f"\n🎉 API Key created successfully!")
            print(f"📋 Details:")
            print("-" * 20)
            print(f"Key: {api_key}")
            print(f"Name: {name}")
            print(f"Rate Limit: {rate_limit} requests/minute")
            print(f"\n🔐 Add this to your .env file:")
            print(f"API_KEY={api_key}")
            print(f"\n📖 Use this key in Authorization headers:")
            print(f"Authorization: Bearer {api_key}")
            return True
        else:
            print("❌ Failed to create API key")
            return False

    except Exception as e:
        print(f"❌ Error creating API key: {e}")
        return False

def list_api_keys():
    """List all API keys"""
    print("📋 API Key List...")
    print("=" * 40)

    try:
        # Setup paths and import after path setup
        setup_python_path()
        from server.db.SQLite.db import SQLiteDB

        sqlite_db = SQLiteDB(test=False, temp=False)
        keys = sqlite_db.get_all_api_keys()

        if not keys:
            print("📋 No API keys found")
            return True

        print(f"\n📋 Found {len(keys)} API key(s):")
        print()
        print(f"{'ID':<5} {'Name':<25} {'Key Preview':<20} {'Active':<8} {'Usage':<8} {'Rate Limit':<12} {'Created':<20}")
        print("-" * 105)

        for key in keys:
            key_preview = key['key'][:16] + "..." if len(key['key']) > 16 else key['key']
            rate_limit_display = str(key['rate_limit']) if key['rate_limit'] else 'Unlimited'
            created_display = key['created_at'][:19] if key['created_at'] else 'Unknown'
            print(f"{key['id']:<5} {(key['name'] or 'Unnamed'):<25} {key_preview:<20} "
                  f"{'Yes' if key['is_active'] else 'No':<8} {key['usage_count']:<8} {rate_limit_display:<12} {created_display:<20}")

        return True

    except Exception as e:
        print(f"❌ Error listing API keys: {e}")
        return False

def manage_api_key():
    """Manage an existing API key (deactivate, update, etc.)"""
    print("🔧 Manage API Key...")
    print("=" * 30)

    try:
        # First, show current keys
        if not list_api_keys():
            return False

        print("\n" + "=" * 40)
        print("Management Options:")
        print("1. Deactivate API Key")
        print("2. Reactivate API Key")
        print("3. Update API Key Name")
        print("4. Update Rate Limit")
        print("5. Delete API Key")
        print("6. Show API Key Usage Stats")
        print("0. Back to main menu")

        choice = input("\nEnter your choice (0-6): ").strip()

        if choice == "0":
            return True
        elif choice == "1":
            return deactivate_api_key()
        elif choice == "2":
            return reactivate_api_key()
        elif choice == "3":
            return update_api_key_name()
        elif choice == "4":
            return update_api_key_rate_limit()
        elif choice == "5":
            return delete_api_key()
        elif choice == "6":
            return show_api_key_usage()
        else:
            print("❌ Invalid choice")
            return False

    except Exception as e:
        print(f"❌ Error managing API key: {e}")
        return False

def deactivate_api_key():
    """Deactivate an API key"""
    try:
        api_key = get_user_input("Enter API key to deactivate", required=True)
        if api_key is None:
            return False

        setup_python_path()
        from server.db.SQLite.db import SQLiteDB

        sqlite_db = SQLiteDB(test=False, temp=False)

        # Verify key exists first
        key_data = sqlite_db.get_api_key(api_key)
        if not key_data:
            print("❌ API key not found")
            return False

        print(f"\n📋 Key to deactivate:")
        print(f"Name: {key_data['name'] or 'Unnamed'}")
        print(f"Currently Active: {'Yes' if key_data['is_active'] else 'No'}")

        if not key_data['is_active']:
            print("⚠️  This key is already inactive")
            return True

        confirm = input("\nAre you sure you want to deactivate this key? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Operation cancelled")
            return False

        success = sqlite_db.update_api_key(api_key, is_active=False)

        if success:
            print("✅ API key deactivated successfully")
            return True
        else:
            print("❌ Failed to deactivate API key")
            return False

    except Exception as e:
        print(f"❌ Error deactivating API key: {e}")
        return False

def reactivate_api_key():
    """Reactivate an API key"""
    try:
        api_key = get_user_input("Enter API key to reactivate", required=True)
        if api_key is None:
            return False

        setup_python_path()
        from server.db.SQLite.db import SQLiteDB

        sqlite_db = SQLiteDB(test=False, temp=False)

        # Verify key exists first
        key_data = sqlite_db.get_api_key(api_key)
        if not key_data:
            print("❌ API key not found")
            return False

        print(f"\n📋 Key to reactivate:")
        print(f"Name: {key_data['name'] or 'Unnamed'}")
        print(f"Currently Active: {'Yes' if key_data['is_active'] else 'No'}")

        if key_data['is_active']:
            print("⚠️  This key is already active")
            return True

        confirm = input("\nAre you sure you want to reactivate this key? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Operation cancelled")
            return False

        success = sqlite_db.update_api_key(api_key, is_active=True)

        if success:
            print("✅ API key reactivated successfully")
            return True
        else:
            print("❌ Failed to reactivate API key")
            return False

    except Exception as e:
        print(f"❌ Error reactivating API key: {e}")
        return False

def update_api_key_name():
    """Update an API key's name"""
    try:
        api_key = get_user_input("Enter API key to update", required=True)
        if api_key is None:
            return False

        setup_python_path()
        from server.db.SQLite.db import SQLiteDB

        sqlite_db = SQLiteDB(test=False, temp=False)

        # Verify key exists first
        key_data = sqlite_db.get_api_key(api_key)
        if not key_data:
            print("❌ API key not found")
            return False

        print(f"\n📋 Current key info:")
        print(f"Name: {key_data['name'] or 'Unnamed'}")

        new_name = get_user_input("Enter new name", required=True)
        if new_name is None:
            return False

        success = sqlite_db.update_api_key(api_key, name=new_name)

        if success:
            print(f"✅ API key name updated to: {new_name}")
            return True
        else:
            print("❌ Failed to update API key name")
            return False

    except Exception as e:
        print(f"❌ Error updating API key name: {e}")
        return False

def update_api_key_rate_limit():
    """Update an API key's rate limit"""
    try:
        api_key = get_user_input("Enter API key to update", required=True)
        if api_key is None:
            return False

        setup_python_path()
        from server.db.SQLite.db import SQLiteDB

        sqlite_db = SQLiteDB(test=False, temp=False)

        # Verify key exists first
        key_data = sqlite_db.get_api_key(api_key)
        if not key_data:
            print("❌ API key not found")
            return False

        print(f"\n📋 Current key info:")
        print(f"Name: {key_data['name'] or 'Unnamed'}")
        print(f"Current Rate Limit: {key_data['rate_limit'] or 'Unlimited'}")

        new_rate_limit_input = get_user_input("Enter new rate limit (requests/minute, or 'unlimited')", required=True)
        if new_rate_limit_input is None:
            return False

        # Parse rate limit
        if new_rate_limit_input.lower() == 'unlimited':
            new_rate_limit = None
        else:
            try:
                new_rate_limit = int(new_rate_limit_input)
                if new_rate_limit <= 0:
                    print("❌ Rate limit must be a positive number")
                    return False
            except ValueError:
                print("❌ Invalid rate limit format")
                return False

        success = sqlite_db.update_api_key(api_key, rate_limit=new_rate_limit)

        if success:
            limit_display = new_rate_limit if new_rate_limit else "Unlimited"
            print(f"✅ API key rate limit updated to: {limit_display}")
            return True
        else:
            print("❌ Failed to update API key rate limit")
            return False

    except Exception as e:
        print(f"❌ Error updating API key rate limit: {e}")
        return False

def delete_api_key():
    """Delete an API key"""
    try:
        api_key = get_user_input("Enter API key to DELETE", required=True)
        if api_key is None:
            return False

        setup_python_path()
        from server.db.SQLite.db import SQLiteDB

        sqlite_db = SQLiteDB(test=False, temp=False)

        # Verify key exists first
        key_data = sqlite_db.get_api_key(api_key)
        if not key_data:
            print("❌ API key not found")
            return False

        print(f"\n⚠️  WARNING: You are about to DELETE this API key:")
        print(f"Name: {key_data['name'] or 'Unnamed'}")
        print(f"Usage Count: {key_data['usage_count']}")
        print(f"Active: {'Yes' if key_data['is_active'] else 'No'}")
        print("\n🚨 This action CANNOT be undone!")

        confirm1 = input("\nType 'DELETE' to confirm: ").strip()
        if confirm1 != 'DELETE':
            print("❌ Operation cancelled")
            return False

        confirm2 = input("Are you absolutely sure? (y/N): ").strip().lower()
        if confirm2 != 'y':
            print("❌ Operation cancelled")
            return False

        success = sqlite_db.delete_api_key(api_key)

        if success:
            print("✅ API key deleted successfully")
            return True
        else:
            print("❌ Failed to delete API key")
            return False

    except Exception as e:
        print(f"❌ Error deleting API key: {e}")
        return False

def show_api_key_usage():
    """Show usage statistics for an API key"""
    try:
        print("\nUsage Statistics Options:")
        print("1. Show usage for specific API key")
        print("2. Show usage for all API keys")

        choice = input("Enter your choice (1-2): ").strip()

        setup_python_path()
        from server.db.SQLite.db import SQLiteDB

        sqlite_db = SQLiteDB(test=False, temp=False)

        if choice == "1":
            api_key = get_user_input("Enter API key", required=True)
            if api_key is None:
                return False

            # Verify key exists
            key_data = sqlite_db.get_api_key(api_key)
            if not key_data:
                print("❌ API key not found")
                return False

            print(f"\n📊 Usage Statistics for: {key_data['name'] or 'Unnamed'}")
            print("-" * 50)
            print(f"Total Usage Count: {key_data['usage_count']}")
            print(f"Last Used: {key_data['last_used'] or 'Never'}")
            print(f"Rate Limit: {key_data['rate_limit'] or 'Unlimited'}")
            print(f"Status: {'Active' if key_data['is_active'] else 'Inactive'}")

            # Get detailed usage logs
            usage_logs = sqlite_db.get_api_key_usage_stats(api_key, 10)

            if usage_logs:
                print(f"\n📋 Recent Usage (last 10 requests):")
                print(f"{'Endpoint':<25} {'Method':<8} {'IP Address':<15} {'Timestamp':<20}")
                print("-" * 75)
                for log in usage_logs:
                    timestamp_display = log['timestamp'][:19] if log['timestamp'] else 'Unknown'
                    print(f"{log['endpoint']:<25} {log['method']:<8} {log['ip_address'] or 'Unknown':<15} {timestamp_display:<20}")
            else:
                print("\n📋 No usage logs found")

        elif choice == "2":
            # Show usage for all keys
            usage_logs = sqlite_db.get_api_key_usage_stats(None, 20)

            if usage_logs:
                print(f"\n📊 Recent Usage Across All API Keys (last 20 requests):")
                print("-" * 80)
                print(f"{'API Key':<20} {'Endpoint':<20} {'Method':<8} {'IP Address':<15} {'Timestamp':<20}")
                print("-" * 90)
                for log in usage_logs:
                    key_display = log['api_key'][:16] + "..." if len(log['api_key']) > 16 else log['api_key']
                    timestamp_display = log['timestamp'][:19] if log['timestamp'] else 'Unknown'
                    print(f"{key_display:<20} {log['endpoint']:<20} {log['method']:<8} {log['ip_address'] or 'Unknown':<15} {timestamp_display:<20}")
            else:
                print("\n📋 No usage logs found")
        else:
            print("❌ Invalid choice")
            return False

        return True

    except Exception as e:
        print(f"❌ Error showing API key usage: {e}")
        return False

# Test functions from original system_test.py
def run_sqlite_tests():
    """Run the SQLite tests"""
    print("🧪 Running SQLite tests...")

    # Setup paths
    setup_python_path()

    # Check if test file exists
    test_file = TESTS_DIR / "test_sqlite.py"

    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        return False

    try:
        # Run pytest on the specific test file
        cmd = [sys.executable, "-m", "pytest", str(test_file), "-v"]
        print(f"Running: {' '.join(cmd)}")

        result = subprocess.run(cmd, cwd=PROJECT_ROOT)
        return result.returncode == 0

    except Exception as e:
        print(f"❌ Error running SQLite tests: {e}")
        return False

def run_mongodb_tests():
    """Run the MongoDB tests"""
    print("🧪 Running MongoDB tests...")

    # Setup paths
    setup_python_path()

    # Check if test file exists
    test_file = TESTS_DIR / "test_mongo.py"

    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        print("💡 Make sure you've created the test_mongo.py file in the tests directory")
        return False

    try:
        # Run pytest on the specific test file
        cmd = [sys.executable, "-m", "pytest", str(test_file), "-v"]
        print(f"Running: {' '.join(cmd)}")

        result = subprocess.run(cmd, cwd=PROJECT_ROOT)
        return result.returncode == 0

    except Exception as e:
        print(f"❌ Error running MongoDB tests: {e}")
        return False

def run_controller_tests():
    """Run the Controller integration tests"""
    print("🧪 Running Controller integration tests...")

    # Setup paths
    setup_python_path()

    # Check if test file exists
    test_file = TESTS_DIR / "test_controller.py"

    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        print("💡 Make sure you've created the test_controller.py file in the tests directory")
        return False

    try:
        # Run pytest on the specific test file
        cmd = [sys.executable, "-m", "pytest", str(test_file), "-v"]
        print(f"Running: {' '.join(cmd)}")

        result = subprocess.run(cmd, cwd=PROJECT_ROOT)
        return result.returncode == 0

    except Exception as e:
        print(f"❌ Error running Controller tests: {e}")
        return False

def run_project_updater_tests():
    """Run the ProjectUpdater tests"""
    print("🧪 Running ProjectUpdater tests...")

    # Setup paths
    setup_python_path()

    # Check if test file exists
    test_file = TESTS_DIR / "test_project_updater.py"

    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        print("💡 Make sure you've created the test_project_updater.py file in the tests directory")
        return False

    try:
        # Run pytest on the specific test file
        cmd = [sys.executable, "-m", "pytest", str(test_file), "-v"]
        print(f"Running: {' '.join(cmd)}")

        result = subprocess.run(cmd, cwd=PROJECT_ROOT)
        return result.returncode == 0

    except Exception as e:
        print(f"❌ Error running ProjectUpdater tests: {e}")
        return False

def run_project_initializer_tests():
    """Run the ProjectInitializer tests"""
    print("🧪 Running ProjectInitializer tests...")

    # Setup paths
    setup_python_path()

    # Check if test file exists
    test_file = TESTS_DIR / "test_project_initializer.py"

    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        print("💡 Make sure you've created the test_project_initializer.py file in the tests directory")
        return False

    try:
        # Run pytest on the specific test file
        cmd = [sys.executable, "-m", "pytest", str(test_file), "-v"]
        print(f"Running: {' '.join(cmd)}")

        result = subprocess.run(cmd, cwd=PROJECT_ROOT)
        return result.returncode == 0

    except Exception as e:
        print(f"❌ Error running ProjectInitializer tests: {e}")
        return False

def run_database_tests():
    """Run both SQLite and MongoDB tests (database layer only)"""
    print("🧪 Running DATABASE tests (SQLite + MongoDB)...")
    print("=" * 50)

    sqlite_success = False
    mongodb_success = False

    # Run SQLite tests first
    print("\n📊 Part 1: SQLite Tests")
    print("-" * 30)
    sqlite_success = run_sqlite_tests()

    print("\n" + "=" * 50)

    # Run MongoDB tests
    print("\n📊 Part 2: MongoDB Tests")
    print("-" * 30)
    mongodb_success = run_mongodb_tests()

    # Summary
    print("\n" + "=" * 50)
    print("📋 DATABASE TESTS SUMMARY:")
    print(f"   SQLite Tests:  {'✅ PASSED' if sqlite_success else '❌ FAILED'}")
    print(f"   MongoDB Tests: {'✅ PASSED' if mongodb_success else '❌ FAILED'}")

    return sqlite_success and mongodb_success

def run_business_logic_tests():
    """Run Controller, ProjectUpdater, and ProjectInitializer tests"""
    print("🧪 Running BUSINESS LOGIC tests (Controller + ProjectUpdater + ProjectInitializer)...")
    print("=" * 50)

    controller_success = False
    project_updater_success = False
    project_initializer_success = False

    # Run Controller tests first
    print("\n📊 Part 1: Controller Integration Tests")
    print("-" * 30)
    controller_success = run_controller_tests()

    print("\n" + "=" * 50)

    # Run ProjectUpdater tests
    print("\n📊 Part 2: ProjectUpdater Tests")
    print("-" * 30)
    project_updater_success = run_project_updater_tests()

    print("\n" + "=" * 50)

    # Run ProjectInitializer tests
    print("\n📊 Part 3: ProjectInitializer Tests")
    print("-" * 30)
    project_initializer_success = run_project_initializer_tests()

    # Summary
    print("\n" + "=" * 50)
    print("📋 BUSINESS LOGIC TESTS SUMMARY:")
    print(f"   Controller Tests:         {'✅ PASSED' if controller_success else '❌ FAILED'}")
    print(f"   ProjectUpdater Tests:     {'✅ PASSED' if project_updater_success else '❌ FAILED'}")
    print(f"   ProjectInitializer Tests: {'✅ PASSED' if project_initializer_success else '❌ FAILED'}")

    return controller_success and project_updater_success and project_initializer_success

def run_all_tests():
    """Run all tests: SQLite, MongoDB, Controller, ProjectUpdater, and ProjectInitializer"""
    print("🧪 Running ALL tests...")
    print("=" * 50)

    sqlite_success = False
    mongodb_success = False
    controller_success = False
    project_updater_success = False
    project_initializer_success = False

    # Run SQLite tests first
    print("\n📊 Part 1: SQLite Tests")
    print("-" * 30)
    sqlite_success = run_sqlite_tests()

    print("\n" + "=" * 50)

    # Run MongoDB tests
    print("\n📊 Part 2: MongoDB Tests")
    print("-" * 30)
    mongodb_success = run_mongodb_tests()

    print("\n" + "=" * 50)

    # Run Controller tests
    print("\n📊 Part 3: Controller Integration Tests")
    print("-" * 30)
    controller_success = run_controller_tests()

    print("\n" + "=" * 50)

    # Run ProjectUpdater tests
    print("\n📊 Part 4: ProjectUpdater Tests")
    print("-" * 30)
    project_updater_success = run_project_updater_tests()

    print("\n" + "=" * 50)

    # Run ProjectInitializer tests
    print("\n📊 Part 5: ProjectInitializer Tests")
    print("-" * 30)
    project_initializer_success = run_project_initializer_tests()

    # Summary
    print("\n" + "=" * 50)
    print("📋 COMPLETE TEST SUMMARY:")
    print(f"   SQLite Tests:             {'✅ PASSED' if sqlite_success else '❌ FAILED'}")
    print(f"   MongoDB Tests:            {'✅ PASSED' if mongodb_success else '❌ FAILED'}")
    print(f"   Controller Tests:         {'✅ PASSED' if controller_success else '❌ FAILED'}")
    print(f"   ProjectUpdater Tests:     {'✅ PASSED' if project_updater_success else '❌ FAILED'}")
    print(f"   ProjectInitializer Tests: {'✅ PASSED' if project_initializer_success else '❌ FAILED'}")
    print(f"   Overall Result:           {'✅ ALL PASSED' if all([sqlite_success, mongodb_success, controller_success, project_updater_success, project_initializer_success]) else '❌ SOME FAILED'}")

    return sqlite_success and mongodb_success and controller_success and project_updater_success and project_initializer_success

def show_menu():
    """Display the interactive menu"""
    print("\n🎯 System Management Menu")
    print("=" * 40)
    print("Choose an option:")
    print()
    print("📊 Data Management:")
    print("1. 📋  Get SQLite Database Counts")
    print("2. 🏗️  Initialize New Project")
    print("3. 💾  Backup Temp Transfers")
    print()
    print("🔑 API Key Management:")
    print("4. 🔑  Create New API Key")
    print("5. 📋  List All API Keys")
    print("6. 🔧  Manage API Keys")
    print()
    print("🧪 Testing Options:")
    print("7. 🗃️  SQLite Tests Only")
    print("8. 🍃  MongoDB Tests Only")
    print("9. 🎛️  Controller Tests Only")
    print("10. 🔄  ProjectUpdater Tests Only")
    print("11. 🏗️ ProjectInitializer Tests Only")
    print("12. 🔗 Database Tests (SQLite + MongoDB)")
    print("13. 🧠 Business Logic Tests (Controller + ProjectUpdater + ProjectInitializer)")
    print("14. 🚀 Run All Tests (Complete Suite)")
    print("15. ❌ Exit")
    print()

def get_user_choice():
    """Get and validate user choice"""
    while True:
        try:
            choice = input("Enter your choice (1-14): ").strip()
            if choice in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14']:
                return int(choice)
            else:
                print("❌ Invalid choice. Please enter 1-14.")
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            sys.exit(0)
        except Exception:
            print("❌ Invalid input. Please enter a number.")

def main():
    """Main function with interactive menu"""
    success = True

    while True:
        show_menu()
        choice = get_user_choice()

        print()  # Add spacing

        if choice == 1:
            print("📋 Getting SQLite Database Counts...")
            print("=" * 40)
            success = get_sqlite_counts()

        elif choice == 2:
            print("🏗️  Initialize New Project...")
            print("=" * 40)
            success = initialize_project()

        elif choice == 3:
            print("💾  Backup Temp Transfers...")
            print("=" * 40)
            success = backup_temp_transfers()

        elif choice == 4:
            print("🔑 Create New API Key...")
            print("=" * 40)
            success = create_api_key()

        elif choice == 5:
            print("📋 List All API Keys...")
            print("=" * 40)
            success = list_api_keys()

        elif choice == 6:
            print("🔧 Manage API Keys...")
            print("=" * 40)
            success = manage_api_key()

        elif choice == 7:
            print("🗃️  Starting SQLite Tests...")
            print("=" * 40)
            success = run_sqlite_tests()

        elif choice == 8:
            print("🍃  Starting MongoDB Tests...")
            print("=" * 40)
            success = run_mongodb_tests()

        elif choice == 9:
            print("🎛️  Starting Controller Integration Tests...")
            print("=" * 40)
            success = run_controller_tests()

        elif choice == 10:
            print("🔄  Starting ProjectUpdater Tests...")
            print("=" * 40)
            success = run_project_updater_tests()

        elif choice == 11:
            print("🏗️  Starting ProjectInitializer Tests...")
            print("=" * 40)
            success = run_project_initializer_tests()

        elif choice == 12:
            print("🔗  Starting Database Tests...")
            print("=" * 40)
            success = run_database_tests()

        elif choice == 13:
            print("🧠  Starting Business Logic Tests...")
            print("=" * 40)
            success = run_business_logic_tests()

        elif choice == 14:
            print("🚀  Starting Complete Test Suite...")
            print("=" * 40)
            success = run_all_tests()

        elif choice == 15:
            print("👋 Goodbye!")
            break

        # Show results
        if success:
            print("\n✅ Operation completed successfully!")
        else:
            print("\n❌ Operation failed or encountered errors")

        # Ask if user wants to continue
        print("\n" + "=" * 40)
        try:
            continue_choice = input("Press Enter to return to menu, or 'q' to quit: ").strip().lower()
            if continue_choice == 'q':
                print("👋 Goodbye!")
                break
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())