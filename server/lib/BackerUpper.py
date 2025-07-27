#!/usr/bin/env python3
"""
Interactive System Menu
Provides a menu to run SQLite tests, MongoDB tests, Controller tests, ProjectUpdater tests,
ProjectInitializer tests, initialize new projects, get database counts, or all tests.
"""

import sys
import subprocess
import os
import json
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(__file__).parent
SERVER_DIR = PROJECT_ROOT / "server"
TESTS_DIR = SERVER_DIR / "tests"

def setup_python_path():
    """Add necessary paths to Python path"""
    sys.path.insert(0, str(PROJECT_ROOT))
    sys.path.insert(0, str(SERVER_DIR))

##########################################################
#              Backup Data To Local Storage              #
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

##########################################################
#                 Initialize New Projects                #
##########################################################

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

        # Create instance of initializer
        initializer = ProjectInitializer(project)

        # Initialize the new project
        print("\n🚀 Initializing project...")
        initializer.initalize_project()

        print("✅ Project initialized successfully!")
        return True

    except Exception as e:
        print(f"❌ Error initializing project: {e}")
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
        print("�101 Make sure you've created the test_project_initializer.py file in the tests directory")
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
    print()
    print("🧪 Testing Options:")
    print("3. 🗃️  SQLite Tests Only")
    print("4. 🍃  MongoDB Tests Only")
    print("5. 🎛️  Controller Tests Only")
    print("6. 🔄  ProjectUpdater Tests Only")
    print("7. 🏗️  ProjectInitializer Tests Only")
    print("8. 🔗  Database Tests (SQLite + MongoDB)")
    print("9. 🧠  Business Logic Tests (Controller + ProjectUpdater + ProjectInitializer)")
    print("10. 🚀 Run All Tests (Complete Suite)")
    print("11. ❌ Exit")
    print()

def get_user_choice():
    """Get and validate user choice"""
    while True:
        try:
            choice = input("Enter your choice (1-11): ").strip()
            if choice in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11']:
                return int(choice)
            else:
                print("❌ Invalid choice. Please enter 1-11.")
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
            print("🗃️  Starting SQLite Tests...")
            print("=" * 40)
            success = run_sqlite_tests()

        elif choice == 4:
            print("🍃  Starting MongoDB Tests...")
            print("=" * 40)
            success = run_mongodb_tests()

        elif choice == 5:
            print("🎛️  Starting Controller Integration Tests...")
            print("=" * 40)
            success = run_controller_tests()

        elif choice == 6:
            print("🔄  Starting ProjectUpdater Tests...")
            print("=" * 40)
            success = run_project_updater_tests()

        elif choice == 7:
            print("🏗️  Starting ProjectInitializer Tests...")
            print("=" * 40)
            success = run_project_initializer_tests()

        elif choice == 8:
            print("🔗  Starting Database Tests...")
            print("=" * 40)
            success = run_database_tests()

        elif choice == 9:
            print("🧠  Starting Business Logic Tests...")
            print("=" * 40)
            success = run_business_logic_tests()

        elif choice == 10:
            print("🚀  Starting Complete Test Suite...")
            print("=" * 40)
            success = run_all_tests()

        elif choice == 11:
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