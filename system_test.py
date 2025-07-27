#!/usr/bin/env python3
"""
Interactive Test Runner
Provides a menu to run SQLite tests, MongoDB tests, Controller tests, ProjectUpdater tests, or all tests.
"""

import sys
import subprocess
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(__file__).parent
SERVER_DIR = PROJECT_ROOT / "server"
TESTS_DIR = SERVER_DIR / "tests"

def setup_python_path():
    """Add necessary paths to Python path"""
    sys.path.insert(0, str(PROJECT_ROOT))
    sys.path.insert(0, str(SERVER_DIR))

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

def run_integration_tests():
    """Run both Controller and ProjectUpdater integration tests"""
    print("🧪 Running INTEGRATION tests (Controller + ProjectUpdater)...")
    print("=" * 50)

    controller_success = False
    project_updater_success = False

    # Run Controller tests first
    print("\n📊 Part 1: Controller Integration Tests")
    print("-" * 30)
    controller_success = run_controller_tests()

    print("\n" + "=" * 50)

    # Run ProjectUpdater tests
    print("\n📊 Part 2: ProjectUpdater Tests")
    print("-" * 30)
    project_updater_success = run_project_updater_tests()

    # Summary
    print("\n" + "=" * 50)
    print("📋 INTEGRATION TESTS SUMMARY:")
    print(f"   Controller Tests:     {'✅ PASSED' if controller_success else '❌ FAILED'}")
    print(f"   ProjectUpdater Tests: {'✅ PASSED' if project_updater_success else '❌ FAILED'}")

    return controller_success and project_updater_success

def run_all_tests():
    """Run all tests: SQLite, MongoDB, Controller, and ProjectUpdater"""
    print("🧪 Running ALL tests...")
    print("=" * 50)

    sqlite_success = False
    mongodb_success = False
    controller_success = False
    project_updater_success = False

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

    # Summary
    print("\n" + "=" * 50)
    print("📋 COMPLETE TEST SUMMARY:")
    print(f"   SQLite Tests:         {'✅ PASSED' if sqlite_success else '❌ FAILED'}")
    print(f"   MongoDB Tests:        {'✅ PASSED' if mongodb_success else '❌ FAILED'}")
    print(f"   Controller Tests:     {'✅ PASSED' if controller_success else '❌ FAILED'}")
    print(f"   ProjectUpdater Tests: {'✅ PASSED' if project_updater_success else '❌ FAILED'}")
    print(f"   Overall Result:       {'✅ ALL PASSED' if all([sqlite_success, mongodb_success, controller_success, project_updater_success]) else '❌ SOME FAILED'}")

    return sqlite_success and mongodb_success and controller_success and project_updater_success

def show_menu():
    """Display the interactive menu"""
    print("\n🎯 Database Test Runner")
    print("=" * 40)
    print("Choose which tests to run:")
    print()
    print("1. 🗃️  SQLite Tests Only")
    print("2. 🍃  MongoDB Tests Only")
    print("3. 🎛️  Controller Tests Only")
    print("4. 🔄  ProjectUpdater Tests Only")
    print("5. 🔗  Database Tests (SQLite + MongoDB)")
    print("6. 🏗️  Integration Tests (Controller + ProjectUpdater)")
    print("7. 🚀  Run All Tests (Complete Suite)")
    print("8. ❌  Exit")
    print()

def get_user_choice():
    """Get and validate user choice"""
    while True:
        try:
            choice = input("Enter your choice (1-8): ").strip()
            if choice in ['1', '2', '3', '4', '5', '6', '7', '8']:
                return int(choice)
            else:
                print("❌ Invalid choice. Please enter 1, 2, 3, 4, 5, 6, 7, or 8.")
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            sys.exit(0)
        except Exception:
            print("❌ Invalid input. Please enter a number.")

def show_troubleshooting():
    """Show common troubleshooting tips"""
    print("\n💡 Common issues and solutions:")
    print("   - Check that your database classes can be imported")
    print("   - Verify your project structure matches the import paths")
    print("   - Make sure mock_data.py is in the tests directory")
    print("   - Ensure you have pytest installed: pip install pytest")
    print("   - For MongoDB tests: check your MongoDB connection and .env file")
    print("   - For SQLite tests: verify file permissions in the project directory")
    print("   - For Controller tests: ensure both SQLite and MongoDB are working")
    print("   - For ProjectUpdater tests: check that Controller tests pass first")
    print("   - For integration tests: check that temp directories can be created")
    print("   - For ProjectUpdater: verify utils modules can be imported properly")

def show_test_descriptions():
    """Show what each test type covers"""
    print("\n📖 Test Descriptions:")
    print("=" * 40)
    print("🗃️  SQLite Tests:")
    print("   - Database operations (CRUD)")
    print("   - Table creation and indexing")
    print("   - Transaction handling")
    print("   - Wallet and token management")
    print()
    print("🍃  MongoDB Tests:")
    print("   - Document insertion and updates")
    print("   - Batch operations")
    print("   - Collection management")
    print("   - API data synchronization")
    print()
    print("🎛️  Controller Tests:")
    print("   - Integration between SQLite and MongoDB")
    print("   - Wallet upsert operations")
    print("   - Token symbol management")
    print("   - Data consistency verification")
    print("   - Error handling and edge cases")
    print()
    print("🔄  ProjectUpdater Tests:")
    print("   - Transaction polling and processing")
    print("   - Distributor data fetching")
    print("   - Transfer extraction and aggregation")
    print("   - Automated update workflows")
    print("   - External API integration (mocked)")
    print("   - Concurrent update prevention")
    print()
    print("🔗  Database Tests:")
    print("   - Runs both SQLite and MongoDB tests")
    print("   - Focuses on database layer functionality")
    print()
    print("🏗️  Integration Tests:")
    print("   - Runs both Controller and ProjectUpdater tests")
    print("   - Tests inter-component communication")
    print("   - Validates complete workflows")
    print()
    print("🚀  All Tests:")
    print("   - Complete test suite")
    print("   - Database + Integration + Business Logic layers")
    print("   - Full system verification")

def main():
    """Main function with interactive menu"""
    success = True

    # Show test descriptions on startup
    show_test_descriptions()

    while True:
        show_menu()
        choice = get_user_choice()

        print()  # Add spacing

        if choice == 1:
            print("🗃️  Starting SQLite Tests...")
            print("=" * 40)
            success = run_sqlite_tests()

        elif choice == 2:
            print("🍃  Starting MongoDB Tests...")
            print("=" * 40)
            success = run_mongodb_tests()

        elif choice == 3:
            print("🎛️  Starting Controller Integration Tests...")
            print("=" * 40)
            success = run_controller_tests()

        elif choice == 4:
            print("🔄  Starting ProjectUpdater Tests...")
            print("=" * 40)
            success = run_project_updater_tests()

        elif choice == 5:
            print("🔗  Starting Database Tests...")
            print("=" * 40)
            success = run_database_tests()

        elif choice == 6:
            print("🏗️  Starting Integration Tests...")
            print("=" * 40)
            success = run_integration_tests()

        elif choice == 7:
            print("🚀  Starting Complete Test Suite...")
            print("=" * 40)
            success = run_all_tests()

        elif choice == 8:
            print("👋 Goodbye!")
            break

        # Show results
        if success:
            print("\n✅ Tests completed successfully!")
        else:
            print("\n❌ Tests failed or encountered errors")
            show_troubleshooting()

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