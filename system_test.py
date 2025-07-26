#!/usr/bin/env python3
"""
Interactive Test Runner
Provides a menu to run SQLite tests, MongoDB tests, or both.
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
        print("💡 Make sure you've created the test_mongodb.py file in the tests directory")
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

def run_all_tests():
    """Run both SQLite and MongoDB tests"""
    print("🧪 Running ALL tests...")
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
    print("📋 TEST SUMMARY:")
    print(f"   SQLite Tests:  {'✅ PASSED' if sqlite_success else '❌ FAILED'}")
    print(f"   MongoDB Tests: {'✅ PASSED' if mongodb_success else '❌ FAILED'}")

    return sqlite_success and mongodb_success

def show_menu():
    """Display the interactive menu"""
    print("\n🎯 Database Test Runner")
    print("=" * 40)
    print("Choose which tests to run:")
    print()
    print("1. 🗃️  SQLite Tests Only")
    print("2. 🍃  MongoDB Tests Only")
    print("3. 🚀  Run All Tests")
    print("4. ❌  Exit")
    print()

def get_user_choice():
    """Get and validate user choice"""
    while True:
        try:
            choice = input("Enter your choice (1-4): ").strip()
            if choice in ['1', '2', '3', '4']:
                return int(choice)
            else:
                print("❌ Invalid choice. Please enter 1, 2, 3, or 4.")
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

def main():
    """Main function with interactive menu"""
    success = True

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
            print("🚀  Starting All Tests...")
            print("=" * 40)
            success = run_all_tests()

        elif choice == 4:
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