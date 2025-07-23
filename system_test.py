#!/usr/bin/env python3
"""
Simple SQLite test runner
Just runs the three SQLite tests you specified.
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

    try:
        # Run pytest on the specific test file
        cmd = [sys.executable, "-m", "pytest", str(test_file), "-v"]
        print(f"Running: {' '.join(cmd)}")

        result = subprocess.run(cmd, cwd=PROJECT_ROOT)
        return result.returncode == 0

    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False

def main():
    """Main function"""
    print("🚀 Simple SQLite Test Runner")
    print("=" * 40)

    # Run the tests
    success = run_sqlite_tests()

    if success:
        print("\n✅ All tests completed!")
    else:
        print("\n❌ Tests failed or encountered errors")
        print("\n💡 Common issues:")
        print("   - Check that SQLiteDB class can be imported")
        print("   - Verify your project structure matches the import paths")
        print("   - Make sure mock_data.py is in the tests directory")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())