#!/usr/bin/env python3
"""
System Controller - Main test runner for Rewards Token Tracker
Location: root/system_controller.py
Tests Location: root/server/tests/

Usage: python system_controller.py [command] [options]
"""

import sys
import subprocess
import argparse
import os
from pathlib import Path

# Add the project root to Python path so imports work
PROJECT_ROOT = Path(__file__).parent
SERVER_DIR = PROJECT_ROOT / "server"
TESTS_DIR = SERVER_DIR / "tests"

# Add both root and server to Python path
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SERVER_DIR))

def setup_python_path():
    """Ensure Python can find all modules"""
    current_path = os.environ.get('PYTHONPATH', '')
    new_paths = [str(PROJECT_ROOT), str(SERVER_DIR)]

    for path in new_paths:
        if path not in current_path:
            current_path = f"{path}:{current_path}" if current_path else path

    os.environ['PYTHONPATH'] = current_path

def run_command(cmd, description="", cwd=None):
    """Run a command and handle errors"""
    print(f"\n{'='*60}")
    print(f"Running: {description or ' '.join(cmd)}")
    print(f"Directory: {cwd or os.getcwd()}")
    print(f"{'='*60}")

    try:
        result = subprocess.run(cmd, check=True, capture_output=False, cwd=cwd)
        print(f"\n✅ {description or 'Command'} completed successfully")
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {description or 'Command'} failed with return code {e.returncode}")
        return e.returncode
    except Exception as e:
        print(f"\n❌ Error running command: {e}")
        return 1

def install_test_dependencies():
    """Install test dependencies using pipenv"""
    print("🔧 Installing test dependencies with pipenv...")

    dependencies = [
        "pytest",
        "pytest-cov",
        "pytest-mock",
        "pytest-asyncio",
        "coverage"
    ]

    # Try pipenv first
    cmd = ["pipenv", "install", "--dev"] + dependencies
    result = run_command(cmd, "Installing test dependencies with pipenv")

    if result != 0:
        print("⚠️ pipenv failed, trying pip instead...")
        cmd = [sys.executable, "-m", "pip", "install"] + dependencies
        return run_command(cmd, "Installing test dependencies with pip")

    return result

def run_tests_with_pytest(test_path="", markers="", verbose=False, coverage=False, parallel=False):
    """Run tests using pytest"""
    setup_python_path()

    # Base pytest command
    cmd = [sys.executable, "-m", "pytest"]

    # Add test directory
    if test_path:
        cmd.append(str(TESTS_DIR / test_path))
    else:
        cmd.append(str(TESTS_DIR))

    # Add markers (e.g., -m unit, -m integration)
    if markers:
        cmd.extend(["-m", markers])

    # Add verbosity
    if verbose:
        cmd.append("-v")

    # Add coverage
    if coverage:
        cmd.extend([
            "--cov=server",
            "--cov-report=html",
            "--cov-report=term-missing",
            f"--cov-config={PROJECT_ROOT}/.coveragerc"
        ])

    # Add parallel execution
    if parallel:
        cmd.extend(["-n", "auto"])

    return run_command(cmd, f"Running tests: {test_path or 'all'}", cwd=PROJECT_ROOT)

def run_unit_tests(verbose=False, coverage=False):
    """Run unit tests only"""
    return run_tests_with_pytest(markers="unit", verbose=verbose, coverage=coverage)

def run_integration_tests(verbose=False):
    """Run integration tests only"""
    return run_tests_with_pytest(markers="integration", verbose=verbose)

def run_mock_data_tests(verbose=False):
    """Run the specific mock data tests"""
    return run_tests_with_pytest("test_with_mock_data.py", verbose=verbose)

def run_controller_tests(verbose=False):
    """Run controller-specific tests"""
    return run_tests_with_pytest("test_controller.py", verbose=verbose)

def run_utils_tests(verbose=False):
    """Run utility function tests"""
    return run_tests_with_pytest("test_utils.py", verbose=verbose)

def run_all_tests(verbose=False, coverage=False, parallel=False):
    """Run all tests"""
    return run_tests_with_pytest(verbose=verbose, coverage=coverage, parallel=parallel)

def run_specific_test(test_path, verbose=False):
    """Run a specific test file or function"""
    return run_tests_with_pytest(test_path, verbose=verbose)

def check_test_structure():
    """Verify test directory structure is correct"""
    print("🔍 Checking test directory structure...")

    required_files = [
        "test_controller.py",
        "test_utils.py",
        "test_with_mock_data.py",
        "conftest.py"
    ]

    missing_files = []
    for file in required_files:
        if not (TESTS_DIR / file).exists():
            missing_files.append(file)

    if missing_files:
        print(f"❌ Missing test files: {missing_files}")
        print(f"📁 Expected location: {TESTS_DIR}")
        return False

    print(f"✅ All test files found in {TESTS_DIR}")
    return True

def lint_code():
    """Run code linting on the server directory"""
    print("🧹 Running code linting...")

    # Install black if not available
    try:
        subprocess.run([sys.executable, "-m", "black", "--version"],
                      capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Installing black...")
        subprocess.run(["pipenv", "install", "--dev", "black"], check=True)

    # Run black on server directory
    cmd = [sys.executable, "-m", "black", "--check", "--diff", str(SERVER_DIR)]
    return run_command(cmd, "Code formatting check")

def generate_coverage_report():
    """Generate detailed coverage report"""
    setup_python_path()

    print("📊 Generating comprehensive coverage report...")

    cmd = [
        sys.executable, "-m", "pytest",
        str(TESTS_DIR),
        "--cov=server",
        "--cov-report=html",
        "--cov-report=term-missing",
        "--cov-report=xml",
        f"--cov-config={PROJECT_ROOT}/.coveragerc"
    ]

    result = run_command(cmd, "Generating coverage report", cwd=PROJECT_ROOT)

    if result == 0:
        print("\n📈 Coverage report generated:")
        print(f"  - HTML report: {PROJECT_ROOT}/htmlcov/index.html")
        print(f"  - XML report: {PROJECT_ROOT}/coverage.xml")
        print("  - Terminal report: displayed above")

    return result

def validate_system():
    """Run comprehensive system validation"""
    print("🧪 Running comprehensive system validation...")

    results = []

    # 1. Check test structure
    print("\n1️⃣ Checking test structure...")
    results.append(0 if check_test_structure() else 1)

    # 2. Install dependencies
    print("\n2️⃣ Installing dependencies...")
    results.append(install_test_dependencies())

    # 3. Lint code
    print("\n3️⃣ Checking code formatting...")
    results.append(lint_code())

    # 4. Run unit tests
    print("\n4️⃣ Running unit tests...")
    results.append(run_unit_tests(verbose=True))

    # 5. Run mock data tests
    print("\n5️⃣ Running mock data tests...")
    results.append(run_mock_data_tests(verbose=True))

    # 6. Run integration tests
    print("\n6️⃣ Running integration tests...")
    results.append(run_integration_tests(verbose=True))

    # 7. Generate coverage report
    print("\n7️⃣ Generating coverage report...")
    results.append(generate_coverage_report())

    # Summary
    print("\n" + "="*60)
    print("SYSTEM VALIDATION SUMMARY")
    print("="*60)

    test_names = [
        "Test Structure Check",
        "Dependencies Installation",
        "Code Formatting",
        "Unit Tests",
        "Mock Data Tests",
        "Integration Tests",
        "Coverage Report"
    ]

    total_passed = 0
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✅ PASSED" if result == 0 else "❌ FAILED"
        print(f"{i+1}. {name}: {status}")
        if result == 0:
            total_passed += 1

    print(f"\nOverall: {total_passed}/{len(results)} checks passed")

    if all(result == 0 for result in results):
        print("\n🎉 All validations passed! Your system is ready.")
        return 0
    else:
        print("\n⚠️  Some validations failed. Please review the output above.")
        return 1

def create_coverage_config():
    """Create .coveragerc file for coverage configuration"""
    coverage_config = """[run]
source = server
omit =
    */tests/*
    */test_*
    */__pycache__/*
    */venv/*
    */env/*

[report]
exclude_lines =
    pragma: no cover
    def __repr__
    if self.debug:
    if settings.DEBUG
    raise AssertionError
    raise NotImplementedError
    if 0:
    if __name__ == .__main__.:
    class .*\\bProtocol\\):
    @(abc\\.)?abstractmethod

[html]
directory = htmlcov
"""

    config_path = PROJECT_ROOT / ".coveragerc"
    with open(config_path, "w") as f:
        f.write(coverage_config)

    print(f"✅ Created coverage config: {config_path}")

def init_test_environment():
    """Initialize the test environment"""
    print("🚀 Initializing test environment...")

    # Create coverage config
    create_coverage_config()

    # Ensure tests directory exists
    TESTS_DIR.mkdir(exist_ok=True)

    # Create __init__.py files for proper imports
    (TESTS_DIR / "__init__.py").touch()
    (SERVER_DIR / "__init__.py").touch()

    print(f"✅ Test environment initialized")
    print(f"📁 Tests directory: {TESTS_DIR}")
    print(f"📁 Server directory: {SERVER_DIR}")

def main():
    parser = argparse.ArgumentParser(
        description="System Controller for Rewards Token Tracker Testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python system_controller.py init                    # Initialize test environment
  python system_controller.py unit                    # Run unit tests
  python system_controller.py integration             # Run integration tests
  python system_controller.py mock-data               # Run mock data tests
  python system_controller.py all --coverage          # Run all tests with coverage
  python system_controller.py validate                # Full system validation
  python system_controller.py test "test_controller.py::TestController::test_initialization"
        """
    )

    parser.add_argument(
        "command",
        choices=[
            "init", "unit", "integration", "mock-data", "controller",
            "utils", "all", "validate", "test", "coverage", "lint",
            "install-deps", "check-structure"
        ],
        help="Command to run"
    )

    parser.add_argument(
        "target",
        nargs="?",
        help="Target test file or function (for 'test' command)"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )

    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Generate coverage report"
    )

    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run tests in parallel"
    )

    args = parser.parse_args()

    try:
        if args.command == "init":
            init_test_environment()
            return 0

        elif args.command == "unit":
            return run_unit_tests(args.verbose, args.coverage)

        elif args.command == "integration":
            return run_integration_tests(args.verbose)

        elif args.command == "mock-data":
            return run_mock_data_tests(args.verbose)

        elif args.command == "controller":
            return run_controller_tests(args.verbose)

        elif args.command == "utils":
            return run_utils_tests(args.verbose)

        elif args.command == "all":
            return run_all_tests(args.verbose, args.coverage, args.parallel)

        elif args.command == "validate":
            return validate_system()

        elif args.command == "test":
            if not args.target:
                print("❌ Error: Please specify a test file or function")
                return 1
            return run_specific_test(args.target, args.verbose)

        elif args.command == "coverage":
            return generate_coverage_report()

        elif args.command == "lint":
            return lint_code()

        elif args.command == "install-deps":
            return install_test_dependencies()

        elif args.command == "check-structure":
            return 0 if check_test_structure() else 1

        else:
            parser.print_help()
            return 1

    except KeyboardInterrupt:
        print("\n\n⚠️ Operation interrupted by user")
        return 130
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())