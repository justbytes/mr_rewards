# conftest.py - Pytest configuration file
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

# Test fixtures that can be used across all test files
@pytest.fixture(scope="session")
def temp_test_dir():
    """Create a temporary directory for all tests in the session"""
    temp_dir = tempfile.mkdtemp(prefix="rewards_test_")
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)

@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing - OPTIONAL"""
    # This fixture is available but not used by default
    # Use it in specific tests if you need to override env vars
    with patch.dict('os.environ', {
        'MONGO_URL': 'mongodb://localhost:27017/test_db',
        'HELIUS_API_KEY': 'test_api_key',
        'HELIUS_RPC_URL': 'https://test.helius.xyz/rpc',
        'REDIS_URL': 'redis://localhost:6379'
    }):
        yield

@pytest.fixture
def load_real_env():
    """Load real environment variables from .env file"""
    from dotenv import load_dotenv
    load_dotenv()
    yield

@pytest.fixture
def sample_wallet_addresses():
    """Sample wallet addresses for testing"""
    return [
        "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
        "A7uqmajxP3NdzbYDXiGQRKhd2bMrVDKhRtHgVQRAyrZz",
        "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
        "C9xsobayxR5PfzbZFYkIQMmf4dOvScH0EtKiXSSczbBb",
        "D0ytpcbayxS6QgzbZGYkJRNmg5eOvTdI2HuL4eTdcCc"
    ]

@pytest.fixture
def sample_token_mints():
    """Sample token mint addresses for testing"""
    return {
        "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "WSOL": "So11111111111111111111111111111111111111112"
    }

# Custom pytest markers
def pytest_configure(config):
    """Configure custom pytest markers"""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as a performance test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )

# Pytest collection hooks
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test names"""
    for item in items:
        # Mark integration tests
        if "integration" in item.nodeid.lower():
            item.add_marker(pytest.mark.integration)

        # Mark performance tests
        if "performance" in item.nodeid.lower():
            item.add_marker(pytest.mark.performance)
            item.add_marker(pytest.mark.slow)

        # Mark unit tests (default)
        if not any(marker.name in ['integration', 'performance'] for marker in item.iter_markers()):
            item.add_marker(pytest.mark.unit)

# Test data cleanup
@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Cleanup after each test"""
    yield
    # Add any cleanup logic here if needed