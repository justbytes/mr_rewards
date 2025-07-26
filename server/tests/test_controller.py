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
from lib.Controller import Controller
from .mock_data import TRANSACTIONS, PROJECT, WALLET, KNOWN_TOKENS


class TestController:
    def setup_method(self):
        """Setup method run before each test"""
        self.controller = Controller(True)  # Use test database

        # Clean up test data before each test
        self.cleanup_test_data()
