#!/usr/bin/env python3
"""
Database setup script for the Wallet Rewards API
Run this script once after setting up MongoDB to create indexes and test the connection
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from db import initialize_db_connection, create_indexes

def main():
    print("Setting up database for Wallet Rewards API...")

    # Test database connection
    print("Testing database connection...")
    if not initialize_db_connection():
        print("❌ Failed to connect to database. Please check your MONGO_URL environment variable.")
        return

    print("✅ Database connection successful!")

    # Create indexes for better performance
    print("Creating database indexes...")
    if create_indexes():
        print("✅ Database indexes created successfully!")
    else:
        print("❌ Failed to create indexes")
        return
c

if __name__ == "__main__":
    main()