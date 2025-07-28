# Helper function to generate API keys
import secrets
import string
import sys
import os

"""
Utility script to manage API keys in your SQLite database
Run this script to create, list, update, or delete API keys
"""

def generate_api_key(prefix: str = "sk_live_", length: int = 48) -> str:
    """
    Generate a new API key

    Args:
        prefix: Prefix for the API key
        length: Total length of the random part

    Returns:
        str: Generated API key
    """
    # Generate random string
    alphabet = string.ascii_lowercase + string.ascii_uppercase + string.digits
    random_part = ''.join(secrets.choice(alphabet) for _ in range(length))

    return f"{prefix}{random_part}"

def create_api_key(sqlite_db, name=None, rate_limit=None):
    """Create a new API key"""
    api_key = generate_api_key()
    success = sqlite_db.insert_api_key(api_key, name, rate_limit)

    if success:
        print(f"\nAPI Key created successfully!")
        print(f"Key: {api_key}")
        print(f"Name: {name or 'Unnamed'}")
        print(f"Rate Limit: {rate_limit or 'Unlimited'} requests/minute")
        print(f"\nUse this key in requests:")
        print(f"Authorization: Bearer {api_key}")
    else:
        print("Failed to create API key")

def list_api_keys(sqlite_db):
    """List all API keys"""
    keys = sqlite_db.get_all_api_keys()

    if not keys:
        print("No API keys found")
        return

    print(f"\n{'ID':<5} {'Name':<20} {'Key':<20} {'Active':<8} {'Usage':<8} {'Created':<20}")
    print("-" * 85)

    for key in keys:
        key_display = key['key'][:16] + "..." if len(key['key']) > 16 else key['key']
        print(f"{key['id']:<5} {(key['name'] or 'Unnamed'):<20} {key_display:<20} "
              f"{'Yes' if key['is_active'] else 'No':<8} {key['usage_count']:<8} {key['created_at']:<20}")

def deactivate_api_key(sqlite_db, api_key):
    """Deactivate an API key"""
    success = sqlite_db.update_api_key(api_key, is_active=False)
    if success:
        print(f"API key deactivated successfully")
    else:
        print("Failed to deactivate API key or key not found")

def main():
    # Initialize SQLite database
    sqlite_db = SQLiteDB(test=False, temp=False)

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python manage_api_keys.py create [name] [rate_limit]")
        print("  python manage_api_keys.py list")
        print("  python manage_api_keys.py deactivate <api_key>")
        return

    command = sys.argv[1]

    if command == "create":
        name = sys.argv[2] if len(sys.argv) > 2 else None
        rate_limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
        create_api_key(sqlite_db, name, rate_limit)

    elif command == "list":
        list_api_keys(sqlite_db)

    elif command == "deactivate":
        if len(sys.argv) < 3:
            print("Please provide the API key to deactivate")
            return
        api_key = sys.argv[2]
        deactivate_api_key(sqlite_db, api_key)

    else:
        print(f"Unknown command: {command}")

    sqlite_db.close_connections()

if __name__ == "__main__":
    main()