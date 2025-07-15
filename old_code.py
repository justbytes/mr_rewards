import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Setup file paths
backup_dir = Path(os.getenv('PROJECTS_FILE_PATH'))
temp_storage_dir = backup_dir / "temp_storage"
temp_data_file = temp_storage_dir / "temp_txs_storage.json"
last_sig_file = temp_storage_dir / "temp_txs_last_sig.json"

# Create directories if they don't exist
temp_storage_dir.mkdir(parents=True, exist_ok=True)

def get_last_sigs(self):
        """Get both before and last_sig values from file"""
        try:
            if last_sig_file.exists():
                with open(last_sig_file, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        before = data.get("before")
                        last_sig = data.get("last_sig")
                        return before, last_sig
                    else:
                        # Handle legacy format where it was just a string
                        return None, data if isinstance(data, str) else None
            return None, None
        except Exception as e:
            print(f"Error reading last signatures: {e}")
            return None, None

def set_last_sig(self, new_sig):
    """Save the last processed signature to file (legacy method)"""
    try:
        # Try to preserve existing data structure
        existing_data = {"before": None, "last_sig": None}
        if last_sig_file.exists():
            with open(last_sig_file, 'r') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    existing_data = data

        # Update last_sig
        existing_data["last_sig"] = new_sig

        with open(last_sig_file, 'w') as f:
            json.dump(existing_data, f, indent=2, default=str)
    except Exception as e:
        print(f"Error saving last signature: {e}")

def set_before_sig(self, new_sig):
    """Save the last processed signature to file"""
    try:
        # Try to preserve existing data structure
        existing_data = {"before": None, "last_sig": None}
        if last_sig_file.exists():
            with open(last_sig_file, 'r') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    existing_data = data

        # Update last_sig
        existing_data["before"] = new_sig

        with open(last_sig_file, 'w') as f:
            json.dump(existing_data, f, indent=2, default=str)
    except Exception as e:
        print(f"Error saving last signature: {e}")

def save_transactions_to_file(self, transactions):
    """Save transactions to the temporary storage file"""
    try:
        # Load existing data if file exists
        existing_data = []
        if temp_data_file.exists():
            with open(temp_data_file, 'r') as f:
                existing_data = json.load(f)

        # Append new transactions
        existing_data.extend(transactions)

        # Save back to file
        with open(temp_data_file, 'w') as f:
            json.dump(existing_data, f, indent=2, default=str)

    except Exception as e:
        print(f"Error saving transactions to file: {e}")
        raise

def clear_temp_files(self):

    """Clear temporary storage files"""
    try:
        if temp_data_file.exists():
            temp_data_file.unlink()
        if last_sig_file.exists():
            last_sig_file.unlink()
        print("Temporary files cleared")
    except Exception as e:
        print(f"Error clearing temp files: {e}")
