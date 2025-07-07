import os
import json
from pathlib import Path
from dotenv import load_dotenv
from MongoDB import MongoDB
from datetime import datetime
load_dotenv()

def get_db_instance():
    """""
    Creates an instance of the DB
    """""
    try:
        db = MongoDB()
        return db
    except Exception as e:
        print(f"There was an error when trying to initialize DB: {e}")
        return None

def backup_supported_projects():
    """
    Backup for the supported projects collection from MongoDB
    """

    # Setup the file path
    data_dir = Path(os.getenv('PROJECTS_FILE_PATH'))
    projects_file = data_dir / "supported_projects.json"

    # Get DB connection
    db = get_db_instance()

    # Make sure we have an instance of MongoDB
    if db is None:
        print("Could not establish database connection")
        return

    try:
        # Get the supported projects from DB
        projects = db.get_supported_projects()

        # Create backup data with timestamp
        backup_data = {
            "last_updated": datetime.now().isoformat(),
            "project_count": len(projects),
            "projects": projects
        }

        # Write to JSON file overwriting the file each time
        with open(projects_file, 'w') as f:
            json.dump(backup_data, f, indent=2, default=str)

        print(f"Successfully backed up {len(projects)} projects to {projects_file}")
        return

    except Exception as e:
        print(f"Could not backup projects to file: {e}")
        return

def backup_known_tokens():
    """
    Backup for the known tokens collection from MongoDB
    """

    # Setup the file path
    data_dir = Path(os.getenv('PROJECTS_FILE_PATH'))
    known_tokens_file = data_dir / "known_tokens.json"

    # Get DB connection
    db = get_db_instance()

    # Make sure we have an instance of MongoDB
    if db is None:
        print("Could not establish database connection")
        return

    try:
        # Get the known tokens from DB
        tokens = db.get_known_tokens()

        # Create backup data with timestamp
        backup_data = {
            "last_updated": datetime.now().isoformat(),
            "tokens_count": len(tokens),
            "known_tokens": tokens
        }

        # Write to JSON file overwriting the file each time
        with open(known_tokens_file, 'w') as f:
            json.dump(backup_data, f, indent=2, default=str)

        print(f"Successfully backed up {len(tokens)} known tokens to {known_tokens_file}")
        return

    except Exception as e:
        print(f"Could not backup projects to file: {e}")
        return

def backup_transfers():
    """
    Backup transfers for each distributor as separate files organized by date
    Structure: transfers/{distributor_address}/{YYYY-MM-DD}.json
    """

    # Setup the file path
    data_dir = Path(os.getenv('PROJECTS_FILE_PATH'))
    transfers_dir = data_dir / "transfers"

    # Get DB connection
    db = get_db_instance()

    if db is None:
        print("Could not establish database connection")
        return

    try:
        # Get a list of the supported projects
        projects = db.get_supported_projects()

        # Get current date for filename
        current_date = datetime.now().strftime("%Y-%m-%d")

        # Backup each distributors transfers
        for project in projects:
            # Get the distributor
            distributor = project.get("distributor")

            # Create distributor-specific directory
            distributor_dir = transfers_dir / distributor

            # Create date-specific backup file
            backup_file = distributor_dir / f"{current_date}.json"

            print(f"Starting backup for distributor: {distributor}")
            distributors_transfers = db.get_all_transfers_for_distributor(distributor)

            if distributors_transfers is None:
                print(f"Failed to get transfers for distributor {distributor}")
                continue

            # Create backup data with timestamp
            backup_data = {
                "backup_date": current_date,
                "backup_timestamp": datetime.now().isoformat(),
                "distributor": distributor,
                "project_name": project.get("name", "unknown"),
                "transfers_count": len(distributors_transfers),
                "transfers": distributors_transfers
            }

            # Write backup to date-specific file
            with open(backup_file, 'w') as f:
                json.dump(backup_data, f, indent=2, default=str)

            print(f"Successfully backed up {len(distributors_transfers)} transfers for {distributor}")
            print(f"Backup saved to: {backup_file}")

        return

    except Exception as e:
        print(f"Error during backup: {e}")
        return

def backup_wallets():
    pass

if __name__ == "__main__":
    backup_transfers()