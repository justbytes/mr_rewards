from server.db.MongoDB import MongoDB
from server.lib.fetch_data import get_and_add_token_metadata, organize_transfers_by_wallet
from server.utils.update_db_from_file import add_project_transfers_from_file
import asyncio
import requests
import os
import json
from pathlib import Path


def main():
   db = MongoDB()

   transfers = db.get_project_transfers("BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ")

   organize_transfers_by_wallet(transfers, "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ")



main()