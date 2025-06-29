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

   # db.create_indexes()

   distributor = "D8gKfTxnwBG3XPTy4ZT6cGJbz1s13htKtv9j69qbhmv4"

   transfers = db.get_project_transfers(distributor)

   organize_transfers_by_wallet(transfers, distributor)



main()