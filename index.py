from server.db.MongoDB import MongoDB
from server.lib.Controller import Controller
from server.utils.update_db_from_file import add_distributor_transactions_from_file, add_projects_from_file
import asyncio
import requests
import os
import json
from pathlib import Path


def main():
   # db = MongoDB()
   # db.create_indexes()
   controller = Controller()
   distributor = "ChGA1Wbh9WN8MDiQ4ggA5PzBspS2Z6QheyaxdVo3XdW6"
   controller.fetch_and_process_distributor_transactions(distributor)


main()