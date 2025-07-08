from server.db.MongoDB import MongoDB
from server.lib.Controller import Controller
import asyncio
import requests
import os
import json
from pathlib import Path


def main():
   # db = MongoDB()
   # db.create_indexes()
   controller = Controller()

   projects = controller.get_supported_projects_from_db()

   for project in projects:
      distributor = project.get("distributor")
      transfers = controller.get_all_transfers_for_distributor_from_db(distributor)
      controller.aggregate_rewards(transfers)

if __name__ == "__main__":
   main()