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
   controller.fetch_and_process_distributor_transactions(distributor)


if __name__ == "__main__":
   main()