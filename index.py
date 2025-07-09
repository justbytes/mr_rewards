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
   controller.update_distributors_transactions()



if __name__ == "__main__":
   main()