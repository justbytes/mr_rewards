import os
import json
from server.lib.BackerUpper import BackerUpper
from server.lib.ProjectInitializer import ProjectInitializer
from server.tests.test_controller import run_all_tests
"""
This is going to be converted into a C.L.I with options to add and backup data
"""
##########################################################
#              Backup Data To Local Storage              #
##########################################################
backer_upper = BackerUpper()

#Check the initial count
#print(backer_uppessr.mongo.get_wallet_rewards("9dPHyjTpBQSTjnfh2vSCYjbHWgnR37k6mPTeRyZrMz4Q"))

def get_counts():
    transfers = backer_upper.sqlite.get_temp_transfers_count()
    projects = backer_upper.sqlite.get_supported_project_count()
    known_tokens = backer_upper.sqlite.get_known_tokens_count()
    wallets = backer_upper.sqlite.get_wallets_count()

    print(f"Temp transfer count: {transfers}")
    print(f"Projects count: {projects}")
    print(f"Known tokens count: {known_tokens}")
    print(f"Wallets count: {wallets}")

    for project in backer_upper.sqlite.get_supported_projects():
        count = backer_upper.sqlite.get_transfers_count(project.get("distributor"))
        print(f"Transfers for {project.get("distributor")}: {count}")

# get_counts()



run_all_tests()


# filepath = f"/Users/xtox/Coding/mr_rewards/temp_backup/transfers/{distributor}/2025-07-07.json"

# with open(filepath, 'r') as file:
#     data = json.load(file)
#     transfers = data['transfers']

#     backer_upper.backup_single_distributor_transfers(distributor, transfers)

# # Check the final count
# print(backer_upper.sqlite.get_transfers_count(distributor))


##########################################################
#                 Initialize New Projects                #
##########################################################
# Project to add
# project = {
#     "name": "",
#     "distributor": "",
#     "token_mint": "",
#     "dev_wallet": None,
#     "last_sig": None
# }

# # Create instance of initializer
# initializer = ProjectInitializer(project)

# # Initialize a new project
# initializer.initalize_project()