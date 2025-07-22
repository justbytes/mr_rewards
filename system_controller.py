import os
import json
from server.lib.BackerUpper import BackerUpper
from server.lib.ProjectInitializer import ProjectInitializer

"""
This is going to be converted into a C.L.I with options to add and backup data
"""
##########################################################
#              Backup Data To Local Storage              #
##########################################################
backer_upper = BackerUpper()

#Check the initial count
backer_upper.backup_wallets()
print(backer_uppessr.mongo.get_wallet_rewards("9dPHyjTpBQSTjnfh2vSCYjbHWgnR37k6mPTeRyZrMz4Q"))



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