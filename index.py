from server.db.MongoDB import MongoDB
# from server.lib.Controller import Controller
from server.apps.telegram_bot import mr_rewards_bot

def main():
   # db = MongoDB()
   # db.create_indexes()
   # controller = Controller()
   # controller.update_distributors_transactions()
    mr_rewards_bot()



if __name__ == "__main__":
   main()