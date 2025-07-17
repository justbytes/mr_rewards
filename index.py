from server.lib.ProjectInitializer import ProjectInitializer

def main():

    # Project to add
    project = {
        "name": "REVS",
        "distributor": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
        "token_mint": "9VxExA1iRPbuLLdSJ2rB3nyBxsyLReT4aqzZBMaBaY1p",
        "dev_wallet": None,
        "last_sig": None
    }

    initializer = ProjectInitializer(project)

    # success = initializer.get_initial_txs()
    # print(success)

    # if success is False:
    #     return

    # success = initializer.process_initial_txs()
    # print(success)

    # if success is False:
    #     return

    success = initializer.insert_and_clean_project()
    print(success)

    if success is False:
        return

    success = initializer.aggregate_rewards_from_transfers()
    print(success)

    if success is False:
        return

    # success = initializer.aggregate_rewards_from_transfers()
    # print(success)

    # if success is False:
    #     return

    # success = initializer.clean_up_and_insert_project()
    # print(success)

    # if success is False:
    #     return

main()

# if __name__ == "__main__":
#     name = "ITD",
#     distributor = "HHBkrmzwY7TbDG3G5C4D52LPPd8JEs5oiKWHaPxksqvd"
#     token_mint = "GAnGTiGGBsnpWwCZDd7FJVro59eGNKNMmbgqAMvccCq9"
#     dev_wallet = None

#     main(name, distributor, token_mint, dev_wallet)