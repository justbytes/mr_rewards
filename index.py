from server.lib.ProjectInitializer import ProjectInitializer

def main():
    distributor = "ChGA1Wbh9WN8MDiQ4ggA5PzBspS2Z6QheyaxdVo3XdW6"
    x = ProjectInitializer(distributor)

    success = x.get_initial_txs()
    print(success)
    if success is True:
        print("Processing")
        x.process_initial_txs()


main()