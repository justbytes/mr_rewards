from server.lib.ProjectInitializer import ProjectInitializer

def main():
    distributor = "HHBkrmzwY7TbDG3G5C4D52LPPd8JEs5oiKWHaPxksqvd"
    x = ProjectInitializer(distributor)

    success = x.get_initial_txs()
    print(success)
    if success is True:
        print("Processing")
        x.process_initial_txs()


main()