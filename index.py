from server.lib.ProjectInitializer import ProjectInitializer

def main():
    distributor = "HHBkrmzwY7TbDG3G5C4D52LPPd8JEs5oiKWHaPxksqvd"
    x = ProjectInitializer(distributor)

    x.get_initial_data()


main()