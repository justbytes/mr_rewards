from server.lib.BackerUpper import BackerUpper

def backup():
    """
    This is used to get a backup of the current state of the production databases and archives them
    to local SQLite DBs
    """
    backer_upper = BackerUpper()

    print(backer_upper.sqlite.get_transfers_count("CvgM6wSDXWCZeCmZnKRQdnh4CSga3UuTXwrCXy9Ju6PC"))

backup()