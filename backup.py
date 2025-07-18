from server.lib.BackerUpper import BackerUpper

def backup():
    """
    This is used to get a backup of the current state of the production databases and archives them
    to local SQLite DBs
    """
    backer_upper = BackerUpper()

    backer_upper.backup_known_tokens()

backup()