from ..server.lib.ProjectInitializer import ProjectInitializer

def initializer():
    """
    This can be used to add new projects the rewards tracker via the ProjectInitializer
    """
    # Project to add
    project = {
        "name": "",
        "distributor": "",
        "token_mint": "",
        "dev_wallet": None,
        "last_sig": None
    }

    # Create instance of initializer
    initializer = ProjectInitializer(project)

    # Initialize a new project
    initializer.initalize_new_project()

initializer()