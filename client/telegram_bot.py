import os
import json
import telebot
import redis
import requests
from telebot import types
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def mr_rewards_bot():
    """Telegram bot that queires the rewards tracker api to get a users rewards data"""

    # Create the instance of the telegram bot using the bot father token
    BOT_TOKEN = os.getenv("TELE_BOT_TOKEN")
    bot = telebot.TeleBot(BOT_TOKEN)

    # Get the list of supported projects
    projects = get_supported_projects()

    # Cache that stores users chat ids which holds their wallet data
    redis_client = redis.from_url(os.getenv("REDIS_URL"))

    # Main menu photo path
    main_menu_photo_path = Path("assets/mr_rewards.png")

    ##########################################################
    #                    Main Menu Handlers                  #
    ##########################################################
    @bot.message_handler(commands=["start", "home"])
    def handle_main_menu_command(message):
        """Create and display the main menu when /start or /home are sent by the user"""

        # Build the main menu view passing the chat id to check if the user
        # added a wallet already
        markup, response_text = create_main_menu_display(message)

        # Display the main menu with photo
        with open(main_menu_photo_path, 'rb') as photo:
            bot.send_photo(
                message.chat.id,
                photo,
                caption=response_text,
                parse_mode="Markdown",
                reply_markup=markup
            )

    @bot.callback_query_handler(func=lambda call: call.data == "home")
    def handle_main_menu_callback(call):
        """Displays the main menu for the home callback"""

        # Acknowledge call
        bot.answer_callback_query(call.id)

        # Build the main menu view passing the chat id to check if the user
        # added a wallet already
        markup, response_text = create_main_menu_display(call.message)

        # Display the main menu with photo
        with open(main_menu_photo_path, 'rb') as photo:
            bot.send_photo(
                call.message.chat.id,
                photo,
                caption=response_text,
                parse_mode="Markdown",
                reply_markup=markup
            )

    ##########################################################
    #                   User Cache Handlers                  #
    ##########################################################
    @bot.message_handler(commands=["set_wallet"])
    def handle_set_wallet_command(message):
        """
        Prompts the user for their wallet address and calls the set_user_wallet_data function
        when the /set_wallet command is sent by the user
        """

        # Prompt user to enter the address
        bot.send_message(
            message.chat.id,  # Chat id for our user
            f"Please enter your wallet address\n (Type 'cancel' to stop)",
        )

        # Once they response with the wallet address we call the set_user_wallet_data
        # to get the wallet data and add it to the cache
        bot.register_next_step_handler(message, set_user_wallet_data)

    @bot.callback_query_handler(func=lambda call: call.data == "set_wallet")
    def handle_set_wallet_callback(call):
        """
        Prompts the user for their wallet address and calls the set_user_wallet_data function
        for the set_wallet callback
        """

        # Acknowledge call
        bot.answer_callback_query(call.id)

        # send user the prompt to enter wallet address
        bot.send_message(
            call.message.chat.id,
            f"Please enter your wallet address\n (Type 'cancel' to stop)",
        )

        # Once they response with the wallet address we call the set_user_wallet_data
        # to get the wallet data and add it to the cache
        bot.register_next_step_handler(call.message, set_user_wallet_data)

    def set_user_wallet_data(message):
        """Adds a user chat id and wallet data into the user cache"""
        # Remove whitespaces from string
        wallet_address = message.text.strip()

        # Users can cancel the operation by typing cancel and will be redirected to
        # the main menu
        if wallet_address.lower() == "cancel":
            bot.send_message(message.chat.id, "❌ Operation cancelled.")
            handle_main_menu_command(message)
            return

        # Validate the wallet address and prompt a retry if its not a valid address
        if len(wallet_address) < 32 or len(wallet_address) > 44:
            bot.send_message(
                message.chat.id,
                f"❌ Invalid wallet address (length: {len(wallet_address)}). \n\n"
                f"Please enter a valid wallet address (32-44 characters) or type 'cancel' to stop:",
            )
            bot.register_next_step_handler(message, set_user_wallet_data)
            return

        # Fetch the rewards data
        rewards_data = get_rewards_data(wallet_address)

        # If theres none notify the user and redirect back to supported projects
        if rewards_data is None:
            bot.send_message(
                message.chat.id, f"❌ No rewards data found for {wallet_address}."
            )
            handle_main_menu_command(message)
            return

        # If the data is of type string then there was an error and we will print the
        # exception to the user and redirect back to supported projects
        elif isinstance(rewards_data, str):
            bot.send_message(message.chat.id, f"❌ {rewards_data}")
            handle_main_menu_command(message)
            return

        data = {
            "rewards_data": rewards_data,
            "last_updated": datetime.now().isoformat(),
        }

        # Add the wallet data to the cache
        cache_key = f"user_id:{message.chat.id}"
        try:
            redis_client.setex(cache_key, 3600, json.dumps(data))
        except Exception as e:
            bot.send_message(
                    message.chat.id, f"❌ Couldn't save wallet address. Please try again later."
                )
            handle_main_menu_command(message)
            return


        # Return to the main menu
        handle_main_menu_command(message)

    def get_user_wallet_data(message):
        """
        Get a users wallet with their chat id and updates the cache if the data
        is stale
        """

        chat_id = message.chat.id

        # Get the cache data
        try:
            cache_key = f"user_id:{chat_id}"
            cached_data_json = redis_client.get(cache_key)

            # Check if data exists in cache
            if cached_data_json is None:
                return None

            # Parse the JSON data
            cached_data = json.loads(cached_data_json)
        except Exception as e:
            print("There was an error getting the cache data")
            redis_client.delete(cache_key)
            return None

        # Parse the timestamp
        last_updated = datetime.fromisoformat(cached_data["last_updated"])
        current_time = datetime.now()

        # Check if data is older than 5 minutes
        time_diff = current_time - last_updated
        if time_diff.total_seconds() > 300:  # 300 seconds = 5 minutes

            # Get users wallet
            wallet_address = cached_data["rewards_data"].get("wallet_address")

            # Refetch the rewards data
            rewards_data = get_rewards_data(wallet_address)

            # If theres none notify the user and redirect back to supported projects
            if rewards_data is None:
                bot.send_message(
                    message.chat.id, f"❌ No rewards data found for {wallet_address}."
                )
                handle_main_menu_command(message)
                return None

            # If the data is of type string then there was an error and we will print the
            # exception to the user and redirect back to supported projects
            elif isinstance(rewards_data, str):
                bot.send_message(message.chat.id, f"❌ {rewards_data}")
                handle_main_menu_command(message)
                return "error"

            # Update cache with fresh data
            updated_data = {
                "rewards_data": rewards_data,
                "last_updated": datetime.now().isoformat(),
            }

            # Update cache with the new data
            try:
                redis_client.setex(cache_key, 3600, json.dumps(updated_data))
            except Exception as e:
                bot.send_message(
                    message.chat.id, f"❌ Couldn't save wallet address. Please try again later."
                )
                handle_main_menu_command(message)
                return "error"

            return updated_data["rewards_data"]

        # Data is fresh, return cached rewards data
        return cached_data["rewards_data"]

    ##########################################################
    #               Supported Projects Handlers              #
    ##########################################################
    @bot.message_handler(commands=["supported_projects"])
    def handle_supported_projects_command(message):
        """Creates and displays the supported projects if the /supported_projects comamand is used"""

        # Build the supported projects display
        try:
            markup = create_supported_projects_display()
        except Exception as e:
            print(f"Error creating markup")
            bot.send_message(message.chat.id, "❌ Error loading projects. Please try again.")
            return

        # Delete the old message (photo message) and send a new text message
        try:
            bot.delete_message(message.chat.id, message.message_id)
            # print("Message deleted successfully")
        except Exception as e:
            print(f"Error deleting message")
            # Continue anyway - deletion failure shouldn't stop us

        # Send new message
        try:
            sent_message = bot.send_message(
                message.chat.id,
                "Please select a project to check rewards:",
                reply_markup=markup,
            )
        except Exception as e:
            print(f"Error sending message in supported projects callback")
            # Send a fallback message without markup
            try:
                bot.send_message(message.chat.id, "❌ Error loading projects menu. Please try /home to restart.")
            except Exception as fallback_error:
                print(f"Fallback message also failed: {fallback_error}")

    @bot.callback_query_handler(func=lambda call: call.data == "supported_projects")
    def handle_supported_projects_callback(call):
        """Creates and displays the supported projects for the supported_projects callback"""

        # Acknowledge the call
        bot.answer_callback_query(call.id)

        # Build the supported projects display
        try:
            markup = create_supported_projects_display()
        except Exception as e:
            print(f"Error creating markup")
            bot.send_message(call.message.chat.id, "❌ Error loading projects. Please try again.")
            return

        # Delete the old message (photo message) and send a new text message
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
            # print("Message deleted successfully")
        except Exception as e:
            print(f"Error deleting message")
            # Continue anyway - deletion failure shouldn't stop us

        # Send new message
        try:
            sent_message = bot.send_message(
                call.message.chat.id,
                "Please select a project to check rewards:",
                reply_markup=markup,
            )
        except Exception as e:
            print(f"Error sending message in supported projects callback")
            # Send a fallback message without markup
            try:
                bot.send_message(call.message.chat.id, "❌ Error loading projects menu. Please try /home to restart.")
            except Exception as fallback_error:
                print(f"Fallback message also failed: {fallback_error}")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("proj_"))
    def handle_supported_project_selection(call):
        """
        This is called when a user clicks a project button. First it slices the "project_" from the call data
        then we prompt the user for the wallet address, next we make the call to get the rewards with the
        wallet address and the project name
        """

        # Cut off the calldata identifier
        callback_data = call.data.replace("proj_", "")
        # Seperate the name from the distributor
        parts = callback_data.rsplit("_", 2)
        name = parts[0]
        distributor = parts[1]
        back_to = parts[2] # Points to where we should navigate the user if they click the "Go Back" button

        # Check the cache to see if we have the users wallet already stored
        rewards_data = get_user_wallet_data(call.message)

        # If users wallet isn't in the cache we will prompt them to add it
        if rewards_data is None:
            # send user the prompt to enter wallet address
            bot.send_message(
                call.message.chat.id,
                f"⚠️ No wallet data found please configure your wallet first.\n\n Please enter your wallet address\n (Type 'cancel' to stop)",
            )

            # Once they response with the wallet address we call the set_user_wallet_data
            # to get the wallet data and add it to the cache
            bot.register_next_step_handler(call.message, set_user_wallet_data)
            return
        elif rewards_data == "error":
            return # User has been redirected to main menu already so stop

        # Get the users wallet address and rewards amounts for the selected distributor
        wallet_address = rewards_data.get("wallet_address")
        rewards_from_project = rewards_data["distributors"].get(distributor)

        # If we have rewards build the rewards display otherwise notify the user that
        # they didn't recieve rewards from that distributor
        if rewards_from_project:
            create_rewards_display(
                call.message, rewards_from_project, name, wallet_address, back_to
            )
        else:
            bot.send_message(
                call.message.chat.id, f"❌ No rewards recieved from {name}"
            )
            handle_supported_projects_command(call.message)
            return

    ##########################################################
    #                   See Rewards Handlers                 #
    ##########################################################
    @bot.message_handler(commands=["rewards"])
    def handle_rewards_command(message):
        """Displays the distributors that have sent the users configured wallet rewards"""
        # Check the cache for a the user configured wallet
        rewards_data = get_user_wallet_data(message)

        # If there is no data then we don't have the users wallet in the cache so we will prompt them to add one
        if rewards_data is None:
            # send user the prompt to enter wallet address
            bot.send_message(
                message.chat.id,
                f"⚠️ You need to configure your wallet first.\n\n Please enter your wallet address\n (Type 'cancel' to stop)",
            )

            # Once they response with the wallet address we call the set_user_wallet_data
            # to get the wallet data and add it to the cache
            bot.register_next_step_handler(message, set_user_wallet_data)
            return

         # User has been redirected to main menu already so stop
        elif rewards_data == "error":
            return

        # Create the display of projects that have issued rewards
        create_wallets_distributors_display(message, rewards_data)

    @bot.callback_query_handler(func=lambda call: call.data == "rewards")
    def handle_rewards_callback(call):
        """Displays the distributors that have sent the users configured wallet rewards"""

        # Check the cache for a the user configured wallet
        rewards_data = get_user_wallet_data(call.message)

        # If there is no data then we don't have the users wallet in the cache so we will prompt them to add one
        if rewards_data is None:
            # send user the prompt to enter wallet address
            bot.send_message(
                call.message.chat.id,
                f"⚠️ You need to configure your wallet first.\n\n Please enter your wallet address\n (Type 'cancel' to stop)",
            )

            # Once they response with the wallet address we call the set_user_wallet_data
            # to get the wallet data and add it to the cache
            bot.register_next_step_handler(call.message, set_user_wallet_data)
            return

        # User has been redirected to main menu already so stop
        elif rewards_data == "error":
            return

        # Create the display of projects that have issued rewards
        create_wallets_distributors_display(call.message, rewards_data)

    ##########################################################
    #                  Unknown Command handler                #
    ##########################################################
    @bot.message_handler(func=lambda message: True)
    def handle_unknown_command(message):
        """Tells the user that they used an unkown command"""
        bot.reply_to(message, "Unknown command. Use /home to go back to the main menu!")

    ##########################################################
    #                      Telegram Views                    #
    ##########################################################
    def create_main_menu_display(message):
        """
        This is the display for the main menu which features two buttons. One to
        to see supported projects the other that shows rewards for a given wallet
        """
        chat_id = message.chat.id
        markup = types.InlineKeyboardMarkup()
        wallet = None

        # Check if we have a users configured wallet in the cache
        rewards_data = get_user_wallet_data(message)

        if rewards_data == "error":
            # Return a basic menu even if there's an error
            pass  # Continue with wallet = None
        # If we have a response then there will be a wallet address
        elif rewards_data is not None:
            wallet_address = rewards_data.get("wallet_address")
            wallet = f"{wallet_address[:4]}...{wallet_address[-4:]}"

        # See rewards button
        rewards_button = types.InlineKeyboardButton(
            text="See Rewards", callback_data="rewards"
        )

        # Supported projects button
        projects_button = types.InlineKeyboardButton(
            text="Supported Projects", callback_data="supported_projects"
        )

        # Configure wallet button
        users_wallet_button = types.InlineKeyboardButton(
            text="Configure Wallet", callback_data="set_wallet"
        )

        # Add the elements to the markup
        markup.add(rewards_button)
        markup.add(projects_button)
        markup.add(users_wallet_button)

        # Add the message body
        response_text = "⎑⎑⎑ Mr. Rewards ⎑⎑⎑\n"
        response_text += "---------------------\n"

        # If we have a wallet address then we should display it otherwise just display the N/A
        if wallet is not None:
            response_text += f"*📬 Wallet*: {wallet}\n\n"
        else:
            response_text += f"*📬 Wallet*: N/A\n\n"

        return markup, response_text

    def create_supported_projects_display():
        """
        This is the markup for the supported projects display. All projects are put in
        buttons with width of 3 per row
        """
        markup = types.InlineKeyboardMarkup()

        # Back button to go to main menu
        back_button = types.InlineKeyboardButton(text="Go Back", callback_data="home")

        buttons = []

        # Create a button for each project
        for project in projects:
            name = project.get("name")
            distributor = project.get("distributor")
            button = types.InlineKeyboardButton(
                text=name,
                callback_data=f"proj_{name}_{distributor}_s",  # s stands for supported projects
            )

            buttons.append(button)

        # Add the buttons to the markup with the set max width
        for i in range(0, len(buttons), 3):
            row = buttons[i : i + 3]  # Get 3 buttons at a time
            markup.row(*row)

        markup.add(back_button)
        return markup

    def create_wallets_distributors_display(message, data):
        markup = types.InlineKeyboardMarkup()

        # Back button to go to main menu
        back_button = types.InlineKeyboardButton(text="Go Back", callback_data="home")

        buttons = []

        # Create a button for each project
        for distributor in data["distributors"]:
            name = get_distributor_name_by_address(distributor, projects)

            button = types.InlineKeyboardButton(
                text=name,
                callback_data=f"proj_{name}_{distributor}_r",  # r means callback for the back button will be set to "rewards"
            )

            buttons.append(button)

        # Add the buttons to the markup with the set max width
        for i in range(0, len(buttons), 3):
            row = buttons[i : i + 3]  # Get 3 buttons at a time
            markup.row(*row)

        markup.add(back_button)
        bot.send_message(
            message.chat.id,
            "Please select a project to see rewards.",
            reply_markup=markup,
            parse_mode="Markdown",
        )

    def create_rewards_display(message, data, project_name, wallet_address, back_to):
        # Format the rewards information
        response_text = f"🤑 *Rewards from {project_name}*\n"
        response_text += f"📬 Wallet: `{wallet_address[:6]}...{wallet_address[-4:]}`\n"

        # Get the tokens data
        tokens = data.get("tokens", {})

        # Create the applicable message response
        if not tokens:
            response_text += "No reward amounts available."
        else:
            response_text += "*🧐 Rewards Received:*\n\n"

            # Loop through the tokens and add each one to the message
            for token_name, token_data in tokens.items():
                total_amount = token_data.get("total_amount", 0)
                response_text += f"*{token_name}*: {total_amount:,.6f}\n"

        markup = types.InlineKeyboardMarkup()

        # R stands for rewards callback. This is so our back button navigates the user back to
        # the display they came from
        if back_to == "r":
            callback = "rewards"
        else:
            callback = "supported_projects"

        # Back button to go to main menu
        back_button = types.InlineKeyboardButton(text="Go Back", callback_data=callback)

        markup.add(back_button)
        # Send the user the data
        bot.send_message(
            message.chat.id, response_text, reply_markup=markup, parse_mode="Markdown"
        )

     # Start a timer that cleans the users cache every 1 hr and 15 minutes(4500 seconds)

    # Begin polling
    bot.infinity_polling()

##########################################################
#                        API Calls                       #
##########################################################
def get_supported_projects():
    """This calls the API to get the list of supported projects"""
    url = f"{os.getenv('API_URL')}/supported_projects"
    try:
        response = requests.get(url, timeout=30)
        return response.json()
    except Exception as e:
        print(f"Could not get supported projects from server. Please try again later.")
        raise

# This needs to handle the 429 errors and return the correct string back so the user knows whats going on
def get_rewards_data(wallet_address):
    """This calls the API to get the rewards data for a given wallet address"""
    url = f"{os.getenv("API_URL")}/rewards/{wallet_address}"
    try:
        response = requests.get(url, timeout=30)
        return response.json()
    except Exception as e:
        print(
            f"Could not get rewards data for wallet address from server. Please try again later."
        )
        return "Could not get rewards data for wallet address from server. Please try again later."

def get_distributor_name_by_address(distributor_address, projects):
    """Gets the distributor name for a given distributor address by searching through supported projects"""
    for project in projects:
        if project.get("distributor") == distributor_address:
            return project.get("name")

    return None

# Start the telegram bot
if __name__ == "__main__":
    mr_rewards_bot()
