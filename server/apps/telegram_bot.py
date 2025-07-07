import os
import json
import telebot
import requests
from telebot import types
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()


def mr_rewards_bot():
    """Telegram bot that queires the rewards tracker api to get a users rewards data"""

    # Create the instance of the telegram bot using the bot father token
    BOT_TOKEN = os.getenv("TELE_BOT_TOKEN")
    bot = telebot.TeleBot(BOT_TOKEN)

    # Get the list of supported projects
    projects = get_supported_projects()

    # Cache that stores users chat ids which holds their wallet data
    user_cache = {}

    ##########################################################
    #                    Main Menu Handlers                  #
    ##########################################################
    @bot.message_handler(commands=["start", "home"])
    def handle_main_menu_command(message):
        """ Create and display the main menu when /start or /home are sent by the user"""

        # Build the main menu view passing the chat id to check if the user
        # added a wallet already
        markup, response_text = create_main_menu_display(message.chat.id)

        # Display the main menu
        bot.send_message(
            message.chat.id,
            response_text,
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
        markup, response_text = create_main_menu_display(call.message.chat.id)

        # Display the main menu
        bot.edit_message_text(
            response_text,
            call.message.chat.id,
            call.message.message_id,
            parse_mode="Markdown",
            reply_markup=markup
        )

    ##########################################################
    #                   Set Wallet Handlers                  #
    ##########################################################
    @bot.message_handler(commands=["set_wallet"])
    def handle_set_wallet_command(message):
        """
        Prompts the user for their wallet address and calls the set_user_wallet function
        when the /set_wallet command is sent by the user
        """

        # Prompt user to enter the address
        bot.send_message(
            message.chat.id, # Chat id for our user
            f"Please enter your wallet address\n (Type 'cancel' to stop)"
        )


        # Once they response with the wallet address we call the set_user_wallet
        # to get the wallet data and add it to the cache
        bot.register_next_step_handler(message, set_user_wallet)

    @bot.callback_query_handler(func=lambda call: call.data == "set_wallet")
    def handle_set_wallet_callback(call):
        """
        Prompts the user for their wallet address and calls the set_user_wallet function
        for the set_wallet callback
        """

        # Acknowledge call
        bot.answer_callback_query(call.id)

        # send user the prompt to enter wallet address
        bot.send_message(
            call.message.chat.id,
            f"Please enter your wallet address\n (Type 'cancel' to stop)"
        )

        # Once they response with the wallet address we call the set_user_wallet
        # to get the wallet data and add it to the cache
        bot.register_next_step_handler(call.message, set_user_wallet)

    def set_user_wallet(message):
        """ Adds a user chat id and wallet data into the user cache"""
        # Remove whitespaces from string
        wallet_address = message.text.strip()

        # Users can cancel the operation by typing cancel and will be redirected to
        # the main menu
        if wallet_address.lower() == "cancel":
            bot.send_message(
                message.chat.id,
                "❌ Operation cancelled."
            )
            handle_main_menu_command(message)
            return

        # Validate the wallet address and prompt a retry if its not a valid address
        if len(wallet_address) < 32 or len(wallet_address) > 44:
            bot.send_message(
                message.chat.id,
                f"❌ Invalid wallet address (length: {len(wallet_address)}). \n\n"
                f"Please enter a valid wallet address (32-44 characters) or type 'cancel' to stop:"
            )
            bot.register_next_step_handler(message, set_user_wallet)
            return

        # Fetch the rewards data
        rewards_data = get_rewards_data(wallet_address)

        # If theres none notify the user and redirect back to supported projects
        if rewards_data is None:
            bot.send_message(
                message.chat.id,
                f"❌ No rewards data found for {wallet_address}."
            )
            handle_main_menu_command(message)
            return

        # If the data is of type string then there was an error and we will print the
        # exception to the user and redirect back to supported projects
        elif isinstance(rewards_data, str):
            bot.send_message(
                message.chat.id,
                f"❌ {rewards_data}"
            )
            handle_main_menu_command(message)
            return

        # Add the wallet data to the cache
        user_cache[message.chat.id] = rewards_data

        # Return to the main menu
        handle_main_menu_command(message)

    ##########################################################
    #               Supported Projects Handlers              #
    ##########################################################
    @bot.message_handler(commands=["supported_projects"])
    def handle_supported_projects_command(message):
        """ Creates and displays the supported projects if the /supported_projects comamand is used """

        # Build the supported projects display
        markup = create_supported_projects_display()

        # Displays the projects
        bot.send_message(
            message.chat.id,
            "Please select a project to check rewards:",
            reply_markup=markup,
        )


    @bot.callback_query_handler(func=lambda call: call.data == "supported_projects")
    def handle_supported_projects_callback(call):
        """ Creates and displays the supported projects for the supported_projects callback """

        # Acknowledge the call
        bot.answer_callback_query(call.id)

        # Build the supported projects display
        markup = create_supported_projects_display()

        # Edit the original message or send a new one
        bot.edit_message_text(
            "Please select a project to check rewards:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )

    @bot.callback_query_handler(func=lambda call: call.data.startswith("project_"))
    def handle_supported_project_selection(call):
        """
        This is called when a user clicks a project button. First it slices the "project_" from the call data
        then we prompt the user for the wallet address, next we make the call to get the rewards with the
        wallet address and the project name
        """

        # Cut off the calldata identifier
        callback_data = call.data.replace("project_", "")

        # Seperate the name from the distributor
        parts = callback_data.rsplit("_", 1)
        name = parts[0]
        distributor = parts[1]

        # Check if we have a wallet in the cache for the chat id
        if call.message.chat.id in user_cache:
            rewards_data = user_cache[call.message.chat.id]
        else:
            # TODO Get the users wallet address and add it to the cache
            return

        # Get the users wallet address and rewards amounts for the selected distributor
        wallet_address = rewards_data.get('wallet_address')
        rewards_from_project = rewards_data['distributors'].get(distributor)

        # If we have rewards build the rewards display otherwise notify the user that
        # they didn't recieve rewards from that distributor
        if rewards_from_project:
            create_rewards_display(call.message, rewards_from_project, name, wallet_address)
        else:
            bot.send_message(
                call.message.chat.id,
                f"❌ No rewards recieved from {name}"
            )
            handle_supported_projects_command(call.message)
            return

    ##########################################################
    #                   See Rewards Handlers                 #
    ##########################################################
    @bot.callback_query_handler(func=lambda call: call.data == "rewards")
    def handle_rewards_callback(call):

        if call.message.chat.id in user_cache:
            rewards_data = user_cache[call.message.chat.id]
        else:
            # TODO Get the users wallet address and add it to the cache
            return

        create_wallets_distributors_display(call.message, rewards_data)

    ##########################################################
    #                  Unknown Command handler                #
    ##########################################################
    @bot.message_handler(func=lambda message: True)
    def handle_unknown_command(message):
        """ Tells the user that they used an unkown command """
        bot.reply_to(message, "Unknown command. Use /home to go back to the main menu!")

    ##########################################################
    #                      Telegram Views                    #
    ##########################################################
    def create_main_menu_display(chat_id):
        """
        This is the display for the main menu which features two buttons. One to
        to see supported projects the other that shows rewards for a given wallet
        """
        markup = types.InlineKeyboardMarkup()
        wallet = None


        if chat_id in user_cache:
            wallet_address = user_cache[chat_id].get('wallet_address')
            wallet = f"{wallet_address[:4]}...{wallet_address[-4:]}"

        rewards_button = types.InlineKeyboardButton(
            text="See Rewards",
            callback_data="rewards"
        )

        projects_button = types.InlineKeyboardButton(
            text="Supported Projects",
            callback_data="supported_projects"
        )

        users_wallet_button = types.InlineKeyboardButton(
            text="Configure Wallet",
            callback_data="set_wallet"
        )

        markup.add(rewards_button)
        markup.add(projects_button)
        markup.add(users_wallet_button)


        response_text = "⎑⎑⎑ Mr. Rewards ⎑⎑⎑\n"
        response_text += "---------------------\n"

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
        back_button = types.InlineKeyboardButton(
            text="Go Back",
            callback_data="home"
        )

        buttons = []

        # Create a button for each project
        for project in projects:
            name = project.get("name")
            distributor = project.get('distributor')
            button = types.InlineKeyboardButton(
                text=name, callback_data=f"project_{name}_{distributor}"
            )

            buttons.append(button)

        # Add the buttons to the markup with the set max width
        for i in range(0, len(buttons), 3):
            row = buttons[i:i+3]  # Get 3 buttons at a time
            markup.row(*row)

        markup.add(back_button)
        return markup

    def create_wallets_distributors_display(message, data):
        markup = types.InlineKeyboardMarkup()

        # Back button to go to main menu
        back_button = types.InlineKeyboardButton(
            text="Go Back",
            callback_data="home"
        )

        buttons = []

        # Create a button for each project
        for distributor in data['distributors']:
            name = get_distributor_name_by_address(distributor)

            button = types.InlineKeyboardButton(
                text=name, callback_data=f"project_{name}_{distributor}"
            )

            buttons.append(button)

        # Add the buttons to the markup with the set max width
        for i in range(0, len(buttons), 3):
            row = buttons[i:i+3]  # Get 3 buttons at a time
            markup.row(*row)

        markup.add(back_button)
        bot.send_message(message.chat.id, "Please select a project to see rewards.", reply_markup=markup, parse_mode="Markdown")

    def create_rewards_display(message, data, project_name, wallet_address):
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

        # Check for errors
        if data.get("error"):
            response_text += f"\n⚠️ *Note:* {data['error']}"

        markup = types.InlineKeyboardMarkup()

        # Back button to go to main menu
        back_button = types.InlineKeyboardButton(
            text="Go Back",
            callback_data="supported_projects"
        )

        markup.add(back_button)
        # Send the user the data
        bot.send_message(message.chat.id, response_text, reply_markup=markup, parse_mode="Markdown")

    # Begin polling
    bot.infinity_polling()

##########################################################
#                        API Calls                       #
##########################################################
def get_supported_projects():
    """This calls the API to get the list of supported projects"""
    url = (
        f"{os.getenv('API_URL')}/supported_projects"
    )
    try:
        response = requests.get(url, timeout=30)
        return response.json()
    except Exception as e:
        print(f"Could not get supported projects from server. Please try again later.")
        raise

def get_rewards_data(wallet_address):
    """This calls the API to get the rewards data for a given wallet address"""
    url = f"{os.getenv("API_URL")}/rewards/{wallet_address}"
    try:
        response = requests.get(url, timeout=30)
        return response.json()
    except Exception as e:
        print(f"Could not get rewards data for wallet address from server. Please try again later.")
        return "Could not get rewards data for wallet address from server. Please try again later."

def get_distributor_address_by_name(distributor_name):
    """Gets the distributor address for a projects name"""
    match distributor_name:
        case "boon":
            return "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"
        case "distribute":
            return "CvgM6wSDXWCZeCmZnKRQdnh4CSga3UuTXwrCXy9Ju6PC"
        case "img":
            return "ChGA1Wbh9WN8MDiQ4ggA5PzBspS2Z6QheyaxdVo3XdW6"
        case "click":
            return "9uJbttvvowG1rVpPt6GMB3mL7BuktaHaNzFQbkACfiNN"
        case "revs":
            return "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE"
        case "iplr":
            return "D8gKfTxnwBG3XPTy4ZT6cGJbz1s13htKtv9j69qbhmv4"
        case "tnt":
            return "GVLwP2iR4sqEX9Tos3cmQQRqAumzRumxKD42qyCbCyCC"
        case _:
            return None

def get_distributor_name_by_address(distributor_address):
    """Gets the distributor address for a projects name"""
    match distributor_address:
        case "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ":
            return "boon"
        case "CvgM6wSDXWCZeCmZnKRQdnh4CSga3UuTXwrCXy9Ju6PC":
            return "distribute"
        case "ChGA1Wbh9WN8MDiQ4ggA5PzBspS2Z6QheyaxdVo3XdW6":
            return "img"
        case "9uJbttvvowG1rVpPt6GMB3mL7BuktaHaNzFQbkACfiNN":
            return "click"
        case "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE":
            return "revs"
        case "D8gKfTxnwBG3XPTy4ZT6cGJbz1s13htKtv9j69qbhmv4":
            return "iplr"
        case "GVLwP2iR4sqEX9Tos3cmQQRqAumzRumxKD42qyCbCyCC":
            return "tnt"
        case _:
            return None
# Start the telegram bot
if __name__ == "__main__":
    mr_rewards_bot()