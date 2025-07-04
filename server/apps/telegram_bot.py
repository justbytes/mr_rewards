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

    @bot.message_handler(commands=["start", "home"])
    def main_menu(message):
        """ Displays the main menu """

        # Build the main menu markup
        markup, response_text = create_main_menu_display()

        bot.send_message(
            message.chat.id,
            response_text,
            parse_mode="Markdown",
            reply_markup=markup
        )

    @bot.callback_query_handler(func=lambda call: call.data == "home")
    def handle_main_menu_callback(call):
        """Displays the main menu for the home callback"""

        bot.answer_callback_query(call.id)
        markup, response_text = create_main_menu_display()


        bot.edit_message_text(
            response_text,
            call.message.chat.id,
            call.message.message_id,
            parse_mode="Markdown",
            reply_markup=markup
        )

    @bot.callback_query_handler(func=lambda call: call.data == "supported_projects")
    def handle_supported_projects_callback(call):
        bot.answer_callback_query(call.id)  # This acknowledges the button press

        # Holds the buttons
        markup = create_supported_projects_display()

        # Edit the original message or send a new one
        bot.edit_message_text(
            "Please select a project to check rewards:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )

    @bot.message_handler(commands=["supported_projects"])
    def handle_supported_projects_command(message):
        """Gets a list of supported projects and displays them in columns """
        # Holds the buttons
        markup = create_supported_projects_display()

        # Sends the message to the user with the options
        bot.send_message(
            message.chat.id,
            "Please select a project to check rewards:",
            reply_markup=markup,
        )

    # Runs when a rewards button is pressed which then gets the rewards data for that project
    @bot.callback_query_handler(func=lambda call: call.data.startswith("project_"))
    def handle_supported_project_selection(call):
        # Cut off the calldata identifier
        project_name = call.data.replace("project_", "")

        # Prompt user to enter the address
        bot.send_message(
            call.message.chat.id, # Chat id for our user
            f"You selected {project_name}. Please enter your wallet address\n (Type 'cancel' to stop)"
        )

        # Set the next step handler to wait for wallet address
        bot.register_next_step_handler(call.message, get_wallet_rewards_with_distributor, project_name)

    @bot.message_handler(func=lambda message: True)
    def handle_unknown_command(message):
        """ Tells the user that they used an unkown command """
        bot.reply_to(message, "Unknown command. Use /home to go back to the main menu!")

    def create_main_menu_display():
        """
        This is the display for the main menu which features two buttons. One to
        to see supported projects the other that shows rewards for a given wallet
        """
        markup = types.InlineKeyboardMarkup()

        rewards_button = types.InlineKeyboardButton(
            text="See Rewards",
            callback_data="rewards"
        )

        projects_button = types.InlineKeyboardButton(
            text="Supported Projects",
            callback_data="supported_projects"
        )

        markup.add(rewards_button)
        markup.add(projects_button)

        response_text = "⎑⎑⎑ Mr. Rewards ⎑⎑⎑\n"
        response_text += "---------------------\n"
        response_text += "Select an option:"

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
            button = types.InlineKeyboardButton(
                text=name, callback_data=f"project_{name}"
            )

            buttons.append(button)

        # Add the buttons to the markup with the set max width
        for i in range(0, len(buttons), 3):
            row = buttons[i:i+3]  # Get 3 buttons at a time
            markup.row(*row)

        markup.add(back_button)
        return markup

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

    def get_wallet_rewards_with_distributor(message, project_name):
        wallet_address = message.text.strip()

        # Users can cancel the operation by typing cancel and will be redirected to
        # supported projects display
        if wallet_address.lower() == "cancel":
            bot.send_message(
                message.chat.id,
                "❌ Operation cancelled."
            )
            handle_supported_projects_command(message)
            return

        # Validate the wallet address and prompt a retry if its not a valid address
        if len(wallet_address) < 32 or len(wallet_address) > 44:
            bot.send_message(
                message.chat.id,
                f"❌ Invalid wallet address (length: {len(wallet_address)}). \n\n"
                f"Please enter a valid wallet address (32-44 characters) or type 'cancel' to stop:"
            )
            bot.register_next_step_handler(message, get_wallet_rewards_with_distributor, project_name)
            return

        # Fetch the rewards data
        rewards_data = get_rewards_data(wallet_address)

        # If theres none notify the user and redirect back to supported projects
        if rewards_data is None:
            bot.send_message(
                message.chat.id,
                f"❌ No rewards data found for {wallet_address}."
            )
            handle_supported_projects_command(message)
            return
        # If the data is of type string then there was an error and we will print the
        # exception to the user and redirect back to supported projects
        elif isinstance(rewards_data, str):
            bot.send_message(
                message.chat.id,
                f"❌ {rewards_data}"
            )
            handle_supported_projects_command(message)
            return

        # Get the distributor address using the name of the project
        distributor = get_distributor_address_by_name(project_name)
        rewards_from_project = rewards_data['distributors'].get(distributor)
        if rewards_from_project:
            create_rewards_display(message, rewards_from_project, project_name, wallet_address)
        else:
            bot.send_message(
                message.chat.id,
                f"❌ No rewards recieved from {project_name}"
            )
            handle_supported_projects_command(message)
            return


    # Activates the bot
    bot.infinity_polling()


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

if __name__ == "__main__":
    mr_rewards_bot()



# def get_wallet_rewards(message):
#         # Get the wallet address from text and remove any whitespaces
#         wallet_address = message.text.strip()

#         # Send a loading message to user
#         bot.send_message(
#             message.chat.id, f"Getting rewards please wait..."
#         )

#         try:
#             # Make request to the API
#             url = f"{os.getenv("API_URL")}/rewards/{wallet_address}"
#             response = requests.get(url, timeout=30)

#             data = response.json()

#             print(data)

#         except requests.exceptions.Timeout:
#             bot.reply_to(message, "Request timed out. Please try again later.")
#         except Exception as e:
#             bot.reply_to(message, f"An error occurred: {str(e)}")
