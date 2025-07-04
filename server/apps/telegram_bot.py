import os
import json
import telebot
import requests
from telebot import types
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()


# Get projects info
def get_supported_projects():
    # Make request to the API for supported projects and return
        url = (
            f"{os.getenv('API_URL')}/supported_projects"
        )
        try:
            response = requests.get(url, timeout=30)
            data = response.json()
            return data
        except requests.exceptions.Timeout:
            self.bot.reply_to(message, "Request timed out. Please try again later.")
        except Exception as e:
            self.bot.reply_to(message, f"Could not get data from server. Please try again later.")

def get_wallet_rewards(message):
        # Get the wallet address from text and remove any whitespaces
        wallet_address = message.text.strip()

        # Send a loading message to user
        bot.send_message(
            message.chat.id, f"Getting rewards please wait..."
        )

        try:
            # Make request to the API
            url = f"{os.getenv("API_URL")}/rewards/{wallet_address}"
            response = requests.get(url, timeout=30)

            data = response.json()

            print(data)

        except requests.exceptions.Timeout:
            bot.reply_to(message, "Request timed out. Please try again later.")
        except Exception as e:
            bot.reply_to(message, f"An error occurred: {str(e)}")


# Starts the telegram bot
def mr_rewards_bot():

    # Create the instance
    BOT_TOKEN = os.getenv("TELE_BOT_TOKEN")
    bot = telebot.TeleBot(BOT_TOKEN)

    # Get our list of projects
    projects = get_supported_projects()

    user_data = {}

    # Welcome message
    @bot.message_handler(commands=["start", "home"])
    def main_menu(message):
        markup = types.InlineKeyboardMarkup()

        response_text = "⎑⎑⎑ Mr. Rewards ⎑⎑⎑\n"
        response_text += "---------------------\n"
        response_text += "Select an option:"

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

        bot.send_message(
            message.chat.id,
            response_text,
            parse_mode="Markdown",
            reply_markup=markup
        )

    # This is for the "See Rewards" button in the /start screen
    # same functionality as calling /rewards command
    @bot.callback_query_handler(func=lambda call: call.data == "rewards")
    def handle_rewards_callback(call):
        bot.answer_callback_query(call.id)  # This acknowledges the button press

        # Holds the buttons
        markup = types.InlineKeyboardMarkup()

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


        for i in range(0, len(buttons), 2):
            row = buttons[i:i+2]  # Get 2 buttons at a time
            markup.row(*row)

        markup.add(back_button)

        # Edit the original message or send a new one
        bot.edit_message_text(
            "Please select a project to check rewards:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )

    # Displays a list of projects to choose from
    @bot.message_handler(commands=["rewards"])
    def show_projects(message):

        # Holds the buttons
        markup = types.InlineKeyboardMarkup(row_width=2)

        buttons = []

        # Create a button for each project
        for project in projects:
            name = project.get("name")
            button = types.InlineKeyboardButton(
                text=name, callback_data=f"project_{name}"
            )

            print(buttons)
            buttons.append(button)

        markup.row(*buttons)

        # Sends the message to the user with the options
        bot.send_message(
            message.chat.id,
            "Please select a project to check rewards:",
            reply_markup=markup,
        )


    # Runs when a rewards button is pressed which then gets the rewards data for that project
    @bot.callback_query_handler(func=lambda call: call.data.startswith("project_"))
    def handle_project_selection(call):
        # Cut off the calldata identifier
        project_name = call.data.replace("project_", "")

        # Prompt user to enter the address
        bot.send_message(
            call.message.chat.id, # Chat id for our user
            f"You selected {project_name}. Please enter your wallet address:",
        )

        # Set the next step handler to wait for wallet address
        bot.register_next_step_handler(call.message, get_wallet_address, project_name)



    # Creates the message to send back the user containing the rewards data
    def format_and_send_rewards(message, data, project_name, wallet_address):

        # Return if nothing was found
        if not data.get("found", False):
            bot.reply_to(message, "No rewards found for this wallet")
            return

        # Format the rewards information
        response_text = f"🤑 *Rewards from {project_name}*\n"
        response_text += f"📬 Wallet: `{wallet_address[:6]}...{wallet_address[-4:]}`\n"
        response_text += f"📊 Total Transfers: {data.get('transfer_count', 0)}\n"

        # Get the reward amounts
        total_amounts = data.get("total_amounts", [])

        # Create the applicable message response
        if not total_amounts:
            response_text += "No reward amounts available."
        else:
            response_text += "*🧐 Rewards Recieved:*\n"

            # Loop through the rewards amounts and add each one to the message
            for i, reward in enumerate(total_amounts, 1):
                token = reward.get("token", "Unknown")
                total_amount = reward.get("total_amount", 0)
                decimals = reward.get("decimals", 0)

                response_text += f"**{token}**\n"
                response_text += f"   Amount: `{total_amount}`\n"

        # Check for errors
        if data.get("error"):
            response_text += f"\n⚠️ *Note:* {data['error']}"

        # send the user the data
        bot.send_message(message.chat.id, response_text, parse_mode="Markdown")


    # Tells the user that they used an unkown command
    @bot.message_handler(func=lambda message: True)
    def handle_unknown_command(message):
        bot.reply_to(message, "Unknown command. Use /help to go back to the main menu!")


    # Activates the bot
    bot.infinity_polling()


if __name__ == "__main__":
    mr_rewards_bot()
