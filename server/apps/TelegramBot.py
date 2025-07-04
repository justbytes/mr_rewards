import os
import json
import telebot
import requests
from telebot import types
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


# Spins up the Mr Rewards telegram bot
class TelegramBot:

    def __init__(self):
        self.bot = telebot.TeleBot(os.getenv("TELE_BOT_TOKEN"))
        self.projects = self.get_supported_projects()

        # Register handlers
        self.bot.message_handler(commands=["start", "help"])(self.start)
        self.bot.message_handler(commands=["rewards"])(self.show_projects)
        self.bot.callback_query_handler(func=lambda call: call.data == "rewards")(
            self.handle_rewards_callback
        )
        self.bot.callback_query_handler(
            func=lambda call: call.data.startswith("project_")
        )(self.handle_project_selection)
        self.bot.message_handler(func=lambda message: True)(self.handle_unknown_command)

        # Activates the bot
        self.bot.infinity_polling()

    # Display the main menu
    def start(self, message):
        markup = types.InlineKeyboardMarkup()

        response_text = "Mr. Rewards\n\n"
        response_text += "Instructions: \n"
        response_text += "1. Click 'See Rewards' or enter /rewards\n"
        response_text += "2. Select an one of the projects from the list\n"
        response_text += "3. Enter your wallet address and press enter\n"
        response_text += "4. See total rewards recieved from the project\n"

        button = types.InlineKeyboardButton(text="See Rewards", callback_data="rewards")

        markup.add(button)

        # Added reply_markup parameter to actually show the button
        self.bot.send_message(
            message.chat.id, response_text, parse_mode="Markdown", reply_markup=markup
        )

    # This is for the "See Rewards" button in the /start screen
    # Had the same functionality as calling /rewards command
    def handle_rewards_callback(self, call):
        self.bot.answer_callback_query(call.id)  # This acknowledges the button press

        # Holds the buttons
        markup = types.InlineKeyboardMarkup()

        # Create a button for each project
        for project_name in self.projects:
            button = types.InlineKeyboardButton(
                text=project_name, callback_data=f"project_{project_name}"
            )
            markup.add(button)

        # Edit the original message or send a new one
        self.bot.edit_message_text(
            "Please select a project to check rewards:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )

    # Displays a list of projects to choose from
    def show_projects(self, message):

        # Holds the buttons
        markup = types.InlineKeyboardMarkup()

        # Create a button for each project
        for project_name in self.projects:
            button = types.InlineKeyboardButton(
                text=project_name, callback_data=f"project_{project_name}"
            )
            markup.add(button)

        # Sends the message to the user with the options
        self.bot.send_message(
            message.chat.id,
            "Please select a project to check rewards:",
            reply_markup=markup,
        )

    # Runs when a rewards button is pressed which then gets the rewards data for that project
    def handle_project_selection(self, call):
        # Cut off the calldata identifier
        project_name = call.data.replace("project_", "")

        # Prompt user to enter the address
        self.bot.send_message(
            call.message.chat.id,  # Chat id for our user
            f"You selected {project_name}. Please enter your wallet address:",
        )

        # Set the next step handler to wait for wallet address
        self.bot.register_next_step_handler(
            call.message, self.process_wallet_address, project_name
        )

    # Fallback for unknown commands
    def handle_unknown_command(self, message):
        self.bot.reply_to(
            message, "Unknown command. Use /help to go back to the main menu!"
        )

    # Makes the api call to get the rewards for a wallet
    def process_wallet_address(self, message, project_name):
        wallet_address = message.text.strip()
        distributor_address = self.projects[project_name]

        # Send a loading message to user
        self.bot.send_message(
            message.chat.id, f"Checking rewards for {project_name}... Please wait."
        )

        try:
            # Make request to the API
            url = (
                f"{os.getenv('API_URL')}/rewards/{wallet_address}"
            )
            response = requests.get(url, timeout=30)
            print(response)
            # Parse the response
            if response.status_code == 200:
                data = response.json()

                # Format the result
                print(data)
            else:
                self.bot.reply_to(
                    message,
                    f"Server error: {response.status_code}. Please try again later.",
                )
        # Timeout
        except requests.exceptions.Timeout:
            self.bot.reply_to(message, "Request timed out. Please try again later.")
        except Exception as e:
            self.bot.reply_to(message, f"An error occurred: {e}")

    # Creates the message to send back the user containing the rewards data
    def format_and_send_rewards(self, message, data, project_name, wallet_address):

        # Return if nothing was found
        if not data.get("found", False):
            self.bot.reply_to(message, "No rewards found for this wallet")
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
        self.bot.send_message(message.chat.id, response_text, parse_mode="Markdown")

    # Get a fresh list of projects in the case we update the supported projects
    def get_supported_projects(self):

        # Make request to the API
        url = (
            f"{os.getenv('API_URL')}/supported_projects"
        )
        response = requests.get(url, timeout=30)
        print(response)
        data = response.json()
        print(data)
        return data