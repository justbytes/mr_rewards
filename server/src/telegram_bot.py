import os
from dotenv import load_dotenv
import telebot
from telebot import types
import requests
from pathlib import Path
import json

load_dotenv()


# Get projects info
def get_projects():
    result = {}
    try:
        # Make request to the API
        url = f"{os.getenv('API_URL')}/projects"
        response = requests.get(url, timeout=30)

        # Parse the response
        if response.status_code == 200:
            projects = response.json()

            for project in projects:
                result[project['name']] = project['distributor']
            return result
        else:
            raise Exception(f"API returned status code: {response.status_code}")

    # Throw an error if server has lost connection
    except requests.exceptions.ConnectionError:
        raise ConnectionError("Could not connect to rewards server")

    # Timeout
    except requests.exceptions.Timeout:
        raise TimeoutError("Request timed out")

    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")


# Spins up the Mrt Rewards telegram bot
def start_bot():

    # Create the instance
    BOT_TOKEN = os.getenv("TELE_BOT_TOKEN")
    bot = telebot.TeleBot(BOT_TOKEN)

    # Get our list of projects
    projects = get_projects()

    # Welcome message
    @bot.message_handler(commands=["start", "help"])

    # Display the main menu
    def start(message):
        markup = types.InlineKeyboardMarkup()

        response_text = "Mr. Rewards\n\n"
        response_text += "Instructions: \n"
        response_text += "1. Click 'See Rewards' or enter /rewards\n"
        response_text += "2. Select an one of the projects from the list\n"
        response_text += "3. Enter your wallet address and press enter\n"
        response_text += "4. See total rewards recieved from the project\n"


        button = types.InlineKeyboardButton(
            text="See Rewards",
            callback_data="rewards"
        )

        markup.add(button)

        # Added reply_markup parameter to actually show the button
        bot.send_message(
            message.chat.id,
            response_text,
            parse_mode="Markdown",

            reply_markup=markup
        )

    # This is for the "See Rewards" button in the /start screen
    # Had the same functionality as calling /rewards command
    @bot.callback_query_handler(func=lambda call: call.data == "rewards")
    def handle_rewards_callback(call):
        bot.answer_callback_query(call.id)  # This acknowledges the button press

        # Holds the buttons
        markup = types.InlineKeyboardMarkup()

        # Create a button for each project
        for project_name in projects:
            button = types.InlineKeyboardButton(
                text=project_name, callback_data=f"project_{project_name}"
            )
            markup.add(button)

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
        markup = types.InlineKeyboardMarkup()

        # Create a button for each project
        for project_name in projects:
            button = types.InlineKeyboardButton(
                text=project_name, callback_data=f"project_{project_name}"
            )
            markup.add(button)

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
        bot.register_next_step_handler(call.message, process_wallet_address, project_name)

        # Tells the user that they used an unkown command


    # Fallback for unknown commands
    @bot.message_handler(func=lambda message: True)
    def handle_unknown_command(message):
        bot.reply_to(message, "Unknown command. Use /help to go back to the main menu!")

    # Makes the api call to get the rewards for a wallet
    def process_wallet_address(message, project_name):
        wallet_address = message.text.strip()
        distributor_address = projects[project_name]

        # Validate wallet address format (basic check)
        if not wallet_address or len(wallet_address) not in [43, 44]:
            bot.reply_to(message, "Invalid wallet address. Please try again with /rewards")
            return

        # Send a loading message to user
        bot.send_message(
            message.chat.id, f"Checking rewards for {project_name}... Please wait."
        )

        try:
            # Make request to the API
            url = f"{os.getenv('API_URL')}/rewards/{wallet_address}/{distributor_address}"
            response = requests.get(url, timeout=30)

            # Parse the response
            if response.status_code == 200:
                data = response.json()

                # Format the result
                format_and_send_rewards(message, data, project_name, wallet_address)
            else:
                bot.reply_to(
                    message,
                    f"Server error: {response.status_code}. Please try again later.",
                )
        # Throw an error if server has lost connection
        except requests.exceptions.ConnectionError:
            bot.reply_to(
                message, "Could not connect to rewards server. Please try again later."
            )
        # Timeout
        except requests.exceptions.Timeout:
            bot.reply_to(message, "Request timed out. Please try again later.")
        except Exception as e:
            bot.reply_to(message, f"An error occurred: {str(e)}")

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

    # Activates the bot
    bot.infinity_polling()


# Configures and starts the telegram bot
if __name__ == "__main__":
    start_bot()