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
    """Telegram bot that queries the rewards tracker API to get user rewards data"""

    BOT_TOKEN = os.getenv("TELE_BOT_TOKEN")
    if not BOT_TOKEN:
        raise ValueError("TELE_BOT_TOKEN environment variable is required but not set!")

    bot = telebot.TeleBot(BOT_TOKEN)

    API_KEY = os.getenv("API_KEY")
    if not API_KEY:
        raise ValueError("API_KEY environment variable is required but not set!")

    # Load supported projects
    try:
        projects = get_supported_projects()
    except Exception as e:
        print(f"Failed to load supported projects: {e}")
        projects = []

    # Initialize Redis cache
    try:
        redis_client = redis.from_url(os.getenv("REDIS_URL"))
        redis_client.ping()
    except Exception as e:
        print(f"Redis connection failed: {e}")
        redis_client = None

    main_menu_photo_path = Path("assets/mr_rewards.png")

    # Track active wallet setup sessions to prevent duplicates
    wallet_setup_sessions = set()

    @bot.message_handler(commands=["start", "home"])
    def handle_main_menu_command(message):
        """Display the main menu"""
        try:
            # Clear any active wallet setup session
            wallet_setup_sessions.discard(message.chat.id)

            markup, response_text = create_main_menu_display(message)
            send_photo_with_menu(message.chat.id, response_text, markup)
        except Exception as e:
            print(f"Error in main menu command: {e}")
            bot.send_message(
                message.chat.id,
                "🤖 Welcome to Mr. Rewards Bot!\n\n❌ Error loading menu. Please try again."
            )

    @bot.callback_query_handler(func=lambda call: call.data == "home")
    def handle_main_menu_callback(call):
        """Display the main menu via callback"""
        try:
            bot.answer_callback_query(call.id)

            # Clear any active wallet setup session
            wallet_setup_sessions.discard(call.message.chat.id)

            markup, response_text = create_main_menu_display(call.message)
            edit_message_with_photo(call.message, response_text, markup)
        except Exception as e:
            print(f"Error in main menu callback: {e}")
            bot.answer_callback_query(call.id, "Error loading menu")

    @bot.message_handler(commands=["set_wallet"])
    def handle_set_wallet_command(message):
        """Prompt user for wallet address"""
        if message.chat.id in wallet_setup_sessions:
            return  # Prevent duplicate sessions

        wallet_setup_sessions.add(message.chat.id)

        markup = types.InlineKeyboardMarkup()
        cancel_button = types.InlineKeyboardButton(text="❌ Cancel", callback_data="home")
        markup.add(cancel_button)

        bot.send_message(
            message.chat.id,
            "📝 Please enter your wallet address:",
            reply_markup=markup
        )
        bot.register_next_step_handler(message, set_user_wallet_data)

    @bot.callback_query_handler(func=lambda call: call.data == "set_wallet")
    def handle_set_wallet_callback(call):
        """Prompt user for wallet address via callback"""
        try:
            bot.answer_callback_query(call.id)

            if call.message.chat.id in wallet_setup_sessions:
                return  # Prevent duplicate sessions

            wallet_setup_sessions.add(call.message.chat.id)

            markup = types.InlineKeyboardMarkup()
            cancel_button = types.InlineKeyboardButton(text="❌ Cancel", callback_data="home")
            markup.add(cancel_button)

            # Edit the current message instead of sending new one
            bot.edit_message_caption(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                caption="📝 Please enter your wallet address:",
                reply_markup=markup
            )
            bot.register_next_step_handler(call.message, set_user_wallet_data)
        except Exception as e:
            print(f"Error in set wallet callback: {e}")
            bot.answer_callback_query(call.id, "Error setting up wallet")

    def set_user_wallet_data(message):
        """Process and cache user wallet data"""
        # Remove from active sessions
        wallet_setup_sessions.discard(message.chat.id)

        wallet_address = message.text.strip()

        if wallet_address.lower() == "cancel":
            markup, response_text = create_main_menu_display(message)
            send_photo_with_menu(message.chat.id, response_text, markup)
            return

        if len(wallet_address) < 32 or len(wallet_address) > 44:
            # Re-add to sessions for retry
            wallet_setup_sessions.add(message.chat.id)

            markup = types.InlineKeyboardMarkup()
            cancel_button = types.InlineKeyboardButton(text="❌ Cancel", callback_data="home")
            markup.add(cancel_button)

            bot.send_message(
                message.chat.id,
                f"❌ Invalid wallet address (length: {len(wallet_address)}).\n\n"
                f"Please enter a valid wallet address (32-44 characters):",
                reply_markup=markup
            )
            bot.register_next_step_handler(message, set_user_wallet_data)
            return

        # Show loading message
        loading_msg = bot.send_message(message.chat.id, "⏳ Checking wallet address...")

        rewards_data = get_rewards_data(wallet_address)

        # Delete loading message
        try:
            bot.delete_message(message.chat.id, loading_msg.message_id)
        except:
            pass

        if rewards_data is None:
            bot.send_message(
                message.chat.id, f"❌ No rewards data found for {wallet_address}."
            )
            # Wait a moment then show main menu
            import time
            time.sleep(1)
            markup, response_text = create_main_menu_display(message)
            send_photo_with_menu(message.chat.id, response_text, markup)
            return
        elif isinstance(rewards_data, str):
            bot.send_message(message.chat.id, f"❌ {rewards_data}")
            # Wait a moment then show main menu
            import time
            time.sleep(1)
            markup, response_text = create_main_menu_display(message)
            send_photo_with_menu(message.chat.id, response_text, markup)
            return

        # Cache data if Redis is available
        if redis_client:
            data = {
                "rewards_data": rewards_data,
                "last_updated": datetime.now().isoformat(),
            }
            try:
                cache_key = f"user_id:{message.chat.id}"
                redis_client.setex(cache_key, 3600, json.dumps(data))
            except Exception as e:
                print(f"Redis cache error: {e}")

        # Show success message and return to main menu
        success_msg = bot.send_message(message.chat.id, "✅ Wallet configured successfully!")

        # Wait a moment then delete success message and show main menu
        import time
        time.sleep(1.5)
        try:
            bot.delete_message(message.chat.id, success_msg.message_id)
        except:
            pass

        markup, response_text = create_main_menu_display(message)
        send_photo_with_menu(message.chat.id, response_text, markup)

    def get_user_wallet_data(message):
        """Get cached user wallet data"""
        if not redis_client:
            return None

        try:
            cache_key = f"user_id:{message.chat.id}"
            cached_data_json = redis_client.get(cache_key)

            if cached_data_json is None:
                return None

            cached_data = json.loads(cached_data_json)

            # Check if data is older than 5 minutes
            last_updated = datetime.fromisoformat(cached_data["last_updated"])
            current_time = datetime.now()
            time_diff = current_time - last_updated

            if time_diff.total_seconds() > 300:
                wallet_address = cached_data["rewards_data"].get("wallet_address")
                rewards_data = get_rewards_data(wallet_address)

                if rewards_data is None or isinstance(rewards_data, str):
                    return None

                # Update cache
                updated_data = {
                    "rewards_data": rewards_data,
                    "last_updated": datetime.now().isoformat(),
                }
                redis_client.setex(cache_key, 3600, json.dumps(updated_data))
                return updated_data["rewards_data"]

            return cached_data["rewards_data"]

        except Exception as e:
            print(f"Error getting user wallet data: {e}")
            return None

    @bot.message_handler(commands=["supported_projects"])
    def handle_supported_projects_command(message):
        """Display supported projects"""
        try:
            markup = create_supported_projects_display()
            send_with_photo_and_text(message.chat.id, "Please select a project to check rewards:", markup)
        except Exception as e:
            print(f"Error in supported projects command: {e}")
            bot.send_message(message.chat.id, "❌ Error loading projects. Please try again.")

    @bot.callback_query_handler(func=lambda call: call.data == "supported_projects")
    def handle_supported_projects_callback(call):
        """Display supported projects via callback"""
        try:
            bot.answer_callback_query(call.id)
            markup = create_supported_projects_display()
            edit_message_with_photo(call.message, "Please select a project to check rewards:", markup)
        except Exception as e:
            print(f"Error in supported projects callback: {e}")
            bot.answer_callback_query(call.id, "Error loading projects")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("proj_"))
    def handle_supported_project_selection(call):
        """Handle project selection"""
        try:
            bot.answer_callback_query(call.id)

            callback_data = call.data.replace("proj_", "")
            parts = callback_data.rsplit("_", 2)
            name = parts[0]
            distributor = parts[1]
            back_to = parts[2]

            rewards_data = get_user_wallet_data(call.message)

            if rewards_data is None:
                # Clear current message and prompt for wallet
                wallet_setup_sessions.add(call.message.chat.id)

                markup = types.InlineKeyboardMarkup()
                cancel_button = types.InlineKeyboardButton(text="❌ Cancel", callback_data="home")
                markup.add(cancel_button)

                bot.edit_message_caption(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    caption="⚠️ No wallet configured. Please enter your wallet address:",
                    reply_markup=markup
                )
                bot.register_next_step_handler(call.message, set_user_wallet_data)
                return
            elif rewards_data == "error":
                return

            wallet_address = rewards_data.get("wallet_address")
            rewards_from_project = rewards_data["distributors"].get(distributor)

            if rewards_from_project:
                create_rewards_display(call.message, rewards_from_project, name, wallet_address, back_to)
            else:
                markup = types.InlineKeyboardMarkup()
                callback = "rewards" if back_to == "r" else "supported_projects"
                back_button = types.InlineKeyboardButton(text="⬅️ Go Back", callback_data=callback)
                markup.add(back_button)

                error_text = f"❌ No rewards received from {name}"
                current_caption = getattr(call.message, 'caption', '') or ''

                if current_caption.strip() != error_text.strip():
                    try:
                        bot.edit_message_caption(
                            chat_id=call.message.chat.id,
                            message_id=call.message.message_id,
                            caption=error_text,
                            reply_markup=markup
                        )
                    except Exception as e:
                        if "message is not modified" not in str(e).lower():
                            print(f"Error editing no rewards message: {e}")

        except Exception as e:
            print(f"Error in project selection: {e}")
            bot.answer_callback_query(call.id, "Error processing selection")

    @bot.message_handler(commands=["rewards"])
    def handle_rewards_command(message):
        """Display user's rewards distributors"""
        rewards_data = get_user_wallet_data(message)

        if rewards_data is None:
            wallet_setup_sessions.add(message.chat.id)

            markup = types.InlineKeyboardMarkup()
            cancel_button = types.InlineKeyboardButton(text="❌ Cancel", callback_data="home")
            markup.add(cancel_button)

            bot.send_message(
                message.chat.id,
                "⚠️ You need to configure your wallet first.\n\n📝 Please enter your wallet address:",
                reply_markup=markup
            )
            bot.register_next_step_handler(message, set_user_wallet_data)
            return
        elif rewards_data == "error":
            return

        create_wallets_distributors_display(message, rewards_data)

    @bot.callback_query_handler(func=lambda call: call.data == "rewards")
    def handle_rewards_callback(call):
        """Display user's rewards distributors via callback"""
        try:
            bot.answer_callback_query(call.id)

            rewards_data = get_user_wallet_data(call.message)

            if rewards_data is None:
                wallet_setup_sessions.add(call.message.chat.id)

                markup = types.InlineKeyboardMarkup()
                cancel_button = types.InlineKeyboardButton(text="❌ Cancel", callback_data="home")
                markup.add(cancel_button)

                bot.edit_message_caption(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    caption="⚠️ You need to configure your wallet first.\n\n📝 Please enter your wallet address:",
                    reply_markup=markup
                )
                bot.register_next_step_handler(call.message, set_user_wallet_data)
                return
            elif rewards_data == "error":
                return

            create_wallets_distributors_display(call.message, rewards_data)
        except Exception as e:
            print(f"Error in rewards callback: {e}")
            bot.answer_callback_query(call.id, "Error loading rewards")

    @bot.message_handler(func=lambda message: True)
    def handle_unknown_command(message):
        """Handle unknown commands"""
        # Only respond if not in wallet setup session
        if message.chat.id not in wallet_setup_sessions:
            bot.reply_to(message, "❓ Unknown command. Use /home to go back to the main menu!")

    def send_photo_with_menu(chat_id, caption, markup):
        """Send photo with menu, fallback to text if no photo"""
        try:
            if main_menu_photo_path.exists():
                with open(main_menu_photo_path, 'rb') as photo:
                    bot.send_photo(
                        chat_id,
                        photo,
                        caption=caption,
                        parse_mode="Markdown",
                        reply_markup=markup
                    )
            else:
                bot.send_message(
                    chat_id,
                    caption,
                    parse_mode="Markdown",
                    reply_markup=markup
                )
        except Exception as e:
            print(f"Error sending photo with menu: {e}")
            bot.send_message(
                chat_id,
                caption,
                parse_mode="Markdown",
                reply_markup=markup
            )

    def send_with_photo_and_text(chat_id, text, markup):
        """Send message with photo and text"""
        try:
            if main_menu_photo_path.exists():
                with open(main_menu_photo_path, 'rb') as photo:
                    bot.send_photo(
                        chat_id,
                        photo,
                        caption=text,
                        reply_markup=markup
                    )
            else:
                bot.send_message(
                    chat_id,
                    text,
                    reply_markup=markup
                )
        except Exception as e:
            print(f"Error sending with photo and text: {e}")
            bot.send_message(chat_id, text, reply_markup=markup)

    def edit_message_with_photo(message, caption, markup):
        """Edit existing message with photo"""
        try:
            if message.photo:
                # Check if content is actually different before editing
                current_caption = getattr(message, 'caption', '') or ''
                if current_caption.strip() == caption.strip():
                    # Content is the same, don't edit
                    return

                # If message already has photo, just edit caption
                bot.edit_message_caption(
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    caption=caption,
                    parse_mode="Markdown",
                    reply_markup=markup
                )
            else:
                # Delete old message and send new one with photo
                try:
                    bot.delete_message(message.chat.id, message.message_id)
                except:
                    pass
                send_photo_with_menu(message.chat.id, caption, markup)
        except Exception as e:
            if "message is not modified" in str(e).lower():
                # Content is identical, no need to edit
                return
            print(f"Error editing message with photo: {e}")
            # Fallback: delete and send new
            try:
                bot.delete_message(message.chat.id, message.message_id)
            except:
                pass
            send_photo_with_menu(message.chat.id, caption, markup)

    def create_main_menu_display(message):
        """Create main menu markup and text"""
        markup = types.InlineKeyboardMarkup()
        wallet = None

        rewards_data = get_user_wallet_data(message)
        if rewards_data and rewards_data != "error":
            wallet_address = rewards_data.get("wallet_address")
            if wallet_address:
                wallet = f"{wallet_address[:4]}...{wallet_address[-4:]}"

        rewards_button = types.InlineKeyboardButton(text="💰 See Rewards", callback_data="rewards")
        projects_button = types.InlineKeyboardButton(text="📋 Supported Projects", callback_data="supported_projects")
        wallet_button = types.InlineKeyboardButton(text="⚙️ Configure Wallet", callback_data="set_wallet")

        markup.add(rewards_button)
        markup.add(projects_button)
        markup.add(wallet_button)

        response_text = "⎑⎑⎑ Mr. Rewards ⎑⎑⎑\n"
        response_text += "---------------------\n"
        response_text += f"*📬 Wallet*: {wallet or 'N/A'}\n\n"
        response_text += "Choose an option below:"

        return markup, response_text

    def create_supported_projects_display():
        """Create supported projects markup"""
        markup = types.InlineKeyboardMarkup()
        back_button = types.InlineKeyboardButton(text="⬅️ Go Back", callback_data="home")

        if not projects:
            markup.add(back_button)
            return markup

        buttons = []
        for project in projects:
            name = project.get("name")
            distributor = project.get("distributor")
            button = types.InlineKeyboardButton(
                text=name,
                callback_data=f"proj_{name}_{distributor}_s"
            )
            buttons.append(button)

        for i in range(0, len(buttons), 2):  # 2 buttons per row for better layout
            row = buttons[i : i + 2]
            markup.row(*row)

        markup.add(back_button)
        return markup

    def create_wallets_distributors_display(message, data):
        """Create wallet distributors display"""
        markup = types.InlineKeyboardMarkup()
        back_button = types.InlineKeyboardButton(text="⬅️ Go Back", callback_data="home")
        buttons = []

        for distributor in data["distributors"]:
            name = get_distributor_name_by_address(distributor, projects)
            if name:
                button = types.InlineKeyboardButton(
                    text=name,
                    callback_data=f"proj_{name}_{distributor}_r"
                )
                buttons.append(button)

        for i in range(0, len(buttons), 2):  # 2 buttons per row
            row = buttons[i : i + 2]
            markup.row(*row)

        markup.add(back_button)

        text = "💰 *Your Rewards*\n\nPlease select a project to see rewards:"

        if hasattr(message, 'photo') and message.photo:
            # Check if content is different before editing
            current_caption = getattr(message, 'caption', '') or ''
            if current_caption.strip() != text.strip():
                # Edit existing message
                try:
                    bot.edit_message_caption(
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        caption=text,
                        reply_markup=markup,
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    if "message is not modified" not in str(e).lower():
                        print(f"Error editing distributors display: {e}")
        else:
            # Send new message with photo
            send_with_photo_and_text(message.chat.id, text, markup)

    def create_rewards_display(message, data, project_name, wallet_address, back_to):
        """Create rewards display"""
        response_text = f"🤑 *Rewards from {project_name}*\n"
        response_text += f"📬 Wallet: `{wallet_address[:6]}...{wallet_address[-4:]}`\n\n"

        tokens = data.get("tokens", {})

        if not tokens:
            response_text += "❌ No reward amounts available."
        else:
            response_text += "*💎 Rewards Received:*\n\n"
            for token_name, token_data in tokens.items():
                total_amount = token_data.get("total_amount", 0)
                response_text += f"• *{token_name}*: `{total_amount:,.6f}`\n"

        markup = types.InlineKeyboardMarkup()
        callback = "rewards" if back_to == "r" else "supported_projects"
        back_button = types.InlineKeyboardButton(text="⬅️ Go Back", callback_data=callback)
        markup.add(back_button)

        # Check if content is different before editing
        current_caption = getattr(message, 'caption', '') or ''
        if current_caption.strip() != response_text.strip():
            try:
                bot.edit_message_caption(
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    caption=response_text,
                    reply_markup=markup,
                    parse_mode="Markdown"
                )
            except Exception as e:
                if "message is not modified" not in str(e).lower():
                    print(f"Error editing rewards display: {e}")

    print("🚀 Starting Mr. Rewards Bot...")


    bot.infinity_polling()

def get_supported_projects():
    """Get list of supported projects from API"""
    url = f"{os.getenv('API_URL')}/supported_projects"
    api_key = os.getenv("API_KEY")

    if not api_key:
        raise ValueError("API_KEY environment variable is required but not set!")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 401:
            raise Exception("Authentication failed")
        elif response.status_code == 429:
            raise Exception("Rate limit exceeded")
        elif response.status_code != 200:
            raise Exception(f"API request failed: {response.status_code}")

        return response.json()

    except requests.exceptions.Timeout:
        raise Exception("Request timed out")
    except requests.exceptions.ConnectionError:
        raise Exception("Could not connect to server")

def get_rewards_data(wallet_address):
    """Get rewards data for a wallet address"""
    url = f"{os.getenv('API_URL')}/rewards/{wallet_address}"
    api_key = os.getenv("API_KEY")

    if not api_key:
        return "API_KEY environment variable is required but not set!"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 401:
            return "Authentication failed. Please check the API configuration."
        elif response.status_code == 429:
            return "Rate limit exceeded. Please try again later."
        elif response.status_code == 404:
            return None
        elif response.status_code != 200:
            return f"API request failed with status code: {response.status_code}"

        return response.json()

    except requests.exceptions.Timeout:
        return "Request timed out. Please try again later."
    except requests.exceptions.ConnectionError:
        return "Could not connect to server. Please check your internet connection."
    except Exception:
        return "Could not get rewards data for wallet address from server. Please try again later."

def get_distributor_name_by_address(distributor_address, projects):
    """Get distributor name by address"""
    for project in projects:
        if project.get("distributor") == distributor_address:
            return project.get("name")
    return None

if __name__ == "__main__":
    try:
        mr_rewards_bot()
    except KeyboardInterrupt:
        print("\n👋 Bot stopped")
    except Exception as e:
        print(f"❌ Failed to start bot: {e}")