![banner_mr_rewards](/assets/mr-rewards-banner.png)

# 🏆 Rewards Token Tracker

Rewards/Tax/Ponzi tokens on Solana have built-in functionality to send holders rewards at set time intervals (5 mins - multiple hours). This project works to track the transactions coming from the distribution wallets of the tokens and record the amounts sent to each wallet. The data collected will power an API holders can access via a custom Telegram bot or website, displaying the aggregated rewards received across different projects.

## 🌐 Live Demo

[View Live Application](https://t.me/mr_rewards_bot)

## ✨ Features

**FastAPI Sever**

- High-performance API with automatic OpenAPI documentation
- 3 GET endpoints for wallet rewards data, list of supported projects, and API health check
- API rate limiting using via Redis and Slowapi

**MongoDB Database**

- Stores project transfers and wallet rewards data
- Indexed collections for fast rewards and transfer lookups
- Automated data insertion with conflict resolution

**Telegram Bot Integration**

- Conversational interface utilizing Redis to cache user rewards data
- Interactive menus for project selection and wallet address input
- Real-time API integration for instant reward calculations

## 🛠 Tech Stack

**Backend**

- Python
- FastAPI
- Uvicorn ASGI Server

**Database & Storage**

- MongoDB
- Redis

**Blockchain Integration**

- Helius RPC API

**Bot & Communication**

- Telegram Bot API

## 🚀 Quick Start

**Prerequisites**

- Python 3.10+
- Pipenv
- MongoDB connection url
- Redis connection url
- Helius RPC access
- Telegram Bot Token

### 1. Environment Setup

Create a `.env` file using the provided example env and fill in the values needed:

```bash
cp .example-env .env
```

### 2. Enter ENV & Install Dependencies

```bash
pipenv shell
pipenv install
```

### 3. Initial Data Setup

Configure and run the initial data collector:

```
DETAILS COMING SOON...
```

### 4. Start the API Server

```bash
python3 server/server.py
```

### 5. Start Telegram Bot

In a separate terminal, start the telegram bot service:

```bash
python3 server/telegram_bot.py
```

**_Note_** The server needs to be started before the telegram bot otherwise it will crash.

## 🔮 Roadmap

### Planned Features

- Add more projects to the supported list
- Reward trends and performance metrics
- Web Interface
