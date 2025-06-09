![banner_mr_rewards](/assets/mr-rewards-banner.png)

# 🏆 Rewards Token Tracker

Rewards/Tax/Ponzi tokens on Solana have built-in functionality to send holders rewards at set time intervals (5 mins - multiple hours). This project works to track the transactions coming from the distribution wallets of the tokens and record the amounts sent to each wallet. The data collected will power an API holders can access via a custom Telegram bot or website, displaying the aggregated rewards received across different projects.

## 🌐 Live Demo
[View Live Application](https://t.me/mr_rewards_bot)
Deployed via Render

## ✨ Features
**Real-time Reward Tracking**
Monitors reward distributions across multiple Solana token projects

**RESTful API Access**
6 comprehensive endpoints for wallet rewards, distributor lookup, and project management

**Telegram Bot Interface**
Interactive bot with inline keyboards for seamless user experience

**Multi-Project Support**
Tracks rewards from Distribute, TNT, IPLR, and other supported projects

**Automated Data Processing**
Blockchain transaction filtering and reward calculation with error handling

**Scalable Architecture**
MongoDB storage with optimized indexing for concurrent user access

## 🛠 Tech Stack
**Backend**
- Python
- FastAPI
- Uvicorn ASGI Server
- Pydantic Data Validation

**Database & Storage**
- MongoDB
- PyMongo 

**Blockchain Integration**
- Helius RPC API
- Solana transaction processing

**Bot & Communication**
- Telegram Bot API
- pyTelegramBotAPI

## 🏗 Architecture
**API-First Design**
This application uses a modular architecture with clear separation of concerns:

**FastAPI Backend**
High-performance async API with automatic OpenAPI documentation
Handles wallet rewards aggregation and distributor queries
Implements proper error handling and data validation

**MongoDB Database**
Document-based storage optimized for blockchain transaction data
Indexed collections for fast wallet and distributor lookups
Automated data insertion with conflict resolution

**Telegram Bot Integration**
Conversational interface with state management and callback handlers
Interactive menus for project selection and wallet address input
Real-time API integration for instant reward calculations

## 🚀 Quick Start
**Prerequisites**
- Python 3.8+
- MongoDB Atlas account
- Helius RPC access
- Telegram Bot Token

### 1. Environment Setup
Create a `.env` file using the provided example.env:

```bash
MONGO_URL=your_mongodb_connection_string
HELIUS_API_KEY=your_helius_api_key
API_URL=http://localhost:8000
TELE_BOT_TOKEN=your_telegram_bot_token
```


Note: Free tier Helius works but collecting full transaction data is resource intensive and may consume significant API credits.

### 2. Install Dependencies
```bash
pipenv install
pipenv shell
```

### 3. Initial Data Setup
Configure and run the initial data collector:
```bash
python3 server/src/data_collector.py
```

Troubleshooting: If you encounter "Too many requests" errors from Helius, wait a few seconds and retry. The system includes error handling, but manual retries may be needed for large data fetches.

### 4. Start the API Server
```bash
python3 server/index.py
```

### 5. Start Telegram Bot
In a separate terminal, start the bot service:
```bash
python3 server/src/telegram_bot.py
```

## 🔧 API Endpoints

- /health - Server health check
- /rewards/{wallet}/{distributor} - Get wallet rewards from specific distributor
- /wallets - List all wallets with recorded rewards
- /wallets/{wallet}/distributors - Get distributors for specific wallet
- /projects - List supported token projects
- /docs - Interactive API documentation

## 🔮 Roadmap
### Planned Features

- Enhanced Token Support
- Expand beyond current supported projects
- Reward trends and performance metrics
- Web Interface
- Extended transaction history and reporting
