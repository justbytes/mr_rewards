![banner_mr_rewards](/assets/mr-rewards-banner.png)

# 🏆 Rewards Token Tracker

Rewards/Tax/Ponzi SPL tokens on Solana have built-in functionality to send holders rewards at set time intervals (5 mins - multiple hours). This project works to track the transactions coming from the distribution wallets of the tokens and record the amounts sent to each wallet. The data collected will power an API holders can access via a custom Telegram bot or website(coming soon), displaying the aggregated rewards received across different projects.

## 🌐 Live Demo

[View Live Application](https://t.me/mr_rewards_bot)

## 🌟 Features

### Core Features

- **Real-time Tracking**: Monitors Solana reward token distributions for seven projects
- **Automatic Aggregation**: Aggregates all rewards by wallet address across different distributors
- **Dual Database System**: Uses SQLite for local backup and MongoDB for production data
- **REST API**: Authenticated API endpoints for accessing reward data
- **Telegram Bot**: Interactive bot for easy reward checking
- **Automatic Updates**: Periodic background updates to fetch new transactions

### Technical Features

- **Rate Limiting**: Built-in API rate limiting with Redis
- **Authentication**: Bearer token authentication system
- **Comprehensive Testing**: Full test suite with 100+ test cases
- **Error Handling**: Robust error handling with retry mechanisms
- **Scalable Architecture**: Modular design supporting multiple reward projects

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- MongoDB Atlas account (or local MongoDB)
- Redis instance
- Helius API key (for Solana data)
- Telegram Bot Token (optional, for bot functionality)

### Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/justbytes/mr_rewards.git
   cd mr_rewards
   ```

2. **Create required directories**

   ```bash
   mkdir backup/ test_backup/
   mkdir backup/transfers test_backup/transfers
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   # or using pipenv
   pipenv install
   ```

4. **Environment Configuration**

   Start Python virtual env

   ```bash
   pipenv shell
   ```

   Create a `.env` file in the root directory:

   ```bash
   cp .env.example .env
   ```

5. **Start API Server**

   Start the Restful api server which will create the tables for SQLite and MongoDB

   ```bash
    # Start the API server
    python3  server/main.py
   ```

6. **Generate API Key**

   To use the api locally you will need to create an api key:

   ```bash
   python3 system_controller.py
   # Choose option 4: Create New API Key
   ```

7. **Start the Telegram Bot (optional)**
   ```bash
    # Start the Telegram bot
    python3 src/telegram_bot.py
   ```

## 🏗️ Architecture

### Data Flow

1. **Project Initialization**: New projects are added and all historical transactions are fetched
2. **Periodic Updates**: System checks for new transactions every 5 minutes
3. **Transaction Processing**: Raw transactions are filtered and converted to reward transfers
4. **Aggregation**: Transfers are aggregated by wallet address and stored
5. **API Access**: Clients access aggregated data via REST API or Telegram bot

## 📚 API Documentation

### Authentication

All API endpoints (except `/health`) require Bearer token authentication:

```bash
curl -H "Authorization: Bearer your_api_key_here" \
     https://your-api-url/rewards/wallet_address
```

### Endpoints

See all responses at `server/routes/models.py`

**Get Wallet Rewards**

```http
GET /rewards/{wallet_address}
```

**_Response:_**

```json
{
  "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
  "distributors": {
    "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ": {
      "tokens": {
        "sol": { "total_amount": 0.12866323 },
        "USDC": { "total_amount": 150.75 }
      }
    }
  }
}
```

**Get Supported Projects**

```http
GET /supported_projects
```

**Response:**

```json
[
  {
    "name": "Example Project",
    "distributor": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
    "token_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "dev_wallet": "DevWalletAddress",
    "last_sig": "LastProcessedSignature"
  }
]
```

**Health Check**

```http
GET /health
```

**Response:**

```json
{
  "status": "healthy",
  "message": "API is running"
}
```

### API Rate Limiting

- **Default Limit**: 1000 requests/minute per API key
- **Technology**: Redis-based rate limiting
- **Customizable**: Per-key rate limit configuration

## 🛠️ System Management

The project includes a comprehensive management system accessible via:

```bash
python3 system_controller.py
```

### Management Options

#### Data Management

- **Database Counts**: View record counts across all databases
- **Project Initialization**: Add new reward projects to track
- **Backup Operations**: Backup temporary transfer data

#### API Key Management

- **Create API Keys**: Generate new authenticated access keys
- **List API Keys**: View all existing keys and their status
- **Manage Keys**: Activate/deactivate, update rate limits, view usage stats

#### Testing Suite

- **Individual Tests**: SQLite, MongoDB, Controller, ProjectUpdater, ProjectInitializer
- **Group Tests**: Database layer, Business logic layer
- **Complete Suite**: All tests with detailed reporting

## 🤖 Telegram Bot

The Telegram bot provides an interactive interface for checking rewards:

### Features

- **Wallet Configuration**: Set and manage your wallet address
- **Rewards Viewing**: Browse rewards by project
- **Project Discovery**: Explore all supported projects
- **Caching**: Automatic data caching for improved performance

### Commands

- `/start` or `/home` - Main menu
- `/set_wallet` - Configure wallet address
- `/rewards` - View your rewards
- `/supported_projects` - Browse all projects

### Setup

1. Create a Telegram bot via [@BotFather](https://t.me/botfather)
2. Add the bot token to your `.env` file
3. Run the bot:

```bash
python3 src/telegram_bot.py
```
