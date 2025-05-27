![banner_mr_rewards](/assets/mr-rewards-banner.png)

# REWARDS TOKEN TRACKER

Rewards/Tax/Ponzi tokens on Solana have built-in functionality to send holders rewards at set time intervals (5 mins - multiple hours). This project works to track the transactions coming from the distribution wallets of the tokens and record the amounts sent to each wallet. The data collected will power an API holders can access via a custom Telegram bot or website, displaying the aggregated rewards received across different projects.

## Usage

To use this app download Telegram and add @mr_rewards_bot. Click start and you will then be able to interact with the bot and the api.

Currently we are in the MVP stage and only have 100 transactions for the distribute project and you can test it with this address if you wish `1JqsvoXbb97qV3UhAJY65TKGsRHjruSdgyQHFxQ1XSu`

### Supported Projects

Currently supporting the following project with more to come.

- Distribute - In progress
- TNT - In progress
- IPLR - In progress


## Steps to test locally

Start by cloning the repo.

### Set up enviornment

Make sure you have pipenv installed on your machine

Activate virtual enviorment: `pipenv shell`

Install dependencies: `pipenv install`


### To get fresh data - OPTIONAL:
To run locally this will requires a Helius RPC url which can be found [here](https://www.helius.dev/). Create an account then navigate to the endpoints tab to find the HTTPS RPC url. Copy it and then paste it into the .env file using the example-env file for reference.

Get the first 100 transactios of a a single distributor wallet like so:

```
python3 server/src/data_collector.py
```

### Start the server
Then start the local server with:

```
python3 server/index.py
```

From there you can make a requests to the server using a web browswer. Open your browser and go to `http://0.0.0.0:8000/docs` here you can interact with the different routes.

### Start the telegram bot

To start up your own telegram bot you will need to create a bot using BotFather and add it. BotFather will give you a bot token which you can put in the .env file. You will also need to update the API_URL in .env file to point to the correct localhost port then run:

```
python3 server/src/telegram_bot.py
```

## Tech
- Render
- Helius
- MongoDB
- pymongo
- pytelegrambotapi
- FastAPI
- Uvicorn
- Requests
- Python-dotenv
- black
