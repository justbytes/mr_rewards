PROJECT = {
    "name": "Revs",
    "distributor": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
    "token_mint": "9VxExA1iRPbuLLdSJ2rB3nyBxsyLReT4aqzZBMaBaY1p",
    "dev_wallet": None,
    "last_sig": None,
}

# List of 7 known tokens
KNOWN_TOKENS = {
        "symbol": "BD",
        "name": "Baby Distribute",
        "mint": "HJ9LvBGce9f975mzkvTRMGn9mveQHcfFjQTwEiozoKqq",
        "decimals": "unknown",
    }


WALLET = {
    "wallet_address": "39HArAz53HLjqFiBHK1rHojNwNnvGV14eu6eoSTN8hrR",
    "distributors": {
        "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ": {
            "tokens": {"sol": {"total_amount": 0.12866323400000002}}
        },
        "GVLwP2iR4sqEX9Tos3cmQQRqAumzRumxKD42qyCbCyCC": {
            "tokens": {
                "TNT": {"total_amount": 254866.252989},
                "WLOS": {"total_amount": 0.072849},
            }
        },
        "D8gKfTxnwBG3XPTy4ZT6cGJbz1s13htKtv9j69qbhmv4": {
            "tokens": {
                "IPLR": {"total_amount": 2923.867621},
                "sol": {"total_amount": 0.22602336900000006},
                "DISTRIBUTE": {"total_amount": 880.8290139999999},
            }
        },
        "CvgM6wSDXWCZeCmZnKRQdnh4CSga3UuTXwrCXy9Ju6PC": {
            "tokens": {
                "sol": {"total_amount": 44.78450247900003},
                "DISTRIBUTE": {"total_amount": 86602.083982},
                "TNT": {"total_amount": 22766.576493},
                "PRIZE": {"total_amount": 48803.67187399999},
                "IPLR": {"total_amount": 2810.98695},
                "USDC": {"total_amount": 3.847179},
                "Fartcoin ": {"total_amount": 1.314942},
                "$WIF": {"total_amount": 8.501438},
                "Bonk": {"total_amount": 178328.41438},
            }
        },
        "9uJbttvvowG1rVpPt6GMB3mL7BuktaHaNzFQbkACfiNN": {
            "tokens": {"sol": {"total_amount": 0.47003920499999996}}
        },
    },
}

TRANSACTIONS = [
    {
        "description": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE transferred a total 0.000032696 SOL to multiple accounts.",
        "type": "TRANSFER",
        "source": "SYSTEM_PROGRAM",
        "fee": 5000,
        "feePayer": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
        "signature": "5QHpu3MRjTxDWw11gkT7Vnjozw8V6Y8QBavRMFHv4Uxwvvbn8To6GRhgLfcbrSyo1aDr81c1qW41LqJvbnpAApHu",
        "slot": 355057217,
        "timestamp": 1753213099,
        "tokenTransfers": [
            {
                "fromTokenAccount": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                "toTokenAccount": "3JapZuZqDhd4FAWUet6pPSfVRtmZas76qMqQ4JVCUsMB",
                "fromUserAccount": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                "toUserAccount": "3JapZuZqDhd4FAWUet6pPSfVRtmZas76qMqQ4JVCUsMB",
                "tokenAmount": 79891.648807,
                "mint": "HJ9LvBGce9f975mzkvTRMGn9mveQHcfFjQTwEiozoKqq"
            }
        ],
        "nativeTransfers": [
            {
                "fromUserAccount": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                "toUserAccount": "3JapZuZqDhd4FAWUet6pPSfVRtmZas76qMqQ4JVCUsMB",
                "amount": 2771,
            },
            {
                "fromUserAccount": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                "toUserAccount": "3Y3vtfR1PSqN1bFjbFvB2cB9c6MSZWqKavRe3jTYFwUW",
                "amount": 2771,
            },
            {
                "fromUserAccount": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                "toUserAccount": "HR2FKTZedm5C2jMvVf9MV1xDGsLoxwJ6SZx88s6C6eh7",
                "amount": 2770,
            },
        ],
        "accountData": [
            {
                "account": "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                "nativeBalanceChange": -32696,
                "tokenBalanceChanges": [],
            },
            {
                "account": "2dUkDgeGMJ78oA4P5qrPJPNXSvLU9CSN7VqbUkBuptNT",
                "nativeBalanceChange": 2770,
                "tokenBalanceChanges": [],
            },
            {
                "account": "2ps6DAcC93ZkGrcCyoade2j3zc5CYjLJKoQmziumd3yP",
                "nativeBalanceChange": 2769,
                "tokenBalanceChanges": [],
            },
            {
                "account": "3JapZuZqDhd4FAWUet6pPSfVRtmZas76qMqQ4JVCUsMB",
                "nativeBalanceChange": 2771,
                "tokenBalanceChanges": [],
            },
            {
                "account": "3Y3vtfR1PSqN1bFjbFvB2cB9c6MSZWqKavRe3jTYFwUW",
                "nativeBalanceChange": 2771,
                "tokenBalanceChanges": [],
            },
            {
                "account": "4KnY6TPt3o4eZaVsN6UBRDmghsKxLk4xoWF8gbjKLNxA",
                "nativeBalanceChange": 2769,
                "tokenBalanceChanges": [],
            },
            {
                "account": "5WPrDbMkcF9rVfqBb6TXSbfHNdotV4LDQrJsjxGQacW2",
                "nativeBalanceChange": 2769,
                "tokenBalanceChanges": [],
            },
            {
                "account": "7yeaF17Pxkv5RfJJWCktXGjRUp3hVATEDeQ59zZ4wiDX",
                "nativeBalanceChange": 2769,
                "tokenBalanceChanges": [],
            },
            {
                "account": "ENMrEzTVmZPh8vJo5H7yuyWufJ8FCpJy4L8SovzQKNQz",
                "nativeBalanceChange": 2769,
                "tokenBalanceChanges": [],
            },
            {
                "account": "GNDipNde67mjZ2puRXtiAMghqv7RFamrsEtyE9rGExAT",
                "nativeBalanceChange": 2769,
                "tokenBalanceChanges": [],
            },
            {
                "account": "HR2FKTZedm5C2jMvVf9MV1xDGsLoxwJ6SZx88s6C6eh7",
                "nativeBalanceChange": 2770,
                "tokenBalanceChanges": [],
            },
            {
                "account": "11111111111111111111111111111111",
                "nativeBalanceChange": 0,
                "tokenBalanceChanges": [],
            },
        ],
        "transactionError": None,
        "instructions": [
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "3JapZuZqDhd4FAWUet6pPSfVRtmZas76qMqQ4JVCUsMB",
                ],
                "data": "3Bxs4cA1ADbAZrpf",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "3Y3vtfR1PSqN1bFjbFvB2cB9c6MSZWqKavRe3jTYFwUW",
                ],
                "data": "3Bxs4cA1ADbAZrpf",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "HR2FKTZedm5C2jMvVf9MV1xDGsLoxwJ6SZx88s6C6eh7",
                ],
                "data": "3Bxs4bzJVCnM5FZ9",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "2dUkDgeGMJ78oA4P5qrPJPNXSvLU9CSN7VqbUkBuptNT",
                ],
                "data": "3Bxs4bzJVCnM5FZ9",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "5WPrDbMkcF9rVfqBb6TXSbfHNdotV4LDQrJsjxGQacW2",
                ],
                "data": "3Bxs4bpbpByXaeHd",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "7yeaF17Pxkv5RfJJWCktXGjRUp3hVATEDeQ59zZ4wiDX",
                ],
                "data": "3Bxs4bpbpByXaeHd",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "4KnY6TPt3o4eZaVsN6UBRDmghsKxLk4xoWF8gbjKLNxA",
                ],
                "data": "3Bxs4bpbpByXaeHd",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "2ps6DAcC93ZkGrcCyoade2j3zc5CYjLJKoQmziumd3yP",
                ],
                "data": "3Bxs4bpbpByXaeHd",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "GNDipNde67mjZ2puRXtiAMghqv7RFamrsEtyE9rGExAT",
                ],
                "data": "3Bxs4bpbpByXaeHd",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
            {
                "accounts": [
                    "72hnXr9PsMjp8WsnFyZjmm5vzHhTqbfouqtHBgLYdDZE",
                    "ENMrEzTVmZPh8vJo5H7yuyWufJ8FCpJy4L8SovzQKNQz",
                ],
                "data": "3Bxs4bpbpByXaeHd",
                "programId": "11111111111111111111111111111111",
                "innerInstructions": [],
            },
        ],
        "events": {},
    },
]
