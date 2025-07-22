# Mock data for testing
MOCK_DISTRIBUTOR = "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ"
MOCK_PROJECT = {
    "name": "Test Project",
    "distributor": MOCK_DISTRIBUTOR,
    "token_mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "dev_wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
    "last_sig": ""
}

# Mock transaction data (10 realistic Solana transactions)
MOCK_TRANSACTIONS = [
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "2ZE7Rz8gKRmCJhntJhX5cKJ8x9QqMzB4YnE6wP7Q8hX3KsH9fL2pWvR6tA4mN5bC8dF7yG9",
        "slot": 250000001,
        "timestamp": 1708000000,
        "token_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 1000.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "3AF8Sz9hLSmDKioOKiY6dLK9z0QrNzC5ZoF7xQ8R9iY4LtI0gM3qXwS7uB5oO6cD9eG8zH0",
        "slot": 250000002,
        "timestamp": 1708000010,
        "token_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "A7uqmajxP3NdzbYDXiGQRKhd2bMrVDKhRtHgVQRAyrZz",
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 500.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "4BG9Ta0iMTnELjpPLjZ7eML0a1RsOzD6ApG8yR9S0jZ5MuJ1hN4rYxT8vC6pP7dE0fH9aI1",
        "slot": 250000003,
        "timestamp": 1708000020,
        "token_transfers": [],
        "native_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "amount": 2000000000  # 2 SOL in lamports
            }
        ]
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "5CH0Ub1jNUoFMkqQMkA8fNM1b2StPaE7BqH9zS0T1kA6NvK2iO5sZyU9wD7qQ8eF1gI0bJ2",
        "slot": 250000004,
        "timestamp": 1708000030,
        "token_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
                "mint": "So11111111111111111111111111111111111111112",  # Wrapped SOL
                "tokenAmount": 1.5
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "6DI1Vc2kOVpGNlrRNlB9gON2c3TuQbF8CrI0aT1U2lB7OwL3jP6tAzV0xE8rR9fG2hJ1cK3",
        "slot": 250000005,
        "timestamp": 1708000040,
        "token_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Same user gets more
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 750.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "7EJ2Wd3lPWqHOmsSOmC0hPO3d4UvRcG9DsJ1bU2V3mC8PxM4kQ7uBaW1yF9sS0gH3iK2dL4",
        "slot": 250000006,
        "timestamp": 1708000050,
        "token_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "C9xsobayxR5PfzbZFYkIQMmf4dOvScH0EtKiXSSczbBb",
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "tokenAmount": 100.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "8FK3Xe4mQXrIPntTPnD1iQP4e5VwSdH0FuK2cV3W4nD9QyN5lR8vCbX2zG0tT1hI4jL3eM5",
        "slot": 250000007,
        "timestamp": 1708000060,
        "token_transfers": [],
        "native_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "A7uqmajxP3NdzbYDXiGQRKhd2bMrVDKhRtHgVQRAyrZz",
                "amount": 1500000000  # 1.5 SOL in lamports
            }
        ]
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "9GL4Yf5nRYsJQouUQoE2jRP5f6WxTeI1GvL3dW4X5oE0RzO6mS9wDcY3aH1uU2iJ5kM4fN6",
        "slot": 250000008,
        "timestamp": 1708000070,
        "token_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "D0ytpcbayxS6QgzbZGYkJRNmg5eOvTdI2HuL4eTdcCc",
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 2000.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "0HM5Zg6oSZtKRpvVRpF3kSQ6g7XyUfJ2IwM4eX5Y6pF1SaP7nT0xEdZ4bI2vV3jK6lN5gO7",
        "slot": 250000009,
        "timestamp": 1708000080,
        "token_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "tokenAmount": 250.0
            }
        ],
        "native_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
                "amount": 500000000  # 0.5 SOL in lamports
            }
        ]
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "1IN6Ah7pTauLSpwWSqG4lTR7h8YzVgK3JxN5fY6Z7qG2TbQ8oU1yFeA5cJ3wW4kL7mO6hP8",
        "slot": 250000010,
        "timestamp": 1708000090,
        "token_transfers": [
            {
                "fromUserAccount": MOCK_DISTRIBUTOR,
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Same user again
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "tokenAmount": 50.0
            }
        ],
        "native_transfers": []
    }
]

# Mock token metadata
MOCK_TOKEN_METADATA = {
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
        "symbol": "USDT",
        "name": "Tether USD",
        "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "decimals": "6"
    },
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
        "symbol": "USDC",
        "name": "USD Coin",
        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "decimals": "6"
    },
    "So11111111111111111111111111111111111111112": {
        "symbol": "WSOL",
        "name": "Wrapped SOL",
        "mint": "So11111111111111111111111111111111111111112",
        "decimals": "9"
    }
}


TEST_TRANSACTIONS = [
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "2ZE7Rz8gKRmCJhntJhX5cKJ8x9QqMzB4YnE6wP7Q8hX3KsH9fL2pWvR6tA4mN5bC8dF7yG9",
        "slot": 250000001,
        "timestamp": 1708000000,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 1000.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "3AF8Sz9hLSmDKioOKiY6dLK9z0QrNzC5ZoF7xQ8R9iY4LtI0gM3qXwS7uB5oO6cD9eG8zH0",
        "slot": 250000002,
        "timestamp": 1708000010,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "A7uqmajxP3NdzbYDXiGQRKhd2bMrVDKhRtHgVQRAyrZz",
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 500.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "4BG9Ta0iMTnELjpPLjZ7eML0a1RsOzD6ApG8yR9S0jZ5MuJ1hN4rYxT8vC6pP7dE0fH9aI1",
        "slot": 250000003,
        "timestamp": 1708000020,
        "token_transfers": [],
        "native_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
                "amount": 2000000000  # 2 SOL in lamports
            }
        ]
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "5CH0Ub1jNUoFMkqQMkA8fNM1b2StPaE7BqH9zS0T1kA6NvK2iO5sZyU9wD7qQ8eF1gI0bJ2",
        "slot": 250000004,
        "timestamp": 1708000030,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
                "mint": "So11111111111111111111111111111111111111112",  # Wrapped SOL
                "tokenAmount": 1.5
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "6DI1Vc2kOVpGNlrRNlB9gON2c3TuQbF8CrI0aT1U2lB7OwL3jP6tAzV0xE8rR9fG2hJ1cK3",
        "slot": 250000005,
        "timestamp": 1708000040,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Same user gets more
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 750.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "7EJ2Wd3lPWqHOmsSOmC0hPO3d4UvRcG9DsJ1bU2V3mC8PxM4kQ7uBaW1yF9sS0gH3iK2dL4",
        "slot": 250000006,
        "timestamp": 1708000050,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "C9xsobayxR5PfzbZFYkIQMmf4dOvScH0EtKiXSSczbBb",
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "tokenAmount": 100.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "8FK3Xe4mQXrIPntTPnD1iQP4e5VwSdH0FuK2cV3W4nD9QyN5lR8vCbX2zG0tT1hI4jL3eM5",
        "slot": 250000007,
        "timestamp": 1708000060,
        "token_transfers": [],
        "native_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "A7uqmajxP3NdzbYDXiGQRKhd2bMrVDKhRtHgVQRAyrZz",
                "amount": 1500000000  # 1.5 SOL in lamports
            }
        ]
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "9GL4Yf5nRYsJQouUQoE2jRP5f6WxTeI1GvL3dW4X5oE0RzO6mS9wDcY3aH1uU2iJ5kM4fN6",
        "slot": 250000008,
        "timestamp": 1708000070,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "D0ytpcbayxS6QgzbZGYkJRNmg5eOvTdI2HuL4eTdcCc",
                "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "tokenAmount": 2000.0
            }
        ],
        "native_transfers": []
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "0HM5Zg6oSZtKRpvVRpF3kSQ6g7XyUfJ2IwM4eX5Y6pF1SaP7nT0xEdZ4bI2vV3jK6lN5gO7",
        "slot": 250000009,
        "timestamp": 1708000080,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "tokenAmount": 250.0
            }
        ],
        "native_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "B8vrnavxQ4OezbYEYjHQSLie3cNsWELhSuIhWRSBzrAa",
                "amount": 500000000  # 0.5 SOL in lamports
            }
        ]
    },
    {
        "fee_payer": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "signature": "1IN6Ah7pTauLSpwWSqG4lTR7h8YzVgK3JxN5fY6Z7qG2TbQ8oU1yFeA5cJ3wW4kL7mO6hP8",
        "slot": 250000010,
        "timestamp": 1708000090,
        "token_transfers": [
            {
                "fromUserAccount": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
                "toUserAccount": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Same user again
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "tokenAmount": 50.0
            }
        ],
        "native_transfers": []
    }
]

TEST_PROJECT = {
    "name": "Test Rewards Project",
    "distributor": "BoonAKjwqfxj3Z1GtZHWeEMnoZLqgkSFEqRwhRsz4oQ",
    "token_mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "dev_wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
    "last_sig": ""
}

TOKEN_SYMBOLS = {
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
    "So11111111111111111111111111111111111111112": "WSOL"
}
