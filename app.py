# ═══════════════════════════════════════════════════════════════════════════════
# CMVNG BOT v2 — CONFIRMATION TRADING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
# Philosophy: NOT prediction trading. CONFIRMATION trading.
# Wait for candle to form. Confirm direction won't reverse. Enter late at high odds.
# MECHANICAL — pure coded rules. Zero API cost. Instant execution.
# ═══════════════════════════════════════════════════════════════════════════════

from flask import Flask, request, jsonify, render_template_string, redirect
import pg8000.native
import os
import re
import threading
import time
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict

app = Flask(__name__)

# ═══════════════════════════════════════════════════════════
# ENVIRONMENT VARIABLES
# ═══════════════════════════════════════════════════════════

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DATABASE_URL     = os.environ.get("DATABASE_URL", "")

# Polymarket CLOB API credentials
POLY_API_KEY       = os.environ.get("POLY_API_KEY", "")
POLY_API_SECRET    = os.environ.get("POLY_API_SECRET", "")
POLY_API_PASSPHRASE = os.environ.get("POLY_API_PASSPHRASE", "")
POLY_FUNDER_ADDRESS = os.environ.get("POLY_FUNDER_ADDRESS", "")
POLY_PROXY_URL     = os.environ.get("POLY_PROXY_URL", "")
LIMITLESS_PRIV_KEY = os.environ.get("LIMITLESS_PRIVATE_KEY", "")

# ── Limitless LIVE trading (DB-backed toggle; env values below are only
# defaults for first boot / cap overrides). The live ON/OFF switch lives in
# the v2_settings table and is flipped from the paper bot page. Per-trade
# and daily caps gate exposure while the strategy proves out.
#
# Prerequisites for live to actually execute (all checked at trade time):
#   * py-limitless installed
#   * LIMITLESS_PRIVATE_KEY env present
#   * Wallet has USDC on Base
#   * Limitless exchange contract approved as USDC spender (one click in app)
LIMITLESS_LIVE_BOOTSTRAP = os.environ.get("LIMITLESS_LIVE", "0").strip().lower() in ("1", "true", "yes", "on")
LIMITLESS_MAX_TRADE_USDC = float(os.environ.get("LIMITLESS_MAX_TRADE_USDC", "1.0"))
# Balance floor: when wallet USDC drops below this, the bot pauses live trading
# until winning trades top it back up. Replaces the old daily-cap model — the
# floor is the natural circuit breaker since balance reflects actual capital.
LIMITLESS_MIN_BALANCE_USDC = float(os.environ.get("LIMITLESS_MIN_BALANCE_USDC", "2.0"))
LIMITLESS_BASE_RPC = os.environ.get("LIMITLESS_BASE_RPC", "https://mainnet.base.org")

# ════════════════════════════════════════════════════════════════════
# LIMITLESS EXCHANGE SDK — inlined source (was py-limitless on PyPI)
# ────────────────────────────────────────────────────────────────────
# Reason: py-limitless==0.1.1 pins eth-account==0.10.0 EXACTLY, which
# conflicts hard with py-clob-client==0.34.6 (eth-account>=0.13.0).
# The SDK code itself works fine on eth-account 0.13 — only the pip
# metadata pin was wrong. Inlining sidesteps the resolver conflict.
# Same code as py-limitless==0.1.1 with relative imports flattened
# and the WebSocket module dropped (we don't use it; would have
# required python-socketio + aiohttp).
# ════════════════════════════════════════════════════════════════════

# ──── from limitless_sdk/constants.py ────
"""
Limitless Exchange Constants
Contract addresses, EIP-712 type definitions, and configuration
"""

from typing import Literal

# Wallet Types
WalletType = Literal["eoa", "smart_wallet"]

# API Configuration
API_BASE_URL = "https://api.limitless.exchange"
WEBSOCKET_URL = "wss://ws.limitless.exchange"

# Contract Addresses (Base Chain)
# DEPRECATED: These are legacy addresses. Use market.venue.exchange instead.
# Markets now have per-market venue exchange addresses returned from the API.
CLOB_ADDRESS = "0xa4409D988CA2218d956BeEFD3874100F444f0DC3"  # Deprecated
NEGRISK_ADDRESS = "0x5a38afc17F7E97ad8d6C547ddb837E40B4aEDfC6"  # Deprecated

# USDC Token (Base Chain)
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

# Gnosis Conditional Token Framework (Base Chain)
# Global ERC-1155 contract that holds all conditional tokens (YES/NO positions)
BASE_CTF_ADDRESS = "0xC9c98965297Bc527861c898329Ee280632B76e18"

# Multicall3 (same address on all EVM chains)
# Used to batch multiple contract calls into a single transaction
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

# Max uint256 for unlimited approval
MAX_UINT256 = 2**256 - 1

# EIP-4337 Account Abstraction
ENTRY_POINT_V06 = "0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789"
ENTRY_POINT_V07 = "0x0000000071727De22E5E9d8BAf0edAc6f37da032"
SAFE_4337_MODULE_V06 = "0xa581c4A4DB7175302464fF3C06380BC3270b4037"
SAFE_4337_MODULE_V07 = "0x75cf11467937ce3F2f357CE24ffc3DBF8fD5c226"
SAFE_4337_MODULE_V07_ERC7579 = "0x7579EE8307284F293B1927136486880611F20002"

# Paymaster
PAYMASTER_ADDRESS = "0x6666666666667849c56f2850848ce1c4da65c68b"
GAS_TOKEN = "0x2105"

# Chain Configuration
BASE_CHAIN_ID = 8453

# EIP-712 Type Definitions for Order Signing
ORDER_TYPES = {
    "Order": [
        {"name": "salt", "type": "uint256"},
        {"name": "maker", "type": "address"},
        {"name": "signer", "type": "address"},
        {"name": "taker", "type": "address"},
        {"name": "tokenId", "type": "uint256"},
        {"name": "makerAmount", "type": "uint256"},
        {"name": "takerAmount", "type": "uint256"},
        {"name": "expiration", "type": "uint256"},
        {"name": "nonce", "type": "uint256"},
        {"name": "feeRateBps", "type": "uint256"},
        {"name": "side", "type": "uint8"},
        {"name": "signatureType", "type": "uint8"},
    ]
}

# EIP-712 Types for Safe 4337 Operations
SAFE_OP_TYPES = {
    "EIP712Domain": [
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "SafeOp": [
        {"name": "safe", "type": "address"},
        {"name": "nonce", "type": "uint256"},
        {"name": "initCode", "type": "bytes"},
        {"name": "callData", "type": "bytes"},
        {"name": "callGasLimit", "type": "uint256"},
        {"name": "verificationGasLimit", "type": "uint256"},
        {"name": "preVerificationGas", "type": "uint256"},
        {"name": "maxFeePerGas", "type": "uint256"},
        {"name": "maxPriorityFeePerGas", "type": "uint256"},
        {"name": "paymasterAndData", "type": "bytes"},
        {"name": "validAfter", "type": "uint48"},
        {"name": "validUntil", "type": "uint48"},
        {"name": "entryPoint", "type": "address"},
    ],
}

# Market Category Mappings
CATEGORY_IDS = {
    2: "Crypto",
    5: "Other",
    19: "Company News",
    23: "Economy",
    29: "Hourly",
    30: "Daily",
    31: "Weekly",
    39: "中文预测专区",
    42: "Korean Market",
}

# Trade Sides
SIDE_BUY = 0
SIDE_SELL = 1

# Order Types
ORDER_TYPE_GTC = "GTC"  # Good Till Cancelled
ORDER_TYPE_FOK = "FOK"  # Fill Or Kill

# Market Types
MARKET_TYPE_CLOB = "CLOB"
MARKET_TYPE_NEGRISK = "NEGRISK"

# Signature Types
SIGNATURE_TYPE_EOA = 0
SIGNATURE_TYPE_EIP712 = 2

# Scaling
USDC_DECIMALS = 6
SCALING_FACTOR = 10**USDC_DECIMALS  # 1e6 for USDC

# Entry Point ABI (minimal for nonce retrieval)
ENTRY_POINT_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "sender", "type": "address"},
            {"internalType": "uint192", "name": "key", "type": "uint192"},
        ],
        "name": "getNonce",
        "outputs": [
            {"internalType": "uint256", "name": "nonce", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

# ERC20 ABI (minimal for allowance and approve)
ERC20_ABI = [
    {
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ERC1155 ABI (minimal for setApprovalForAll and isApprovedForAll)
# Used for conditional token (CTF) approval when selling positions
ERC1155_ABI = [
    {
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Conditional Token Framework (CTF) ABI - for position redemption
# Gnosis CTF contract on Base: 0xC9c98965297Bc527861c898329Ee280632B76e18
CTF_ABI = [
    # redeemPositions - claim winnings from resolved markets
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    # balanceOf - check position balance
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Multicall3 ABI - for batching multiple contract calls
# https://github.com/mds1/multicall
MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "target", "type": "address"},
                    {"name": "allowFailure", "type": "bool"},
                    {"name": "callData", "type": "bytes"},
                ],
                "name": "calls",
                "type": "tuple[]",
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    {"name": "success", "type": "bool"},
                    {"name": "returnData", "type": "bytes"},
                ],
                "name": "returnData",
                "type": "tuple[]",
            }
        ],
        "stateMutability": "payable",
        "type": "function",
    },
]

# ──── from limitless_sdk/utils.py ────
"""
Limitless Exchange Utilities
Common helper functions
"""

from importlib.metadata import version, PackageNotFoundError


def string_to_hex(text: str) -> str:
    """Convert string to hex representation with 0x prefix."""
    return "0x" + text.encode("utf-8").hex()


def assert_eth_account_version(required: str = "0.10.0") -> None:
    """
    Assert that eth-account version matches the required version.
    
    Args:
        required: Required version string (default: "0.10.0")
        
    Raises:
        RuntimeError: If eth-account is not installed or version mismatches
    """
    try:
        installed = version("eth-account")
    except PackageNotFoundError:
        raise RuntimeError("eth-account is not installed")

    if installed != required:
        raise RuntimeError(
            f"eth-account version mismatch: {installed} (expected {required})"
        )


def scale_amount(amount: float, decimals: int = 6) -> int:
    """
    Scale a decimal amount to integer representation.
    
    Args:
        amount: Decimal amount (e.g., 10.5 for 10.5 USDC)
        decimals: Number of decimals (default: 6 for USDC)
        
    Returns:
        Scaled integer amount
    """
    return round(amount * (10 ** decimals))


def unscale_amount(amount: int, decimals: int = 6) -> float:
    """
    Unscale an integer amount to decimal representation.
    
    Args:
        amount: Scaled integer amount
        decimals: Number of decimals (default: 6 for USDC)
        
    Returns:
        Decimal amount
    """
    return amount / (10 ** decimals)


def cents_to_dollars(cents: int | float) -> float:
    """Convert price in cents to dollars."""
    return cents / 100


def dollars_to_cents(dollars: float) -> int:
    """Convert price in dollars to cents."""
    return round(dollars * 100)


def format_address(address: str) -> str:
    """
    Ensure address has 0x prefix.
    
    Args:
        address: Ethereum address with or without 0x prefix
        
    Returns:
        Address with 0x prefix
    """
    if not address.startswith("0x"):
        return "0x" + address
    return address


def format_private_key(private_key: str) -> str:
    """
    Ensure private key has 0x prefix.
    
    Args:
        private_key: Private key with or without 0x prefix
        
    Returns:
        Private key with 0x prefix
    """
    return format_address(private_key)


def strip_0x(hex_string: str) -> str:
    """Remove 0x prefix from hex string if present."""
    if hex_string.startswith("0x"):
        return hex_string[2:]
    return hex_string

# ──── from limitless_sdk/cache.py ────
"""
LRU Cache implementation for the Limitless SDK.

Provides a size-limited cache with Least Recently Used eviction policy.
"""

from collections import OrderedDict
from typing import Any, Optional


class LRUCache:
    """Size-limited cache with LRU (Least Recently Used) eviction.

    Uses OrderedDict to track access order. When cache is full,
    the least recently accessed item is evicted.

    Args:
        maxsize: Maximum number of items to cache. Defaults to 100.
    """

    def __init__(self, maxsize: int = 100):
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._maxsize = maxsize

    def __contains__(self, key: str) -> bool:
        """Check if key exists in cache."""
        return key in self._cache

    def get(self, key: str, default: Any = None) -> Optional[Any]:
        """Get value from cache, marking it as recently used.

        Args:
            key: Cache key to look up.
            default: Value to return if key not found.

        Returns:
            Cached value if found, default otherwise.
        """
        if key in self._cache:
            self._cache.move_to_end(key)  # Mark as recently used
            return self._cache[key]
        return default

    def __getitem__(self, key: str) -> Any:
        """Get value from cache using bracket notation.

        Args:
            key: Cache key to look up.

        Returns:
            Cached value.

        Raises:
            KeyError: If key not in cache.
        """
        if key in self._cache:
            self._cache.move_to_end(key)  # Mark as recently used
            return self._cache[key]
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        """Set value in cache, evicting LRU item if full.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            if len(self._cache) >= self._maxsize:
                self._cache.popitem(last=False)  # Evict least recently used
        self._cache[key] = value

    def __len__(self) -> int:
        """Return number of items in cache."""
        return len(self._cache)

    def clear(self) -> None:
        """Clear all items from cache."""
        self._cache.clear()

# ──── from limitless_sdk/auth.py ────
"""
Limitless Exchange Authentication
Authentication utilities for API access
"""

import requests
from eth_account import Account
from eth_account.messages import encode_defunct



def get_signing_message(api_base_url: str = API_BASE_URL) -> str:
    """
    Fetch the signing message from the API.

    Args:
        api_base_url: Base URL for the API

    Returns:
        The signing message to be signed

    Raises:
        Exception: If the request fails
    """
    response = requests.get(f"{api_base_url}/auth/signing-message")
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to get signing message: {response.status_code}")


def sign_message(private_key: str, message: str) -> str:
    """
    Sign a message using a private key.

    Args:
        private_key: Private key for signing
        message: Message to sign

    Returns:
        Hex-encoded signature with 0x prefix
    """
    private_key = format_private_key(private_key)
    account = Account.from_key(private_key)

    message_hash = encode_defunct(text=message)
    signed_message = account.sign_message(message_hash)

    sig_hex = signed_message.signature.hex()
    if not sig_hex.startswith("0x"):
        sig_hex = "0x" + sig_hex

    return sig_hex


def authenticate(
    private_key: str,
    signing_message: str | None = None,
    api_base_url: str = API_BASE_URL,
    referral_code: str | None = None,
) -> tuple[str, dict]:
    """
    Authenticate with the Limitless Exchange API.

    Args:
        private_key: Your wallet's private key
        signing_message: Optional pre-fetched signing message
        api_base_url: Base URL for the API
        referral_code: Optional referral code

    Returns:
        Tuple of (session_cookie, user_data)

    Raises:
        Exception: If authentication fails
    """
    private_key = format_private_key(private_key)
    account = Account.from_key(private_key)
    ethereum_address = account.address

    # Get signing message if not provided
    if signing_message is None:
        signing_message = get_signing_message(api_base_url)

    hex_message = string_to_hex(signing_message)

    # Sign the message
    signature = sign_message(private_key, signing_message)

    headers = {
        "x-account": ethereum_address,
        "x-signing-message": hex_message,
        "x-signature": signature,
        "Content-Type": "application/json",
    }

    payload = {"client": "eoa"}
    if referral_code:
        payload["r"] = referral_code

    response = requests.post(f"{api_base_url}/auth/login", headers=headers, json=payload)

    if response.status_code == 200:
        session_cookie = response.cookies.get("limitless_session")
        return session_cookie, response.json()
    else:
        raise Exception(f"Authentication failed: {response.status_code} - {response.text}")


def get_auth_headers(session_cookie: str) -> dict:
    """
    Get headers for authenticated API requests.

    Args:
        session_cookie: Session cookie from authentication

    Returns:
        Headers dict ready for requests
    """
    return {
        "cookie": f"limitless_session={session_cookie}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def get_smart_wallet(private_key: str, api_base_url: str = API_BASE_URL) -> str | None:
    """
    Get the smart wallet address associated with a private key.

    Args:
        private_key: Private key for authentication
        api_base_url: Base URL for the API

    Returns:
        Smart wallet address or None if not found
    """
    try:
        _, user_data = authenticate(private_key, api_base_url=api_base_url)
        return user_data.get("smartWallet")
    except Exception as e:
        print(f"Could not fetch smart wallet: {e}")
        return None

# ──── from limitless_sdk/approval.py ────
"""
Limitless Exchange Approval Helpers
Functions for checking and setting allowances for venue exchanges.

Includes:
- USDC (ERC-20) approval for buying
- Conditional token (ERC-1155) approval for selling
"""

from eth_account import Account
from web3 import Web3



def get_usdc_contract(w3: Web3) -> "Web3.eth.contract":
    """Get the USDC contract instance.

    Args:
        w3: Web3 instance connected to Base.

    Returns:
        USDC contract instance.
    """
    return w3.eth.contract(
        address=Web3.to_checksum_address(USDC_ADDRESS),
        abi=ERC20_ABI,
    )


def check_usdc_allowance(w3: Web3, owner: str, spender: str) -> int:
    """Check current USDC allowance for a spender.

    Args:
        w3: Web3 instance connected to Base.
        owner: Owner address (the wallet granting approval).
        spender: Spender address (the venue exchange contract).

    Returns:
        Current allowance in raw units (6 decimals).
    """
    usdc = get_usdc_contract(w3)
    return usdc.functions.allowance(
        Web3.to_checksum_address(owner),
        Web3.to_checksum_address(spender),
    ).call()


def approve_usdc(
    w3: Web3,
    private_key: str,
    spender: str,
    amount: int = MAX_UINT256,
    wait_for_receipt: bool = True,
) -> str:
    """Approve USDC spending for a spender address.

    Args:
        w3: Web3 instance connected to Base.
        private_key: Private key of the owner wallet.
        spender: Spender address (the venue exchange contract).
        amount: Amount to approve (default: unlimited).
        wait_for_receipt: Whether to wait for transaction confirmation.

    Returns:
        Transaction hash as hex string.

    Raises:
        Exception: If transaction fails.
    """
    private_key = format_private_key(private_key)
    account = Account.from_key(private_key)
    owner_address = account.address

    usdc = get_usdc_contract(w3)

    # Build transaction
    nonce = w3.eth.get_transaction_count(owner_address)
    gas_price = w3.eth.gas_price

    approve_tx = usdc.functions.approve(
        Web3.to_checksum_address(spender),
        amount,
    ).build_transaction(
        {
            "from": owner_address,
            "nonce": nonce,
            "gas": 100000,  # Approve typically uses ~50k gas
            "gasPrice": gas_price,
            "chainId": BASE_CHAIN_ID,
        }
    )

    # Sign and send
    signed_tx = w3.eth.account.sign_transaction(approve_tx, private_key)
    # Handle both old (rawTransaction) and new (raw_transaction) attribute names
    raw_tx = getattr(signed_tx, "raw_transaction", None) or signed_tx.rawTransaction
    tx_hash = w3.eth.send_raw_transaction(raw_tx)

    if wait_for_receipt:
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        if receipt.status != 1:
            raise Exception(f"USDC approval transaction failed: {tx_hash.hex()}")

    return tx_hash.hex()


def ensure_usdc_approved(
    w3: Web3,
    private_key: str,
    spender: str,
    min_amount: int = 0,
) -> dict:
    """Check USDC allowance and approve if needed.

    Args:
        w3: Web3 instance connected to Base.
        private_key: Private key of the owner wallet.
        spender: Spender address (the venue exchange contract).
        min_amount: Minimum required allowance (0 means any approval is sufficient).

    Returns:
        Dict with:
            - already_approved: bool - True if approval was already sufficient
            - tx_hash: str | None - Transaction hash if approval was needed
            - allowance: int - Current allowance after operation
    """
    private_key = format_private_key(private_key)
    account = Account.from_key(private_key)
    owner_address = account.address

    current_allowance = check_usdc_allowance(w3, owner_address, spender)

    # Check if approval is sufficient
    if min_amount == 0:
        # Any approval is fine, check if unlimited
        if current_allowance >= MAX_UINT256 // 2:
            return {
                "already_approved": True,
                "tx_hash": None,
                "allowance": current_allowance,
            }
    elif current_allowance >= min_amount:
        return {
            "already_approved": True,
            "tx_hash": None,
            "allowance": current_allowance,
        }

    # Need to approve
    tx_hash = approve_usdc(w3, private_key, spender)
    new_allowance = check_usdc_allowance(w3, owner_address, spender)

    return {
        "already_approved": False,
        "tx_hash": tx_hash,
        "allowance": new_allowance,
    }


def get_usdc_balance(w3: Web3, address: str) -> int:
    """Get USDC balance for an address.

    Args:
        w3: Web3 instance connected to Base.
        address: Wallet address.

    Returns:
        USDC balance in raw units (6 decimals).
    """
    usdc = get_usdc_contract(w3)
    return usdc.functions.balanceOf(
        Web3.to_checksum_address(address),
    ).call()


# =============================================================================
# Conditional Token (ERC-1155) Approval Functions
# Used when selling positions - must approve venue exchange to transfer tokens
# =============================================================================


def get_ctf_contract(w3: Web3, ctf_address: str) -> "Web3.eth.contract":
    """Get the Conditional Token Framework (ERC-1155) contract instance.

    Args:
        w3: Web3 instance connected to Base.
        ctf_address: Address of the CTF contract.

    Returns:
        CTF contract instance.
    """
    return w3.eth.contract(
        address=Web3.to_checksum_address(ctf_address),
        abi=ERC1155_ABI,
    )


def check_ctf_approval(w3: Web3, ctf_address: str, owner: str, operator: str) -> bool:
    """Check if an operator is approved to transfer all tokens for an owner.

    Args:
        w3: Web3 instance connected to Base.
        ctf_address: Address of the CTF contract.
        owner: Owner address (the wallet granting approval).
        operator: Operator address (the venue exchange contract).

    Returns:
        True if approved, False otherwise.
    """
    ctf = get_ctf_contract(w3, ctf_address)
    return ctf.functions.isApprovedForAll(
        Web3.to_checksum_address(owner),
        Web3.to_checksum_address(operator),
    ).call()


def approve_ctf(
    w3: Web3,
    private_key: str,
    ctf_address: str,
    operator: str,
    approved: bool = True,
    wait_for_receipt: bool = True,
) -> str:
    """Approve an operator to transfer all conditional tokens.

    This is required before selling positions. Uses ERC-1155 setApprovalForAll.

    Args:
        w3: Web3 instance connected to Base.
        private_key: Private key of the owner wallet.
        ctf_address: Address of the CTF contract.
        operator: Operator address (the venue exchange contract).
        approved: Whether to approve (True) or revoke (False).
        wait_for_receipt: Whether to wait for transaction confirmation.

    Returns:
        Transaction hash as hex string.

    Raises:
        Exception: If transaction fails.
    """
    private_key = format_private_key(private_key)
    account = Account.from_key(private_key)
    owner_address = account.address

    ctf = get_ctf_contract(w3, ctf_address)

    # Build transaction
    nonce = w3.eth.get_transaction_count(owner_address)
    gas_price = w3.eth.gas_price

    approve_tx = ctf.functions.setApprovalForAll(
        Web3.to_checksum_address(operator),
        approved,
    ).build_transaction(
        {
            "from": owner_address,
            "nonce": nonce,
            "gas": 100000,  # setApprovalForAll typically uses ~50k gas
            "gasPrice": gas_price,
            "chainId": BASE_CHAIN_ID,
        }
    )

    # Sign and send
    signed_tx = w3.eth.account.sign_transaction(approve_tx, private_key)
    # Handle both old (rawTransaction) and new (raw_transaction) attribute names
    raw_tx = getattr(signed_tx, "raw_transaction", None) or signed_tx.rawTransaction
    tx_hash = w3.eth.send_raw_transaction(raw_tx)

    if wait_for_receipt:
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        if receipt.status != 1:
            raise Exception(f"CTF approval transaction failed: {tx_hash.hex()}")

    return tx_hash.hex()


def ensure_ctf_approved(
    w3: Web3,
    private_key: str,
    ctf_address: str,
    operator: str,
) -> dict:
    """Check CTF approval and approve if needed.

    Args:
        w3: Web3 instance connected to Base.
        private_key: Private key of the owner wallet.
        ctf_address: Address of the CTF contract.
        operator: Operator address (the venue exchange contract).

    Returns:
        Dict with:
            - already_approved: bool - True if approval was already granted
            - tx_hash: str | None - Transaction hash if approval was needed
    """
    private_key = format_private_key(private_key)
    account = Account.from_key(private_key)
    owner_address = account.address

    is_approved = check_ctf_approval(w3, ctf_address, owner_address, operator)

    if is_approved:
        return {
            "already_approved": True,
            "tx_hash": None,
        }

    # Need to approve
    tx_hash = approve_ctf(w3, private_key, ctf_address, operator)

    return {
        "already_approved": False,
        "tx_hash": tx_hash,
    }

# ──── from limitless_sdk/orders.py ────
"""
Limitless Exchange Order Management
Order creation, EIP-712 signing, and API submission
"""

import time
from typing import Literal

import requests
from eth_account import Account

# Import with version compatibility
try:
    from eth_account.messages import encode_typed_data
except ImportError:
    from eth_account.messages import encode_structured_data as encode_typed_data



def get_eip712_domain(venue_exchange_address: str) -> dict:
    """
    Get the EIP-712 domain for order signing.

    Args:
        venue_exchange_address: The venue's exchange contract address from market data.

    Returns:
        EIP-712 domain object
    """
    return {
        "name": "Limitless CTF Exchange",
        "version": "1",
        "chainId": BASE_CHAIN_ID,
        "verifyingContract": venue_exchange_address,
    }


def get_eip712_domain_legacy(market_type: Literal["CLOB", "NEGRISK"] = "CLOB") -> dict:
    """
    DEPRECATED: Use get_eip712_domain(venue_exchange_address) instead.

    Get the EIP-712 domain for order signing using legacy hardcoded addresses.

    Args:
        market_type: 'CLOB' or 'NEGRISK'

    Returns:
        EIP-712 domain object
    """
    contract_address = CLOB_ADDRESS if market_type == "CLOB" else NEGRISK_ADDRESS
    return {
        "name": "Limitless CTF Exchange",
        "version": "1",
        "chainId": BASE_CHAIN_ID,
        "verifyingContract": contract_address,
    }


def create_order_payload(
    maker_address: str,
    signer_address: str,
    token_id: str,
    maker_amount: int,
    taker_amount: int,
    fee_rate_bps: int,
    side: int = SIDE_BUY,
    expiration: int = 0,
    nonce: int = 0,
    signature_type: int = SIGNATURE_TYPE_EIP712,
) -> dict:
    """
    Create the base order payload without signature.

    Args:
        maker_address: The maker's wallet address (smart wallet)
        signer_address: The signer's address (embedded account)
        token_id: The token ID to trade (YES or NO token)
        maker_amount: Amount the maker is offering (scaled by 1e6)
        taker_amount: Amount the maker wants in return (scaled by 1e6)
        fee_rate_bps: Fee rate in basis points
        side: 0 for BUY, 1 for SELL
        expiration: Order expiration timestamp (0 for no expiration)
        nonce: Order nonce
        signature_type: Signature type (2 for EIP-712)

    Returns:
        Order payload ready for signing
    """
    salt = int(time.time() * 1000) + (24 * 60 * 60 * 1000)  # Current time + 24h in ms

    return {
        "salt": salt,
        "maker": maker_address,
        "signer": signer_address,
        "taker": "0x0000000000000000000000000000000000000000",  # Open to any taker
        "tokenId": str(token_id),  # Keep as string for API
        "makerAmount": maker_amount,
        "takerAmount": taker_amount,
        "expiration": str(expiration),
        "nonce": nonce,
        "feeRateBps": fee_rate_bps,
        "side": side,
        "signatureType": signature_type,
    }


def sign_order(
    order_payload: dict,
    private_key: str,
    venue_exchange_address: str,
) -> str:
    """
    Sign an order payload using EIP-712.

    Args:
        order_payload: The order data to sign
        private_key: Private key for signing
        venue_exchange_address: The venue's exchange contract address from market data.

    Returns:
        Hex-encoded signature
    """
    private_key = format_private_key(private_key)
    account = Account.from_key(private_key)

    domain_data = get_eip712_domain(venue_exchange_address)

    # Convert string fields to int for signing
    message_data = {
        "salt": order_payload["salt"],
        "maker": order_payload["maker"],
        "signer": order_payload["signer"],
        "taker": order_payload["taker"],
        "tokenId": int(order_payload["tokenId"]),
        "makerAmount": order_payload["makerAmount"],
        "takerAmount": order_payload["takerAmount"],
        "expiration": int(order_payload["expiration"]) if order_payload["expiration"] else 0,
        "nonce": order_payload["nonce"],
        "feeRateBps": order_payload["feeRateBps"],
        "side": order_payload["side"],
        "signatureType": order_payload["signatureType"],
    }

    # Sign using EIP-712
    encoded_message = encode_typed_data(domain_data, ORDER_TYPES, message_data)
    signed_message = account.sign_message(encoded_message)

    # hexbytes 1.x changed .hex() to NOT include the '0x' prefix (was
    # included pre-1.0 when this SDK was written). Limitless's API
    # validates "signature must be a 0x-prefixed hex string" and rejects
    # bare-hex with HTTP 400. Mirror the auth-path safety check at line ~571.
    sig_hex = signed_message.signature.hex()
    if not sig_hex.startswith("0x"):
        sig_hex = "0x" + sig_hex
    return sig_hex


def submit_order(
    order_payload: dict,
    signature: str,
    owner_id: str,
    market_slug: str,
    price: float,
    order_type: str,
    session_cookie: str,
    api_base_url: str = API_BASE_URL,
) -> dict:
    """
    Submit an order to the API.

    Args:
        order_payload: Order payload with order parameters
        signature: EIP-712 signature
        owner_id: User's owner ID
        market_slug: Market slug identifier
        price: Price in decimal format
        order_type: "GTC" or "FOK"
        session_cookie: Authentication session cookie
        api_base_url: Base URL for the API

    Returns:
        API response with order details. Structure::

            {
                "order": {
                    "id": "uuid",           # Order UUID
                    "createdAt": "...",     # ISO timestamp
                    "makerAmount": 1400000, # Raw maker amount (6 decimals)
                    "takerAmount": 2000000, # Raw taker amount (6 decimals)
                    "price": 0.7,           # Price as decimal
                    "side": 0,              # 0=BUY, 1=SELL
                    "tokenId": "...",       # Position token ID
                    "marketId": 12345,      # Market ID
                    "ownerId": 123,         # Owner ID
                    "status": "LIVE",       # Order status
                    "market": {...},        # Full market details
                    "owner": {...}          # Owner details
                }
            }

    Raises:
        Exception: If order submission fails
    """
    headers = get_auth_headers(session_cookie)

    final_payload = {
        "order": {
            **order_payload,
            "signature": signature,
        },
        "ownerId": owner_id,
        "orderType": order_type,
        "marketSlug": market_slug,
    }

    # Only add price for non-FOK orders
    if order_type != "FOK":
        final_payload["order"]["price"] = price

    response = requests.post(
        f"{api_base_url}/orders", headers=headers, json=final_payload, timeout=35
    )

    if response.status_code != 201:
        raise Exception(f"API Error {response.status_code}: {response.text}")

    return response.json()


def cancel_order(
    order_id: str,
    session_cookie: str,
    api_base_url: str = API_BASE_URL,
) -> dict:
    """
    Cancel an existing order.

    Args:
        order_id: UUID of the order to cancel
        session_cookie: Authentication session cookie
        api_base_url: Base URL for the API

    Returns:
        API response

    Raises:
        Exception: If cancellation fails
    """
    headers = get_auth_headers(session_cookie)
    headers.pop("Content-Type")  # Remove Content-Type header to prevent API validation error

    response = requests.delete(
        f"{api_base_url}/orders/{order_id}",
        headers=headers,
    )

    if response.status_code not in (200, 204):
        raise Exception(f"Cancel failed {response.status_code}: {response.text}")

    return response.json() if response.text else {}


def get_user_orders(
    market_slug: str,
    session_cookie: str,
    api_base_url: str = API_BASE_URL,
) -> list:
    """
    Get user's orders for a specific market.

    Args:
        market_slug: Market slug identifier
        session_cookie: Authentication session cookie
        api_base_url: Base URL for the API

    Returns:
        List of user's orders

    Raises:
        Exception: If request fails
    """
    headers = get_auth_headers(session_cookie)

    response = requests.get(
        f"{api_base_url}/markets/{market_slug}/user-orders",
        headers=headers,
    )

    if response.status_code != 200:
        raise Exception(f"Failed to get orders: {response.status_code} - {response.text}")

    return response.json()


# Alias for the SDK's Client.get_user_orders method (~line 2802) which calls
# `_get_user_orders` (with leading underscore) as the module-private helper.
# The original SDK source pair probably had `_get_user_orders` as the private
# helper and the Client method as a public re-export; the inlining process
# flattened both names to `get_user_orders`. Adding the alias makes both
# names point to the same function so the Client method works AND any
# external caller of the public name keeps working.
_get_user_orders = get_user_orders


def cancel_all_orders(
    market_slug: str,
    session_cookie: str,
    api_base_url: str = API_BASE_URL,
) -> dict:
    """
    Cancel all orders in a specific market.

    Args:
        market_slug: Market slug identifier
        session_cookie: Authentication session cookie
        api_base_url: Base URL for the API

    Returns:
        API response

    Raises:
        Exception: If cancellation fails
    """
    headers = get_auth_headers(session_cookie)
    headers.pop("Content-Type")  # Remove Content-Type header to prevent API validation error

    response = requests.delete(
        f"{api_base_url}/orders/all/{market_slug}",
        headers=headers,
    )

    if response.status_code not in (200, 204):
        raise Exception(f"Cancel all failed {response.status_code}: {response.text}")

    return response.json() if response.text else {}


def cancel_orders_batch(
    order_ids: list[str],
    session_cookie: str,
    api_base_url: str = API_BASE_URL,
) -> dict:
    """
    Cancel multiple orders in a single batch operation.

    Args:
        order_ids: List of order IDs to cancel
        session_cookie: Authentication session cookie
        api_base_url: Base URL for the API

    Returns:
        API response with 'message', 'canceled', and 'failed' keys

    Raises:
        Exception: If batch cancellation fails
    """
    headers = get_auth_headers(session_cookie)

    response = requests.post(
        f"{api_base_url}/orders/cancel-batch",
        headers=headers,
        json={"orderIds": order_ids},
    )

    if response.status_code not in (200, 207):
        raise Exception(f"Cancel batch failed {response.status_code}: {response.text}")

    return response.json()


def calculate_trade_amounts(
    price_cents: int | float,
    amount: float,
    side: int = SIDE_BUY,
    scaling_factor: int = 1_000_000,
    order_type: str = "GTC",
) -> tuple[int, int]:
    """
    Calculate maker and taker amounts for a trade.

    Args:
        price_cents: Price in cents (e.g., 65 for 65¢). For FOK orders, this should
            be the current market price + slippage tolerance to ensure fill.
        amount: Number of shares
        side: 0 for BUY, 1 for SELL
        scaling_factor: Scaling factor (default 1e6 for USDC)
        order_type: "GTC" or "FOK"

    Returns:
        Tuple of (maker_amount, taker_amount)

    Note:
        For FOK (Fill or Kill) orders, takerAmount is set to 1 per Limitless API
        semantics. This signals "market order" behavior where makerAmount determines
        the maximum USDC to spend and the exchange fills as much as possible at the
        best available prices. The price_cents parameter is critical for FOK orders
        as it determines the makerAmount budget.
    """
    price_dollars = price_cents / 100
    total_cost = price_dollars * amount

    maker_amount = round(total_cost * scaling_factor)
    taker_amount = round(amount * scaling_factor)

    if side == 1:  # SELL
        maker_amount, taker_amount = taker_amount, maker_amount

    if order_type == "FOK":
        # FOK market order semantics per Limitless API docs:
        # takerAmount=1 signals "fill as much as possible with given makerAmount"
        # The makerAmount controls how much USDC is spent
        taker_amount = 1

    return maker_amount, taker_amount

# ──── from limitless_sdk/redemption.py ────
"""
Limitless Exchange Position Redemption

Supports two modes:
- EOA: Direct on-chain redemption (user pays gas)
- Smart Wallet: EIP-4337 Account Abstraction with gasless transactions
"""

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import requests
from eth_abi import encode
from eth_account import Account
from eth_utils import keccak
from web3 import Web3

# Import with version compatibility
try:
    from eth_account.messages import encode_typed_data
except ImportError:
    from eth_account.messages import encode_structured_data as encode_typed_data


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class RedeemablePosition:
    """A position that can be redeemed from a resolved market."""

    condition_id: str
    market_slug: str
    market_title: str
    winning_token: str  # "YES" or "NO"
    balance: Decimal  # Number of winning tokens (USDC value)


# =============================================================================
# Helper Functions
# =============================================================================


def get_redeemable_positions(portfolio_data: dict) -> list[RedeemablePosition]:
    """
    Extract redeemable positions from portfolio API response.

    Args:
        portfolio_data: Response from client.get_portfolio_positions()

    Returns:
        List of RedeemablePosition objects for resolved markets with winning tokens

    Example:
        ```python
        portfolio = client.get_portfolio_positions()
        redeemable = get_redeemable_positions(portfolio)
        for pos in redeemable:
            print(f"{pos.market_title}: {pos.balance} {pos.winning_token}")
        ```
    """
    redeemable = []

    for market_data in portfolio_data.get("clob", []):
        market = market_data.get("market", {})

        # Only process resolved markets
        if market.get("status") != "RESOLVED":
            continue

        condition_id = market.get("conditionId")
        winning_index = market.get("winningOutcomeIndex")

        if condition_id is None or winning_index is None:
            continue

        # Determine winning token type (0 = YES, 1 = NO)
        winning_token = "YES" if winning_index == 0 else "NO"
        token_key = winning_token.lower()

        # Check if user has winning tokens
        tokens_balance = market_data.get("tokensBalance", {})
        raw_balance = int(tokens_balance.get(token_key, 0) or 0)

        if raw_balance <= 0:
            continue

        # Scale balance from raw units (6 decimals for USDC)
        balance = Decimal(str(raw_balance)) / Decimal(str(10**USDC_DECIMALS))

        redeemable.append(
            RedeemablePosition(
                condition_id=condition_id,
                market_slug=market.get("slug", ""),
                market_title=market.get("title", ""),
                winning_token=winning_token,
                balance=balance,
            )
        )

    return redeemable


# =============================================================================
# EOA Position Redeemer
# =============================================================================


class EOAPositionRedeemer:
    """
    Handles direct on-chain position redemption for EOA wallets.

    Unlike the smart wallet redeemer, this requires the user to pay gas
    in ETH for the transaction.

    Example:
        ```python
        redeemer = EOAPositionRedeemer(
            private_key="0x...",
            rpc_url="https://mainnet.base.org"
        )
        tx_hash = redeemer.redeem_position(condition_id="0x...")
        receipt = redeemer.wait_for_receipt(tx_hash)
        ```
    """

    def __init__(
        self,
        private_key: str,
        rpc_url: str = "https://mainnet.base.org",
        ctf_address: str = BASE_CTF_ADDRESS,
        collateral_token: str = USDC_ADDRESS,
        chain_id: int = BASE_CHAIN_ID,
    ):
        """
        Initialize the EOA position redeemer.

        Args:
            private_key: EOA private key for signing transactions
            rpc_url: Base chain RPC URL
            ctf_address: Conditional Token Framework contract address
            collateral_token: Collateral token address (USDC)
            chain_id: Chain ID (8453 for Base)
        """
        self.private_key = format_private_key(private_key)
        self.ctf_address = Web3.to_checksum_address(ctf_address)
        self.collateral_token = Web3.to_checksum_address(collateral_token)
        self.chain_id = chain_id

        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.account = Account.from_key(self.private_key)
        self.address = self.account.address

        self.ctf_contract = self.w3.eth.contract(address=self.ctf_address, abi=CTF_ABI)

        # Track last used nonce to detect RPC sync delays
        self._last_used_nonce: int | None = None

    def _get_next_nonce(self, max_retries: int = 10, delay_ms: int = 200) -> int:
        """
        Get the next nonce, waiting for RPC to sync if needed.

        If the fetched nonce equals the last used nonce, the RPC hasn't
        synced yet. Wait and retry until we get a fresh nonce.
        """
        import time

        for attempt in range(max_retries):
            nonce = self.w3.eth.get_transaction_count(self.address, "pending")

            # First call or nonce has incremented - good to go
            if self._last_used_nonce is None or nonce > self._last_used_nonce:
                return nonce

            # Nonce hasn't incremented yet, RPC is stale - wait and retry
            time.sleep(delay_ms / 1000)

        # After max retries, force increment to avoid infinite loop
        return self._last_used_nonce + 1

    def redeem_position(
        self,
        condition_id: str,
        gas_limit: int = 150000,
        max_priority_fee_gwei: float = 0.01,
    ) -> str:
        """
        Redeem a resolved position by calling CTF.redeemPositions().

        Args:
            condition_id: Condition ID of the resolved market (32 bytes hex)
            gas_limit: Gas limit for the transaction
            max_priority_fee_gwei: Max priority fee in gwei

        Returns:
            Transaction hash

        Raises:
            ValueError: If condition_id is invalid
            Exception: If transaction fails
        """
        # Validate and format condition_id
        cid = condition_id.lower().replace("0x", "")
        if len(cid) != 64:
            raise ValueError("Condition ID must be 32 bytes (64 hex chars)")
        condition_id_bytes = bytes.fromhex(cid)

        # Parent collection ID is all zeros for root conditions
        parent_collection_id = bytes(32)

        # Index sets: 1 = YES (2^0), 2 = NO (2^1) - redeem both
        index_sets = [1, 2]

        # Fetch nonce, waiting for RPC to sync if needed
        nonce = self._get_next_nonce()

        # Get gas prices
        base_fee = self.w3.eth.get_block("latest")["baseFeePerGas"]
        max_priority_fee = self.w3.to_wei(max_priority_fee_gwei, "gwei")
        max_fee = base_fee * 2 + max_priority_fee

        tx = self.ctf_contract.functions.redeemPositions(
            self.collateral_token,
            parent_collection_id,
            condition_id_bytes,
            index_sets,
        ).build_transaction(
            {
                "from": self.address,
                "nonce": nonce,
                "gas": gas_limit,
                "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": max_priority_fee,
                "chainId": self.chain_id,
            }
        )

        # Sign and send
        signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)

        # Handle both old (rawTransaction) and new (raw_transaction) attribute names
        raw_tx = getattr(signed_tx, "raw_transaction", None) or signed_tx.rawTransaction
        tx_hash = self.w3.eth.send_raw_transaction(raw_tx)

        # Track nonce after successful send (caller should wait_for_receipt before next call)
        self._last_used_nonce = nonce

        # hexbytes 1.x dropped the '0x' prefix from .hex() — same fix pattern
        # we applied to the order signature path.  Without this, the BaseScan
        # link in the dashboard ends up as e.g. "abc123..." instead of
        # "0xabc123..." and the tx page 404s.
        hash_str = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)
        if not hash_str.startswith("0x"):
            hash_str = "0x" + hash_str
        return hash_str

    def wait_for_receipt(self, tx_hash: str, timeout: int = 120) -> dict:
        """
        Wait for transaction receipt.

        Args:
            tx_hash: Transaction hash
            timeout: Timeout in seconds

        Returns:
            Transaction receipt

        Raises:
            TimeoutError: If transaction not mined within timeout
        """
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
        return dict(receipt)

    def estimate_gas(self, condition_id: str) -> int:
        """
        Estimate gas for redemption transaction.

        Args:
            condition_id: Condition ID of the resolved market

        Returns:
            Estimated gas units
        """
        cid = condition_id.lower().replace("0x", "")
        condition_id_bytes = bytes.fromhex(cid)
        parent_collection_id = bytes(32)
        index_sets = [1, 2]

        return self.ctf_contract.functions.redeemPositions(
            self.collateral_token,
            parent_collection_id,
            condition_id_bytes,
            index_sets,
        ).estimate_gas({"from": self.address})

    def get_eth_balance(self) -> Decimal:
        """Get ETH balance for gas estimation display."""
        balance_wei = self.w3.eth.get_balance(self.address)
        return Decimal(str(balance_wei)) / Decimal("1e18")


# Dummy signature for gas estimation
DUMMY_SIGNATURE = (
    "0x000000000000000000000000fffffffffffffffffffffffffffffff0"
    "000000000000000000000000000000007aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1c"
)


class PositionRedeemer:
    """
    Handles EIP-4337 UserOperation flow for redeeming positions.

    Supports claiming resolved market positions via Pimlico paymaster,
    enabling gasless transactions.

    Example:
        ```python
        redeemer = PositionRedeemer(
            private_key="0x...",
            smart_wallet="0x...",
            pimlico_api_key="pim_..."
        )
        result = redeemer.redeem_position(condition_id="0x...")
        ```
    """

    def __init__(
        self,
        private_key: str,
        smart_wallet: str,
        pimlico_api_key: str,
        rpc_url: str = "https://base-mainnet.infura.io/v3/9aadf67222e842aba70a6238829e66cc",
        entry_point: str = ENTRY_POINT_V06,
        safe_module: str = SAFE_4337_MODULE_V06,
        chain_id: int = BASE_CHAIN_ID,
    ):
        """
        Initialize the position redeemer.

        Args:
            private_key: Private key for signing UserOperations
            smart_wallet: Smart wallet address (sender)
            pimlico_api_key: Pimlico API key for bundler/paymaster
            rpc_url: Base chain RPC URL
            entry_point: EntryPoint contract address
            safe_module: Safe 4337 module address
            chain_id: Chain ID (8453 for Base)
        """
        self.private_key = format_private_key(private_key)
        self.smart_wallet = smart_wallet
        self.pimlico_api_key = pimlico_api_key
        self.entry_point = entry_point
        self.safe_module = safe_module
        self.chain_id = chain_id

        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.bundler_url = f"https://api.pimlico.io/v2/{chain_id}/rpc?apikey={pimlico_api_key}"

        self.entry_point_contract = self.w3.eth.contract(address=entry_point, abi=ENTRY_POINT_ABI)

    def _rpc_call(self, method: str, params: list, id_: int = 1) -> dict:
        """Make a JSON-RPC call to the bundler."""
        payload = {
            "jsonrpc": "2.0",
            "id": id_,
            "method": method,
            "params": params,
        }
        resp = requests.post(self.bundler_url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"RPC error in {method}: {data['error']}")
        return data["result"]

    def _get_nonce(self) -> str:
        """Get current nonce for the smart wallet."""
        nonce_key = int(time.time() * 1000)
        nonce_int = self.entry_point_contract.functions.getNonce(
            self.smart_wallet, nonce_key
        ).call()
        return hex(nonce_int)

    def _make_call_data(self, condition_id: str) -> str:
        """
        Build callData for redeeming a position.

        Args:
            condition_id: Condition ID of the resolved market (32 bytes hex)

        Returns:
            Encoded callData for the redemption transaction
        """
        cid = condition_id.lower().replace("0x", "")
        if len(cid) != 64:
            raise ValueError("Condition ID must be 32 bytes (64 hex chars)")

        # Static parts of the callData template
        prefix = (
            "0x541d63c8000000000000000000000000c9c98965297bc527861c898329ee280632b76e18"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000080"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000e4"
            "01b7037c000000000000000000000000833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            "0000000000000000000000000000000000000000000000000000000000000000"
        )

        suffix = (
            "0000000000000000000000000000000000000000000000000000000000000080"
            "0000000000000000000000000000000000000000000000000000000000000002"
            "0000000000000000000000000000000000000000000000000000000000000001"
            "0000000000000000000000000000000000000000000000000000000000000002"
            "0000000000000000000000000000000000000000000000000000000000000000"
        )

        return prefix + cid + suffix

    def _build_user_op(
        self,
        condition_id: str,
        max_fee_per_gas: str,
        max_priority_fee_per_gas: str,
    ) -> dict:
        """Build base UserOperation."""
        return {
            "callData": self._make_call_data(condition_id),
            "callGasLimit": "0x0",
            "initCode": "0x",
            "maxFeePerGas": max_fee_per_gas,
            "maxPriorityFeePerGas": max_priority_fee_per_gas,
            "nonce": self._get_nonce(),
            "paymasterAndData": "0x",
            "preVerificationGas": "0x0",
            "sender": self.smart_wallet,
            "signature": DUMMY_SIGNATURE,
            "verificationGasLimit": "0x0",
        }

    def _sign_user_op(self, user_op: dict) -> str:
        """
        Generate EIP-712 signature for Safe 4337 account.

        Format: validAfter(6) + validUntil(6) + r(32) + s(32) + v(1)
        """
        valid_after = 0
        valid_until = 0

        # EIP-712 domain for Safe module
        domain = {"chainId": self.chain_id, "verifyingContract": self.safe_module}

        # Message to sign
        message = {
            "safe": user_op["sender"],
            "nonce": int(user_op["nonce"], 16),
            "initCode": bytes.fromhex(user_op["initCode"][2:])
            if user_op["initCode"] != "0x"
            else b"",
            "callData": bytes.fromhex(user_op["callData"][2:]),
            "callGasLimit": int(user_op["callGasLimit"], 16),
            "verificationGasLimit": int(user_op["verificationGasLimit"], 16),
            "preVerificationGas": int(user_op["preVerificationGas"], 16),
            "maxFeePerGas": int(user_op["maxFeePerGas"], 16),
            "maxPriorityFeePerGas": int(user_op["maxPriorityFeePerGas"], 16),
            "paymasterAndData": bytes.fromhex(user_op["paymasterAndData"][2:])
            if user_op["paymasterAndData"] != "0x"
            else b"",
            "validAfter": valid_after,
            "validUntil": valid_until,
            "entryPoint": self.entry_point,
        }

        # Create EIP-712 structured data
        structured_data = {
            "types": SAFE_OP_TYPES,
            "primaryType": "SafeOp",
            "domain": domain,
            "message": message,
        }

        # Sign
        account = Account.from_key(self.private_key)
        signable_message = encode_typed_data(full_message=structured_data)
        signature = account.sign_message(signable_message)

        # Pack: validAfter(6) + validUntil(6) + r(32) + s(32) + v(1)
        packed_sig = (
            valid_after.to_bytes(6, "big")
            + valid_until.to_bytes(6, "big")
            + signature.r.to_bytes(32, "big")
            + signature.s.to_bytes(32, "big")
            + signature.v.to_bytes(1, "big")
        )

        return "0x" + packed_sig.hex()

    def redeem_position(
        self,
        condition_id: str,
        gas_token: str = GAS_TOKEN,
    ) -> str:
        """
        Redeem a resolved position using EIP-4337 UserOperation.

        Args:
            condition_id: Condition ID of the resolved market
            gas_token: Gas token for paymaster (default: native token)

        Returns:
            UserOperation hash

        Raises:
            RuntimeError: If any step in the flow fails
        """
        # 1. Get gas prices
        gas_prices = self._rpc_call("pimlico_getUserOperationGasPrice", [])
        fast = gas_prices["fast"]
        max_fee = fast["maxFeePerGas"]
        max_priority = fast["maxPriorityFeePerGas"]

        # 2. Build base UserOp
        user_op = self._build_user_op(condition_id, max_fee, max_priority)

        # 3. Get paymaster stub data
        stub_result = self._rpc_call(
            "pm_getPaymasterStubData", [user_op, self.entry_point, gas_token, None]
        )
        user_op["paymasterAndData"] = stub_result["paymasterAndData"]

        # 4. Estimate gas
        gas_estimate = self._rpc_call("eth_estimateUserOperationGas", [user_op, self.entry_point])
        user_op["callGasLimit"] = gas_estimate["callGasLimit"]
        user_op["verificationGasLimit"] = gas_estimate["verificationGasLimit"]
        user_op["preVerificationGas"] = gas_estimate["preVerificationGas"]

        # 5. Get final paymaster data
        paymaster_result = self._rpc_call(
            "pm_getPaymasterData", [user_op, self.entry_point, gas_token, None]
        )
        user_op["paymasterAndData"] = paymaster_result["paymasterAndData"]

        # 6. Sign the UserOp
        user_op["signature"] = self._sign_user_op(user_op)

        # 7. Send UserOperation
        result = self._rpc_call("eth_sendUserOperation", [user_op, self.entry_point])

        return result

    def get_user_op_receipt(self, user_op_hash: str) -> Optional[dict]:
        """
        Get receipt for a UserOperation.

        Args:
            user_op_hash: UserOperation hash from redeem_position

        Returns:
            Receipt dict or None if not found/pending
        """
        try:
            return self._rpc_call("eth_getUserOperationReceipt", [user_op_hash])
        except RuntimeError:
            return None

# ──── from limitless_sdk/client.py ────
"""
Limitless Exchange Client
Main client class for trading on Limitless Exchange
"""

from typing import Literal, Optional

import requests
from eth_account import Account
from web3 import Web3


# =============================================================================
# Web3 Instance Cache
# =============================================================================

# Module-level cache for Web3 instances by RPC URL
_web3_instances: dict[str, Web3] = {}


def _get_web3(rpc_url: str) -> Web3:
    """Get or create a cached Web3 instance for the given RPC URL.

    This avoids creating new HTTP connections for every RPC call,
    improving performance for repeated blockchain interactions.

    Args:
        rpc_url: The RPC URL to connect to.

    Returns:
        Web3 instance connected to the RPC URL.
    """
    if rpc_url not in _web3_instances:
        _web3_instances[rpc_url] = Web3(Web3.HTTPProvider(rpc_url))
    return _web3_instances[rpc_url]


class Limitless:
    """
    Main client for interacting with Limitless Exchange.

    Provides methods for authentication, trading, and market data.
    Exposes session credentials for custom API requests.
    Supports both EOA (Externally Owned Account) and Smart Wallet modes.

    Example:
        ```python
        pass  # limitless_sdk inlined above

        # EOA Mode (simple, single key) - default
        client = Limitless(private_key="0x...", wallet_type="eoa")
        client.authenticate()
        client.buy(market_slug="...", token_id="...", price_cents=50, amount=2)

        # Smart Wallet Mode (separate auth and signing keys)
        client = Limitless(
            private_key="0x...",  # auth key
            signing_wallet_pk="0x...",  # signing key
            wallet_type="smart_wallet"
        )
        client.authenticate()
        client.buy(market_slug="...", token_id="...", price_cents=50, amount=2)

        # Make custom requests using exposed credentials
        headers = client.get_headers()
        response = requests.get("https://api.limitless.exchange/custom", headers=headers)
        ```

    Attributes:
        session_cookie: Authentication session cookie (available after authenticate())
        user_data: User account data from authentication
        account: eth_account.Account instance
        wallet_type: "eoa" or "smart_wallet"
    """

    def __init__(
        self,
        private_key: str,
        signing_wallet_pk: Optional[str] = None,
        wallet_type: WalletType = "eoa",
        api_base_url: str = API_BASE_URL,
        referral_code: Optional[str] = None,
    ):
        """
        Initialize the Limitless client.

        Args:
            private_key: Primary private key (used for auth in both modes, and signing in EOA mode)
            signing_wallet_pk: Signing wallet private key (required for smart_wallet mode)
            wallet_type: "eoa" (default) or "smart_wallet"
            api_base_url: Base URL for the API
            referral_code: Optional referral code for authentication

        Raises:
            ValueError: If wallet_type is "smart_wallet" but signing_wallet_pk is not provided
        """
        self.wallet_type = wallet_type
        self._private_key = format_private_key(private_key)
        self.api_base_url = api_base_url
        self.account = Account.from_key(self._private_key)

        # Handle signing key based on wallet type
        if wallet_type == "smart_wallet":
            if signing_wallet_pk is None:
                raise ValueError("signing_wallet_pk is required for smart_wallet mode")
            self._signing_key = format_private_key(signing_wallet_pk)
        else:
            # EOA mode: use the same key for signing
            self._signing_key = self._private_key

        # Set after authentication
        self.session_cookie: Optional[str] = None
        self.user_data: Optional[dict] = None
        self._referral_code = referral_code

        # HTTP session for connection pooling
        self._session = requests.Session()

        # Venue exchange cache (internal use only for get_venue_exchange)
        # LRU cache with max 100 entries to prevent unbounded growth
        self._venue_exchange_cache: LRUCache = LRUCache(maxsize=100)

    @property
    def address(self) -> str:
        """Get the wallet address."""
        return self.account.address

    @property
    def smart_wallet(self) -> Optional[str]:
        """Get the smart wallet address (available after authentication)."""
        if self.user_data:
            return self.user_data.get("smartWallet")
        return None

    @property
    def embedded_account(self) -> Optional[str]:
        """Get the embedded account address (signer, available after authentication)."""
        if self.user_data:
            return self.user_data.get("embeddedAccount")
        return None

    @property
    def user_id(self) -> Optional[str]:
        """Get the user ID (available after authentication)."""
        if self.user_data:
            return self.user_data.get("id")
        return None

    @property
    def fee_rate_bps(self) -> int:
        """Get the user's fee rate in basis points."""
        if self.user_data:
            rank = self.user_data.get("rank", {})
            return rank.get("feeRateBps", 0)
        return 0

    @property
    def maker_address(self) -> Optional[str]:
        """
        Get the maker address for orders.

        For EOA mode: returns the EOA wallet address.
        For smart_wallet mode: returns the smart wallet address.

        Returns:
            Maker address or None if not authenticated (smart_wallet mode)
        """
        if self.wallet_type == "eoa":
            return self.address
        return self.smart_wallet

    @property
    def signer_address(self) -> Optional[str]:
        """
        Get the signer address for orders.

        For EOA mode: returns the EOA wallet address (same as maker).
        For smart_wallet mode: returns the embedded account address.

        Returns:
            Signer address or None if not authenticated (smart_wallet mode)
        """
        if self.wallet_type == "eoa":
            return self.address
        return self.embedded_account

    @property
    def signature_type(self) -> int:
        """
        Get the signature type for orders.

        Returns:
            0 for EOA mode, 2 for smart_wallet mode
        """
        if self.wallet_type == "eoa":
            return SIGNATURE_TYPE_EOA
        return SIGNATURE_TYPE_EIP712

    @property
    def is_authenticated(self) -> bool:
        """Check if the client is authenticated."""
        return self.session_cookie is not None

    @property
    def trade_wallet_option(self) -> Optional[str]:
        """
        Get the server-side tradeWalletOption (available after authentication).

        Returns:
            "eoa" or "smartWallet", or None if not authenticated
        """
        if self.user_data:
            return self.user_data.get("tradeWalletOption")
        return None

    def authenticate(self) -> dict:
        """
        Authenticate with the Limitless Exchange API.

        Validates that the configured wallet_type matches the user's tradeWalletOption.
        If they don't match, automatically updates the tradeWalletOption on the server.

        Returns:
            User data dictionary

        Raises:
            Exception: If authentication fails
        """
        signing_message = get_signing_message(self.api_base_url)
        self.session_cookie, self.user_data = authenticate(
            self._private_key,
            signing_message,
            self.api_base_url,
            referral_code=self._referral_code,
        )

        # Validate and sync wallet type with server's tradeWalletOption
        self._sync_trade_wallet_option()

        return self.user_data

    def _get_expected_trade_wallet_option(self) -> str:
        """Map client wallet_type to API tradeWalletOption value."""
        if self.wallet_type == "eoa":
            return "eoa"
        return "smartWallet"

    def _sync_trade_wallet_option(self) -> None:
        """
        Sync the client's wallet_type with the server's tradeWalletOption.

        If they don't match, updates the server to match the client configuration.
        """
        if not self.user_data:
            return

        current_option = self.user_data.get("tradeWalletOption")
        expected_option = self._get_expected_trade_wallet_option()

        if current_option == expected_option:
            return  # Already in sync

        # Update the trade wallet option on the server
        self._update_trade_wallet_option(expected_option)

    def _update_trade_wallet_option(self, trade_wallet_option: str) -> None:
        """
        Update the tradeWalletOption on the server.

        Args:
            trade_wallet_option: "eoa" or "smartWallet"

        Raises:
            Exception: If the update fails
        """
        # Determine the display name based on wallet option
        if trade_wallet_option == "eoa":
            display_name = self.address
        else:
            display_name = self.smart_wallet

        if not display_name:
            raise RuntimeError(
                f"Cannot update to {trade_wallet_option}: required address not available"
            )

        headers = get_auth_headers(self.session_cookie)

        payload = {
            "tradeWalletOption": trade_wallet_option,
            "displayName": display_name,
        }

        response = self._session.put(
            f"{self.api_base_url}/profiles",
            headers=headers,
            json=payload,
        )

        if response.status_code != 200:
            raise Exception(
                f"Failed to update tradeWalletOption: {response.status_code} - {response.text}"
            )

        # Update local user_data with the new values
        self.user_data["tradeWalletOption"] = trade_wallet_option
        self.user_data["displayName"] = display_name

    def get_headers(self) -> dict:
        """
        Get headers for authenticated API requests.

        Use this for making custom API calls.

        Returns:
            Headers dict with authentication cookie

        Raises:
            RuntimeError: If not authenticated
        """
        if not self.session_cookie:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        return get_auth_headers(self.session_cookie)

    def request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Make an authenticated HTTP request to the API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            endpoint: API endpoint (e.g., "/orders" or full URL)
            **kwargs: Additional arguments passed to requests

        Returns:
            Response object

        Example:
            ```python
            # Get market data
            response = client.request("GET", "/markets/my-market-slug")
            data = response.json()

            # Custom POST request
            response = client.request("POST", "/some-endpoint", json={"key": "value"})
            ```
        """
        headers = self.get_headers()

        # Merge with any provided headers
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        # Build full URL if endpoint doesn't start with http
        if not endpoint.startswith("http"):
            url = f"{self.api_base_url}{endpoint}"
        else:
            url = endpoint

        return self._session.request(method, url, headers=headers, **kwargs)

    def get_venue_exchange(self, market_slug: str) -> str:
        """
        Get the venue exchange address for a market.

        This address is used as the verifyingContract for EIP-712 signing.
        Market data is cached since venue is static per market.

        Args:
            market_slug: Market slug identifier

        Returns:
            Venue exchange address (checksummed)

        Raises:
            ValueError: If market does not have venue data
        """
        # Check cache first (internal use only)
        market_data = self._venue_exchange_cache.get(market_slug)
        if market_data is None:
            # Fetch and cache
            market_data = self.get_market(market_slug)
            self._venue_exchange_cache[market_slug] = market_data

        venue = market_data.get("venue")
        if not venue or not venue.get("exchange"):
            raise ValueError(f"Market {market_slug} does not have venue data")

        return venue["exchange"]

    def get_ctf_address(self, market_slug: str | None = None) -> str:
        """
        Get the CTF (Conditional Token Framework) contract address.

        This is the global ERC-1155 contract that holds all conditional tokens
        (YES/NO position tokens) for all Limitless markets on Base.
        Must be approved before selling positions.

        Args:
            market_slug: Unused - kept for API compatibility. CTF is global.

        Returns:
            CTF contract address (checksummed)
        """
        return BASE_CTF_ADDRESS

    def execute_trade(
        self,
        market_slug: str,
        token_id: str,
        price_cents: int | float,
        amount: float,
        side: Literal["BUY", "SELL"] | int = "BUY",
        token_type: Literal["YES", "NO"] = "YES",
        order_type: str = ORDER_TYPE_GTC,
    ) -> dict:
        """
        Execute a trade on Limitless Exchange.

        Automatically fetches venue exchange address from market data for signing.

        Args:
            market_slug: Market slug identifier
            token_id: Token ID to trade (YES or NO token)
            price_cents: Price in cents (e.g., 65 for 65¢)
            amount: Number of shares
            side: "BUY" or "SELL" (or 0/1)
            token_type: "YES" or "NO" (for logging)
            order_type: "GTC" (Good Till Cancelled) or "FOK" (Fill Or Kill)

        Returns:
            Order result from API

        Raises:
            RuntimeError: If not authenticated
            Exception: If trade execution fails
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        # Get venue exchange address for signing
        venue_exchange = self.get_venue_exchange(market_slug)

        # Convert side string to int
        if isinstance(side, str):
            side_int = SIDE_BUY if side.upper() == "BUY" else SIDE_SELL
        else:
            side_int = side

        # Calculate amounts
        maker_amount, taker_amount = calculate_trade_amounts(
            price_cents, amount, side_int, SCALING_FACTOR, order_type
        )

        # Create order payload with wallet-type-aware addresses and signature type
        order_payload = create_order_payload(
            maker_address=self.maker_address,
            signer_address=self.signer_address,
            token_id=token_id,
            maker_amount=maker_amount,
            taker_amount=taker_amount,
            fee_rate_bps=self.fee_rate_bps,
            side=side_int,
            signature_type=self.signature_type,
        )

        # Sign the order with the venue's exchange address
        signature = sign_order(order_payload, self._signing_key, venue_exchange)

        # Submit to API
        price_dollars = round(price_cents / 100, 3)
        result = submit_order(
            order_payload=order_payload,
            signature=signature,
            owner_id=self.user_id,
            market_slug=market_slug,
            price=price_dollars,
            order_type=order_type,
            session_cookie=self.session_cookie,
            api_base_url=self.api_base_url,
        )

        return result

    def buy(
        self,
        market_slug: str,
        token_id: str,
        price_cents: int | float,
        amount: float,
        token_type: Literal["YES", "NO"] = "YES",
        order_type: str = ORDER_TYPE_GTC,
    ) -> dict:
        """
        Place a buy order.

        Convenience method for execute_trade with side="BUY".

        Args:
            market_slug: Market slug identifier
            token_id: Token ID to buy
            price_cents: Price in cents
            amount: Number of shares
            token_type: "YES" or "NO"
            order_type: "GTC" or "FOK"

        Returns:
            Order result from API
        """
        return self.execute_trade(
            market_slug=market_slug,
            token_id=token_id,
            price_cents=price_cents,
            amount=amount,
            side="BUY",
            token_type=token_type,
            order_type=order_type,
        )

    def sell(
        self,
        market_slug: str,
        token_id: str,
        price_cents: int | float,
        amount: float,
        token_type: Literal["YES", "NO"] = "YES",
        order_type: str = ORDER_TYPE_GTC,
    ) -> dict:
        """
        Place a sell order.

        Convenience method for execute_trade with side="SELL".

        Args:
            market_slug: Market slug identifier
            token_id: Token ID to sell
            price_cents: Price in cents
            amount: Number of shares
            token_type: "YES" or "NO"
            order_type: "GTC" or "FOK"

        Returns:
            Order result from API
        """
        return self.execute_trade(
            market_slug=market_slug,
            token_id=token_id,
            price_cents=price_cents,
            amount=amount,
            side="SELL",
            token_type=token_type,
            order_type=order_type,
        )

    def cancel_order(self, order_id: str) -> dict:
        """
        Cancel an existing order.

        Args:
            order_id: UUID of the order to cancel

        Returns:
            API response

        Raises:
            RuntimeError: If not authenticated
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        return _cancel_order(
            order_id=order_id,
            session_cookie=self.session_cookie,
            api_base_url=self.api_base_url,
        )

    def cancel_all_orders(self, market_slug: str) -> dict:
        """
        Cancel all orders in a specific market.

        Args:
            market_slug: Market slug identifier

        Returns:
            API response

        Raises:
            RuntimeError: If not authenticated
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        return _cancel_all_orders(
            market_slug=market_slug,
            session_cookie=self.session_cookie,
            api_base_url=self.api_base_url,
        )

    def cancel_orders_batch(self, order_ids: list[str]) -> dict:
        """
        Cancel multiple orders in a single batch request.

        NOTE: All orders must be from the same market. Use cancel_all_user_orders()
        for canceling orders across multiple markets.

        Args:
            order_ids: List of order IDs to cancel (must all be from the same market)

        Returns:
            Dict with:
                - message: Success message
                - canceled: List of successfully canceled order IDs
                - failed: List of failed cancellations with reasons

        Raises:
            RuntimeError: If not authenticated
            requests.HTTPError: If the API request fails (e.g., orders from multiple markets)
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        if not order_ids:
            return {"message": "No orders to cancel", "canceled": [], "failed": []}

        response = self.request("POST", "/orders/cancel-batch", json={"orderIds": order_ids})

        # Raise exception on HTTP errors (4xx, 5xx)
        response.raise_for_status()

        return response.json()

    def _get_market_cached(self, market_slug: str) -> dict:
        """Get market data, using cache if available.

        Args:
            market_slug: Market slug to fetch.

        Returns:
            Market data dictionary.
        """
        if market_slug not in self._venue_exchange_cache:
            self._venue_exchange_cache[market_slug] = self.get_market(market_slug)
        return self._venue_exchange_cache[market_slug]

    def get_user_orders_smart(self, market_slugs: list[str] = None) -> list[dict]:
        """
        Get user orders using fast/slow path strategy.

        Combines two query strategies:
        1. Fast per-market queries for specified markets (authoritative)
        2. Portfolio query for comprehensive coverage (only for non-specified markets)

        For any market queried via fast path, those results are authoritative.
        If fast path returns no orders for a specified market, slow path orders
        for that market are ignored (they're likely stale).

        Args:
            market_slugs: Optional list of market slugs to query directly (fast path).
                         If None, only slow path (portfolio) is used.

        Returns:
            Deduplicated list of order dictionaries with market_slug included.
        """
        all_orders = {}  # order_id -> order dict (for deduplication)
        fast_path_markets = set()

        # 1. Fast path: Query specific markets directly (authoritative)
        if market_slugs:
            for market_slug in market_slugs:
                fast_path_markets.add(market_slug)
                try:
                    market_orders = self.get_user_orders(market_slug)

                    for order in market_orders:
                        # Only include live orders
                        if order.get("status", "").upper() == "LIVE":
                            all_orders[order["id"]] = {
                                **order,
                                "market_slug": market_slug,
                            }
                except Exception:
                    # If market query fails, continue (fallback to portfolio)
                    pass

        # 2. Slow path: Portfolio query to catch any orders missed by fast path
        # Always run this to find orders in markets not in the provided list
        portfolio_orders = self._get_orders_from_portfolio()
        for order in portfolio_orders:
            market_slug = order.get("market_slug", "")
            # Skip orders from markets already queried via fast path
            if market_slug in fast_path_markets:
                continue
            # Add orders from markets not covered by fast path
            if order["id"] not in all_orders:
                all_orders[order["id"]] = order

        return list(all_orders.values())

    def _get_orders_from_portfolio(self) -> list[dict]:
        """Extract orders from portfolio positions (slow path)."""
        portfolio = self.get_portfolio_positions()
        clob_positions = portfolio.get("clob", [])

        orders = []
        for position in clob_positions:
            market = position.get("market", {})
            market_slug = market.get("slug", "")

            orders_data = position.get("orders", {})
            live_orders = orders_data.get("liveOrders", [])

            for order in live_orders:
                # Normalize order format to match get_user_orders
                normalized_order = {
                    "id": order.get("id", ""),
                    "market_slug": market_slug,
                    "side": order.get("side", "buy").lower(),
                    "token": order.get("token", ""),
                    "price": order.get("price", ""),
                    "originalSize": order.get("originalSize", ""),
                    "filledSize": order.get("filledSize", ""),
                    "status": order.get("status", ""),
                }
                orders.append(normalized_order)

        return orders

    def cancel_all_user_orders(self, market_slugs: list[str] = None) -> dict:
        """
        Cancel all user orders using smart fast/slow path logic with batch cancellation.

        Groups orders by market and cancels each market's orders in a separate batch
        request (API requires all orders in a batch to be from the same market).

        Args:
            market_slugs: Optional list of market slugs to prioritize (fast path).
                         If provided, these markets are queried directly first.

        Returns:
            Dict with summary of canceled and failed orders:
                - markets_processed: Number of markets that had orders
                - total_canceled: List of successfully canceled order IDs
                - total_failed: List of failed cancellations with reasons
                - market_results: Per-market breakdown

        Raises:
            RuntimeError: If not authenticated
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        # Get most up-to-date orders using fast/slow path logic
        all_orders = self.get_user_orders_smart(market_slugs)

        if not all_orders:
            return {
                "markets_processed": 0,
                "total_canceled": [],
                "total_failed": [],
                "market_results": [],
            }

        # Group orders by market (API requires all orders in batch to be from same market)
        orders_by_market: dict[str, list[dict]] = {}
        for order in all_orders:
            market_slug = order.get("market_slug", "")
            if market_slug not in orders_by_market:
                orders_by_market[market_slug] = []
            orders_by_market[market_slug].append(order)

        # Cancel orders market by market
        total_canceled: list[str] = []
        total_failed: list[dict] = []
        market_results: list[dict] = []

        for market_slug, orders in orders_by_market.items():
            order_ids = [order["id"] for order in orders]
            market_canceled: list[str] = []
            market_failed: list[dict] = []

            try:
                batch_result = self.cancel_orders_batch(order_ids)

                # Extract canceled/failed from API result
                canceled = batch_result.get("canceled", [])
                failed = batch_result.get("failed", [])

                # If API doesn't provide lists, assume success (request didn't raise)
                if not canceled and not failed:
                    canceled = order_ids

                market_canceled = canceled
                market_failed = failed

            except Exception as e:
                # Batch request failed - mark all orders as failed
                for order_id in order_ids:
                    market_failed.append(
                        {
                            "orderId": order_id,
                            "reason": "BATCH_ERROR",
                            "message": str(e),
                        }
                    )

            total_canceled.extend(market_canceled)
            total_failed.extend(market_failed)
            market_results.append(
                {
                    "market_slug": market_slug,
                    "orders_count": len(orders),
                    "result": {
                        "canceled": market_canceled,
                        "failed": market_failed,
                    },
                }
            )

        return {
            "markets_processed": len(orders_by_market),
            "total_canceled": total_canceled,
            "total_failed": total_failed,
            "market_results": market_results,
        }

    def get_user_orders(self, market_slug: str) -> list:
        """
        Get user's orders for a specific market.

        Args:
            market_slug: Market slug identifier

        Returns:
            List of user's orders

        Raises:
            RuntimeError: If not authenticated
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        return _get_user_orders(
            market_slug=market_slug,
            session_cookie=self.session_cookie,
            api_base_url=self.api_base_url,
        )

    def get_portfolio_positions(self) -> dict:
        """
        Get user's portfolio positions.

        Returns:
            Portfolio positions data

        Raises:
            RuntimeError: If not authenticated
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        response = self.request("GET", "/portfolio/positions")
        return response.json()

    # Market Data Methods (no authentication required)

    def get_market(self, market_slug: str) -> dict:
        """
        Get market details by slug.

        Args:
            market_slug: Market slug identifier

        Returns:
            Market data dictionary
        """
        response = self._session.get(f"{self.api_base_url}/markets/{market_slug}")
        response.raise_for_status()
        return response.json()

    def get_orderbook(self, market_slug: str) -> dict:
        """
        Get orderbook data for a market.

        Args:
            market_slug: Market slug identifier

        Returns:
            Orderbook data dictionary
        """
        response = self._session.get(f"{self.api_base_url}/markets/{market_slug}/orderbook")
        response.raise_for_status()
        return response.json()

    def get_active_markets(
        self,
        category_id: int,
        page: int = 1,
        limit: int = 10,
        sort_by: str = "newest",
    ) -> dict:
        """
        Get active markets in a category.

        Args:
            category_id: Category ID (see CATEGORY_IDS constant)
            page: Page number
            limit: Results per page
            sort_by: Sort order

        Returns:
            Dictionary with "data" list of markets
        """
        response = self._session.get(
            f"{self.api_base_url}/markets/active/{category_id}",
            params={
                "page": str(page),
                "limit": str(limit),
                "sortBy": sort_by,
            },
        )
        response.raise_for_status()
        return response.json()

    def get_categories(self) -> dict:
        """
        Get market categories with counts.

        Returns:
            Categories data
        """
        response = self._session.get(f"{self.api_base_url}/markets/categories/count")
        response.raise_for_status()
        return response.json()

    @staticmethod
    def get_category_name(category_id: int) -> str:
        """
        Get human-readable category name.

        Args:
            category_id: Category ID

        Returns:
            Category name or "Unknown"
        """
        return CATEGORY_IDS.get(category_id, "Unknown")

    # USDC Approval Methods (for EOA mode)

    def check_usdc_allowance_for_market(
        self,
        market_slug: str,
        rpc_url: str = "https://mainnet.base.org",
    ) -> int:
        """
        Check USDC allowance for a market's venue exchange.

        Args:
            market_slug: Market slug identifier
            rpc_url: Base RPC URL

        Returns:
            Current allowance in raw units (6 decimals)
        """
        venue_exchange = self.get_venue_exchange(market_slug)
        w3 = _get_web3(rpc_url)
        return check_usdc_allowance(w3, self.address, venue_exchange)

    def approve_usdc_for_market(
        self,
        market_slug: str,
        rpc_url: str = "https://mainnet.base.org",
        amount: Optional[int] = None,
    ) -> str:
        """
        Approve USDC spending for a market's venue exchange.

        Args:
            market_slug: Market slug identifier
            rpc_url: Base RPC URL
            amount: Amount to approve (default: unlimited)

        Returns:
            Transaction hash
        """
        venue_exchange = self.get_venue_exchange(market_slug)
        w3 = _get_web3(rpc_url)

        if amount is not None:
            return approve_usdc(w3, self._private_key, venue_exchange, amount)
        return approve_usdc(w3, self._private_key, venue_exchange)

    def ensure_usdc_approved_for_market(
        self,
        market_slug: str,
        rpc_url: str = "https://mainnet.base.org",
        min_amount: int = 0,
    ) -> dict:
        """
        Check and approve USDC for a market if needed.

        Args:
            market_slug: Market slug identifier
            rpc_url: Base RPC URL
            min_amount: Minimum required allowance (0 for any)

        Returns:
            Dict with:
                - already_approved: bool
                - tx_hash: str | None
                - allowance: int
        """
        venue_exchange = self.get_venue_exchange(market_slug)
        w3 = _get_web3(rpc_url)
        return ensure_usdc_approved(w3, self._private_key, venue_exchange, min_amount)

    def get_usdc_balance(self, rpc_url: str = "https://mainnet.base.org") -> int:
        """
        Get USDC balance for this wallet.

        Args:
            rpc_url: Base RPC URL

        Returns:
            USDC balance in raw units (6 decimals)
        """
        w3 = _get_web3(rpc_url)
        return get_usdc_balance(w3, self.address)

    # CTF (Conditional Token) Approval Methods (for selling)

    def check_ctf_approval_for_market(
        self,
        market_slug: str,
        rpc_url: str = "https://mainnet.base.org",
    ) -> bool:
        """
        Check if CTF tokens are approved for a market's venue exchange.

        This approval is required before selling positions.

        Args:
            market_slug: Market slug identifier
            rpc_url: Base RPC URL

        Returns:
            True if approved, False otherwise
        """
        ctf_address = self.get_ctf_address(market_slug)
        venue_exchange = self.get_venue_exchange(market_slug)
        w3 = _get_web3(rpc_url)
        return check_ctf_approval(w3, ctf_address, self.address, venue_exchange)

    def approve_ctf_for_market(
        self,
        market_slug: str,
        rpc_url: str = "https://mainnet.base.org",
        approved: bool = True,
    ) -> str:
        """
        Approve CTF token transfers for a market's venue exchange.

        This is required before selling positions.

        Args:
            market_slug: Market slug identifier
            rpc_url: Base RPC URL
            approved: Whether to approve (True) or revoke (False)

        Returns:
            Transaction hash
        """
        ctf_address = self.get_ctf_address(market_slug)
        venue_exchange = self.get_venue_exchange(market_slug)
        w3 = _get_web3(rpc_url)
        return approve_ctf(w3, self._private_key, ctf_address, venue_exchange, approved)

    def ensure_ctf_approved_for_market(
        self,
        market_slug: str,
        rpc_url: str = "https://mainnet.base.org",
    ) -> dict:
        """
        Check and approve CTF for a market if needed.

        This approval is required before selling positions.

        Args:
            market_slug: Market slug identifier
            rpc_url: Base RPC URL

        Returns:
            Dict with:
                - already_approved: bool
                - tx_hash: str | None
        """
        ctf_address = self.get_ctf_address(market_slug)
        venue_exchange = self.get_venue_exchange(market_slug)
        w3 = _get_web3(rpc_url)
        return ensure_ctf_approved(w3, self._private_key, ctf_address, venue_exchange)

    # Position Redemption Methods (for EOA wallets)

    def get_redeemable_positions(self) -> list[RedeemablePosition]:
        """
        Get all redeemable positions from resolved markets.

        Returns positions where the user holds winning tokens that can be
        redeemed for USDC.

        Returns:
            List of RedeemablePosition objects

        Example:
            ```python
            redeemable = client.get_redeemable_positions()
            for pos in redeemable:
                print(f"{pos.market_title}: ${pos.balance} {pos.winning_token}")
            ```
        """
        portfolio = self.get_portfolio_positions()
        return get_redeemable_positions(portfolio)

    def redeem_position(
        self,
        condition_id: str,
        rpc_url: str = "https://mainnet.base.org",
        wait_for_receipt: bool = True,
    ) -> dict:
        """
        Redeem a resolved position for USDC.

        Calls the CTF contract to redeem winning tokens.
        Requires ETH for gas.

        Args:
            condition_id: Condition ID of the resolved market (32 bytes hex)
            rpc_url: Base RPC URL
            wait_for_receipt: Whether to wait for transaction confirmation

        Returns:
            Dict with:
                - tx_hash: Transaction hash
                - receipt: Transaction receipt (if wait_for_receipt=True)
                - success: Whether the transaction was successful

        Raises:
            ValueError: If condition_id is invalid
            Exception: If transaction fails

        Example:
            ```python
            result = client.redeem_position("0x8fee844847e80120f263...")
            print(f"Redeemed: {result['tx_hash']}")
            ```
        """
        redeemer = EOAPositionRedeemer(
            private_key=self._private_key,
            rpc_url=rpc_url,
        )

        tx_hash = redeemer.redeem_position(condition_id)

        result = {
            "tx_hash": tx_hash,
            "receipt": None,
            "success": False,
        }

        if wait_for_receipt:
            receipt = redeemer.wait_for_receipt(tx_hash)
            result["receipt"] = receipt
            result["success"] = receipt.get("status") == 1

        return result

    def redeem_all_positions(
        self,
        rpc_url: str = "https://mainnet.base.org",
    ) -> list[dict]:
        """
        Redeem all redeemable positions sequentially.

        Each position is redeemed in a separate transaction. The method waits
        for each transaction to be confirmed before starting the next one.

        Args:
            rpc_url: Base RPC URL

        Returns:
            List of result dicts, each with:
                - condition_id: The condition ID
                - market_title: Market title
                - balance: Amount redeemed
                - tx_hash: Transaction hash
                - success: Whether the transaction was successful
                - error: Error message if failed

        Example:
            ```python
            results = client.redeem_all_positions()
            for r in results:
                if r['success']:
                    print(f"Redeemed ${r['balance']} from {r['market_title']}")
            ```
        """
        redeemable = self.get_redeemable_positions()

        if not redeemable:
            return []

        redeemer = EOAPositionRedeemer(
            private_key=self._private_key,
            rpc_url=rpc_url,
        )

        # Redeem one-by-one, waiting for each to confirm before the next
        results = []
        for pos in redeemable:
            result = {
                "condition_id": pos.condition_id,
                "market_title": pos.market_title,
                "balance": pos.balance,
                "tx_hash": None,
                "success": False,
                "error": None,
            }

            try:
                tx_hash = redeemer.redeem_position(pos.condition_id)
                result["tx_hash"] = tx_hash

                # Wait for confirmation before proceeding to next
                receipt = redeemer.wait_for_receipt(tx_hash)
                result["success"] = receipt.get("status") == 1

            except Exception as e:
                result["error"] = str(e)

            results.append(result)

        return results

    def estimate_redemption_gas(
        self,
        condition_id: str,
        rpc_url: str = "https://mainnet.base.org",
    ) -> dict:
        """
        Estimate gas for redeeming a position.

        Args:
            condition_id: Condition ID of the resolved market
            rpc_url: Base RPC URL

        Returns:
            Dict with:
                - gas_units: Estimated gas units
                - gas_price_gwei: Current gas price in gwei
                - estimated_cost_eth: Estimated cost in ETH
                - eth_balance: Current ETH balance
                - has_sufficient_gas: Whether user has enough ETH
        """
        redeemer = EOAPositionRedeemer(
            private_key=self._private_key,
            rpc_url=rpc_url,
        )

        gas_units = redeemer.estimate_gas(condition_id)
        base_fee = redeemer.w3.eth.get_block("latest")["baseFeePerGas"]
        gas_price_gwei = base_fee / 1e9
        estimated_cost_eth = (gas_units * base_fee) / 1e18
        eth_balance = redeemer.get_eth_balance()

        return {
            "gas_units": gas_units,
            "gas_price_gwei": float(gas_price_gwei),
            "estimated_cost_eth": float(estimated_cost_eth),
            "eth_balance": float(eth_balance),
            "has_sufficient_gas": eth_balance >= estimated_cost_eth * 1.2,  # 20% buffer
        }

# ════════════════════════════════════════════════════════════════════
# END LIMITLESS SDK
# ════════════════════════════════════════════════════════════════════



# ═══════════════════════════════════════════════════════════
# PROXY PATCH — must happen before ClobClient import
# ═══════════════════════════════════════════════════════════

_POLY_PROXY_PATCHED = False
if POLY_PROXY_URL:
    print("[STARTUP] Patching py_clob_client_v2 with proxy: {}...".format(POLY_PROXY_URL[:30]))
    try:
        import httpx as _early_httpx
        try:
            from py_clob_client_v2.http_helpers import helpers as _early_v2h
            _early_v2h._http_client = _early_httpx.Client(
                http2=True, proxy=POLY_PROXY_URL, timeout=30.0,
            )
            _POLY_PROXY_PATCHED = True
            print("[STARTUP] ✓ proxy patched")
        except ImportError as _ie:
            print("[STARTUP] ✗ py_clob_client_v2 not available — {}".format(_ie))
    except Exception as _pe:
        print("[STARTUP] ✗ Proxy pre-patch failed: {}".format(_pe))
else:
    print("[STARTUP] No POLY_PROXY_URL — direct connection")

# ═══════════════════════════════════════════════════════════
# CONSTANTS & CACHES
# ═══════════════════════════════════════════════════════════

LAGOS_TZ = timezone(timedelta(hours=1))
LIMITLESS_API = "https://api.limitless.exchange"
POLY_GAMMA_API = "https://gamma-api.polymarket.com"

BINANCE_MAP = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT",
    "XRP": "XRPUSDT", "DOGE": "DOGEUSDT", "ADA": "ADAUSDT",
    "BNB": "BNBUSDT", "AVAX": "AVAXUSDT", "LINK": "LINKUSDT",
    "DOT": "DOTUSDT", "LTC": "LTCUSDT", "BCH": "BCHUSDT",
    "XLM": "XLMUSDT", "UNI": "UNIUSDT", "ATOM": "ATOMUSDT",
    "NEAR": "NEARUSDT", "OP": "OPUSDT", "ARB": "ARBUSDT",
    "TRX": "TRXUSDT", "TON": "TONUSDT", "ONDO": "ONDOUSDT",
    "XMR": "XMRUSDT", "ZEC": "ZECUSDT", "APT": "APTUSDT",
    "HYPE": "HYPEUSDT", "MNT": "MNTUSDT",
}

YAHOO_MAP = {
    "BTC":"BTC-USD", "ETH":"ETH-USD", "SOL":"SOL-USD",
    "ADA":"ADA-USD", "BNB":"BNB-USD", "DOGE":"DOGE-USD",
    "XRP":"XRP-USD", "AVAX":"AVAX-USD","LINK":"LINK-USD",
    "LTC":"LTC-USD", "BCH":"BCH-USD", "XLM":"XLM-USD",
    "ZEC":"ZEC-USD", "ONDO":"ONDO-USD",
    "DOT":"DOT-USD", "UNI":"UNI-USD", "ATOM":"ATOM-USD",
    "TRX":"TRX-USD", "APT":"APT-USD", "ARB":"ARB-USD",
    "OP":"OP-USD", "NEAR":"NEAR-USD","TON":"TON-USD",
}

# Chainlink RTDS caches
_chainlink_prices = {}   # {"BTC": 78900.50, ...}
_chainlink_ptb = {}      # {"BTC_15M": (end_ts, price), ...}
_chainlink_connected = False


# ═══════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════

def get_db():
    import urllib.parse
    db_url = DATABASE_URL.replace('postgres://', 'postgresql://')
    url = urllib.parse.urlparse(db_url)
    return pg8000.native.Connection(
        host=url.hostname, port=url.port or 5432,
        database=url.path.lstrip('/'),
        user=url.username, password=url.password,
        ssl_context=True
    )

def init_db():
    conn = get_db()
    # v2 paper trades table — confirmation trading
    conn.run("""
        CREATE TABLE IF NOT EXISTS v2_paper_trades (
            id              SERIAL PRIMARY KEY,
            platform        TEXT NOT NULL,
            timeframe       TEXT NOT NULL,
            asset           TEXT NOT NULL,
            direction       TEXT NOT NULL,
            ptb             REAL,
            entry_odds      REAL,
            entry_price     REAL,
            stake           REAL DEFAULT 2.50,
            entry_note      TEXT,
            hh_count        INTEGER,
            hl_count        INTEGER,
            ll_count        INTEGER,
            lh_count        INTEGER,
            grind_rate      TEXT,
            ptb_distance    REAL,
            session_label   TEXT,
            volatility      TEXT,
            prev_candle     TEXT,
            hedged          BOOLEAN DEFAULT FALSE,
            hedge_odds      REAL,
            hedge_direction TEXT,
            hedge_note      TEXT,
            hedge_pnl       REAL,
            market_id       TEXT,
            slug            TEXT,
            condition_id    TEXT,
            up_token        TEXT,
            down_token      TEXT,
            open_price      REAL,
            close_price     REAL,
            actual_result   TEXT,
            outcome         TEXT,
            pnl             REAL,
            balance_after   REAL,
            status          TEXT DEFAULT 'OPEN',
            fired_at        TIMESTAMPTZ DEFAULT NOW(),
            resolved_at     TIMESTAMPTZ,
            confidence      TEXT,
            market_url      TEXT,
            limit_price     REAL,
            book_ask        REAL,
            filled_at       TIMESTAMPTZ,
            order_status    TEXT DEFAULT 'FILLED'
        )
    """)
    # Migrations for existing DBs
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS market_url TEXT")
    except:
        pass
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS limit_price REAL")
    except:
        pass
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS book_ask REAL")
    except:
        pass
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS filled_at TIMESTAMPTZ")
    except:
        pass
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS order_status TEXT DEFAULT 'FILLED'")
    except:
        pass
    # ── v2 LIVE trades — real Limitless fills (FOK only). Mirrors paper schema
    # for easy side-by-side comparison; paper_trade_id links each live trade to
    # the paper twin so we can measure the adverse-fill gap once real data lands.
    conn.run("""
        CREATE TABLE IF NOT EXISTS v2_live_trades (
            id                  SERIAL PRIMARY KEY,
            paper_trade_id      INTEGER,
            platform            TEXT NOT NULL DEFAULT 'limitless',
            timeframe           TEXT, asset TEXT, direction TEXT,
            market_slug         TEXT, token_id TEXT, condition_id TEXT,
            limit_price_cents   REAL,
            stake_usdc          REAL,
            size_shares         REAL,
            fill_status         TEXT,        -- 'FILLED' | 'CANCELLED' | 'ERROR' | 'CAPPED'
            order_id            TEXT,
            fill_price_cents    REAL,
            filled_size         REAL,
            raw_response        TEXT,
            error_message       TEXT,
            fired_at            TIMESTAMPTZ DEFAULT NOW(),
            -- Resolution lifecycle (filled in by _v2_resolve_live_trades) --
            actual_result       TEXT,        -- 'UP' | 'DOWN' | 'FLAT'
            outcome             TEXT,        -- 'WIN' | 'LOSS' | 'PUSH' | NULL (unresolved)
            pnl                 REAL,
            resolved_at         TIMESTAMPTZ,
            -- On-chain redemption (auto-claim for WIN rows) --
            redeem_status       TEXT,        -- 'PENDING' | 'DONE' | 'FAILED' | 'SKIPPED'
            redeem_tx_hash      TEXT,
            redeem_attempts     INTEGER DEFAULT 0,
            redeem_last_attempt TIMESTAMPTZ
        )
    """)
    # ── Migrations for existing v2_live_trades rows (older deploys won't
    # have the resolution/redemption columns). Each ALTER is idempotent via
    # IF NOT EXISTS so re-runs are safe.
    for _alter in (
        "ALTER TABLE v2_live_trades ADD COLUMN IF NOT EXISTS condition_id TEXT",
        "ALTER TABLE v2_live_trades ADD COLUMN IF NOT EXISTS actual_result TEXT",
        "ALTER TABLE v2_live_trades ADD COLUMN IF NOT EXISTS outcome TEXT",
        "ALTER TABLE v2_live_trades ADD COLUMN IF NOT EXISTS pnl REAL",
        "ALTER TABLE v2_live_trades ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ",
        "ALTER TABLE v2_live_trades ADD COLUMN IF NOT EXISTS redeem_status TEXT",
        "ALTER TABLE v2_live_trades ADD COLUMN IF NOT EXISTS redeem_tx_hash TEXT",
        "ALTER TABLE v2_live_trades ADD COLUMN IF NOT EXISTS redeem_attempts INTEGER DEFAULT 0",
        "ALTER TABLE v2_live_trades ADD COLUMN IF NOT EXISTS redeem_last_attempt TIMESTAMPTZ",
    ):
        try:
            conn.run(_alter)
        except Exception as _e:
            print("[V2-MIGRATE] {} -> {}".format(_alter[:60], str(_e)[:80]))
    # ── v2 settings (key/value) — DB-backed runtime config so toggles flip
    # without redeploy and survive restarts. Live ON/OFF lives here.
    conn.run("""
        CREATE TABLE IF NOT EXISTS v2_settings (
            key         TEXT PRIMARY KEY,
            value       TEXT,
            updated_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    # Persistent dedup log for sports-pick Telegram alerts. With the once-a-day
    # sports scanner schedule, the same (home, away, pick) prediction will
    # recur in the model output across days until the game is played — we use
    # this table to ensure each pick fires Telegram exactly once, ever.
    conn.run("""
        CREATE TABLE IF NOT EXISTS sports_alerts_log (
            id              SERIAL PRIMARY KEY,
            home_lower      TEXT    NOT NULL,
            away_lower      TEXT    NOT NULL,
            pick            TEXT    NOT NULL,
            market_label    TEXT,
            url             TEXT,
            confidence      INTEGER,
            alerted_at      BIGINT  NOT NULL
        )
    """)
    conn.run("""
        CREATE INDEX IF NOT EXISTS idx_sports_alerts_dedup
            ON sports_alerts_log (home_lower, away_lower, pick)
    """)
    # v2 balance tracking
    conn.run("""
        CREATE TABLE IF NOT EXISTS v2_balances (
            id              SERIAL PRIMARY KEY,
            platform        TEXT NOT NULL,
            balance         REAL DEFAULT 100.0,
            peak_balance    REAL DEFAULT 100.0,
            wins            INTEGER DEFAULT 0,
            losses          INTEGER DEFAULT 0,
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    # Insert default balances if not exist
    for platform in ["polymarket", "limitless"]:
        try:
            existing = conn.run(
                "SELECT id FROM v2_balances WHERE platform = :p", p=platform)
            if not list(existing):
                conn.run(
                    "INSERT INTO v2_balances (platform, balance, peak_balance) VALUES (:p, 50.0, 50.0)",
                    p=platform)
        except:
            pass
    conn.close()
    print("[V2] Database initialized")


def reset_db():
    """Reset all paper trades and balances for a fresh start."""
    try:
        conn = get_db()
        # Delete all existing trades
        conn.run("DELETE FROM v2_paper_trades")
        # Reset balances to $50 each
        conn.run("UPDATE v2_balances SET balance = 50.0, peak_balance = 50.0, wins = 0, losses = 0, updated_at = NOW()")
        conn.close()
        # Reset in-memory balances
        _v2_balances["polymarket"] = {"balance": 50.0, "peak_balance": 50.0, "wins": 0, "losses": 0}
        _v2_balances["limitless"] = {"balance": 50.0, "peak_balance": 50.0, "wins": 0, "losses": 0}
        print("[V2] *** DATABASE RESET — all trades cleared, balances reset to $50 ***")
    except Exception as e:
        print("[V2] Reset error: {}".format(e))


# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

def send_telegram(message):
    if not TELEGRAM_TOKEN:
        return
    _m = message or ""
    is_code = ("SPORTYBET CODES" in _m or "PREMIUM BET BUILDERS" in _m
               or "shareCode=" in _m)
    codes_channel = os.environ.get("TELEGRAM_CODES_CHANNEL_ID", "").strip()
    # Routing:
    #  - TELEGRAM_CHAT_ID (the bot) gets EVERYTHING — codes and every other
    #    alert — exactly like before any filtering existed.
    #  - if TELEGRAM_CODES_CHANNEL_ID is set, SportyBet codes are ALSO sent
    #    there, and ONLY codes — so that channel stays codes-only.
    targets = []
    if TELEGRAM_CHAT_ID:
        targets.append(TELEGRAM_CHAT_ID)
    if is_code and codes_channel and codes_channel != TELEGRAM_CHAT_ID:
        targets.append(codes_channel)
    if not targets:
        return

    def _send():
        try:
            import requests
        except Exception as e:
            print("[TG] requests import error: {}".format(e))
            return
        # Telegram hard-caps one message at 4096 chars. A full codes set (all
        # tiers, with a 12-leg 1000-odds slip) exceeds that, so split into
        # <=3800-char parts on line breaks (every HTML tag is self-contained
        # within a single line in our formatters, so this stays parse-safe).
        def _chunks(text, limit=3800):
            if len(text) <= limit:
                return [text]
            out, buf = [], ""
            for line in text.split("\n"):
                if len(buf) + len(line) + 1 > limit:
                    if buf:
                        out.append(buf)
                    while len(line) > limit:
                        out.append(line[:limit]); line = line[limit:]
                    buf = line
                else:
                    buf = line if not buf else buf + "\n" + line
            if buf:
                out.append(buf)
            return out
        for _chat in targets:
            for part in _chunks(message):
                try:
                    r = requests.post(
                        "https://api.telegram.org/bot{}/sendMessage".format(TELEGRAM_TOKEN),
                        json={"chat_id": _chat, "text": part, "parse_mode": "HTML"},
                        timeout=10
                    )
                    ok = False
                    try:
                        ok = bool(r.json().get("ok"))
                    except Exception:
                        ok = (r.status_code == 200)
                    if not ok:
                        print("[TG] send to {} failed (HTTP {}): {}".format(
                            _chat, r.status_code, (r.text or "")[:200]))
                        # retry once without HTML in case of an entity-parse error
                        r2 = requests.post(
                            "https://api.telegram.org/bot{}/sendMessage".format(TELEGRAM_TOKEN),
                            json={"chat_id": _chat, "text": part}, timeout=10)
                        if not (r2.status_code == 200):
                            print("[TG] plain retry to {} also failed (HTTP {}): {}".format(
                                _chat, r2.status_code, (r2.text or "")[:200]))
                except Exception as e:
                    print("[TG] send error to {}: {}".format(_chat, e))
    threading.Thread(target=_send, daemon=True).start()


# ═══════════════════════════════════════════════════════════
# BINANCE DATA
# ═══════════════════════════════════════════════════════════

def _fetch_binance_candles(asset, interval="15m", limit=100):
    """Fetch OHLCV candles from Binance. Returns list of dicts with o,h,l,c,v,t."""
    import requests as req
    symbol = BINANCE_MAP.get(asset.upper())
    if not symbol:
        return None
    try:
        r = req.get("https://api.binance.com/api/v3/klines",
                     params={"symbol": symbol, "interval": interval, "limit": limit},
                     timeout=3)
        if r.status_code != 200:
            return None
        klines = r.json()
        if not klines or len(klines) < 5:
            return None
        candles = []
        for k in klines:
            candles.append({
                "o": float(k[1]), "h": float(k[2]),
                "l": float(k[3]), "c": float(k[4]),
                "v": float(k[5]), "t": int(k[0]),
            })
        return candles
    except Exception as e:
        print("Binance candle error {} {}: {}".format(asset, interval, e))
        return None

def _get_binance_price(asset):
    """Get current price from Binance — instant."""
    import requests as req
    symbol = BINANCE_MAP.get(asset.upper())
    if not symbol:
        return None
    try:
        r = req.get("https://api.binance.com/api/v3/ticker/price",
                     params={"symbol": symbol}, timeout=2)
        if r.status_code == 200:
            return float(r.json().get("price", 0))
    except:
        pass
    return None

def get_price(asset):
    """Get current price — Binance first, yfinance fallback."""
    bp = _get_binance_price(asset)
    if bp and bp > 0:
        return bp
    try:
        import yfinance as yf
        symbol = YAHOO_MAP.get(asset.upper())
        if not symbol:
            return None
        ticker = yf.Ticker(symbol)
        try:
            price = ticker.fast_info.last_price
            if price and price > 0:
                return float(price)
        except:
            pass
        hist = ticker.history(period="1d", interval="1m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        print("yfinance error {}: {}".format(asset, e))
    return None


# ═══════════════════════════════════════════════════════════
# POLYMARKET CLOB CLIENT (singleton)
# ═══════════════════════════════════════════════════════════

_poly_clob_client = None

def _poly_has_creds():
    return bool(POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE
                and LIMITLESS_PRIV_KEY and POLY_FUNDER_ADDRESS)

def _get_poly_client():
    """Get or create Polymarket CLOB client."""
    global _poly_clob_client
    if _poly_clob_client is not None:
        return _poly_clob_client
    if not _poly_has_creds():
        return None
    try:
        try:
            from py_clob_client_v2 import ClobClient, ApiCreds
            client = ClobClient(
                host="https://clob.polymarket.com",
                chain_id=137,
                key=LIMITLESS_PRIV_KEY,
                signature_type=2,
                funder=POLY_FUNDER_ADDRESS,
                creds=ApiCreds(
                    api_key=POLY_API_KEY,
                    api_secret=POLY_API_SECRET,
                    api_passphrase=POLY_API_PASSPHRASE,
                ),
            )
            _poly_clob_client = client
            print("Polymarket CLOB V2 client initialized")
        except ImportError:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
            creds = ApiCreds(
                api_key=POLY_API_KEY,
                api_secret=POLY_API_SECRET,
                api_passphrase=POLY_API_PASSPHRASE
            )
            client = ClobClient(
                "https://clob.polymarket.com",
                key=LIMITLESS_PRIV_KEY,
                chain_id=137, signature_type=2,
                funder=POLY_FUNDER_ADDRESS,
            )
            client.set_api_creds(creds)
            _poly_clob_client = client
            print("Polymarket CLOB V1 client initialized")
        # Proxy injection
        if POLY_PROXY_URL and not _POLY_PROXY_PATCHED:
            try:
                import httpx as _httpx
                from py_clob_client_v2.http_helpers import helpers as _v2h
                _v2h._http_client = _httpx.Client(
                    http2=True, proxy=POLY_PROXY_URL, timeout=30.0,
                )
                print("Polymarket proxy injected at client init")
            except:
                pass
        return _poly_clob_client
    except Exception as e:
        print("CLOB init error: {}".format(e))
        return None


# ═══════════════════════════════════════════════════════════
# CHAINLINK RTDS WEBSOCKET
# ═══════════════════════════════════════════════════════════

POLY_RTDS_URL = "wss://ws-live-data.polymarket.com"

def _rtds_price_to_beat(asset, timeframe, end_ts):
    """Get the Price to Beat from Chainlink boundary capture."""
    key = "{}_{}".format(asset, timeframe)
    entry = _chainlink_ptb.get(key)
    if entry:
        stored_ts, stored_price = entry
        tf_sec = {"5M": 300, "15M": 900, "1H": 3600, "DAILY": 86400}.get(timeframe, 300)
        if abs(stored_ts - end_ts) <= tf_sec * 2:
            return stored_price
    return None

def _rtds_current_price(asset):
    return _chainlink_prices.get(asset)

def _rtds_loop():
    """Background thread: Chainlink RTDS WebSocket for real-time prices."""
    global _chainlink_connected
    import websocket

    pair_map = {
        "btc/usd": "BTC", "eth/usd": "ETH", "sol/usd": "SOL",
        "xrp/usd": "XRP", "doge/usd": "DOGE", "bnb/usd": "BNB",
        "hype/usd": "HYPE", "ada/usd": "ADA", "avax/usd": "AVAX",
        "link/usd": "LINK", "dot/usd": "DOT", "ltc/usd": "LTC",
    }
    _msg_count = [0]

    def _store_ptb(asset, price, ts_sec):
        for tf_label, tf_sec, max_delay in [("5M", 300, 5), ("15M", 900, 10), ("1H", 3600, 10)]:
            window_start = (ts_sec // tf_sec) * tf_sec
            window_end = window_start + tf_sec
            key = "{}_{}".format(asset, tf_label)
            existing = _chainlink_ptb.get(key)
            if ts_sec - window_start <= max_delay:
                if not existing or existing[0] != window_end:
                    _chainlink_ptb[key] = (window_end, price)

    def on_message(ws, message):
        global _chainlink_connected
        _chainlink_connected = True
        try:
            if message == "PONG":
                return
            _msg_count[0] += 1
            if message.startswith("{") or message.startswith("["):
                data = json.loads(message)
                if isinstance(data, dict):
                    payload = data.get("payload")
                    if payload and isinstance(payload, dict):
                        symbol = (payload.get("symbol") or "").lower()
                        value = payload.get("value") or payload.get("price")
                        ts = payload.get("timestamp") or data.get("timestamp") or 0
                        if symbol and value:
                            price = float(value)
                            asset = pair_map.get(symbol)
                            if asset:
                                _chainlink_prices[asset] = price
                                ts_sec = int(ts) // 1000 if ts > 1e12 else int(ts) if isinstance(ts, (int, float)) else int(time.time())
                                _store_ptb(asset, price, ts_sec)
                return
            parts = message.split(",")
            if len(parts) >= 4:
                ts_ms = int(parts[0])
                pair = parts[2].strip()
                price = float(parts[3].strip())
                asset = pair_map.get(pair)
                if asset:
                    _chainlink_prices[asset] = price
                    _store_ptb(asset, price, ts_ms // 1000)
        except Exception as e:
            if _msg_count[0] <= 5:
                print("RTDS parse error: {}".format(e))

    def on_error(ws, error):
        global _chainlink_connected
        _chainlink_connected = False
        print("RTDS error: {}".format(error))

    def on_close(ws, close_status, close_msg):
        global _chainlink_connected
        _chainlink_connected = False

    def on_open(ws):
        global _chainlink_connected
        _chainlink_connected = True
        sub = json.dumps({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "filters": {}
            }]
        })
        ws.send(sub)
        print("RTDS connected + subscribed")

    while True:
        try:
            ws = websocket.WebSocketApp(
                POLY_RTDS_URL,
                on_message=on_message, on_error=on_error,
                on_close=on_close, on_open=on_open
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print("RTDS loop error: {}".format(e))
        time.sleep(5)


# ═══════════════════════════════════════════════════════════
# POLYMARKET MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════

def _poly_parse_market(market, timeframe_hint=None):
    """Parse a Polymarket crypto Up/Down market from Gamma API data."""
    try:
        question = market.get("question") or market.get("title") or ""
        slug = market.get("slug") or ""
        condition_id = market.get("conditionId") or market.get("condition_id") or ""
        slug_lower = slug.lower()
        q_lower = question.lower()

        # Detect asset
        asset = None
        asset_patterns = [
            (["btc-", "bitcoin-"], "BTC"), (["eth-", "ethereum-"], "ETH"),
            (["sol-", "solana-"], "SOL"), (["xrp-"], "XRP"),
            (["doge-", "dogecoin-"], "DOGE"), (["hype-", "hyperliquid-"], "HYPE"),
            (["bnb-"], "BNB"), (["ada-", "cardano-"], "ADA"),
            (["avax-", "avalanche-"], "AVAX"), (["link-", "chainlink-"], "LINK"),
        ]
        for prefixes, sym in asset_patterns:
            if any(p in slug_lower for p in prefixes):
                asset = sym
                break
        if not asset:
            for word, sym in [("bitcoin", "BTC"), ("ethereum", "ETH"), ("solana", "SOL"),
                              ("xrp", "XRP"), ("dogecoin", "DOGE"), ("hyperliquid", "HYPE")]:
                if word in q_lower:
                    asset = sym
                    break
        if not asset:
            return None
        if "up or down" not in q_lower and "updown" not in slug_lower:
            return None

        # Expiry
        now = datetime.now(timezone.utc)
        end_date = market.get("endDate") or market.get("end_date_iso") or ""
        exp_ts = market.get("expirationTimestamp") or market.get("expiration_timestamp")
        expiry_dt = None
        if exp_ts:
            try:
                if isinstance(exp_ts, str): exp_ts = int(exp_ts)
                if exp_ts > 1e12: exp_ts = exp_ts / 1000
                expiry_dt = datetime.fromtimestamp(exp_ts, tz=timezone.utc)
            except:
                exp_ts = None
        if not expiry_dt and end_date:
            try:
                expiry_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                exp_ts = int(expiry_dt.timestamp())
            except:
                return None
        if not expiry_dt:
            return None
        mins_left = (expiry_dt - now).total_seconds() / 60
        if mins_left <= 0:
            return None

        # Timeframe
        timeframe = None
        if "-15m-" in slug_lower or "-15m" in slug_lower:
            timeframe = "15M"
        elif "-5m-" in slug_lower or "-5m" in slug_lower:
            timeframe = "5M"
        elif "-1h-" in slug_lower or "-1h" in slug_lower or "hourly" in slug_lower:
            timeframe = "1H"
        # Detect 1H from "up-or-down-{month}-{day}" format (no -5m/-15m suffix)
        elif "up-or-down-" in slug_lower and "-updown-" not in slug_lower and "up-or-down-on-" not in slug_lower:
            timeframe = "1H"
        # Daily: "bitcoin-up-or-down-on-may-27"
        elif "up-or-down-on-" in slug_lower:
            timeframe = "DAILY"
        if not timeframe:
            created = market.get("createdAt") or ""
            if created and exp_ts:
                try:
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    dur = (expiry_dt - created_dt).total_seconds() / 60
                    if 55 <= dur <= 65: timeframe = "1H"
                    elif 13 <= dur <= 17: timeframe = "15M"
                    elif 4 <= dur <= 6: timeframe = "5M"
                    elif dur > 600: timeframe = "DAILY"
                except:
                    pass
        if not timeframe and timeframe_hint:
            timeframe = timeframe_hint
        if not timeframe:
            return None

        # Odds
        outcome_prices = market.get("outcomePrices") or market.get("outcome_prices")
        up_odds = 50.0
        if outcome_prices:
            if isinstance(outcome_prices, str):
                try: outcome_prices = json.loads(outcome_prices)
                except: outcome_prices = None
            if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                up_raw = float(outcome_prices[0])
                up_odds = up_raw * 100 if up_raw <= 1.0 else up_raw

        # Token IDs + outcome ordering
        clob_tokens = market.get("clobTokenIds")
        if isinstance(clob_tokens, str):
            try: clob_tokens = json.loads(clob_tokens)
            except: clob_tokens = []
        outcomes_raw = market.get("outcomes")
        if isinstance(outcomes_raw, str):
            try: outcomes_raw = json.loads(outcomes_raw)
            except: outcomes_raw = None
        up_index, down_index = 0, 1
        if isinstance(outcomes_raw, list) and len(outcomes_raw) >= 2:
            o0 = str(outcomes_raw[0]).lower().strip()
            if o0 in ("no", "down", "below"):
                up_index, down_index = 1, 0
                if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                    up_raw = float(outcome_prices[1])
                    up_odds = up_raw * 100 if up_raw <= 1.0 else up_raw

        # Baseline from market text
        baseline = None
        for field in ["question", "description", "resolutionSource", "rules", "title"]:
            text = str(market.get(field) or "")
            if "$" in text:
                all_prices = re.findall(r'\$([0-9,]+\.?\d*)', text)
                for p in all_prices:
                    try:
                        val = float(p.replace(",", ""))
                        if asset == "BTC" and 10000 < val < 200000: baseline = val; break
                        elif asset == "ETH" and 500 < val < 10000: baseline = val; break
                        elif asset == "SOL" and 5 < val < 500: baseline = val; break
                        elif asset == "XRP" and 0.1 < val < 10: baseline = val; break
                        elif asset == "DOGE" and 0.01 < val < 2: baseline = val; break
                    except:
                        pass
                if baseline: break

        market_id = str(market.get("id") or condition_id or slug)
        up_token = str(clob_tokens[up_index]) if clob_tokens and len(clob_tokens) > up_index else ""
        down_token = str(clob_tokens[down_index]) if clob_tokens and len(clob_tokens) > down_index else ""

        return {
            "market_id": market_id, "title": question, "asset": asset,
            "baseline": baseline, "expiry_dt": expiry_dt,
            "mins_left": mins_left, "hours_left": mins_left / 60,
            "yes_odds": up_odds, "slug": slug,
            "condition_id": condition_id, "timeframe": timeframe,
            "clob_tokens": clob_tokens or [],
            "up_token": up_token, "down_token": down_token,
            "up_token_index": up_index, "down_token_index": down_index,
        }
    except Exception as e:
        print("POLY PARSE ERR: {}".format(e))
        return None


def _poly_fetch_markets():
    """Fetch active crypto Up/Down markets from Polymarket."""
    import requests as req
    now = datetime.now(timezone.utc)
    markets = []
    current_ts = int(now.timestamp())

    # Strategy 1: Public search
    try:
        r = req.get("{}/public-search".format(POLY_GAMMA_API),
                    params={"query": "up or down", "limit": 50}, timeout=12)
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get("events", data.get("markets", data.get("data", []))) if isinstance(data, dict) else []
            for item in items:
                item_markets = []
                if isinstance(item, dict):
                    if item.get("markets"):
                        item_markets = item["markets"]
                    elif item.get("clobTokenIds") or item.get("conditionId"):
                        item_markets = [item]
                for m in item_markets:
                    q = (m.get("question") or m.get("title") or "").lower()
                    if ("up" in q and "down" in q) or "updown" in q:
                        parsed = _poly_parse_market(m)
                        if parsed:
                            markets.append(parsed)
            if markets:
                return markets
    except Exception as e:
        print("Poly search error: {}".format(e))

    # Strategy 2: Slug lookup
    assets = [("btc", "BTC"), ("eth", "ETH"), ("sol", "SOL"), ("xrp", "XRP")]
    _1h_full_names = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "xrp"}
    try:
        from zoneinfo import ZoneInfo
        _ET = ZoneInfo("America/New_York")
    except:
        _ET = timezone(timedelta(hours=-4))
    now_et = now.astimezone(_ET)
    _1h_start = now_et.replace(minute=0, second=0, microsecond=0)
    _1h_h12 = _1h_start.hour % 12 or 12
    _1h_ap = "am" if _1h_start.hour < 12 else "pm"
    _1h_mo = _1h_start.strftime("%B").lower()
    _1h_dy = _1h_start.day
    _1h_yr = _1h_start.year

    for asset_slug, _ in assets:
        slugs = []
        for tf_slug, tf_sec in [("5m", 300), ("15m", 900)]:
            ws = (current_ts // tf_sec) * tf_sec
            slugs.append("{}-updown-{}-{}".format(asset_slug, tf_slug, ws))
        _1h_name = _1h_full_names.get(asset_slug, asset_slug)
        # 1H slug: "bitcoin-up-or-down-may-27-10pm-et" (no year)
        slugs.append("{}-up-or-down-{}-{}-{}{}-et".format(
            _1h_name, _1h_mo, _1h_dy, _1h_h12, _1h_ap))
        # 1H with year fallback
        slugs.append("{}-up-or-down-{}-{}-{}-{}{}-et".format(
            _1h_name, _1h_mo, _1h_dy, _1h_yr, _1h_h12, _1h_ap))
        # Daily slug: "bitcoin-up-or-down-on-may-27" (no year)
        slugs.append("{}-up-or-down-on-{}-{}".format(
            _1h_name, _1h_mo, _1h_dy))
        # Daily with year fallback
        slugs.append("{}-up-or-down-on-{}-{}-{}".format(
            _1h_name, _1h_mo, _1h_dy, _1h_yr))

        for s in slugs:
            _is_daily = "-up-or-down-on-" in s
            _is_1h = "-up-or-down-" in s and not _is_daily
            tf_hint = "DAILY" if _is_daily else "1H" if _is_1h else None
            for url in ["{}/events/slug/{}".format(POLY_GAMMA_API, s),
                        "{}/events".format(POLY_GAMMA_API)]:
                try:
                    params = {"slug": s} if "/slug/" not in url else {}
                    r = req.get(url, params=params, timeout=8)
                    if r.status_code == 200:
                        data = r.json()
                        em = []
                        if isinstance(data, list) and data:
                            em = data[0].get("markets", []) if isinstance(data[0], dict) else []
                        elif isinstance(data, dict):
                            em = data.get("markets", [])
                        for m in em:
                            parsed = _poly_parse_market(m, timeframe_hint=tf_hint)
                            if parsed:
                                markets.append(parsed)
                        if em: break
                except:
                    pass

    if markets:
        return markets

    # Strategy 3: Broad scan
    try:
        r = req.get("{}/markets".format(POLY_GAMMA_API),
                    params={"active": "true", "closed": "false", "limit": 100,
                            "order": "volume24hr", "ascending": "false"}, timeout=15)
        if r.status_code == 200:
            batch = r.json() if isinstance(r.json(), list) else []
            for m in batch:
                q = (m.get("question") or "").lower()
                if "up or down" in q or "updown" in q:
                    parsed = _poly_parse_market(m)
                    if parsed:
                        markets.append(parsed)
    except Exception as e:
        print("Poly broad error: {}".format(e))

    return markets


def _poly_get_baseline(parsed, price=None):
    """Get PTB: (1) market title, (2) Chainlink boundary, (3) stream."""
    asset = parsed.get("asset", "")
    tf = parsed.get("timeframe", "")
    if parsed.get("baseline") and parsed["baseline"] > 0:
        return parsed["baseline"]
    key = "{}_{}".format(asset, tf)
    entry = _chainlink_ptb.get(key)
    if entry:
        return entry[1]
    chainlink = _chainlink_prices.get(asset)
    if chainlink:
        return chainlink
    return price



# ═══════════════════════════════════════════════════════════
# V2 CONFIRMATION ENGINE — CORE ANALYSIS
# ═══════════════════════════════════════════════════════════

def _v2_session_filter(utc_hour):
    """AVOID London/US cross + Peak US + Peak Asia.
    PREFER: Late US/early Asian + Early morning."""
    if 4 <= utc_hour <= 11:
        return "EARLY_MORNING", True
    elif 17 <= utc_hour <= 22:
        return "LATE_US_ASIA", True
    elif 11 <= utc_hour < 12:
        return "PRE_LONDON", True
    elif 12 <= utc_hour <= 17:
        return "US_SESSION", False
    elif 23 <= utc_hour or utc_hour < 4:
        return "PEAK_ASIA", False
    else:
        return "TRANSITION", True


def _v2_analyze_structure(candles):
    """Analyze HH/HL structure from intra-period candles.
    Candle intervals per timeframe (set by caller):
    - Hourly watcher: 15M candles (3 completed at T+45)
    - 15M watcher: 5M candles (2 completed at T+10)
    - Daily watcher: 4H candles (4-5 by quiet hours)

    With 2-5 candles, compare each consecutively for HH/HL/LH/LL.
    Spike = one candle did >50% of the total range."""

    if not candles or len(candles) < 2:
        return None

    # Count HH, HL, LH, LL by comparing consecutive candle highs and lows
    hh_count = 0
    lh_count = 0
    for i in range(1, len(candles)):
        if candles[i]["h"] > candles[i-1]["h"]:
            hh_count += 1
        elif candles[i]["h"] < candles[i-1]["h"]:
            lh_count += 1

    hl_count = 0
    ll_count = 0
    for i in range(1, len(candles)):
        if candles[i]["l"] > candles[i-1]["l"]:
            hl_count += 1
        elif candles[i]["l"] < candles[i-1]["l"]:
            ll_count += 1

    # Spike detection — did one candle do all the work?
    # Measure by BODY MOVE (close - open), not range (high - low)
    # In a steady grind, each candle contributes similar body moves
    # In a spike, one candle has a huge body while others are small/flat
    body_moves = [abs(c["c"] - c["o"]) for c in candles]
    total_body = sum(body_moves)
    max_body = max(body_moves) if body_moves else 0

    if total_body > 0:
        body_concentration = max_body / total_body
    else:
        body_concentration = 0

    # Also check: did the total period move happen gradually?
    total_range = max(c["h"] for c in candles) - min(c["l"] for c in candles)

    # Spike = one candle's body is more than 70% of the total body movement
    # This means one candle did most of the work
    if body_concentration > 0.70:
        grind_type = "spike"
    elif body_concentration < 0.45:
        grind_type = "steady"
    else:
        grind_type = "normal"

    # Direction — HH>=2 AND HL>=2 = clean trend, else FLAT
    if hh_count >= 2 and hl_count >= 2 and hh_count > lh_count:
        direction = "UP"
    elif ll_count >= 2 and lh_count >= 2 and ll_count > hh_count:
        direction = "DOWN"
    elif hh_count >= 2 and hl_count >= 2 and lh_count == 0:
        direction = "UP"
    elif ll_count >= 2 and lh_count >= 2 and hh_count == 0:
        direction = "DOWN"
    else:
        direction = "FLAT"

    return {
        "hh_count": hh_count, "hl_count": hl_count,
        "lh_count": lh_count, "ll_count": ll_count,
        "grind_type": grind_type, "direction": direction,
        "concentration": round(body_concentration, 3),
    }


def _v2_analyze_prev_candle(candle):
    """Analyze previous period's candle for strength/direction."""
    if not candle:
        return None
    o, h, l, c = candle["o"], candle["h"], candle["l"], candle["c"]
    rng = max(h - l, 0.0001)
    body = abs(c - o)
    body_pct = body / rng
    close_pos = (c - l) / rng
    green = c > o
    upper_wick = (h - max(o, c)) / rng
    lower_wick = (min(o, c) - l) / rng

    if body_pct > 0.6 and close_pos > 0.7 and green:
        strength = "STRONG_BULL"
    elif body_pct > 0.6 and close_pos < 0.3 and not green:
        strength = "STRONG_BEAR"
    elif body_pct < 0.15:
        strength = "DOJI"
    elif green:
        strength = "MILD_BULL"
    else:
        strength = "MILD_BEAR"

    return {
        "green": green, "body_pct": round(body_pct, 3),
        "close_pos": round(close_pos, 3), "strength": strength,
        "upper_wick": round(upper_wick, 3), "lower_wick": round(lower_wick, 3),
        "range": rng,
    }


def _v2_volatility_check(candles, current_range=None):
    """ATR-based volatility check."""
    if not candles or len(candles) < 3:
        return "unknown", True
    ranges = [c["h"] - c["l"] for c in candles[-10:]]
    atr = sum(ranges) / len(ranges) if ranges else 0
    if current_range is None:
        current_range = candles[-1]["h"] - candles[-1]["l"]
    if atr <= 0:
        return "unknown", True
    ratio = current_range / atr
    if ratio > 2.5:
        return "extreme", False
    elif ratio > 1.8:
        return "high", False
    else:
        return "normal", True


def _v2_should_enter(price, ptb, asset, structure, prev_candle,
                     vol_safe, session_safe, timeframe, secs_remaining):
    """Master entry decision — CONFIRMATION, not prediction.

    The question: "Will this close above/below the PTB?"

    1. Where is price relative to PTB? (must be meaningfully on one side)
    2. How did it get there? (steady grind = safe, spike = dangerous)
    3. Previous candle supports the bias?
    4. Structure confirms the path? (no reversal signs)
    5. Session quiet? Volatility normal?
    6. Given the distance and time left, is it unlikely to reverse back?
    """

    # Hard filters
    if not session_safe and timeframe != "DAILY":
        return False, None, 0, "Volatile session — skip"

    if not vol_safe:
        return False, None, 0, "Volatility too high — skip"

    if not price or not ptb or ptb <= 0:
        return False, None, 0, "No price or PTB data"

    if not structure:
        return False, None, 0, "No structure data"

    if not prev_candle:
        return False, None, 0, "No previous candle data"

    # 1. Distance from PTB
    distance_pct = ((price - ptb) / ptb) * 100
    abs_dist = abs(distance_pct)

    min_dist = {
        "BTC": 0.05, "ETH": 0.08, "SOL": 0.10,
        "XRP": 0.15, "DOGE": 0.20, "BNB": 0.10,
    }.get(asset, 0.10)

    if abs_dist < min_dist:
        return False, None, 0, "Too close to PTB ({:+.3f}%) — coin flip".format(distance_pct)

    direction = "UP" if distance_pct > 0 else "DOWN"

    # 2. How did price get here?
    grind = structure.get("grind_type", "normal")
    if grind == "spike":
        return False, None, 0, "Spike — one candle did {:.0f}% of the body move".format(
            structure.get("concentration", 0) * 100)

    # 3. Previous candle must align with direction
    if direction == "UP" and prev_candle["strength"] in ("STRONG_BEAR", "MILD_BEAR"):
        return False, None, 0, "Prev candle RED — no bullish momentum behind this"

    if direction == "DOWN" and prev_candle["strength"] in ("STRONG_BULL", "MILD_BULL"):
        return False, None, 0, "Prev candle GREEN — no bearish momentum behind this"

    # 4. Structure must not contradict
    struct_dir = structure.get("direction", "FLAT")
    hh = structure.get("hh_count", 0)
    hl = structure.get("hl_count", 0)
    lh = structure.get("lh_count", 0)
    ll = structure.get("ll_count", 0)

    # If structure shows clear opposite direction, skip
    if direction == "UP" and struct_dir == "DOWN":
        return False, None, 0, "Price above PTB but structure DOWN — conflicting"

    if direction == "DOWN" and struct_dir == "UP":
        return False, None, 0, "Price below PTB but structure UP — conflicting"

    # Any reversal signs = skip
    if direction == "UP" and lh >= 1:
        return False, None, 0, "LH={} — momentum fading, may drop back to PTB".format(lh)

    if direction == "DOWN" and hl >= 1:
        return False, None, 0, "HL={} — momentum fading, may rise back to PTB".format(hl)

    # 5. Build confidence
    confidence = 60

    # Distance bonus — further from PTB = safer
    if abs_dist > 0.30:
        confidence += 15
    elif abs_dist > 0.15:
        confidence += 10
    elif abs_dist > min_dist:
        confidence += 5

    # Previous candle strength
    if direction == "UP":
        if prev_candle["strength"] == "STRONG_BULL":
            confidence += 15
        elif prev_candle["strength"] == "MILD_BULL":
            confidence += 10
    else:
        if prev_candle["strength"] == "STRONG_BEAR":
            confidence += 15
        elif prev_candle["strength"] == "MILD_BEAR":
            confidence += 10

    # Doji prev = no bonus but not a skip (distance and structure carry it)
    # Structure HH/HL bonus
    if direction == "UP":
        confidence += min(hh * 3, 10)
        confidence += min(hl * 3, 10)
    else:
        confidence += min(ll * 3, 10)
        confidence += min(lh * 3, 10)

    # Steady grind bonus
    if grind == "steady":
        confidence += 5

    confidence = min(confidence, 99)

    # Build reason
    reason = "{} {:+.3f}% from PTB | {} | HH={} HL={} LH={} LL={} | {} | {}min left".format(
        direction, distance_pct, prev_candle["strength"],
        hh, hl, lh, ll, grind, int(secs_remaining / 60))

    # Minimum confidence per timeframe
    min_conf = {"1H": 70, "15M": 70, "DAILY": 75}.get(timeframe, 70)
    if confidence < min_conf:
        return False, None, confidence, "Conf {} < {} — {}".format(confidence, min_conf, reason)

    return True, direction, confidence, reason


def _v2_build_entry_note(asset, timeframe, direction, prev_candle, structure,
                         ptb, price, session_label, vol_label, confidence,
                         secs_remaining=0):
    """Build human-readable entry note."""
    prev_str = ""
    if prev_candle:
        color = "green" if prev_candle["green"] else "red"
        prev_str = "Prev: {} {}, body={:.0f}%, close@{:.0f}%".format(
            prev_candle["strength"], color,
            prev_candle["body_pct"] * 100, prev_candle["close_pos"] * 100)

    struct_str = ""
    if structure:
        struct_str = "HH={} HL={} LH={} LL={} | {}".format(
            structure["hh_count"], structure["hl_count"],
            structure["lh_count"], structure["ll_count"],
            structure["grind_type"])

    ptb_str = ""
    if ptb and price:
        dist = ((price - ptb) / ptb) * 100
        ptb_str = "PTB dist: {:+.3f}%".format(dist)

    return "{} {} {} | {} | {} | {} | Session: {} | Vol: {} | Conf: {} | {}min left".format(
        timeframe, asset, direction,
        prev_str, struct_str, ptb_str,
        session_label, vol_label, confidence,
        int(secs_remaining / 60) if secs_remaining else "?")


def _v2_market_url(platform, market_data=None, asset=None, timeframe=None):
    """Build clickable URL to the market on Polymarket/Limitless."""
    if platform == "polymarket":
        slug = market_data.get("slug", "") if market_data else ""
        condition_id = market_data.get("condition_id", "") if market_data else ""
        if slug:
            return "https://polymarket.com/event/{}".format(slug)
        elif condition_id:
            return "https://polymarket.com/market/{}".format(condition_id)
        return "https://polymarket.com"
    elif platform == "limitless":
        slug = market_data.get("slug", "") if market_data else ""
        if slug:
            return "https://limitless.exchange/markets/{}".format(slug)
        return "https://limitless.exchange"
    return ""

# V2 HEDGE LOGIC
# ═══════════════════════════════════════════════════════════

def _v2_check_hedge(trade, current_structure, candles=None, ptb=None):
    """Check if an open trade should be hedged.
    HEDGE ONLY when there's strong evidence of reversal — not noise.
    
    Requirements for hedge:
    1. Structure must show DOMINANT opposing signals (LH >= 3 AND LL >= 2 for UP trades)
    2. Price must have crossed back through PTB against the trade direction
    3. The grind type must NOT be choppy (choppy = no real trend either way)
    """
    if not trade or not current_structure:
        return False, None

    direction = trade.get("direction")
    hh = current_structure.get("hh_count", 0)
    hl = current_structure.get("hl_count", 0)
    lh = current_structure.get("lh_count", 0)
    ll = current_structure.get("ll_count", 0)
    grind = current_structure.get("grind_type", "")

    # Don't hedge in choppy markets — no clear reversal, just noise
    if grind == "choppy":
        return False, None

    if direction == "UP":
        # Need STRONG reversal: multiple lower highs AND lower lows
        # AND the opposing signals must dominate (more LH/LL than HH/HL)
        if lh >= 3 and ll >= 2 and lh > hh and ll > hl:
            reason = "Strong reversal: LH={} LL={} dominate HH={} HL={} | {}".format(lh, ll, hh, hl, grind)
            
            # Extra confirmation: price crossed back below PTB
            if candles and ptb and ptb > 0:
                current_price = candles[-1]["c"]
                if current_price < ptb:
                    reason += " | Price below PTB"
                    return True, reason
                else:
                    # Structure says reversal but price still above PTB — not confirmed yet
                    return False, None
            
            # No PTB data — rely on structure alone but be strict
            if lh >= 4 and ll >= 3:
                return True, reason
            return False, None

    elif direction == "DOWN":
        if hh >= 3 and hl >= 2 and hh > lh and hl > ll:
            reason = "Strong reversal: HH={} HL={} dominate LH={} LL={} | {}".format(hh, hl, lh, ll, grind)
            
            if candles and ptb and ptb > 0:
                current_price = candles[-1]["c"]
                if current_price > ptb:
                    reason += " | Price above PTB"
                    return True, reason
                else:
                    return False, None
            
            if hh >= 4 and hl >= 3:
                return True, reason
            return False, None

    return False, None


# ═══════════════════════════════════════════════════════════
# V2 PAPER TRADING — BALANCE & DB
# ═══════════════════════════════════════════════════════════

_v2_balances = {
    "polymarket": {"balance": 50.0, "peak_balance": 50.0, "wins": 0, "losses": 0},
    "limitless": {"balance": 50.0, "peak_balance": 50.0, "wins": 0, "losses": 0},
}

def _v2_load_balances():
    """Load balances from DB."""
    try:
        conn = get_db()
        rows = conn.run("SELECT platform, balance, peak_balance, wins, losses FROM v2_balances")
        for r in rows:
            _v2_balances[r[0]] = {
                "balance": float(r[1]), "peak_balance": float(r[2]),
                "wins": int(r[3]), "losses": int(r[4]),
            }
        conn.close()
    except Exception as e:
        print("[V2] Load balances error: {}".format(e))

def _v2_save_balance(platform):
    """Save balance to DB."""
    try:
        bal = _v2_balances.get(platform, {})
        conn = get_db()
        conn.run("""
            UPDATE v2_balances SET balance = :b, peak_balance = :p,
            wins = :w, losses = :l, updated_at = NOW()
            WHERE platform = :plat
        """, b=bal.get("balance", 100), p=bal.get("peak_balance", 100),
            w=bal.get("wins", 0), l=bal.get("losses", 0), plat=platform)
        conn.close()
    except Exception as e:
        print("[V2] Save balance error: {}".format(e))


def _v2_record_paper_trade(platform, timeframe, asset, direction, ptb,
                           entry_odds, stake, entry_note, structure,
                           session_label, volatility_label, prev_candle_str,
                           market_data=None, confidence=None):
    """Record a new paper trade in the database."""
    market_url = _v2_market_url(platform, market_data, asset, timeframe)
    try:
        conn = get_db()
        conn.run("""
            INSERT INTO v2_paper_trades (
                platform, timeframe, asset, direction, ptb, entry_odds,
                entry_price, stake, entry_note, hh_count, hl_count, ll_count, lh_count,
                grind_rate, ptb_distance, session_label, volatility,
                prev_candle, market_id, slug, condition_id,
                up_token, down_token, confidence, market_url, status
            ) VALUES (
                :plat, :tf, :asset, :dir, :ptb, :odds,
                :price, :stake, :note, :hh, :hl, :ll, :lh,
                :grind, :ptb_dist, :sess, :vol,
                :prev, :mid, :slug, :cid,
                :up_tok, :dn_tok, :conf, :murl, 'OPEN'
            )
        """,
            plat=platform, tf=timeframe, asset=asset, dir=direction,
            ptb=ptb, odds=entry_odds, price=_get_binance_price(asset),
            stake=stake, note=entry_note,
            hh=structure.get("hh_count", 0) if structure else 0,
            hl=structure.get("hl_count", 0) if structure else 0,
            ll=structure.get("ll_count", 0) if structure else 0,
            lh=structure.get("lh_count", 0) if structure else 0,
            grind=structure.get("grind_type", "") if structure else "",
            ptb_dist=0, sess=session_label, vol=volatility_label,
            prev=prev_candle_str or "",
            mid=market_data.get("market_id", "") if market_data else "",
            slug=market_data.get("slug", "") if market_data else "",
            cid=market_data.get("condition_id", "") if market_data else "",
            up_tok=market_data.get("up_token", "") if market_data else "",
            dn_tok=market_data.get("down_token", "") if market_data else "",
            conf=str(confidence) if confidence else "",
            murl=market_url,
        )
        conn.close()
        print("[V2] Paper trade: {} {} {} @ {:.0f}c".format(
            platform, asset, direction, (entry_odds or 50)))
    except Exception as e:
        print("[V2] Record trade error: {}".format(e))


# ═══════════════════════════════════════════════════════════
# LIMITLESS LIVE EXECUTION  (DB-backed toggle, FOK orders only)
# ═══════════════════════════════════════════════════════════
#
# Behaviour: the v2_settings row {limitless_live=1} flips the scanner into
# live mode (either-or — paper does NOT record while live is on). On a
# confirmed Limitless signal we place a FOK BUY at the paper limit price as
# a slippage cap; FOK fills the full size at <= price, OR cancels entirely
# — no resting orders, so the GTC adverse-fill dynamic can't occur.
#
# Gates (any failure -> CAPPED/ERROR audit row, no exception bubbles up):
#   * py-limitless installed
#   * LIMITLESS_PRIVATE_KEY env present
#   * USDC approved as a spender on the Limitless exchange contract
#     (one-click from the paper bot page)
#   * Trade stake within LIMITLESS_MAX_TRADE_USDC
#   * Wallet USDC balance at or above LIMITLESS_MIN_BALANCE_USDC (the floor —
#     bot auto-pauses below it and auto-resumes when redemptions top it up)

_lmts_client_singleton = None
_lmts_client_error_logged = False
_lmts_live_cache = {"value": None, "ts": 0.0}   # in-process cache, refreshes every few sec


# ─────────────────────────────────────────────────────────────
# WALL-CLOCK SLOT SCHEDULER
# Used by both the football accumulator (08:00 + 16:00 Lagos) and the
# sports-picks Telegram alerter (08:00 Lagos only).
#
# Lagos = WAT = UTC+1 year-round (no DST). So:
#   08:00 Lagos = 07:00 UTC
#   16:00 Lagos = 15:00 UTC
#
# Each scheduler stores a per-slot completion marker in v2_settings so we
# never double-run a slot, and so a deploy/restart can tell which slots
# already happened.  Boot policy: NO catch-up.  If a slot's wall-clock
# time has already passed today, mark it complete-without-running so the
# scheduler only fires for FUTURE slots (per user requirement).
# ─────────────────────────────────────────────────────────────

def _current_scheduled_slot_id(slot_hours_utc):
    """Return the slot_id of the most recent passed slot today, or None if
    no slot has fired yet today (e.g. 06:00 UTC with hours=(7,15))."""
    from datetime import datetime, time as _time, timezone
    now = datetime.now(timezone.utc)
    today = now.date()
    passed = []
    for h in slot_hours_utc:
        slot_dt = datetime.combine(today, _time(int(h), 0), tzinfo=timezone.utc)
        if slot_dt <= now:
            passed.append("{}_{:02d}".format(today.isoformat(), int(h)))
    return passed[-1] if passed else None


def _init_scheduler_no_catchup(slot_hours_utc, last_slot_key, name):
    """On boot: mark any slot that has already passed today (or yesterday's
    last slot if today's first hasn't passed yet) as completed, without
    running it.  Honours the 'wait for next slot, no catch-up' rule.

    Idempotent: safe to call repeatedly. Only writes a marker if the
    most-recent-passed-slot id is different from what's already stored,
    AND newer (so a later boot can't UN-complete a slot we already ran)."""
    from datetime import datetime, time as _time, timezone, timedelta
    now = datetime.now(timezone.utc)
    today = now.date()
    yesterday = today - timedelta(days=1)
    candidates = []
    for d in (yesterday, today):
        for h in slot_hours_utc:
            dt = datetime.combine(d, _time(int(h), 0), tzinfo=timezone.utc)
            if dt <= now:
                candidates.append(("{}_{:02d}".format(d.isoformat(), int(h)), dt))
    if not candidates:
        return  # nothing has passed yet (extremely early UTC time edge case)
    latest_id, latest_dt = max(candidates, key=lambda x: x[1])
    existing = _settings_get(last_slot_key)
    # Only update if the existing marker is older (or absent). Compares as
    # strings — slot_ids are YYYY-MM-DD_HH so lexicographic == chronological.
    if not existing or existing < latest_id:
        _settings_set(last_slot_key, latest_id)
        print("[{}] boot: scheduler initialized — latest passed slot '{}' marked complete (no catch-up)".format(
            name, latest_id))
    else:
        print("[{}] boot: scheduler resumed — last completed slot was '{}'".format(name, existing))


def _scheduler_run_due_slot(slot_hours_utc, last_slot_key, run_fn, name):
    """Called inside a tick loop. If there is a passed slot today AND we
    haven't completed it yet, invoke run_fn() and mark it complete.
    Returns True if a slot ran, False otherwise. run_fn must be safe to
    call from any thread context."""
    current = _current_scheduled_slot_id(slot_hours_utc)
    if current is None:
        return False
    last = _settings_get(last_slot_key)
    if current == last:
        return False
    print("[{}] firing scheduled slot '{}' (last completed: '{}')".format(
        name, current, last or "(none)"))
    try:
        run_fn()
        _settings_set(last_slot_key, current)
        return True
    except Exception as e:
        # If the run fails we deliberately DO NOT mark the slot complete,
        # so the next tick will retry.  Avoids a fault eating the day.
        import traceback
        print("[{}] slot '{}' run failed: {}".format(name, current, e))
        traceback.print_exc()
        return False


# ─────────────────────────────────────────────────────────────
# SPORTS-PICKS DEDUP
# A given (home, away, pick) combination fires Telegram alerts at most
# once, ever.  The dedup log survives deploys and restarts.
# ─────────────────────────────────────────────────────────────

def _sports_alert_seen(home, away, pick):
    """True if we've already alerted this team/pick combo previously."""
    try:
        conn = get_db()
        rows = list(conn.run(
            "SELECT 1 FROM sports_alerts_log "
            "WHERE home_lower = :h AND away_lower = :a AND pick = :p LIMIT 1",
            h=(home or "").strip().lower(),
            a=(away or "").strip().lower(),
            p=(pick or "").strip()))
        conn.close()
        return bool(rows)
    except Exception as e:
        # If the dedup table read fails, fail OPEN (allow the alert) — it's
        # better to risk a duplicate than to suppress a fresh pick.
        print("[SPORTS] dedup read failed: {}".format(str(e)[:120]))
        return False


def _sports_alert_record(home, away, pick, market_label, url, confidence):
    """Record an alert as sent so it won't fire again. Best-effort INSERT."""
    try:
        conn = get_db()
        conn.run(
            "INSERT INTO sports_alerts_log "
            "(home_lower, away_lower, pick, market_label, url, confidence, alerted_at) "
            "VALUES (:h, :a, :p, :m, :u, :c, :ts)",
            h=(home or "").strip().lower(),
            a=(away or "").strip().lower(),
            p=(pick or "").strip(),
            m=(market_label or "")[:200],
            u=(url or "")[:500],
            c=int(confidence) if confidence is not None else None,
            ts=int(time.time()))
        conn.close()
    except Exception as e:
        print("[SPORTS] dedup write failed: {}".format(str(e)[:120]))


def _settings_get(key, default=None):
    """Read a single v2_settings value. Returns default on any error."""
    try:
        conn = get_db()
        rows = conn.run("SELECT value FROM v2_settings WHERE key = :k", k=key)
        conn.close()
        if rows and rows[0] and rows[0][0] is not None:
            return rows[0][0]
    except Exception:
        pass
    return default


def _settings_set(key, value):
    """Upsert a v2_settings value."""
    try:
        conn = get_db()
        conn.run(
            "INSERT INTO v2_settings (key, value, updated_at) VALUES (:k, :v, NOW()) "
            "ON CONFLICT (key) DO UPDATE SET value = :v, updated_at = NOW()",
            k=key, v=str(value))
        conn.close()
        return True
    except Exception as e:
        print("[SETTINGS] write error {}={}: {}".format(key, value, e))
        return False


def _lmts_live_enabled():
    """True if Limitless live trading is currently switched ON. DB-backed and
    cached for ~3s so the per-signal hot path doesn't hit DB every check.
    Falls back to LIMITLESS_LIVE_BOOTSTRAP only when no DB row exists yet (first
    boot migration); after first read DB becomes the single source of truth."""
    now = time.time()
    if _lmts_live_cache["value"] is not None and (now - _lmts_live_cache["ts"]) < 3.0:
        return _lmts_live_cache["value"]
    v = _settings_get("limitless_live")
    if v is None:
        v = "1" if LIMITLESS_LIVE_BOOTSTRAP else "0"
        _settings_set("limitless_live", v)
    enabled = str(v).strip().lower() in ("1", "true", "yes", "on")
    _lmts_live_cache["value"] = enabled
    _lmts_live_cache["ts"] = now
    return enabled


def _lmts_live_set(enabled):
    """Flip the live switch and bust the in-process cache so the next scanner
    pass picks up the change immediately."""
    ok = _settings_set("limitless_live", "1" if enabled else "0")
    _lmts_live_cache["value"] = bool(enabled) if ok else _lmts_live_cache["value"]
    _lmts_live_cache["ts"] = time.time() if ok else _lmts_live_cache["ts"]
    return ok


def _lmts_get_client():
    """Lazily import py-limitless and authenticate. Returns None on any
    failure — caller treats None as 'live not available, keep going on paper'.
    Cached after the first successful auth."""
    global _lmts_client_singleton, _lmts_client_error_logged
    if _lmts_client_singleton is not None:
        return _lmts_client_singleton
    if not LIMITLESS_PRIV_KEY:
        if not _lmts_client_error_logged:
            print("[LMTS-LIVE] no LIMITLESS_PRIVATE_KEY — live disabled")
            _lmts_client_error_logged = True
        return None
    try:
        pass  # limitless_sdk inlined above
    except Exception as e:
        if not _lmts_client_error_logged:
            print("[LMTS-LIVE] py-limitless not installed ({}). Add `py-limitless` to requirements to enable live.".format(e))
            _lmts_client_error_logged = True
        return None
    try:
        client = Limitless(private_key=LIMITLESS_PRIV_KEY, wallet_type="eoa")
        client.authenticate()
        print("[LMTS-LIVE] client authenticated as {}".format(client.address))
        _lmts_client_singleton = client
        return client
    except Exception as e:
        if not _lmts_client_error_logged:
            print("[LMTS-LIVE] authenticate error: {}".format(e))
            _lmts_client_error_logged = True
        return None


def _lmts_today_spend_usdc():
    """Sum of stake_usdc on v2_live_trades from the last 24h. Informational
    only (shown on the dashboard) — NOT a gate. The gate is the balance floor
    in _lmts_get_balance_usdc(). Defensive: any DB error returns 0."""
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT COALESCE(SUM(stake_usdc), 0) FROM v2_live_trades "
            "WHERE fired_at > NOW() - INTERVAL '24 hours' "
            "AND fill_status IN ('FILLED', 'CANCELLED')")
        conn.close()
        if rows and rows[0] and rows[0][0] is not None:
            return float(rows[0][0])
    except Exception as e:
        print("[LMTS-LIVE] daily spend lookup error: {}".format(e))
    return 0.0


# Wallet balance cache — RPC reads are ~500ms so we cache for 30s. Winning
# redemptions invalidate freshness naturally on the next read after the cache
# expires (worst case 30s lag before a topped-up wallet starts trading again).
_lmts_balance_cache = {"value": None, "ts": 0.0}


def _lmts_get_balance_usdc(force=False):
    """Read the wallet's USDC balance on Base. Returns float USDC or None if
    the RPC fails (the placer treats None as 'pause for safety')."""
    import time as _t
    now = _t.time()
    if (not force and _lmts_balance_cache["value"] is not None
            and (now - _lmts_balance_cache["ts"]) < 30.0):
        return _lmts_balance_cache["value"]
    if not LIMITLESS_PRIV_KEY:
        return None
    try:
        pass  # limitless_sdk inlined above
        from web3 import Web3
        from eth_account import Account
        w3 = Web3(Web3.HTTPProvider(LIMITLESS_BASE_RPC))
        addr = Account.from_key(LIMITLESS_PRIV_KEY).address
        balance_raw = get_usdc_balance(w3, addr)
        value = float(balance_raw) / (10 ** USDC_DECIMALS)
        _lmts_balance_cache["value"] = value
        _lmts_balance_cache["ts"] = now
        return value
    except Exception as e:
        print("[LMTS-LIVE] balance read error: {}".format(str(e)[:120]))
        return None


def _lmts_extract_tokens(market):
    """Extract (up_token, down_token) from a Limitless /markets/{slug} response.
    The Limitless API doesn't publish a stable field name for token IDs and the
    SDK doesn't reference one directly, so we try the conventions used by
    CTF-fork markets in order of likelihood. Returns ("", "") if none match.

    YES (UP) is outcome index 0; NO (DOWN) is index 1 — same convention as
    Polymarket (confirmed by limitless_sdk.redemption.get_redeemable_positions
    which uses winningOutcomeIndex 0=YES, 1=NO)."""
    if not isinstance(market, dict):
        return "", ""

    # 1) Polymarket-style: clobTokenIds = ["yes_tok", "no_tok"] OR a JSON string.
    clob = market.get("clobTokenIds")
    if isinstance(clob, str):
        try:
            clob = json.loads(clob)
        except Exception:
            clob = None
    if isinstance(clob, list) and len(clob) >= 2:
        return str(clob[0]), str(clob[1])

    # 2) Limitless real schema (confirmed against api.limitless.exchange in
    #    Jun 2026): tokens is an OBJECT with "yes" / "no" keys, each mapped to
    #    a numeric ERC-1155 token-id string.
    #        "tokens": {"yes": "51293...", "no": "88391..."}
    #    YES = UP, NO = DOWN — matches the prices array convention
    #    (prices[0] = YES price, prices[1] = NO price).
    toks = market.get("tokens")
    if isinstance(toks, dict):
        up = str(toks.get("yes") or toks.get("YES")
                 or toks.get("up")  or toks.get("UP") or "")
        dn = str(toks.get("no")  or toks.get("NO")
                 or toks.get("down") or toks.get("DOWN") or "")
        if up or dn:
            return up, dn

    # 3) Legacy / Polymarket-style: tokens is a LIST of dicts with token_id +
    #    outcome labels. Kept as a defensive fallback in case Limitless ever
    #    returns this shape on a different market type.
    toks = market.get("tokens")
    if isinstance(toks, list) and len(toks) >= 2:
        up = dn = ""
        for t in toks:
            if not isinstance(t, dict):
                continue
            tid = str(t.get("token_id") or t.get("tokenId") or t.get("id") or "")
            label = str(t.get("outcome") or t.get("label") or t.get("name") or "").upper()
            if not tid:
                continue
            if label in ("YES", "UP", "ABOVE") and not up:
                up = tid
            elif label in ("NO", "DOWN", "BELOW") and not dn:
                dn = tid
        if up or dn:
            # Fallback: if only one labelled, assume the other is the remaining
            if not up and len(toks) >= 2:
                for t in toks:
                    tid = str((t or {}).get("token_id") or (t or {}).get("tokenId") or "")
                    if tid and tid != dn:
                        up = tid; break
            if not dn and len(toks) >= 2:
                for t in toks:
                    tid = str((t or {}).get("token_id") or (t or {}).get("tokenId") or "")
                    if tid and tid != up:
                        dn = tid; break
            return up, dn

    # 3) positionIds: [yes_id, no_id]
    pids = market.get("positionIds") or market.get("position_ids")
    if isinstance(pids, list) and len(pids) >= 2:
        return str(pids[0]), str(pids[1])

    # 4) Nested venue/clob/outcomes containers
    for container_key in ("clob", "outcomes", "venue"):
        sub = market.get(container_key)
        if isinstance(sub, dict):
            up, dn = _lmts_extract_tokens(sub)   # one-level recurse
            if up or dn:
                return up, dn
        if isinstance(sub, list) and len(sub) >= 2:
            up = str(((sub[0] or {}).get("tokenId") or (sub[0] or {}).get("token_id") or "")
                     if isinstance(sub[0], dict) else sub[0])
            dn = str(((sub[1] or {}).get("tokenId") or (sub[1] or {}).get("token_id") or "")
                     if isinstance(sub[1], dict) else sub[1])
            if up or dn:
                return up, dn

    return "", ""


def _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                            market_slug, token_id, limit_cents, stake,
                            shares, fill_status, order_id=None,
                            fill_price=None, filled_size=None,
                            raw_response=None, error_message=None,
                            condition_id=None):
    """Single audit row per live attempt — FILLED / CANCELLED / ERROR / CAPPED.
    condition_id is captured so the resolver/redeemer can find the on-chain
    market without re-fetching from the API later."""
    try:
        conn = get_db()
        conn.run("""
            INSERT INTO v2_live_trades (
                paper_trade_id, platform, timeframe, asset, direction,
                market_slug, token_id, condition_id, limit_price_cents,
                stake_usdc, size_shares, fill_status, order_id,
                fill_price_cents, filled_size, raw_response, error_message
            ) VALUES (
                :ptid, 'limitless', :tf, :asset, :dir,
                :slug, :tok, :cid, :lim,
                :stake, :shares, :st, :oid,
                :fp, :fs, :raw, :err
            )
        """,
            ptid=paper_trade_id, tf=tf_label, asset=asset, dir=direction,
            slug=market_slug, tok=token_id, cid=(condition_id or None),
            lim=limit_cents, stake=stake, shares=shares, st=fill_status,
            oid=order_id, fp=fill_price, fs=filled_size,
            raw=(json.dumps(raw_response)[:4000] if raw_response else None),
            err=(error_message[:1000] if error_message else None))
        conn.close()
    except Exception as e:
        print("[LMTS-LIVE] record error: {}".format(e))


def _lmts_place_live(paper_trade_id, market_data, asset, tf_label, direction,
                     limit_price_cents, stake_usdc):
    """Place an FOK BUY mirroring a just-recorded paper trade. All paths record
    to v2_live_trades for audit (CAPPED for cap rejections, ERROR for failures).
    Returns the fill_status string. NEVER raises — paper recording upstream
    must be unaffected by any live failure."""
    market_slug = (market_data or {}).get("slug") or ""
    token_id = ((market_data or {}).get("up_token") if direction == "UP"
                else (market_data or {}).get("down_token")) or ""
    condition_id = (market_data or {}).get("condition_id") or ""

    # The lightweight /markets/active/slugs feed used by the V2 scanner does
    # NOT include conditionId OR the per-outcome token IDs. The SDK's
    # client.buy() needs token_id; the redeemer needs condition_id. So we
    # lazily fetch the full market once at order time and extract both.
    # ~1 extra HTTP call per live trade — negligible at <20/day. We CAPTURE
    # the raw response (status + body keys + first 2000 chars) so when the
    # extractor returns ("","") we can see in the dashboard exactly what
    # Limitless sent back and patch _lmts_extract_tokens against the real
    # schema.
    _fetch_debug = None
    if (not condition_id or not token_id) and market_slug:
        try:
            import requests as _req
            _hdrs = {}
            _api_key = os.environ.get("LIMITLESS_API_KEY", "").strip()
            if _api_key:
                # Limitless v2 uses X-API-Key for the docs-shown endpoint; pass
                # it through if the operator set one. Without it most public
                # endpoints still work but positionIds may be omitted.
                _hdrs["X-API-Key"] = _api_key
            _r = _req.get("{}/markets/{}".format(LIMITLESS_API, market_slug),
                          headers=_hdrs, timeout=6)
            _status = _r.status_code
            try:
                _m = _r.json() if _status == 200 else {}
            except Exception:
                _m = {}
            _body_text = (_r.text or "")[:2000]
            _fetch_debug = {
                "status": _status,
                "url": "{}/markets/{}".format(LIMITLESS_API, market_slug),
                "auth_header_sent": bool(_api_key),
                "top_level_keys": sorted(list(_m.keys())) if isinstance(_m, dict) else None,
                "body_first_2000": _body_text,
            }
            if _status == 200 and isinstance(_m, dict):
                if not condition_id:
                    condition_id = (_m.get("conditionId")
                                    or _m.get("condition_id") or "")
                if not token_id:
                    up_tok, dn_tok = _lmts_extract_tokens(_m)
                    token_id = up_tok if direction == "UP" else dn_tok
        except Exception as _e:
            _fetch_debug = {"status": None, "exception": str(_e)[:300],
                            "url": "{}/markets/{}".format(LIMITLESS_API, market_slug)}
            print("[LMTS-LIVE] market fetch failed for {}: {}".format(
                market_slug[:40], str(_e)[:80]))

    # Clamp stake to per-trade cap; reject if essential data missing.
    if not market_slug or not token_id:
        # Promote the debug info to raw_response so it shows in the dashboard
        # — without it, "missing slug or token_id" is unactionable.
        _err = "missing slug or token_id"
        if _fetch_debug:
            _err = "missing slug or token_id (HTTP {} keys={})".format(
                _fetch_debug.get("status"), _fetch_debug.get("top_level_keys"))
        _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                                market_slug, token_id, limit_price_cents,
                                stake_usdc, 0.0, "ERROR",
                                error_message=_err,
                                raw_response=_fetch_debug,
                                condition_id=condition_id)
        return "ERROR"

    capped_stake = min(float(stake_usdc), LIMITLESS_MAX_TRADE_USDC)
    if capped_stake <= 0 or limit_price_cents <= 0:
        _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                                market_slug, token_id, limit_price_cents,
                                stake_usdc, 0.0, "CAPPED",
                                error_message="stake or price non-positive",
                                condition_id=condition_id)
        return "CAPPED"

    # Balance floor gate — read live USDC balance on Base and pause if below
    # the floor. Replaces the old daily-cap model. Failing closed: if the RPC
    # read returns None (transient failure), we PAUSE rather than guess.
    # The cache (30s) ensures successful redemptions resume trading promptly.
    balance = _lmts_get_balance_usdc()
    if balance is None:
        _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                                market_slug, token_id, limit_price_cents,
                                stake_usdc, 0.0, "CAPPED",
                                error_message="balance read failed (RPC) — pausing for safety",
                                condition_id=condition_id)
        print("[LMTS-LIVE] balance read failed — pausing this signal")
        return "CAPPED"
    if balance < LIMITLESS_MIN_BALANCE_USDC:
        _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                                market_slug, token_id, limit_price_cents,
                                stake_usdc, 0.0, "CAPPED",
                                error_message="balance ${:.2f} below floor ${:.2f} — paused".format(
                                    balance, LIMITLESS_MIN_BALANCE_USDC),
                                condition_id=condition_id)
        print("[LMTS-LIVE] balance ${:.2f} < floor ${:.2f} — paused, waiting for top-up".format(
            balance, LIMITLESS_MIN_BALANCE_USDC))
        return "CAPPED"
    # Also guard against trying to spend more than we have (in case stake > balance)
    if balance < capped_stake:
        _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                                market_slug, token_id, limit_price_cents,
                                stake_usdc, 0.0, "CAPPED",
                                error_message="balance ${:.2f} insufficient for ${:.2f} stake".format(
                                    balance, capped_stake),
                                condition_id=condition_id)
        return "CAPPED"

    client = _lmts_get_client()
    if client is None:
        _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                                market_slug, token_id, limit_price_cents,
                                capped_stake, 0.0, "ERROR",
                                error_message="client unavailable",
                                condition_id=condition_id)
        return "ERROR"

    # shares = USDC budget / (price in dollars). e.g. $1 @ 76.9c -> ~1.3004 shares.
    #
    # CRITICAL TICK QUANTIZATION (Limitless rule discovered Jun 2026):
    #   The exchange requires (price × contracts) to be a whole number of
    #   micro-USDC, where contracts = shares × 10⁶.  Limitless prices can be
    #   3-decimal (e.g. 0.769) — that means contracts must be a multiple of
    #   1000 (so the product clears the 1000 denominator).  Equivalently:
    #   shares must be a multiple of 0.001.
    #
    #   The old code rounded to 4 decimals (e.g. 1.3004) — that fails on
    #   3-decimal prices with HTTP 400 "Order amounts tick violation:
    #   price(0.769) * contracts(1300400) = 1000007.6 is not a whole (int)".
    #
    #   We FLOOR (not round) so we never overshoot the stake budget.
    import math as _math
    raw_shares = capped_stake / max(limit_price_cents / 100.0, 0.01)
    shares = _math.floor(raw_shares * 1000) / 1000.0

    # ORDER_TYPE_FOK is a module-level constant (defined ~line 176 alongside
    # the rest of the inlined SDK constants). No try/except shim needed.

    try:
        resp = client.buy(
            market_slug=market_slug,
            token_id=str(token_id),
            price_cents=float(limit_price_cents),
            amount=float(shares),
            token_type=("YES" if direction == "UP" else "NO"),
            order_type=ORDER_TYPE_GTC,  # GTC: rest on book until matched OR
                                        # cancelled (by us at candle expiry, or
                                        # by the exchange when the market
                                        # settles). Matches paper-trading
                                        # semantics where the limit is honoured
                                        # if the book ever touches it. The
                                        # poll loop (_lmts_poll_pending_orders)
                                        # promotes PENDING → FILLED/CANCELLED.
        )
    except Exception as e:
        _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                                market_slug, token_id, limit_price_cents,
                                capped_stake, shares, "ERROR",
                                error_message=str(e)[:1000],
                                condition_id=condition_id)
        print("[LMTS-LIVE] order error {} {} {}: {}".format(
            asset, tf_label, direction, str(e)[:200]))
        return "ERROR"

    # Parse common response shapes defensively — exact response is API-shaped
    # and we record the raw JSON for any field we don't pull out.
    resp_d = resp if isinstance(resp, dict) else {}

    # ─── Limitless's actual response shape (confirmed via diagnostic dump, Jun 2026) ───
    # Wrapped object: { "order": {...}, "execution": {...} }
    #
    #   order.id              → the UUID order id we save
    #   order.orderType       → "GTC" / "FOK" / etc.
    #   order.price           → fill price (or limit if not filled)
    #   execution.matched     → bool
    #   execution.settlementStatus → "UNMATCHED" | "MATCHED" | "PARTIAL" | "CANCELLED"
    #   execution.totalsRaw.contractsNet → contracts filled (micro-shares, 1e6 = 1 share)
    #   execution.totalsRaw.usdNet       → USDC paid (micro-USDC)
    #
    # CRITICAL: UNMATCHED on a GTC order does NOT mean the order failed.
    # It means the order is RESTING on the book, waiting for a maker. The
    # poll loop will promote it to FILLED when matched, or CANCELLED when
    # the candle expires. UNMATCHED on FOK = killed (correct CANCELLED).
    if isinstance(resp_d.get("order"), dict) and "execution" in resp_d:
        order_obj = resp_d.get("order") or {}
        exec_obj  = resp_d.get("execution") or {}
        order_id  = str(order_obj.get("id") or "")
        order_type_resp = str(order_obj.get("orderType") or "GTC").upper()
        matched   = bool(exec_obj.get("matched"))
        settlement = str(exec_obj.get("settlementStatus") or "").upper()

        totals = exec_obj.get("totalsRaw") or {}
        try:
            contracts_net = float(totals.get("contractsNet", 0) or 0)  # micro-shares
            usd_net       = float(totals.get("usdNet", 0) or 0)         # micro-USDC
        except Exception:
            contracts_net = usd_net = 0.0
        filled_size = contracts_net / 1_000_000.0  # → shares (e.g. 1300000 → 1.3)
        # Avg fill price in cents: (usd_net / contracts_net) gives dollars; ×100 = cents
        fill_price = None
        if contracts_net > 0 and usd_net > 0:
            fill_price = (usd_net / contracts_net) * 100.0

        if matched or settlement in ("MATCHED", "FILLED", "PARTIAL",
                                     "PARTIALLY_FILLED", "MINED", "SETTLED",
                                     "COMPLETED", "FINALIZED"):
            # PARTIAL counts as filled — we keep the partial position; the
            # resolver settles whatever we hold at market resolution.
            fill_status = "FILLED"
        elif settlement == "UNMATCHED" and order_type_resp in ("GTC", "GTD"):
            # Order is resting on the book — poll loop will check it.
            fill_status = "PENDING"
        elif settlement in ("CANCELLED", "CANCELED", "EXPIRED", "REJECTED"):
            fill_status = "CANCELLED"
        else:
            # FOK + UNMATCHED, or any unrecognized settlement state.
            fill_status = "CANCELLED"
            try:
                print("[LMTS-LIVE] CANCELLED (wrapped resp, settlement={}, orderType={}): {}".format(
                    settlement, order_type_resp, json.dumps(resp_d)[:400]))
            except Exception:
                pass

        _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                                market_slug, token_id, limit_price_cents,
                                capped_stake, shares, fill_status,
                                order_id=order_id or None,
                                fill_price=fill_price, filled_size=filled_size,
                                raw_response=resp_d, condition_id=condition_id)
        print("[LMTS-LIVE] {} {} {} {} @ {:.1f}c x {:.3f} shares -> {} (settlement={})".format(
            fill_status, asset, tf_label, direction, limit_price_cents,
            shares, order_id[:18] if order_id else "(no id)", settlement))
        return fill_status

    # ─── Legacy / flat response-shape fallback (kept for safety) ───
    order_id = (resp_d.get("orderId") or resp_d.get("order_id")
                or resp_d.get("id") or resp_d.get("uuid") or "")
    matches = (resp_d.get("makerMatches") or resp_d.get("maker_matches")
               or resp_d.get("matches") or resp_d.get("fills")
               or resp_d.get("executions") or [])
    filled_size = sum(float(m.get("size", m.get("amount", m.get("quantity", 0))) or 0)
                      for m in matches) if isinstance(matches, list) else 0.0
    if filled_size <= 0:
        top_filled = (resp_d.get("filledSize") or resp_d.get("filled_size")
                      or resp_d.get("filledAmount") or resp_d.get("filledQuantity")
                      or resp_d.get("executedQuantity"))
        try:
            if top_filled is not None:
                filled_size = float(top_filled)
        except Exception:
            pass
    fill_price = None
    if matches and isinstance(matches, list):
        prices = [float(m.get("price", 0) or 0) for m in matches if m.get("price")]
        if prices:
            fill_price = sum(prices) / len(prices)
    if fill_price is None:
        top_price = (resp_d.get("avgPrice") or resp_d.get("avg_price")
                     or resp_d.get("fillPrice") or resp_d.get("fill_price")
                     or resp_d.get("executedPrice"))
        try:
            if top_price is not None:
                fill_price = float(top_price)
        except Exception:
            pass
    status = (resp_d.get("status") or "").upper()
    bool_filled = bool(resp_d.get("filled") or resp_d.get("isFilled")
                       or resp_d.get("matched") or resp_d.get("executed"))
    if (filled_size > 0 or bool_filled
            or status in ("FILLED", "MATCHED", "EXECUTED", "DONE",
                          "COMPLETE", "COMPLETED", "SUCCESS")):
        fill_status = "FILLED"
    elif status in ("OPEN", "PENDING", "ACTIVE", "RESTING",
                    "NEW", "PLACED", "ACCEPTED", "PARTIALLY_FILLED",
                    "WORKING", "LIVE"):
        fill_status = "PENDING"
    elif not status and order_id:
        fill_status = "PENDING"
    else:
        fill_status = "CANCELLED"
        try:
            print("[LMTS-LIVE] CANCELLED with raw response: {}".format(
                json.dumps(resp_d)[:600]))
        except Exception:
            print("[LMTS-LIVE] CANCELLED with raw response (non-JSON): {}".format(
                str(resp)[:600]))

    _lmts_record_live_trade(paper_trade_id, asset, tf_label, direction,
                            market_slug, token_id, limit_price_cents,
                            capped_stake, shares, fill_status,
                            order_id=str(order_id) if order_id else None,
                            fill_price=fill_price, filled_size=filled_size,
                            raw_response=resp_d, condition_id=condition_id)
    print("[LMTS-LIVE] {} {} {} {} @ {:.1f}c x {:.3f} shares -> {}".format(
        fill_status, asset, tf_label, direction, limit_price_cents, shares, order_id or "(no id)"))
    return fill_status


# ═══════════════════════════════════════════════════════════
# V2 ORDER BOOK READING (for paper odds accuracy)
# ═══════════════════════════════════════════════════════════

def _v2_get_live_odds(market_data, direction):
    """Read live price from Polymarket CLOB using get_price().
    NOTE: get_order_book() is BROKEN (GitHub Issue #180). Use get_price() instead.
    Single call — fast fail on 404 (expired market) or timeout."""
    client = _get_poly_client()
    if not client or not market_data:
        return None

    try:
        token = market_data.get("up_token") if direction == "UP" else market_data.get("down_token")
        if not token:
            return None

        # Single call — BUY side = best ask (what we'd pay to buy)
        buy_price = None
        try:
            buy_result = client.get_price(str(token), side="BUY")
            if buy_result:
                if isinstance(buy_result, dict):
                    buy_price = float(buy_result.get("price", 0))
                elif isinstance(buy_result, (int, float, str)):
                    buy_price = float(buy_result)
        except Exception as e:
            err_str = str(e)
            if "404" in err_str or "No orderbook" in err_str:
                # Market expired — don't log spam, just return None
                return None
            if "timed out" in err_str.lower() or "timeout" in err_str.lower():
                return None
            print("[V2] POLY price error: {}".format(err_str[:80]))
            return None

        if buy_price and 0.01 <= buy_price <= 0.99:
            asset = market_data.get("asset", "?")
            tf = market_data.get("timeframe", "?")
            print("[V2] POLY PRICE {} {} {} = {:.0f}c".format(asset, tf, direction, buy_price * 100))
            return round(buy_price * 100, 1)

    except Exception as e:
        print("[V2] Poly price error: {}".format(e))
    return None


# ═══════════════════════════════════════════════════════════
# LIMITLESS MARKET DISCOVERY + ORDERBOOK
# ═══════════════════════════════════════════════════════════

_LMTS_TF_DIAG = [0]  # cap Limitless timeframe-detection diagnostics


def _limitless_fetch_markets():
    """Fetch active crypto Up/Down markets from Limitless Exchange.
    Uses GET /markets/active/slugs for lightweight discovery,
    then GET /markets/{slug} for full details."""
    import requests as req
    markets = []
    now = datetime.now(timezone.utc)

    try:
        # Get all active market slugs with metadata
        r = req.get("{}/markets/active/slugs".format(LIMITLESS_API), timeout=12)
        if r.status_code != 200:
            print("[LMTS] Active slugs status: {}".format(r.status_code))
            return markets

        slugs_data = r.json()
        if not isinstance(slugs_data, list):
            return markets

        for entry in slugs_data:
            slug = entry.get("slug", "")
            ticker = entry.get("ticker", "")
            strike = entry.get("strikePrice")
            deadline = entry.get("deadline")

            # Filter: crypto Up/Down markets only
            slug_lower = slug.lower()
            if not ticker:
                continue

            # Detect asset from ticker
            asset = ticker.upper() if ticker.upper() in BINANCE_MAP else None
            if not asset:
                continue

            # Must be an above/below or up/down market
            is_updown = any(kw in slug_lower for kw in ["above", "below", "up-or-down", "updown"])
            if not is_updown:
                continue

            # Parse deadline for timeframe detection
            if not deadline:
                continue
            try:
                deadline_dt = datetime.fromisoformat(deadline.replace("Z", "+00:00"))
            except:
                continue
            mins_left = (deadline_dt - now).total_seconds() / 60
            if mins_left <= 0:
                continue

            # Parse strike price as PTB
            baseline = None
            if strike:
                try:
                    baseline = float(strike)
                except:
                    pass

            # Detect timeframe from slug patterns or deadline proximity.
            # Limitless slugs look like: {asset}-up-or-down-{period}-{ts}
            timeframe = None
            if "hourly" in slug_lower or "-1h-" in slug_lower:
                timeframe = "1H"
            elif "daily" in slug_lower or "-1d-" in slug_lower:
                timeframe = "DAILY"
            elif any(k in slug_lower for k in ("-15m", "15-min", "15min", "quarter")):
                timeframe = "15M"
            elif any(k in slug_lower for k in ("-5m", "5-min", "5min")):
                timeframe = "5M"
            elif any(k in slug_lower for k in ("weekly", "-1w-", "-7d-")):
                timeframe = "WEEKLY"
            elif "-on-" in slug_lower or slug_lower.endswith("-on") or "daily" in slug_lower:
                timeframe = "DAILY"

            # Fallback: estimate from time remaining (only if slug gave nothing)
            if not timeframe:
                if mins_left <= 1:
                    continue
                elif mins_left <= 7:
                    timeframe = "5M"
                elif mins_left <= 20:
                    timeframe = "15M"
                elif mins_left <= 75:
                    timeframe = "1H"
                elif mins_left <= 1500:
                    timeframe = "DAILY"
                else:
                    timeframe = "WEEKLY"
                # One-time visibility into real slug patterns we couldn't name
                if _LMTS_TF_DIAG[0] < 10:
                    _LMTS_TF_DIAG[0] += 1
                    print("[LMTS-TF-DIAG] slug='{}' mins_left={:.0f} -> guessed {}".format(
                        slug[:50], mins_left, timeframe))

            if not timeframe:
                continue

            # Handle group markets (nested)
            nested = entry.get("markets")
            if nested and isinstance(nested, list):
                for nm in nested:
                    ns = nm.get("slug", "")
                    if ns:
                        markets.append({
                            "slug": ns, "asset": asset, "timeframe": timeframe,
                            "baseline": baseline, "expiry_dt": deadline_dt,
                            "mins_left": mins_left, "market_id": ns,
                            "platform": "limitless",
                        })
            else:
                markets.append({
                    "slug": slug, "asset": asset, "timeframe": timeframe,
                    "baseline": baseline, "expiry_dt": deadline_dt,
                    "mins_left": mins_left, "market_id": slug,
                    "platform": "limitless",
                })

    except Exception as e:
        print("[LMTS] Fetch markets error: {}".format(e))

    if markets:
        print("[LMTS] Found {} crypto markets".format(len(markets)))
    return markets


def _limitless_get_orderbook_odds(slug, direction):
    """Read Limitless order book for a market.
    GET /markets/{slug}/orderbook → {bids, asks, adjustedMidpoint, lastTradePrice}
    Returns odds as percentage (e.g. 72.0 for 72c) or None."""
    import requests as req
    try:
        r = req.get("{}/markets/{}/orderbook".format(LIMITLESS_API, slug), timeout=8)
        if r.status_code != 200:
            return None
        book = r.json()
        if not book:
            return None

        asks = book.get("asks", [])
        bids = book.get("bids", [])
        mid = book.get("adjustedMidpoint")
        ltp = book.get("lastTradePrice")

        best_ask = float(asks[0].get("price", 0)) if asks else None
        best_bid = float(bids[0].get("price", 0)) if bids else None

        print("[LMTS] BOOK {} {} | ask={} bid={} mid={} ltp={} depth={}a/{}b".format(
            slug[:30], direction,
            "{:.4f}".format(best_ask) if best_ask else "None",
            "{:.4f}".format(best_bid) if best_bid else "None",
            "{:.4f}".format(float(mid)) if mid else "None",
            "{:.4f}".format(float(ltp)) if ltp else "None",
            len(asks), len(bids)))

        # For UP/YES direction, read the asks (price to buy YES shares)
        # For DOWN/NO direction, we buy NO shares
        # Limitless: asks = sell orders for YES, bids = buy orders for YES
        # To buy YES: we take the best ask
        # To buy NO: equivalent to selling YES at best bid, OR 1 - best_ask for NO
        if direction == "UP":
            if best_ask and 0.01 <= best_ask <= 0.99:
                return round(best_ask * 100, 1)
        else:
            # DOWN = buy NO shares. Price of NO = 1 - price of YES
            if best_ask and 0.01 <= best_ask <= 0.99:
                no_price = 1.0 - best_ask
                if 0.01 <= no_price <= 0.99:
                    return round(no_price * 100, 1)

        # Fallback to midpoint
        if mid:
            mid_f = float(mid)
            if direction == "UP":
                return round(mid_f * 100, 1)
            else:
                return round((1.0 - mid_f) * 100, 1)

        # Last trade price as final fallback
        if ltp:
            ltp_f = float(ltp)
            if direction == "UP":
                return round(ltp_f * 100, 1)
            else:
                return round((1.0 - ltp_f) * 100, 1)

    except Exception as e:
        print("[LMTS] Orderbook error {}: {}".format(slug, e))
    return None


def _v2_get_odds(platform, market_data, direction):
    """Unified odds reading — routes to the right order book per platform."""
    if platform == "polymarket":
        return _v2_get_live_odds(market_data, direction)
    elif platform == "limitless":
        slug = market_data.get("slug", "") if market_data else ""
        if slug:
            return _limitless_get_orderbook_odds(slug, direction)
    return None


def _v2_calc_limit_price(book_ask, confidence):
    """Calculate limit order price. Per spec: typical entry 70-90c.
    Place limit slightly below ask to get filled on any dip.
    Minimum limit: 65c — below that there's not enough confirmation."""

    if not book_ask or book_ask <= 0:
        return None, False

    # Below 65c means the market isn't confirming this direction — skip
    if book_ask < 65:
        return None, False

    # Place limit 0.5-2c below ask depending on confidence
    if book_ask >= 90:
        # Very high odds — limit just barely below
        limit = book_ask - 0.5 if confidence >= 85 else book_ask - 1.0
    elif book_ask >= 75:
        # Good range — small undercut
        limit = book_ask - 1.0 if confidence >= 85 else book_ask - 2.0
    else:
        # 65-75c range — slightly more undercut
        limit = book_ask - 2.0 if confidence >= 85 else book_ask - 3.0

    # Floor at 65c
    limit = max(65, limit)

    return round(limit, 1), True


# ═══════════════════════════════════════════════════════════
# V2 RESOLUTION — Check outcomes of paper trades
# ═══════════════════════════════════════════════════════════

_SB_LMTS_DIAG = [0]  # cap Limitless resolution diagnostics per process
_POLY_RESOLVE_DIAG = [0]  # cap Polymarket resolution diagnostics per process


def _v2_resolve_short_from_candle(asset, slug, ptb):
    """Resolve a synthetic 15M/5M Polymarket up/down trade from the ACTUAL
    closed Binance candle — this matches how the market resolves (price at the
    candle boundary vs the price-to-beat / Chainlink open). The slug encodes the
    window start, e.g. 'btc-updown-15m-1780049700'. Returns
    ('UP'|'DOWN'|'FLAT', close_price) once that candle has closed, else None
    (caller then leaves it for the gated price fallback)."""
    import re as _re
    m = _re.search(r"-updown-(5m|15m)-(\d+)", slug or "")
    if not m or not ptb or ptb <= 0:
        return None
    tf_slug = m.group(1)
    ws = int(m.group(2))
    tf_sec = 900 if tf_slug == "15m" else 300
    interval = "15m" if tf_slug == "15m" else "5m"
    candle_close_ts = ws + tf_sec
    if int(time.time()) < candle_close_ts:
        return None  # the candle hasn't closed yet — wait
    candles = _fetch_binance_candles(asset, interval=interval, limit=30)
    if not candles:
        if _POLY_RESOLVE_DIAG[0] < 12:
            _POLY_RESOLVE_DIAG[0] += 1
            print("[POLY-RESOLVE-DIAG] candle: no Binance candles for {} {}".format(asset, interval))
        return None
    target_open_ms = ws * 1000
    close_px = None
    for c in candles:
        if c["t"] == target_open_ms:
            close_px = c["c"]
            break
    if close_px is None:
        if _POLY_RESOLVE_DIAG[0] < 12:
            _POLY_RESOLVE_DIAG[0] += 1
            ts = [c["t"] // 1000 for c in candles]
            print("[POLY-RESOLVE-DIAG] candle: window {} not in feed (have {}..{}) for {}".format(
                ws, min(ts), max(ts), asset))
        return None
    # Binance may not be the oracle the platform used (some pairs resolve via
    # Chainlink). On a decisive move every source agrees; only TIGHT moves are
    # dangerous, so we refuse to call those from Binance and leave them to the
    # authoritative platform read. ~0.05% margin ≈ wider than normal cross-oracle
    # deviation for majors.
    margin = abs(close_px - ptb) / ptb if ptb else 0
    if margin < 0.0005:
        if _POLY_RESOLVE_DIAG[0] < 12:
            _POLY_RESOLVE_DIAG[0] += 1
            print("[POLY-RESOLVE-DIAG] candle: {} too tight ({:.3f}%) close={} ptb={} — waiting".format(
                asset, margin * 100, close_px, ptb))
        return None
    if close_px > ptb:
        return ("UP", close_px)
    if close_px < ptb:
        return ("DOWN", close_px)
    return ("FLAT", close_px)


def _poly_outcome_from_market(market):
    """Read the resolved winner from a Polymarket market object.
    Returns 'UP'|'DOWN' if the market has closed with a clear winner, else None."""
    if not market or not market.get("closed"):
        return None
    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try: outcome_prices = json.loads(outcome_prices)
        except: outcome_prices = None
    if not (isinstance(outcome_prices, list) and len(outcome_prices) >= 2):
        return None
    outcomes_raw = market.get("outcomes")
    if isinstance(outcomes_raw, str):
        try: outcomes_raw = json.loads(outcomes_raw)
        except: outcomes_raw = None
    up_idx = 0
    if isinstance(outcomes_raw, list) and len(outcomes_raw) >= 2:
        if str(outcomes_raw[0]).lower().strip() in ("no", "down", "below"):
            up_idx = 1
    try:
        up_price = float(outcome_prices[up_idx])
    except (ValueError, TypeError):
        return None
    if up_price > 0.9:
        return "UP"
    if up_price < 0.1:
        return "DOWN"
    return None


def _poly_read_resolution(slug, cond_id):
    """Read the resolved winner from Polymarket for a market that has CLOSED.
    Per Gamma docs, list queries default to closed=false and silently exclude
    resolved markets, so we must pass closed=true / use the slug path endpoints.
    Returns ('UP'|'DOWN', market) or (None, market|None)."""
    import requests as req
    urls = []
    if slug:
        urls.append(("{}/markets".format(POLY_GAMMA_API), {"slug": slug, "closed": "true"}))
        urls.append(("{}/markets/slug/{}".format(POLY_GAMMA_API, slug), None))
        urls.append(("{}/events/slug/{}".format(POLY_GAMMA_API, slug), None))
    if cond_id:
        urls.append(("{}/markets".format(POLY_GAMMA_API),
                     {"condition_ids": cond_id, "closed": "true"}))
    last_market = None
    for url, params in urls:
        try:
            r = req.get(url, params=params, timeout=8) if params is not None else req.get(url, timeout=8)
            if r.status_code != 200:
                continue
            data = r.json()
            market = None
            if isinstance(data, list) and data:
                market = data[0]
            elif isinstance(data, dict):
                if data.get("markets"):
                    market = data["markets"][0]
                elif "outcomePrices" in data or "closed" in data:
                    market = data
            if market:
                last_market = market
                out = _poly_outcome_from_market(market)
                if out:
                    return out, market
        except Exception:
            continue
    return None, last_market


def _v2_resolve_trades():
    """Resolve paper trades by checking the ACTUAL platform outcome.
    Polymarket: Gamma API outcomePrices → [1.0, 0.0] = UP won, [0.0, 1.0] = DOWN won
    Limitless: GET /markets/{slug} → check resolution status
    Falls back to Binance price vs PTB if platform check fails."""
    import requests as req
    try:
        conn = get_db()
        rows = conn.run("""
            SELECT id, platform, timeframe, asset, direction, ptb, entry_odds,
                   stake, market_id, slug, fired_at, hedged, hedge_odds, hedge_direction,
                   condition_id
            FROM v2_paper_trades WHERE status = 'OPEN'
            AND (order_status = 'FILLED' OR order_status IS NULL)
        """)
        cols = ["id", "platform", "timeframe", "asset", "direction", "ptb",
                "entry_odds", "stake", "market_id", "slug", "fired_at",
                "hedged", "hedge_odds", "hedge_direction", "condition_id"]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()

        if not trades:
            return 0

        resolved = 0
        for t in trades:
            # Skip if trade is less than the timeframe duration old
            if t.get("fired_at"):
                fired = t["fired_at"]
                if isinstance(fired, str):
                    try:
                        fired = datetime.fromisoformat(fired.replace("Z", "+00:00"))
                    except:
                        continue
                if not fired.tzinfo:
                    fired = fired.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                tf = t["timeframe"]
                min_age = {"5M": 1, "15M": 2, "1H": 61, "DAILY": 1441}.get(tf, 61)
                if (now - fired).total_seconds() / 60 < min_age:
                    continue

            asset = t["asset"]
            slug = t.get("slug", "")
            platform = t["platform"]
            actual = None
            cond_id = t.get("condition_id", "") or ""

            # METHOD 1: Read the ACTUAL resolved result from Polymarket.
            # Closed markets are excluded by Gamma's default closed=false filter,
            # so the reader explicitly requests closed=true across the slug /
            # condition_id endpoints — this is the result shown on the market page.
            if platform == "polymarket":
                try:
                    actual, _mk = _poly_read_resolution(slug, cond_id)
                    if _POLY_RESOLVE_DIAG[0] < 12:
                        _POLY_RESOLVE_DIAG[0] += 1
                        if _mk:
                            print("[POLY-RESOLVE-DIAG] {} {} closed={} prices={} -> {}".format(
                                t["timeframe"], (slug or cond_id)[:42],
                                _mk.get("closed"), _mk.get("outcomePrices"), actual))
                        else:
                            print("[POLY-RESOLVE-DIAG] {} {} — not found even with closed=true".format(
                                t["timeframe"], (slug or cond_id)[:42]))
                except Exception as e:
                    print("[V2] Poly resolve error {}: {}".format((slug or cond_id)[:24], e))

            if platform == "limitless" and slug:
                try:
                    r = req.get("{}/markets/{}".format(LIMITLESS_API, slug), timeout=8)
                    if r.status_code == 200:
                        market = r.json()
                        status = str(market.get("status", "")).lower()
                        is_done = (status in ("resolved", "closed", "settled", "expired", "ended")
                                   or market.get("closed") is True
                                   or market.get("resolved") is True
                                   or market.get("expired") is True
                                   or market.get("winningOutcomeIndex") is not None
                                   or market.get("winningOutcome") not in (None, ""))
                        if is_done:
                            winner = (market.get("winningOutcome") or market.get("winner") or "")
                            widx = market.get("winningOutcomeIndex")
                            if str(winner).lower() in ("yes", "up", "above"):
                                actual = "UP"
                            elif str(winner).lower() in ("no", "down", "below"):
                                actual = "DOWN"
                            elif widx is not None:
                                actual = "UP" if int(widx) == 0 else "DOWN"
                            else:
                                op = market.get("outcomePrices") or market.get("prices")
                                if isinstance(op, list) and len(op) >= 2:
                                    if float(op[0]) > 0.9: actual = "UP"
                                    elif float(op[0]) < 0.1: actual = "DOWN"
                        if not actual and _SB_LMTS_DIAG[0] < 6:
                            _SB_LMTS_DIAG[0] += 1
                            print("[LMTS-RESOLVE-DIAG] {} status={} keys={} winner={} widx={}".format(
                                slug[:40], status, list(market.keys())[:14],
                                market.get("winningOutcome"), market.get("winningOutcomeIndex")))
                        # NOTE: no 'continue' here — if undetermined, the gated
                        # price fallback below resolves genuinely stuck trades.
                except Exception as e:
                    print("[V2] Limitless resolve check error {}: {}".format(slug[:30], e))

            # METHOD 1c: Safety net for synthetic 15M/5M markets the platform
            # lookups couldn't read — resolve from the closed Binance candle the
            # slug points to (price at the boundary vs PTB), which is exactly how
            # these markets settle. Accurate and available the moment the candle
            # closes, so we don't sit in the slow fallback below.
            if not actual and platform == "polymarket" and slug and (
                    "-updown-15m-" in slug or "-updown-5m-" in slug):
                cres = _v2_resolve_short_from_candle(asset, slug, t.get("ptb"))
                if cres:
                    actual, _cpx = cres
                    if _POLY_RESOLVE_DIAG[0] < 12:
                        _POLY_RESOLVE_DIAG[0] += 1
                        print("[POLY-RESOLVE-DIAG] {} {} candle-resolved close={} ptb={} -> {}".format(
                            t["timeframe"], slug[:42], _cpx, t.get("ptb"), actual))

            # METHOD 2: Fallback — ONLY for genuinely stuck trades. The platform
            # (Polymarket/Limitless) is authoritative and matches what you see on
            # the market page. We only guess from price if the platform hasn't
            # resolved well past expiry (3x the timeframe) — otherwise we WAIT,
            # so we never post a wrong result before the real one is available.
            if not actual:
                tf_min = {"15M": 15, "1H": 60, "DAILY": 1440}.get(t["timeframe"], 60)
                age_min = 0
                if t.get("fired_at"):
                    try:
                        age_min = (datetime.now(timezone.utc) - fired).total_seconds() / 60
                    except Exception:
                        age_min = 0
                if age_min < tf_min * 3:
                    continue  # give the platform time to resolve correctly
                close_price = _get_binance_price(asset)
                ptb = t.get("ptb")
                if not close_price or not ptb or ptb <= 0:
                    continue
                # Don't post a guess on a tight move — Binance may not be the
                # platform's oracle (some pairs use Chainlink), and only decisive
                # moves are safe to call from price. Keep waiting for the platform.
                if abs(close_price - ptb) / ptb < 0.0005:
                    continue
                print("[V2] FALLBACK resolve {} {} {} (platform unresolved {:.0f}min) close={} ptb={}".format(
                    t["timeframe"], asset, t["direction"], age_min, close_price, ptb))
                if close_price > ptb:
                    actual = "UP"
                elif close_price < ptb:
                    actual = "DOWN"
                else:
                    actual = "FLAT"

            direction = t["direction"]
            entry_odds = t.get("entry_odds", 50) or 50
            stake = t.get("stake", 3.0) or 3.0

            # Calculate P&L
            if actual == direction:
                odds_decimal = entry_odds / 100
                payout = (stake / odds_decimal) - stake if odds_decimal > 0 else 0
                outcome = "WIN"
                pnl = payout
            elif actual == "FLAT":
                outcome = "PUSH"
                pnl = 0
            else:
                outcome = "LOSS"
                pnl = -stake

            # Hedge P&L
            if t.get("hedged") and t.get("hedge_odds"):
                hedge_odds = t["hedge_odds"]
                hedge_dir = t.get("hedge_direction")
                hedge_stake = stake * 0.5
                if actual == hedge_dir:
                    hedge_pnl = (hedge_stake / (hedge_odds / 100)) - hedge_stake
                else:
                    hedge_pnl = -hedge_stake
                pnl += hedge_pnl
            else:
                hedge_pnl = 0

            # Update balance
            bal = _v2_balances.get(platform, {})
            bal["balance"] = bal.get("balance", 100) + pnl
            if outcome == "WIN":
                bal["wins"] = bal.get("wins", 0) + 1
            elif outcome == "LOSS":
                bal["losses"] = bal.get("losses", 0) + 1
            bal["peak_balance"] = max(bal.get("peak_balance", 100), bal["balance"])
            _v2_balances[platform] = bal
            _v2_save_balance(platform)

            # Update trade record
            try:
                conn2 = get_db()
                conn2.run("""
                    UPDATE v2_paper_trades SET
                    close_price = :cp, actual_result = :ar, outcome = :oc,
                    pnl = :pnl, balance_after = :bal, hedge_pnl = :hpnl,
                    status = :st, resolved_at = NOW()
                    WHERE id = :tid
                """, cp=_get_binance_price(asset), ar=actual, oc=outcome,
                    pnl=round(pnl, 4), bal=round(bal["balance"], 2),
                    hpnl=round(hedge_pnl, 4) if hedge_pnl else None,
                    st="RESOLVED", tid=t["id"])
                conn2.close()
                resolved += 1
            except Exception as e:
                print("[V2] Resolve update error: {}".format(e))

            emoji = "✅" if outcome == "WIN" else "❌"
            send_telegram("{} V2 {} {} {} {} @ {:.0f}c → {} | P&L ${:+.2f} | Bal ${:.2f}".format(
                emoji, t["timeframe"], asset, direction,
                platform[:4].upper(), entry_odds, outcome,
                pnl, bal["balance"]))

        return resolved
    except Exception as e:
        print("[V2] Resolve error: {}".format(e))
        return 0


# ═══════════════════════════════════════════════════════════
# LIMITLESS LIVE — RESOLUTION + ON-CHAIN REDEMPTION
# ═══════════════════════════════════════════════════════════
# Mirrors the paper resolver but reads v2_live_trades. When a FILLED row's
# market resolves, we mark WIN/LOSS/PUSH, compute realized P&L from the
# actual fill (not the limit), then queue a WIN row for on-chain redemption
# of the YES/NO conditional tokens into USDC via the CTF contract.
#
# The redeemer runs as its own pass so a flaky RPC, a not-yet-settled-on-chain
# condition, or a single failure can't block resolution of new rows.

_LMTS_LIVE_RESOLVE_DIAG = [0]   # at-most-N diagnostic prints per process
_LMTS_RESOLVE_LOG_TS = {}        # per-slug timestamp dict (rate-limit verbose logs to once per 5 min)
_LMTS_REDEEM_MAX_ATTEMPTS = 10  # docs warn API can mark resolved before CTF on-chain;
                                # we retry with cooldown so the pass-through is forgiving


def _lmts_poll_pending_orders():
    """For every v2_live_trades row in PENDING status: ask the Limitless API
    what the order's current state is, and promote PENDING → FILLED /
    CANCELLED accordingly. Also cancels orders that have sat unfilled past
    the candle period (15M → 15min, 1H → 1h, DAILY → 24h) — a stale resting
    order on a price-snap binary is just inventory risk after its thesis
    candle closes.

    Returns the number of orders whose fill_status changed.
    """
    client = _lmts_get_client()
    if client is None:
        return 0
    try:
        conn = get_db()
        rows = list(conn.run("""
            SELECT id, market_slug, order_id, timeframe, fired_at, direction,
                   limit_price_cents, stake_usdc, size_shares
              FROM v2_live_trades
             WHERE fill_status = 'PENDING'
               AND order_id IS NOT NULL
               AND order_id <> ''
             ORDER BY id DESC
             LIMIT 50
        """))
        conn.close()
    except Exception as e:
        print("[LMTS-LIVE] poll DB read failed: {}".format(str(e)[:160]))
        return 0
    if not rows:
        return 0

    # Group by market_slug — one get_user_orders call covers every PENDING
    # order on that market, so two orders on the same market = one API call.
    by_slug = {}
    for r in rows:
        by_slug.setdefault(r[1], []).append(r)

    import time as _t
    now_ms = int(_t.time() * 1000)
    tf_to_ms = {"15M": 15 * 60_000, "1H": 60 * 60_000,
                "HOURLY": 60 * 60_000, "DAILY": 24 * 60 * 60_000}
    changed = 0

    for slug, slug_rows in by_slug.items():
        try:
            user_orders = client.get_user_orders(slug)
        except Exception as e:
            # Many transient causes (404 on expired market, network blip);
            # leave PENDING for the next poll cycle.
            print("[LMTS-LIVE] poll get_user_orders({}) failed: {}".format(
                slug[:40], str(e)[:120]))
            continue
        if not isinstance(user_orders, list):
            user_orders = []
        # Index by order_id for O(1) lookup
        oid_to_order = {}
        for o in user_orders:
            if not isinstance(o, dict):
                continue
            oid = (o.get("id") or o.get("orderId") or o.get("order_id") or "")
            if oid:
                oid_to_order[str(oid)] = o

        for r in slug_rows:
            (row_id, _slug, order_id, tf_label, fired_at,
             direction, limit_cents, stake, shares) = r
            order = oid_to_order.get(str(order_id))

            new_status = None
            fill_price = None
            filled_size = None

            if order is None:
                # API doesn't list this order anymore. Two cases:
                #   1. It was fully filled and moved off the open-orders feed
                #      → check portfolio positions (best-effort)
                #   2. It was cancelled (by exchange or by us)
                # Without a definitive signal we mark CANCELLED, since the
                # resolver's outcome step will still pay out a real fill via
                # the position read at market settlement.
                new_status = "CANCELLED"
            else:
                api_status = str(order.get("status") or "").upper()
                api_filled = float(order.get("filledSize")
                                   or order.get("filled_size")
                                   or order.get("filledAmount") or 0)
                if api_status in ("FILLED", "MATCHED") or api_filled >= float(shares or 0) * 0.999:
                    new_status = "FILLED"
                    fill_price = (order.get("avgPrice") or order.get("avg_price")
                                  or order.get("price"))
                    filled_size = api_filled or shares
                elif api_status in ("CANCELLED", "CANCELED", "EXPIRED", "REJECTED"):
                    new_status = "CANCELLED"
                else:
                    # Still open. Cancel if past the candle period.
                    #
                    # fired_at can come back in one of THREE shapes depending
                    # on how it was originally inserted and the pg8000 binding:
                    #   - datetime.datetime (TIMESTAMPTZ column → most common)
                    #   - int/float epoch seconds (legacy rows < 1e12)
                    #   - int epoch milliseconds (legacy rows ≥ 1e12)
                    # Old code did `fired_at < 1e12` directly and crashed the
                    # ENTIRE poll loop on every tick with `'<' not supported
                    # between instances of 'datetime.datetime' and 'float'`.
                    import datetime as _dt
                    if fired_at is None:
                        fired_ms = 0
                    elif isinstance(fired_at, _dt.datetime):
                        fired_ms = int(fired_at.timestamp() * 1000)
                    else:
                        try:
                            v = float(fired_at)
                            fired_ms = int(v * 1000) if v < 1e12 else int(v)
                        except (TypeError, ValueError):
                            fired_ms = 0
                    age_ms = now_ms - fired_ms if fired_ms else 0
                    candle_ms = tf_to_ms.get(str(tf_label).upper(), 15 * 60_000)
                    if age_ms > candle_ms:
                        try:
                            client.cancel_order(str(order_id))
                            new_status = "CANCELLED"
                            print("[LMTS-LIVE] poll: cancelled stale order {} ({} {}m old, candle={}m)".format(
                                str(order_id)[:12], tf_label, age_ms // 60_000, candle_ms // 60_000))
                        except Exception as e:
                            print("[LMTS-LIVE] poll: cancel failed for {}: {}".format(
                                str(order_id)[:12], str(e)[:120]))

            if new_status:
                try:
                    conn = get_db()
                    if new_status == "FILLED":
                        conn.run("""
                            UPDATE v2_live_trades
                               SET fill_status = :s,
                                   fill_price_cents = COALESCE(:fp, fill_price_cents),
                                   filled_size = COALESCE(:fs, filled_size)
                             WHERE id = :id
                        """, s=new_status, fp=fill_price, fs=filled_size, id=row_id)
                        print("[LMTS-LIVE] poll: FILLED order {} ({} {} @ {}c)".format(
                            str(order_id)[:12], tf_label, direction, fill_price or limit_cents))
                    else:
                        conn.run(
                            "UPDATE v2_live_trades SET fill_status = :s WHERE id = :id",
                            s=new_status, id=row_id)
                    conn.close()
                    changed += 1
                except Exception as e:
                    print("[LMTS-LIVE] poll DB update failed for row {}: {}".format(
                        row_id, str(e)[:160]))
    return changed


def _v2_resolve_live_trades():
    """Resolve FILLED rows in v2_live_trades by polling Limitless for market
    outcome (same logic as the paper resolver's limitless branch). Updates
    outcome / pnl / actual_result / resolved_at, and queues WIN rows for
    on-chain redemption by setting redeem_status='PENDING'."""
    import requests as req
    try:
        conn = get_db()
        rows = conn.run("""
            SELECT id, timeframe, asset, direction, market_slug, condition_id,
                   limit_price_cents, fill_price_cents, filled_size, stake_usdc,
                   fired_at
            FROM v2_live_trades
            WHERE fill_status = 'FILLED' AND outcome IS NULL
            ORDER BY id ASC
        """)
        cols = ["id", "timeframe", "asset", "direction", "market_slug",
                "condition_id", "limit_price_cents", "fill_price_cents",
                "filled_size", "stake_usdc", "fired_at"]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()

        if not trades:
            return 0

        resolved = 0
        for t in trades:
            slug = t.get("market_slug") or ""
            asset = t.get("asset") or ""
            direction = t.get("direction") or ""
            if not slug or not direction:
                continue

            # Skip if too young — give the market time to actually resolve.
            fired = t.get("fired_at")
            if fired:
                if isinstance(fired, str):
                    try:
                        fired = datetime.fromisoformat(fired.replace("Z", "+00:00"))
                    except Exception:
                        fired = None
                if fired:
                    if not fired.tzinfo:
                        fired = fired.replace(tzinfo=timezone.utc)
                    tf = t.get("timeframe") or "1H"
                    min_age = {"5M": 1, "15M": 2, "1H": 61, "DAILY": 1441}.get(tf, 61)
                    if (datetime.now(timezone.utc) - fired).total_seconds() / 60 < min_age:
                        continue

            # Poll Limitless for the resolved outcome — exact same logic as
            # the paper resolver's limitless branch (lines ~2111 of paper path).
            actual = None
            captured_cid = None
            try:
                r = req.get("{}/markets/{}".format(LIMITLESS_API, slug), timeout=8)
                if r.status_code == 200:
                    market = r.json()
                    # Defensive: if this row was placed before the order-time
                    # condition_id fetch landed, grab it here so the redeemer
                    # can still claim winnings.
                    if not t.get("condition_id"):
                        captured_cid = (market.get("conditionId")
                                        or market.get("condition_id") or None)
                    status = str(market.get("status", "")).lower()
                    is_done = (status in ("resolved", "closed", "settled", "expired", "ended")
                               or market.get("closed") is True
                               or market.get("resolved") is True
                               or market.get("expired") is True
                               or market.get("winningOutcomeIndex") is not None
                               or market.get("winningOutcome") not in (None, ""))
                    if is_done:
                        winner = (market.get("winningOutcome") or market.get("winner") or "")
                        widx = market.get("winningOutcomeIndex")
                        if str(winner).lower() in ("yes", "up", "above"):
                            actual = "UP"
                        elif str(winner).lower() in ("no", "down", "below"):
                            actual = "DOWN"
                        elif widx is not None:
                            actual = "UP" if int(widx) == 0 else "DOWN"
                        else:
                            op = market.get("outcomePrices") or market.get("prices")
                            if isinstance(op, list) and len(op) >= 2:
                                if float(op[0]) > 0.9: actual = "UP"
                                elif float(op[0]) < 0.1: actual = "DOWN"
                    if not actual:
                        # Verbose: print every time we check a FILLED row and
                        # Limitless says the market isn't resolved yet. Use a
                        # per-slug rate-limit (once per 5 min per slug) so
                        # we don't flood logs across 100 retries.  This is
                        # what the user was missing — they couldn't tell
                        # whether the resolver was even running.
                        slug_key = "lmts_resolve_log:" + slug
                        last_log = _LMTS_RESOLVE_LOG_TS.get(slug_key, 0)
                        nowts = time.time()
                        if nowts - last_log > 300:  # 5 min throttle per slug
                            _LMTS_RESOLVE_LOG_TS[slug_key] = nowts
                            print("[LMTS-LIVE] resolver: row {} ({} {} {}) — market not yet resolved "
                                  "(status={!r}, winner={!r}, widx={}, prices={})".format(
                                t["id"], t.get("timeframe"), asset, direction,
                                status, market.get("winningOutcome"),
                                market.get("winningOutcomeIndex"),
                                market.get("prices") or market.get("outcomePrices")))
            except Exception as e:
                print("[LMTS-LIVE] resolve check error {}: {}".format(slug[:30], e))

            if not actual:
                continue  # try again next cycle

            # Realized P&L from the actual fill (not the limit). Each share is
            # worth $1 USDC if direction wins, $0 otherwise.
            filled_size = float(t.get("filled_size") or 0)
            fill_px = float(t.get("fill_price_cents") or t.get("limit_price_cents") or 0)
            cost = filled_size * (fill_px / 100.0)
            if actual == direction:
                outcome = "WIN"
                pnl = filled_size - cost            # receive $1/share, paid `cost`
                redeem_status = "PENDING"           # queue for on-chain claim
            elif actual == "FLAT":
                outcome = "PUSH"
                pnl = 0.0
                redeem_status = "SKIPPED"
            else:
                outcome = "LOSS"
                pnl = -cost
                redeem_status = "SKIPPED"

            try:
                conn2 = get_db()
                # If we captured a condition_id this pass, persist it too so
                # the redeemer can pick this row up next cycle.
                if captured_cid:
                    conn2.run("""
                        UPDATE v2_live_trades SET
                            actual_result = :ar,
                            outcome       = :oc,
                            pnl           = :pnl,
                            resolved_at   = NOW(),
                            redeem_status = :rs,
                            condition_id  = :cid
                        WHERE id = :tid
                    """, ar=actual, oc=outcome, pnl=round(pnl, 4),
                        rs=redeem_status, cid=captured_cid, tid=t["id"])
                else:
                    conn2.run("""
                        UPDATE v2_live_trades SET
                            actual_result = :ar,
                            outcome       = :oc,
                            pnl           = :pnl,
                            resolved_at   = NOW(),
                            redeem_status = :rs
                        WHERE id = :tid
                    """, ar=actual, oc=outcome, pnl=round(pnl, 4),
                        rs=redeem_status, tid=t["id"])
                conn2.close()
                resolved += 1
            except Exception as e:
                print("[LMTS-LIVE] resolve update error: {}".format(e))
                continue

            emoji = "✅" if outcome == "WIN" else ("❌" if outcome == "LOSS" else "➖")
            send_telegram("{} LIVE {} {} {} @ {:.0f}c → {} | P&L ${:+.2f}".format(
                emoji, t.get("timeframe") or "?", asset, direction,
                fill_px, outcome, pnl))

        return resolved
    except Exception as e:
        print("[LMTS-LIVE] resolve error: {}".format(e))
        return 0


def _lmts_redeem_winnings(condition_id):
    """Submit an on-chain redeemPositions() call for a resolved condition.
    Returns {ok, tx_hash, error}. Caller persists tx_hash + status. The CTF
    accepts redemption from any holder; calling it for a non-winning condition
    just zeroes the dust positions (no harm, but a small gas cost), so we
    only call this for WIN rows."""
    if not LIMITLESS_PRIV_KEY:
        return {"ok": False, "error": "LIMITLESS_PRIVATE_KEY not set"}
    if not condition_id:
        return {"ok": False, "error": "missing condition_id"}
    try:
        pass  # limitless_sdk inlined above
        pass  # limitless_sdk inlined above
        redeemer = EOAPositionRedeemer(
            private_key=LIMITLESS_PRIV_KEY,
            rpc_url=LIMITLESS_BASE_RPC,
            ctf_address=BASE_CTF_ADDRESS,
            collateral_token=USDC_ADDRESS,
            chain_id=BASE_CHAIN_ID,
        )
        tx_hash = redeemer.redeem_position(condition_id)
        return {"ok": True, "tx_hash": tx_hash}
    except ImportError as e:
        return {"ok": False, "error": "py-limitless missing: {}".format(e)}
    except Exception as e:
        # Common: "execution reverted" before CTF is settled on-chain (docs
        # warn this can lag the API). We propagate the message; the caller
        # decides whether to retry on a future pass.
        return {"ok": False, "error": str(e)[:500]}


def _v2_redeem_pending_live():
    """Pick up PENDING rows from v2_live_trades and try on-chain redemption.
    Re-tries with a per-row attempts counter. After _LMTS_REDEEM_MAX_ATTEMPTS
    the row is marked FAILED so we stop spamming the RPC; the operator can
    re-queue it manually from the dashboard later."""
    try:
        conn = get_db()
        rows = conn.run("""
            SELECT id, condition_id, asset, timeframe, direction, pnl,
                   COALESCE(redeem_attempts, 0) AS attempts
            FROM v2_live_trades
            WHERE redeem_status = 'PENDING'
              AND condition_id IS NOT NULL AND condition_id <> ''
            ORDER BY id ASC
            LIMIT 8
        """)
        cols = ["id", "condition_id", "asset", "timeframe", "direction",
                "pnl", "attempts"]
        pending = [dict(zip(cols, r)) for r in rows]
        conn.close()
    except Exception as e:
        print("[LMTS-LIVE] redeem fetch error: {}".format(e))
        return 0

    if not pending:
        return 0

    done = 0
    for p in pending:
        attempts = int(p.get("attempts") or 0)
        cid = p.get("condition_id") or ""
        res = _lmts_redeem_winnings(cid)
        if res.get("ok"):
            tx_hash = res.get("tx_hash") or ""
            try:
                conn2 = get_db()
                conn2.run("""
                    UPDATE v2_live_trades SET
                        redeem_status       = 'DONE',
                        redeem_tx_hash      = :tx,
                        redeem_attempts     = :a,
                        redeem_last_attempt = NOW()
                    WHERE id = :tid
                """, tx=tx_hash, a=attempts + 1, tid=p["id"])
                conn2.close()
                done += 1
                print("[LMTS-LIVE] redeemed {} {} {} -> {}".format(
                    p.get("timeframe"), p.get("asset"), p.get("direction"),
                    tx_hash[:14] + "…" if len(tx_hash) > 14 else tx_hash))
                send_telegram("💰 LIVE redeemed {} {} {} → tx {}".format(
                    p.get("timeframe"), p.get("asset"), p.get("direction"),
                    tx_hash[:10] + "…"))
            except Exception as e:
                print("[LMTS-LIVE] redeem update error: {}".format(e))
        else:
            err = res.get("error") or "unknown"
            new_status = "FAILED" if attempts + 1 >= _LMTS_REDEEM_MAX_ATTEMPTS else "PENDING"
            try:
                conn2 = get_db()
                conn2.run("""
                    UPDATE v2_live_trades SET
                        redeem_status       = :rs,
                        redeem_attempts     = :a,
                        redeem_last_attempt = NOW(),
                        error_message       = COALESCE(error_message, '') || :err_suffix
                    WHERE id = :tid
                """, rs=new_status, a=attempts + 1,
                    err_suffix=(" [redeem {}: {}]".format(attempts + 1, err))[:300],
                    tid=p["id"])
                conn2.close()
                print("[LMTS-LIVE] redeem attempt {} for id={} -> {} ({})".format(
                    attempts + 1, p["id"], new_status, err[:120]))
            except Exception as e:
                print("[LMTS-LIVE] redeem fail-update error: {}".format(e))

    return done


# ═══════════════════════════════════════════════════════════
# V2 WATCHER THREADS
# ═══════════════════════════════════════════════════════════

# Track active trades per boundary to avoid duplicates
_v2_active_boundaries = {}  # {"BTC_1H_1748390400": True, ...}
_fill_failures = {}  # Track consecutive 404s per order for expiry
FLAT_STAKE = 3.00  # $3 flat per confirmed entry

def _v2_scan_timeframe(timeframe):
    """Core scanning logic shared by 1H, 15M, and DAILY watchers.
    Scans BOTH Polymarket and Limitless.
    SELECTIVE: only enters the best 2 trades per scan cycle, ranked by confidence."""

    ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB"]
    MAX_ENTRIES_PER_SCAN = 2  # Only the best 2 trades per cycle
    tf_label = timeframe

    # Timeframe-specific config — correct candle intervals
    if tf_label == "1H":
        intra_interval = "15m"      # 15M candles for hourly structure
        prev_interval = "1h"        # Previous 1H candle
        min_intra_candles = 2       # Need 2+ completed 15M candles (at T+45 we have 3)
        boundary_secs = 3600
        poly_tf_filter = "1H"
        scan_sleep = 120
        entry_window_start = 2700   # T+45min
        entry_window_end = 3540     # T+59min
    elif tf_label == "15M":
        intra_interval = "5m"       # 5M candles for 15M structure
        prev_interval = "15m"       # Previous 15M candle
        min_intra_candles = 1       # Need 1+ completed 5M candle (at T+5 we have 1, at T+10 we have 2)
        boundary_secs = 900
        poly_tf_filter = "15M"
        scan_sleep = 60
        entry_window_start = 300    # T+5min
        entry_window_end = 840      # T+14min
    else:  # DAILY
        intra_interval = "4h"       # 4H candles for daily structure
        prev_interval = "1d"        # Previous daily candle
        min_intra_candles = 3       # Need 3+ completed 4H candles
        boundary_secs = 86400
        poly_tf_filter = "DAILY"
        scan_sleep = 1800
        entry_window_start = 0      # Handled by quiet hours
        entry_window_end = 86400

    while True:
        try:
            now = datetime.now(timezone.utc)
            now_ts = int(now.timestamp())

            if tf_label == "DAILY":
                # Daily boundary = midnight UTC
                boundary_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
                secs_into_period = now_ts - boundary_ts
                # Spec: check 3-6 hours before close during quiet hours
                # Quiet hours: 17-22 UTC or 4-11 UTC
                h = now.hour
                is_quiet = (17 <= h <= 22) or (4 <= h <= 11)
                if not is_quiet:
                    time.sleep(600)
                    continue
                # Need at least 6 hours of data
                if secs_into_period < 21600:
                    time.sleep(max(60, 21600 - secs_into_period))
                    continue
            else:
                boundary_ts = (now_ts // boundary_secs) * boundary_secs
                secs_into_period = now_ts - boundary_ts
                # Spec: only enter within the entry window
                if secs_into_period < entry_window_start:
                    time.sleep(entry_window_start - secs_into_period + 5)
                    continue
                if secs_into_period > entry_window_end:
                    # Past the window — wait for next period
                    time.sleep(boundary_secs - secs_into_period + 5)
                    continue

            # Session filter
            session_label, session_safe = _v2_session_filter(now.hour)

            # Fetch markets from BOTH platforms
            poly_markets = _poly_fetch_markets()
            poly_tf = {m["asset"]: m for m in (poly_markets or []) if m.get("timeframe") == poly_tf_filter}

            lmts_markets = _limitless_fetch_markets()
            lmts_tf = {m["asset"]: m for m in (lmts_markets or []) if m.get("timeframe") == tf_label}

            # Log what was found for this timeframe
            poly_assets = sorted(poly_tf.keys()) if poly_tf else []
            lmts_assets = sorted(lmts_tf.keys()) if lmts_tf else []
            if poly_assets or lmts_assets:
                print("[V2] {} scan: POLY={} LMTS={}".format(
                    tf_label, ",".join(poly_assets) or "none", ",".join(lmts_assets) or "none"))
            else:
                print("[V2] {} scan: no markets found on either platform".format(tf_label))

            # Collect candidates, then enter only the best
            _scan_candidates = []

            for asset in ASSETS:
                # Try both platforms for this asset
                platforms_to_try = []
                if asset in poly_tf:
                    platforms_to_try.append(("polymarket", poly_tf[asset]))
                if asset in lmts_tf:
                    platforms_to_try.append(("limitless", lmts_tf[asset]))
                if not platforms_to_try:
                    continue  # No market on either platform — skip

                for platform, market_data in platforms_to_try:
                    boundary_key = "{}_{}_{}_{}".format(asset, tf_label, platform[:4], boundary_ts)
                    if boundary_key in _v2_active_boundaries:
                        continue

                    # Fetch intra-period candles from Binance
                    intra_candles = _fetch_binance_candles(asset, interval=intra_interval, limit=30)
                    if not intra_candles or len(intra_candles) < 3:
                        print("[V2] SKIP {} {} {} — no candles from Binance ({})".format(
                            tf_label, asset, platform[:4], intra_interval))
                        continue

                    # Filter to THIS period only
                    period_candles = [c for c in intra_candles if c["t"] >= boundary_ts * 1000]
                    if len(period_candles) < min_intra_candles:
                        print("[V2] SKIP {} {} {} — only {} candles in period (need {})".format(
                            tf_label, asset, platform[:4], len(period_candles), min_intra_candles))
                        continue

                    # Previous completed candle
                    prev_candles = _fetch_binance_candles(asset, interval=prev_interval, limit=5)
                    if not prev_candles or len(prev_candles) < 2:
                        print("[V2] SKIP {} {} {} — no prev candles".format(tf_label, asset, platform[:4]))
                        continue
                    prev_candle = _v2_analyze_prev_candle(prev_candles[-2])

                    # Structure analysis
                    structure = _v2_analyze_structure(period_candles)
                    if not structure:
                        print("[V2] SKIP {} {} {} — structure analysis returned None".format(
                            tf_label, asset, platform[:4]))
                        continue

                    # Volatility
                    current_range = max(c["h"] for c in period_candles) - min(c["l"] for c in period_candles)
                    vol_label, vol_safe = _v2_volatility_check(prev_candles[:-1], current_range)

                    # Get current price — prefer Chainlink RTDS (what markets resolve against)
                    price = _rtds_current_price(asset)
                    if not price or price <= 0:
                        price = _get_binance_price(asset)

                    # Get PTB — this is the opening price of the period
                    # Priority: (1) market title baseline, (2) Chainlink boundary capture,
                    # (3) period's first candle open — NOT current price
                    ptb = None
                    # From market data (parsed from title/description)
                    if market_data and market_data.get("baseline") and market_data["baseline"] > 0:
                        ptb = market_data["baseline"]
                    # From Chainlink boundary capture at period start
                    if not ptb:
                        key = "{}_{}".format(asset, tf_label)
                        entry = _chainlink_ptb.get(key)
                        if entry:
                            ptb = entry[1]
                    # From the opening price of the first intra-period candle
                    if not ptb and period_candles:
                        ptb = period_candles[0]["o"]
                    # Last resort: from the full candle data open
                    if not ptb and intra_candles:
                        # Find the candle at boundary start
                        for c in intra_candles:
                            if c["t"] >= boundary_ts * 1000:
                                ptb = c["o"]
                                break

                    if not ptb or ptb <= 0:
                        print("[V2] SKIP {} {} {} — no PTB found".format(tf_label, asset, platform[:4]))
                        continue

                    # Calculate time remaining
                    secs_remaining = boundary_secs - secs_into_period

                    # Entry decision — new signature
                    should, direction, confidence, reason = _v2_should_enter(
                        price, ptb, asset, structure, prev_candle,
                        vol_safe,
                        session_safe if tf_label != "DAILY" else True,
                        tf_label, secs_remaining
                    )

                    if not should:
                        print("[V2] REJECT {} {} {} — conf={} reason={}".format(
                            tf_label, asset, platform[:4], confidence, reason[:80] if reason else "none"))
                        continue

                    # Collect as candidate — don't enter yet
                    _scan_candidates.append({
                        "asset": asset, "platform": platform, "market_data": market_data,
                        "direction": direction, "confidence": confidence, "reason": reason,
                        "structure": structure, "prev_candle": prev_candle,
                        "prev_str": "{} body={:.0f}%".format(
                            prev_candle["strength"], prev_candle["body_pct"] * 100) if prev_candle else "",
                        "ptb": ptb, "price": price, "session_label": session_label,
                        "vol_label": vol_label, "boundary_key": boundary_key,
                        "secs_remaining": secs_remaining,
                    })

            # === SELECTIVITY: Rank candidates by confidence, enter only the best ===
            if _scan_candidates:
                # Sort by confidence descending
                _scan_candidates.sort(key=lambda c: c.get("confidence", 0), reverse=True)
                entered = 0

                for cand in _scan_candidates:
                    if entered >= MAX_ENTRIES_PER_SCAN:
                        break

                    asset = cand["asset"]
                    platform = cand["platform"]
                    market_data = cand["market_data"]
                    direction = cand["direction"]
                    confidence = cand["confidence"]
                    reason = cand["reason"]
                    structure = cand["structure"]
                    prev_candle = cand["prev_candle"]
                    prev_str = cand["prev_str"]
                    ptb = cand["ptb"]
                    price = cand["price"]
                    session_label = cand["session_label"]
                    vol_label = cand["vol_label"]
                    boundary_key = cand["boundary_key"]

                    if boundary_key in _v2_active_boundaries:
                        continue

                    # Get REAL book ask from order book
                    book_ask = _v2_get_odds(platform, market_data, direction)

                    # Calculate limit price
                    if book_ask:
                        limit_price, should_place = _v2_calc_limit_price(book_ask, confidence)
                        if not should_place:
                            print("[V2] {} {} {} — book_ask={:.0f}c, below 65c minimum".format(
                                tf_label, asset, direction, book_ask))
                            continue
                    else:
                        # No book data = can't confirm odds = skip
                        print("[V2] {} {} {} — no book data, skip".format(tf_label, asset, direction))
                        continue

                    entry_odds = limit_price

                    # Build entry note
                    secs_rem = cand.get("secs_remaining", 0)
                    note = _v2_build_entry_note(
                        asset, tf_label, direction, prev_candle, structure,
                        ptb, price, session_label, vol_label, confidence,
                        secs_remaining=secs_rem)
                    if book_ask:
                        note += " | Book: {:.0f}c → Limit: {:.0f}c".format(book_ask, limit_price)

                    # Record paper trade as PENDING
                    market_url = _v2_market_url(platform, market_data, asset, tf_label)

                    # ── Either-or mode (toggled from the paper bot page) ──
                    # When live trading is ON for a Limitless market, the scanner
                    # SKIPS the paper insert entirely and places a real FOK on
                    # Limitless — paper does not record. When OFF (or non-Limitless
                    # platform), the original paper path runs unchanged.
                    go_live = (platform == "limitless" and _lmts_live_enabled())

                    if go_live:
                        try:
                            live_status = _lmts_place_live(
                                None, market_data, asset, tf_label, direction,
                                float(limit_price), float(FLAT_STAKE))
                        except Exception as _lmts_e:
                            print("[LMTS-LIVE] place exception (signal not recorded): {}".format(_lmts_e))
                            continue
                    else:
                        try:
                            conn = get_db()
                            conn.run("""
                                INSERT INTO v2_paper_trades (
                                    platform, timeframe, asset, direction, ptb, entry_odds,
                                    entry_price, stake, entry_note, hh_count, hl_count, ll_count, lh_count,
                                    grind_rate, ptb_distance, session_label, volatility,
                                    prev_candle, market_id, slug, condition_id,
                                    up_token, down_token, confidence, market_url,
                                    limit_price, book_ask, order_status, status
                                ) VALUES (
                                    :plat, :tf, :asset, :dir, :ptb, :odds,
                                    :price, :stake, :note, :hh, :hl, :ll, :lh,
                                    :grind, :ptb_dist, :sess, :vol,
                                    :prev, :mid, :slug, :cid,
                                    :up_tok, :dn_tok, :conf, :murl,
                                    :lim, :bask, 'PENDING', 'OPEN'
                                )
                            """,
                                plat=platform, tf=tf_label, asset=asset, dir=direction,
                                ptb=ptb, odds=entry_odds, price=price,
                                stake=FLAT_STAKE, note=note,
                                hh=structure.get("hh_count", 0) if structure else 0,
                                hl=structure.get("hl_count", 0) if structure else 0,
                                ll=structure.get("ll_count", 0) if structure else 0,
                                lh=structure.get("lh_count", 0) if structure else 0,
                                grind=structure.get("grind_type", "") if structure else "",
                                ptb_dist=0, sess=session_label, vol=vol_label,
                                prev=prev_str or "",
                                mid=market_data.get("market_id", "") if market_data else "",
                                slug=market_data.get("slug", "") if market_data else "",
                                cid=market_data.get("condition_id", "") if market_data else "",
                                up_tok=market_data.get("up_token", "") if market_data else "",
                                dn_tok=market_data.get("down_token", "") if market_data else "",
                                conf=str(confidence) if confidence else "",
                                murl=market_url,
                                lim=limit_price, bask=book_ask,
                            )
                            conn.close()
                        except Exception as e:
                            print("[V2] Record PENDING error: {}".format(e))
                            continue

                    _v2_active_boundaries[boundary_key] = True
                    entered += 1

                    url_str = "\n🔗 {}".format(market_url) if market_url else ""
                    if go_live:
                        send_telegram(
                            "🟢 LIVE FOK {} {} {} {} @ {:.0f}c → {}\n"
                            "Conf {} | ${:.2f} | {}/{}{}".format(
                                platform[:4].upper(), tf_label, asset, direction,
                                limit_price, live_status,
                                confidence, FLAT_STAKE, entered, len(_scan_candidates), url_str))
                    else:
                        send_telegram(
                            "📋 V2 LIMIT {} {} {} {} @ {:.0f}c (book {:.0f}c)\n"
                            "Conf {} | ${:.2f} | BEST {}/{}{}".format(
                                platform[:4].upper(), tf_label, asset, direction,
                                limit_price, book_ask or 0, confidence,
                                FLAT_STAKE, entered, len(_scan_candidates), url_str))

                if _scan_candidates:
                    print("[V2] {} scan: {} candidates, entered {}".format(
                        tf_label, len(_scan_candidates), entered))

            time.sleep(scan_sleep)

        except Exception as e:
            print("[V2] {} watcher error: {}".format(tf_label, e))
            import traceback; traceback.print_exc()
            time.sleep(30)


def _v2_hourly_watcher():
    """HOURLY WATCHER — scans every 2 minutes. Both Polymarket + Limitless."""
    print("[V2] Hourly watcher started")
    _v2_scan_timeframe("1H")


def _v2_fifteen_min_watcher():
    """15M WATCHER — scans every 1 minute. Stricter confidence (75+)."""
    print("[V2] 15M watcher started")
    _v2_scan_timeframe("15M")


def _v2_daily_watcher():
    """DAILY WATCHER — scans every 10 minutes. Both Polymarket + Limitless."""
    print("[V2] Daily watcher started")
    _v2_scan_timeframe("DAILY")


def _v2_monitor_thread():
    """Monitor open positions for structure breaks → hedge.
    Hedge = buy opposite side at REAL order book odds."""
    print("[V2] Monitor thread started")

    while True:
        try:
            conn = get_db()
            rows = conn.run("""
                SELECT id, platform, timeframe, asset, direction, ptb, entry_odds,
                       stake, fired_at, hedged, market_id, slug, condition_id,
                       up_token, down_token
                FROM v2_paper_trades WHERE status = 'OPEN' AND hedged = FALSE
                AND (order_status = 'FILLED' OR order_status IS NULL)
            """)
            cols = ["id", "platform", "timeframe", "asset", "direction", "ptb",
                    "entry_odds", "stake", "fired_at", "hedged", "market_id",
                    "slug", "condition_id", "up_token", "down_token"]
            trades = [dict(zip(cols, r)) for r in rows]
            conn.close()

            for t in trades:
                asset = t["asset"]
                tf = t["timeframe"]

                # Get current intra-period candles for structure check
                interval = "1m" if tf == "15M" else "5m"
                candles = _fetch_binance_candles(asset, interval=interval, limit=15)
                if not candles or len(candles) < 3:
                    continue

                structure = _v2_analyze_structure(candles[-10:])
                
                # Get current price and PTB for hedge confirmation
                current_ptb = t.get("ptb")
                should_hedge, hedge_reason = _v2_check_hedge(t, structure, candles, current_ptb)

                if not should_hedge:
                    continue

                # Hedge direction is opposite of original trade
                hedge_dir = "DOWN" if t["direction"] == "UP" else "UP"

                # Get REAL opposite-side odds from order book
                hedge_odds = None
                market_data = {
                    "up_token": t.get("up_token", ""),
                    "down_token": t.get("down_token", ""),
                    "slug": t.get("slug", ""),
                    "condition_id": t.get("condition_id", ""),
                    "market_id": t.get("market_id", ""),
                }
                if market_data.get("up_token") or market_data.get("down_token") or market_data.get("slug"):
                    hedge_odds = _v2_get_odds(t.get("platform", "polymarket"), market_data, hedge_dir)

                if not hedge_odds:
                    hedge_odds = 30.0  # Cheap hedge assumption for paper

                # Hedge stake = 50% of original
                hedge_stake = (t.get("stake", FLAT_STAKE) or FLAT_STAKE) * 0.5

                # Record hedge on the trade
                try:
                    conn2 = get_db()
                    conn2.run("""
                        UPDATE v2_paper_trades SET
                        hedged = TRUE, hedge_odds = :ho,
                        hedge_direction = :hd, hedge_note = :hn
                        WHERE id = :tid
                    """, ho=hedge_odds, hd=hedge_dir,
                        hn="{} | Hedge stake=${:.2f}".format(hedge_reason, hedge_stake),
                        tid=t["id"])
                    conn2.close()
                except:
                    pass

                market_url = _v2_market_url("polymarket", market_data, asset, tf)
                url_str = "\n🔗 {}".format(market_url) if market_url else ""

                send_telegram(
                    "🛡️ V2 HEDGE {} {} {} → {} @ {:.0f}c | ${:.2f}\n"
                    "📝 {}{}".format(
                        tf, asset, t["direction"], hedge_dir,
                        hedge_odds, hedge_stake, hedge_reason[:80], url_str))

            time.sleep(30)

        except Exception as e:
            print("[V2] Monitor error: {}".format(e))
            time.sleep(60)


def _v2_resolve_loop():
    """Background thread to resolve paper + live trades, then attempt on-chain
    redemption of pending live wins. All four pieces run on the same 60s
    cadence; each is wrapped so one failing path can't stall the others.

    Order matters:
      1. Paper resolve (PolyPaper + LmtsPaper)
      2. Live fill poll  ← promotes PENDING (resting GTC) → FILLED/CANCELLED
      3. Live resolve    ← reads FILLED rows, computes outcome at market expiry
      4. Live redeem     ← picks redeem_status=PENDING wins, sends on-chain tx
    """
    print("[V2] Resolve loop started")
    while True:
        try:
            resolved = _v2_resolve_trades()
            if resolved:
                print("[V2] Resolved {} trades".format(resolved))
        except Exception as e:
            print("[V2] Resolve loop error: {}".format(e))
        try:
            polled = _lmts_poll_pending_orders()
            if polled:
                print("[LMTS-LIVE] Polled {} pending orders".format(polled))
        except Exception as e:
            print("[LMTS-LIVE] poll loop error: {}".format(e))
        try:
            live_resolved = _v2_resolve_live_trades()
            if live_resolved:
                print("[LMTS-LIVE] Resolved {} live trades".format(live_resolved))
        except Exception as e:
            print("[LMTS-LIVE] resolve loop error: {}".format(e))
        try:
            redeemed = _v2_redeem_pending_live()
            if redeemed:
                print("[LMTS-LIVE] Redeemed {} positions on-chain".format(redeemed))
        except Exception as e:
            print("[LMTS-LIVE] redeem loop error: {}".format(e))
        time.sleep(60)


def _v2_fill_checker():
    """Check PENDING limit orders — fill them if the book ask has dropped
    to our limit price or below. Expire them if the market period has ended."""
    print("[V2] Fill checker started")

    while True:
        try:
            conn = get_db()
            rows = conn.run("""
                SELECT id, platform, timeframe, asset, direction, limit_price,
                       market_id, slug, condition_id, up_token, down_token,
                       fired_at
                FROM v2_paper_trades
                WHERE order_status = 'PENDING' AND status = 'OPEN'
            """)
            cols = ["id", "platform", "timeframe", "asset", "direction", "limit_price",
                    "market_id", "slug", "condition_id", "up_token", "down_token", "fired_at"]
            orders = [dict(zip(cols, r)) for r in rows]
            conn.close()

            for o in orders:
                # Check if market period has expired (order should expire unfilled)
                if o.get("fired_at"):
                    fired = o["fired_at"]
                    if isinstance(fired, str):
                        try: fired = datetime.fromisoformat(fired.replace("Z", "+00:00"))
                        except: continue
                    if not fired.tzinfo:
                        fired = fired.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    tf = o["timeframe"]
                    max_age = {"15M": 15, "1H": 60, "DAILY": 1440}.get(tf, 60)
                    if (now - fired).total_seconds() / 60 > max_age:
                        # Expired unfilled — cancel the order
                        try:
                            conn2 = get_db()
                            conn2.run("""
                                UPDATE v2_paper_trades SET order_status = 'EXPIRED', status = 'RESOLVED',
                                outcome = 'EXPIRED', resolved_at = NOW()
                                WHERE id = :tid
                            """, tid=o["id"])
                            conn2.close()
                            print("[V2] EXPIRED unfilled: {} {} {}".format(o["timeframe"], o["asset"], o["direction"]))
                        except:
                            pass
                        continue

                # Check current book ask
                market_data = {
                    "up_token": o.get("up_token", ""),
                    "down_token": o.get("down_token", ""),
                    "slug": o.get("slug", ""),
                    "condition_id": o.get("condition_id", ""),
                    "market_id": o.get("market_id", ""),
                    "asset": o.get("asset", ""),
                    "timeframe": o.get("timeframe", ""),
                }
                current_ask = _v2_get_odds(o["platform"], market_data, o["direction"])
                # Respect Limitless rate limits (300ms between calls)
                if o["platform"] == "limitless":
                    time.sleep(0.35)

                if not current_ask:
                    # Track consecutive failures — expire after 3
                    fail_key = "fail_{}".format(o["id"])
                    _fill_failures[fail_key] = _fill_failures.get(fail_key, 0) + 1
                    if _fill_failures[fail_key] >= 3:
                        # Token is dead — expire the order
                        try:
                            conn2 = get_db()
                            conn2.run("""
                                UPDATE v2_paper_trades SET order_status = 'EXPIRED', status = 'RESOLVED',
                                outcome = 'EXPIRED', resolved_at = NOW()
                                WHERE id = :tid
                            """, tid=o["id"])
                            conn2.close()
                            del _fill_failures[fail_key]
                            print("[V2] EXPIRED (dead token): {} {} {}".format(
                                o["timeframe"], o["asset"], o["direction"]))
                        except:
                            pass
                    continue

                limit = o.get("limit_price", 0) or 0

                # FILL LOGIC (guaranteed fills so we never miss a fast market):
                #  • first 30s: only fill if the ask drops to our limit (better price)
                #  • after 30s: fill at the live market ask (1:1) — don't miss it
                # Only a sane ask (>= 10c) counts; <10c = stale/dead data.
                if current_ask < 10:
                    continue
                try:
                    age_sec = (datetime.now(timezone.utc) - fired).total_seconds()
                except Exception:
                    age_sec = 999
                GRACE_SEC = 30
                should_fill = (current_ask <= limit) or (age_sec >= GRACE_SEC)
                if should_fill:
                    try:
                        conn2 = get_db()
                        conn2.run("""
                            UPDATE v2_paper_trades SET
                            order_status = 'FILLED', entry_odds = :odds,
                            book_ask = :bask, filled_at = NOW()
                            WHERE id = :tid
                        """, odds=current_ask, bask=current_ask, tid=o["id"])
                        conn2.close()
                        fill_kind = "limit" if current_ask <= limit else "market"
                        print("[V2] FILLED ({}): {} {} {} @ {:.0f}c (limit was {:.0f}c)".format(
                            fill_kind, o["timeframe"], o["asset"], o["direction"], current_ask, limit))

                        send_telegram(
                            "✅ V2 FILLED {} {} {} @ {:.0f}c (limit {:.0f}c)\n"
                            "${:.2f} stake now active".format(
                                o["timeframe"], o["asset"], o["direction"],
                                current_ask, limit, FLAT_STAKE))
                    except Exception as e:
                        print("[V2] Fill update error: {}".format(e))

            time.sleep(30)  # Check fills every 30 seconds

        except Exception as e:
            print("[V2] Fill checker error: {}".format(e))
            time.sleep(30)


def _v2_cleanup_loop():
    """Clean up old boundary keys every hour."""
    while True:
        time.sleep(3600)
        now_ts = int(time.time())
        old_keys = [k for k, v in _v2_active_boundaries.items()
                    if now_ts - int(k.split("_")[-1]) > 86400]
        for k in old_keys:
            del _v2_active_boundaries[k]


# ═══════════════════════════════════════════════════════════
# DASHBOARD HTML
# ═══════════════════════════════════════════════════════════

DASHBOARD_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700;800&family=DM+Sans:wght@400;500;700;900&family=JetBrains+Mono:wght@400;500;700&display=swap');
:root{
  --brand:#2f6bd6; --brand2:#4f86ee; --brand-deep:#1f54b0; --brand-soft:#e8f0fd;
  --bg1:#eef3fa; --bg2:#e3eaf5;
  --ink:#0c1320; --ink2:#3a465c; --muted:#73819b;
  --surface:#ffffff; --surface2:#f6f9fd;
  --line:rgba(12,19,32,0.09); --line2:rgba(12,19,32,0.06);
  --glass:rgba(255,255,255,0.72); --glass-line:rgba(12,19,32,0.07);
  --shadow:0 16px 44px rgba(20,40,90,0.10);
  --good:#1f9d6b; --good-soft:rgba(31,157,107,0.13);
  --red:#e1556a; --red-soft:rgba(225,85,106,0.13);
  --orange:#e08a3c; --grid:rgba(47,107,214,0.05);
}
[data-theme="dark"]{
  --brand:#5a8cf0; --brand2:#7aa2f5; --brand-deep:#3f78e6; --brand-soft:rgba(90,140,240,0.16);
  --bg1:#0a0e16; --bg2:#06090f;
  --ink:#eaf0fb; --ink2:#c3cde0; --muted:#8696b4;
  --surface:#121826; --surface2:#0f1420;
  --line:rgba(255,255,255,0.09); --line2:rgba(255,255,255,0.06);
  --glass:rgba(255,255,255,0.055); --glass-line:rgba(255,255,255,0.10);
  --shadow:0 20px 54px rgba(0,0,0,0.55);
  --good:#56d3a0; --good-soft:rgba(86,211,160,0.16);
  --red:#fca5a5; --red-soft:rgba(248,113,113,0.16);
  --orange:#fb923c; --grid:rgba(120,160,255,0.05);
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:'DM Sans',sans-serif; color:var(--ink); min-height:100vh; transition:background .35s ease,color .35s ease;
  background:radial-gradient(1200px 600px at 10% -10%, var(--brand-soft), transparent 60%),
    radial-gradient(1000px 500px at 90% 0%, var(--brand-soft), transparent 55%),
    radial-gradient(130% 90% at 50% -10%, var(--bg1), var(--bg2)); }
.container { max-width:1100px; margin:0 auto; padding:22px 18px 60px; }
.header { display:flex; justify-content:space-between; align-items:center; padding:10px 0 18px; margin-bottom:18px; }
.header h1 { font-family:'Sora',sans-serif; font-size:1.6rem; color:var(--brand-deep); font-weight:800; letter-spacing:-0.5px; }
.header .subtitle { font-size:0.85rem; color:var(--muted); font-weight:500; }
.hd-right { display:flex; align-items:center; gap:12px; font-family:'JetBrains Mono',monospace; font-size:0.72rem; color:var(--muted); }
.nav { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:22px; }
.nav a { color:var(--ink2); text-decoration:none; font-size:0.8rem; font-weight:700; padding:8px 14px; border-radius:999px; transition:all 0.15s; }
.nav a:hover { background:var(--brand-soft); }
.nav a.active { background:var(--brand); color:#fff; }
.stats-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(150px, 1fr)); gap:12px; margin-bottom:22px; }
.stat-card { background:var(--glass); backdrop-filter:blur(20px); -webkit-backdrop-filter:blur(20px);
  border:1px solid var(--glass-line); border-radius:18px; padding:16px; box-shadow:var(--shadow); }
.stat-card .label { font-size:0.7rem; color:var(--muted); text-transform:uppercase; letter-spacing:0.5px; font-weight:700; }
.stat-card .value { font-size:1.5rem; font-weight:700; font-family:'JetBrains Mono', monospace; margin-top:4px; color:var(--ink); }
.stat-card .value.green { color:var(--good); }
.stat-card .value.red { color:var(--red); }
.stat-card .value.blue { color:var(--brand); }
.table-wrap { background:var(--glass); backdrop-filter:blur(20px); -webkit-backdrop-filter:blur(20px); border:1px solid var(--glass-line); border-radius:18px; padding:8px; overflow-x:auto; box-shadow:var(--shadow); }
table { width:100%; border-collapse:collapse; font-size:0.82rem; }
thead th { color:var(--muted); text-transform:uppercase; font-size:0.66rem; letter-spacing:0.5px; padding:10px 8px; text-align:left; border-bottom:1px solid var(--line); }
tbody td { padding:10px 8px; border-bottom:1px solid var(--line2); font-family:'JetBrains Mono', monospace; font-size:0.76rem; color:var(--ink2); }
tbody tr:hover { background:var(--brand-soft); }
.up { color:var(--good); }
.down { color:var(--red); }
.win { color:var(--good); font-weight:700; }
.loss { color:var(--red); font-weight:700; }
.pend { color:var(--orange); }
.hedge-badge { background:var(--brand-soft); color:var(--brand-deep); padding:2px 6px; border-radius:6px; font-size:0.7rem; }
.conf-high { color:var(--good); }
.conf-med { color:var(--orange); }
.conf-low { color:var(--red); }
.note-cell { max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; cursor:pointer; color:var(--muted); font-size:0.72rem; }
.note-cell:hover { white-space:normal; color:var(--ink); }
.filter-bar { display:flex; gap:8px; margin-bottom:16px; flex-wrap:wrap; }
.filter-btn { background:var(--surface); border:1px solid var(--line); color:var(--ink2); padding:7px 14px; border-radius:999px; cursor:pointer; font-size:0.8rem; font-weight:600; }
.filter-btn.active { background:var(--brand); color:#fff; border-color:var(--brand); }
.rtds-dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:6px; }
.rtds-dot.on { background:var(--good); box-shadow:0 0 6px var(--good); }
.rtds-dot.off { background:var(--red); }
.empty { text-align:center; padding:40px; color:var(--muted); }
@media (max-width:600px){
  .header h1 { font-size:1.25rem; }
  .nav { gap:4px; overflow-x:auto; flex-wrap:nowrap; }
  .nav a { padding:7px 11px; font-size:0.72rem; white-space:nowrap; }
  .stats-grid { grid-template-columns:repeat(2, 1fr); }
  .container { padding:18px 12px 50px; }
}
</style>
"""


_ICONS = {
    'home': '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:block"><path d="M3 10.4 12 3l9 7.4"/><path d="M5.5 9.2V20a1 1 0 0 0 1 1h11a1 1 0 0 0 1-1V9.2"/><path d="M9.5 21v-5.5a1 1 0 0 1 1-1h3a1 1 0 0 1 1 1V21"/></svg>',
    'picks': '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:block"><circle cx="12" cy="12" r="9"/><path d="m12 7.1 3.3 2.4-1.26 3.9H9.96L8.7 9.5z"/><path d="M12 3v4.1M5.1 9l2.85 2.25M18.9 9l-2.85 2.25M8.45 20.1 9.96 13.4M15.55 20.1 14.04 13.4"/></svg>',
    'codes': '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:block"><path d="M3.5 8.6A1.6 1.6 0 0 0 5 7V6.2A1.2 1.2 0 0 1 6.2 5h11.6A1.2 1.2 0 0 1 19 6.2V7a1.6 1.6 0 0 0 1.5 1.6v2A1.6 1.6 0 0 0 19 12.2v5.6a1.2 1.2 0 0 1-1.2 1.2H6.2A1.2 1.2 0 0 1 5 17.8v-5.6A1.6 1.6 0 0 0 3.5 10.6z"/><path d="M12 7.5v1.6M12 11.2v1.6M12 14.9v1.6"/></svg>',
    'builder': '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:block"><line x1="21" y1="5.5" x2="14" y2="5.5"/><line x1="10" y1="5.5" x2="3" y2="5.5"/><line x1="21" y1="12" x2="12.5" y2="12"/><line x1="8.5" y1="12" x2="3" y2="12"/><line x1="21" y1="18.5" x2="16" y2="18.5"/><line x1="12" y1="18.5" x2="3" y2="18.5"/><line x1="14" y1="3.5" x2="14" y2="7.5"/><line x1="8.5" y1="10" x2="8.5" y2="14"/><line x1="16" y1="16.5" x2="16" y2="20.5"/></svg>',
    'cards': '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:block"><rect x="3" y="4.5" width="18" height="15" rx="2.5"/><circle cx="8.5" cy="9.5" r="1.6"/><path d="m3.8 17.5 4.4-4.3a2 2 0 0 1 2.8 0l5.2 5.1"/><path d="m13.5 14 2-2a2 2 0 0 1 2.8 0l2 2"/></svg>',
    'crypto': '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:block"><circle cx="12" cy="12" r="9"/><path d="M12 6.4v11.2"/><path d="M14.9 9c-.5-.9-1.6-1.45-2.9-1.45-1.75 0-3.05.95-3.05 2.35 0 1.3 1 1.95 3.05 2.35s3.05.95 3.05 2.35c0 1.4-1.3 2.35-3.05 2.35-1.35 0-2.45-.55-2.95-1.45"/></svg>',
    'results': '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:block"><path d="M3 3v16.5A1.5 1.5 0 0 0 4.5 21H21"/><path d="m7 14.5 3.4-3.4 3 3 4.6-5.1"/><path d="M18 5h2v2"/></svg>',
}


def _tabbar_dark(active):
    """Self-contained bottom tab bar + theme toggle for the landing + crypto pages."""
    tabs = [("home", "/", "🏠", "Home"), ("picks", "/app/picks", "⚽", "Picks"),
            ("codes", "/app/codes", "🎫", "Codes"),
            ("cards", "/app/cards", "🖼️", "Cards"),
            ("crypto", "/app/paper-poly", "💰", "Crypto"),
            ("results", "/app/results", "📈", "Results")]
    items = "".join(
        '<a href="{}" class="{}"><span class="ic">{}</span>'
        '<span class="tl">{}</span></a>'.format(
            u, "active" if k == active else "", _ICONS.get(k, ""), l)
        for k, u, ic, l in tabs)
    css = (
        '<style>'
        '.cb-tabbar{position:fixed;bottom:0;left:0;right:0;z-index:9999;display:flex;'
        'justify-content:space-around;background:color-mix(in srgb,var(--surface) 90%, transparent);'
        'backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);'
        'border-top:1px solid var(--line);padding:6px 4px calc(6px + env(safe-area-inset-bottom));'
        'box-shadow:0 -6px 26px rgba(20,40,90,0.10);}'
        '.cb-tabbar a{flex:1;display:flex;flex-direction:column;align-items:center;gap:3px;'
        'text-decoration:none;color:var(--muted);padding:6px 2px;border-radius:12px;}'
        '.cb-tabbar a .ic{font-size:1.3rem;line-height:1;}'
        '.cb-tabbar a .tl{font-size:0.62rem;font-weight:700;letter-spacing:0.2px;}'
        '.cb-tabbar a.active{color:var(--brand);}'
        '.cb-tabbar a.active .tl{font-weight:900;}'
        '.theme-toggle{width:34px;height:34px;border-radius:10px;border:1px solid var(--line);'
        'background:var(--surface2);color:var(--ink);font-size:15px;cursor:pointer;line-height:1;'
        'display:flex;align-items:center;justify-content:center;transition:.18s;}'
        '.theme-toggle:hover{border-color:var(--brand);}'
        'body{padding-bottom:88px !important;}'
        '</style>')
    script = (
        '<script>(function(){var t=localStorage.getItem("cmvng-theme")||"light";'
        'document.documentElement.setAttribute("data-theme",t);})();'
        'function cmvngToggleTheme(){var d=document.documentElement,'
        'n=d.getAttribute("data-theme")==="dark"?"light":"dark";'
        'd.setAttribute("data-theme",n);localStorage.setItem("cmvng-theme",n);'
        'var b=document.getElementById("cmvngThemeBtn");if(b)b.textContent=n==="dark"?"☀️":"🌙";}'
        'document.addEventListener("DOMContentLoaded",function(){'
        'var t=localStorage.getItem("cmvng-theme")||"light";'
        'var b=document.getElementById("cmvngThemeBtn");if(b)b.textContent=t==="dark"?"☀️":"🌙";});'
        '</script>')
    return css + '<div class="cb-tabbar">' + items + '</div>' + script


def _v2_dashboard_html(platform, trades, bal):
    """Build dashboard HTML for a platform."""
    import html as _html

    total = len(trades)
    wins = sum(1 for t in trades if t.get("outcome") == "WIN")
    losses = sum(1 for t in trades if t.get("outcome") == "LOSS")
    active = sum(1 for t in trades if t.get("status") == "OPEN")
    resolved = wins + losses
    wr = round(wins / resolved * 100, 1) if resolved > 0 else 0
    balance = bal.get("balance", 100)
    peak = bal.get("peak_balance", 100)
    total_pnl = sum(t.get("pnl", 0) or 0 for t in trades if t.get("pnl") is not None)

    h = '<!DOCTYPE html><html><head><meta charset="utf-8">'
    h += '<meta name="viewport" content="width=device-width, initial-scale=1">'
    h += '<title>Cmvng Bot v2 — {}</title>'.format(platform.title())
    h += DASHBOARD_CSS
    h += '</head><body><div class="container">'

    # Header
    h += '<div class="header">'
    h += '<div><h1>CMVNG BOT v2</h1>'
    h += '<div class="subtitle">Confirmation Trading — {} Paper</div></div>'.format(platform.title())
    h += '<div class="hd-right"><span class="rtds-dot {}"></span><span>{}</span>'.format(
        "on" if _chainlink_connected else "off",
        "RTDS Live" if _chainlink_connected else "RTDS Off")
    h += '<button class="theme-toggle" id="cmvngThemeBtn" onclick="cmvngToggleTheme()" aria-label="Toggle theme">🌙</button></div>'
    h += '</div>'

    # Nav
    h += '<div class="nav">'
    h += '<a href="/app/paper-poly" class="{}">Polymarket</a>'.format("active" if platform == "polymarket" else "")
    h += '<a href="/app/paper-limitless" class="{}">Limitless</a>'.format("active" if platform == "limitless" else "")
    if platform == "limitless":
        h += '<a href="/app/live-limitless">Limitless Live</a>'
    h += '<a href="/app/picks">⚽ Picks</a>'
    h += '<a href="/app/codes">🎫 Codes</a>'
    h += '<a href="/app/results">📈 Results</a>'
    h += '<a href="/v2/status">Engine Status</a>'
    h += '</div>'

    # ── Limitless mode banner (toggle + approve + status, in-page) ──
    if platform == "limitless":
        live = _lmts_live_enabled()
        appr = _lmts_get_approval_state()
        addr = appr.get("wallet", "—")
        err = appr.get("error")
        flow_msg = request.args.get("msg", "") if request else ""
        flow_ok = request.args.get("approved") == "1" if request else False
        mode_bg = "#16a34a" if live else "#475569"
        mode_txt = "LIVE TRADING" if live else "PAPER MODE"
        flip_to = "0" if live else "1"
        flip_label = "Switch to Paper" if live else "Go Live →"
        flip_bg = "#475569" if live else "#16a34a"
        flip_confirm = ("'Switch to PAPER mode? Live trading will stop until you turn it back on.'"
                        if live else
                        "'Activate LIVE trading? Real USDC on Base will be used. $%.2f per trade, pauses below $%.2f balance.'" % (
                            LIMITLESS_MAX_TRADE_USDC, LIMITLESS_MIN_BALANCE_USDC))
        # Approval state line
        if err:
            appr_html = ("<span style='color:#b91c1c;font:12px ui-monospace,monospace'>"
                         "Approval check failed: {}</span>").format(_html.escape(err)[:160])
            approve_btn = ""
        else:
            allow = appr.get("allowance_usdc", 0)
            bal_u = appr.get("balance_usdc", 0)
            eth = appr.get("eth_balance", 0)
            needs = appr.get("needs_approval", True)
            allow_color = "#16a34a" if not needs else "#a16207"
            appr_html = ("<span style='color:#475569;font:12px ui-monospace,monospace'>"
                         "USDC <b style='color:{ac}'>${al:.2f}</b> approved | "
                         "Balance <b>${bu:.2f}</b> | ETH gas <b>{eth:.5f}</b>"
                         "</span>").format(ac=allow_color, al=allow, bu=bal_u, eth=eth)
            if needs:
                approve_btn = ('<form action="/app/limitless-approve" method="post" style="margin:0">'
                               '<button type="submit" onclick="return confirm(\'Approve USDC for the Limitless exchange contract on Base? This is a one-time on-chain transaction and costs a tiny amount of ETH for gas.\')"'
                               ' style="background:#2563eb;color:#fff;border:0;padding:8px 14px;border-radius:8px;'
                               'font:600 13px system-ui;cursor:pointer">Approve USDC</button></form>')
            else:
                approve_btn = ('<span style="color:#16a34a;font:600 12px system-ui">✓ Approved</span>')

        flash_html = ""
        if flow_msg:
            flash_bg = "#dcfce7" if flow_ok else "#fee2e2"
            flash_color = "#166534" if flow_ok else "#7f1d1d"
            flash_html = ('<div style="background:{bg};color:{c};padding:8px 14px;border-radius:8px;'
                          'margin:8px 0;font:13px system-ui">{m}</div>').format(
                              bg=flash_bg, c=flash_color, m=_html.escape(flow_msg.replace("+", " "))[:200])

        h += ('<div style="background:var(--card-bg,#f8fafc);border:1px solid var(--border,#e5e7eb);'
              'border-radius:12px;padding:14px 16px;margin:12px 0 16px">{flash}'
              '<div style="display:flex;flex-wrap:wrap;align-items:center;gap:14px">'
              '<span style="background:{bg};color:#fff;padding:6px 14px;border-radius:8px;'
              'font:700 13px system-ui">{txt}</span>'
              '<span style="color:var(--muted,#64748b);font:12px ui-monospace,monospace">{addr}</span>'
              '{appr}'
              '<div style="margin-left:auto;display:flex;gap:8px;align-items:center">'
              '{approve}'
              '<form action="/app/limitless-toggle" method="post" style="margin:0">'
              '<input type="hidden" name="to" value="{to}">'
              '<button type="submit" onclick="return confirm({conf})" '
              'style="background:{bbg};color:#fff;border:0;padding:8px 16px;border-radius:8px;'
              'font:600 13px system-ui;cursor:pointer">{lbl}</button></form>'
              '</div></div></div>').format(
                  flash=flash_html, bg=mode_bg, txt=mode_txt, addr=addr,
                  appr=appr_html, approve=approve_btn, to=flip_to, conf=flip_confirm,
                  bbg=flip_bg, lbl=flip_label)

    # Stats
    h += '<div class="stats-grid">'
    h += '<div class="stat-card"><div class="label">Balance</div><div class="value green">${:.2f}</div></div>'.format(balance)
    h += '<div class="stat-card"><div class="label">Peak</div><div class="value blue">${:.2f}</div></div>'.format(peak)
    h += '<div class="stat-card"><div class="label">Win Rate</div><div class="value {}">{:.1f}%</div></div>'.format(
        "green" if wr >= 70 else "red" if wr < 50 else "", wr)
    h += '<div class="stat-card"><div class="label">Record</div><div class="value">{}W / {}L</div></div>'.format(wins, losses)
    h += '<div class="stat-card"><div class="label">Active</div><div class="value blue">{}</div></div>'.format(active)
    h += '<div class="stat-card"><div class="label">Total P&L</div><div class="value {}">${:+.2f}</div></div>'.format(
        "green" if total_pnl >= 0 else "red", total_pnl)
    h += '</div>'

    # Trade table
    h += '<div class="table-wrap"><table><thead><tr>'
    h += '<th>#</th><th>Time</th><th>TF</th><th>Asset</th><th>Dir</th>'
    h += '<th>Limit</th><th>Ask</th><th>Fill</th><th>Conf</th><th>PTB</th><th>Result</th>'
    h += '<th>P&L</th><th>Bal</th><th>Hedge</th><th>Market</th><th>Note</th>'
    h += '</tr></thead><tbody>'

    if not trades:
        h += '<tr><td colspan="16" class="empty">No trades yet — watchers are scanning...</td></tr>'
    else:
        for t in trades:
            tid = t.get("id", "")
            fired = t.get("fired_at", "")
            if isinstance(fired, datetime):
                fired_str = fired.strftime("%m-%d %H:%M")
            elif fired:
                fired_str = str(fired)[:16]
            else:
                fired_str = ""

            tf = t.get("timeframe", "")
            asset = t.get("asset", "")
            direction = t.get("direction", "")
            dir_cls = "up" if direction == "UP" else "down"
            limit_p = t.get("limit_price")
            limit_str = "{:.0f}c".format(limit_p) if limit_p else "-"
            book_a = t.get("book_ask")
            ask_str = "{:.0f}c".format(book_a) if book_a else "-"
            order_st = t.get("order_status", "FILLED") or "FILLED"
            if order_st == "FILLED":
                fill_cls = "win"
                fill_str = "FILLED"
            elif order_st == "PENDING":
                fill_cls = "pend"
                fill_str = "PENDING"
            elif order_st == "EXPIRED":
                fill_cls = "loss"
                fill_str = "EXPIRED"
            else:
                fill_cls = ""
                fill_str = order_st
            conf = t.get("confidence", "")
            conf_val = int(conf) if conf and str(conf).isdigit() else 0
            conf_cls = "conf-high" if conf_val >= 80 else "conf-med" if conf_val >= 65 else "conf-low"
            ptb = t.get("ptb")
            ptb_str = "${:,.2f}".format(ptb) if ptb else "-"
            outcome = t.get("outcome") or t.get("status", "OPEN")
            oc_cls = "win" if outcome == "WIN" else "loss" if outcome == "LOSS" else "pend"
            pnl = t.get("pnl")
            pnl_str = "${:+.2f}".format(pnl) if pnl is not None else "-"
            bal_after = t.get("balance_after")
            bal_str = "${:.2f}".format(bal_after) if bal_after else "-"
            hedged = t.get("hedged")
            hedge_str = '<span class="hedge-badge">HEDGED</span>' if hedged else ""
            market_url = t.get("market_url", "")
            if market_url:
                link_str = '<a href="{}" target="_blank" style="color:var(--brand);text-decoration:none;font-size:0.72rem;">View ↗</a>'.format(
                    _html.escape(market_url))
            else:
                link_str = "-"
            note = _html.escape(str(t.get("entry_note", "") or ""))

            h += '<tr>'
            h += '<td>{}</td><td>{}</td><td>{}</td><td>{}</td>'.format(tid, fired_str, tf, asset)
            h += '<td class="{}">{}</td>'.format(dir_cls, direction)
            h += '<td>{}</td><td>{}</td><td class="{}">{}</td>'.format(limit_str, ask_str, fill_cls, fill_str)
            h += '<td class="{}">{}</td><td>{}</td>'.format(conf_cls, conf, ptb_str)
            h += '<td class="{}">{}</td>'.format(oc_cls, outcome)
            h += '<td>{}</td><td>{}</td><td>{}</td><td>{}</td>'.format(pnl_str, bal_str, hedge_str, link_str)
            h += '<td class="note-cell" title="{}">{}</td>'.format(note, note[:60])
            h += '</tr>'

    h += '</tbody></table></div></div>' + _tabbar_dark("crypto") + '</body></html>'
    return h


# ═══════════════════════════════════════════════════════════
# LANDING PAGE
# ═══════════════════════════════════════════════════════════

LANDING_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cmvng Bot — Confirmation Trading Engine</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@400;600;700;800&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
<style>
:root{
  --brand:#2f6bd6; --brand2:#4f86ee; --brand-deep:#1f54b0; --brand-soft:#e8f0fd;
  --bg1:#eef3fa; --bg2:#e3eaf5;
  --ink:#0c1320; --ink2:#3a465c; --muted:#73819b;
  --surface:#ffffff; --surface2:#f6f9fd;
  --line:rgba(12,19,32,0.09); --line2:rgba(12,19,32,0.06);
  --glass:rgba(255,255,255,0.72); --glass-line:rgba(12,19,32,0.07);
  --shadow:0 16px 44px rgba(20,40,90,0.10);
  --good:#1f9d6b; --good-soft:rgba(31,157,107,0.13);
  --red:#e1556a; --red-soft:rgba(225,85,106,0.13);
  --orange:#e08a3c; --grid:rgba(47,107,214,0.05);
}
[data-theme="dark"]{
  --brand:#5a8cf0; --brand2:#7aa2f5; --brand-deep:#3f78e6; --brand-soft:rgba(90,140,240,0.16);
  --bg1:#0a0e16; --bg2:#06090f;
  --ink:#eaf0fb; --ink2:#c3cde0; --muted:#8696b4;
  --surface:#121826; --surface2:#0f1420;
  --line:rgba(255,255,255,0.09); --line2:rgba(255,255,255,0.06);
  --glass:rgba(255,255,255,0.055); --glass-line:rgba(255,255,255,0.10);
  --shadow:0 20px 54px rgba(0,0,0,0.55);
  --good:#56d3a0; --good-soft:rgba(86,211,160,0.16);
  --red:#fca5a5; --red-soft:rgba(248,113,113,0.16);
  --orange:#fb923c; --grid:rgba(120,160,255,0.05);
}
*{margin:0;padding:0;box-sizing:border-box;}
html{scroll-behavior:smooth;}
body{ font-family:'DM Sans',sans-serif; color:var(--ink); min-height:100vh; overflow-x:hidden; position:relative;
  background:radial-gradient(130% 90% at 50% -10%, var(--bg1), var(--bg2)); transition:background .35s ease,color .35s ease; }
body::before{ content:""; position:fixed; inset:0; z-index:0; pointer-events:none;
  background:radial-gradient(900px 520px at 18% -8%, var(--brand-soft), transparent 60%),
    radial-gradient(760px 520px at 88% 4%, var(--brand-soft), transparent 58%),
    radial-gradient(700px 700px at 50% 120%, var(--brand-soft), transparent 60%);
  animation:drift 16s ease-in-out infinite alternate; }
body::after{ content:""; position:fixed; inset:0; z-index:0; pointer-events:none; opacity:.6;
  background-image:linear-gradient(var(--grid) 1px, transparent 1px), linear-gradient(90deg, var(--grid) 1px, transparent 1px);
  background-size:46px 46px; mask-image:radial-gradient(circle at 50% 30%, black, transparent 78%);
  -webkit-mask-image:radial-gradient(circle at 50% 30%, black, transparent 78%); }
@keyframes drift{ from{transform:translate3d(0,0,0);} to{transform:translate3d(0,-18px,0);} }
.shell{position:relative; z-index:1; max-width:1080px; margin:0 auto; padding:30px 22px 40px;}
.bar{display:flex; align-items:center; justify-content:space-between;}
.bar-right{display:flex; align-items:center; gap:12px;}
.brand{font-family:'Sora',sans-serif; font-weight:800; font-size:1.05rem; letter-spacing:-.5px; color:var(--ink);}
.brand b{color:var(--brand);}
.bar .live{ display:inline-flex; align-items:center; gap:7px; font-family:'JetBrains Mono',monospace;
  font-size:.66rem; letter-spacing:1.5px; color:var(--brand-deep); text-transform:uppercase;
  border:1px solid var(--line); border-radius:999px; padding:6px 12px; background:var(--surface); }
.dot{width:7px; height:7px; border-radius:50%; background:var(--brand); box-shadow:0 0 0 0 rgba(47,107,214,.6); animation:pulse 1.8s infinite;}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(47,107,214,.5);}70%{box-shadow:0 0 0 9px rgba(47,107,214,0);}100%{box-shadow:0 0 0 0 rgba(47,107,214,0);}}
.theme-toggle{width:34px;height:34px;border-radius:10px;border:1px solid var(--line);background:var(--surface);color:var(--ink);font-size:15px;cursor:pointer;line-height:1;display:flex;align-items:center;justify-content:center;transition:.18s;}
.theme-toggle:hover{border-color:var(--brand);}
.hero{padding:64px 0 26px; text-align:center;}
.hero .kicker{ font-family:'JetBrains Mono',monospace; font-size:.72rem; letter-spacing:3px;
  color:var(--muted); text-transform:uppercase; opacity:0; animation:rise .7s .05s forwards; }
.hero h1{ font-family:'Sora',sans-serif; font-weight:800; letter-spacing:-2.5px; line-height:.96;
  font-size:clamp(3.2rem,11vw,6.4rem); margin:14px 0 6px; opacity:0;
  background:linear-gradient(180deg,var(--ink) 0%, var(--brand-deep) 70%, var(--brand) 100%);
  -webkit-background-clip:text; background-clip:text; -webkit-text-fill-color:transparent;
  filter:drop-shadow(0 12px 36px rgba(47,107,214,.18)); animation:rise .8s .12s forwards; }
.hero h1 sup{font-size:.26em; color:var(--brand); -webkit-text-fill-color:var(--brand); vertical-align:super; letter-spacing:0;}
.hero .lede{ max-width:560px; margin:18px auto 0; color:var(--ink2); font-size:1.05rem; line-height:1.6; opacity:0; animation:rise .8s .2s forwards; }
@keyframes rise{from{opacity:0; transform:translateY(16px);} to{opacity:1; transform:translateY(0);}}
.curve{margin:34px auto 0; max-width:620px; opacity:0; animation:rise .9s .3s forwards;}
.curve svg{width:100%; height:90px; display:block;}
.curve path.line{fill:none; stroke:var(--brand); stroke-width:2.4; stroke-linecap:round;
  stroke-dasharray:1400; stroke-dashoffset:1400; animation:draw 2.2s .5s ease-out forwards;
  filter:drop-shadow(0 4px 14px rgba(47,107,214,.35));}
.curve path.fill{fill:url(#grad); opacity:0; animation:fade 1.4s 1.6s forwards;}
@keyframes draw{to{stroke-dashoffset:0;}}
@keyframes fade{to{opacity:1;}}
.stats{display:flex; justify-content:center; gap:14px; flex-wrap:wrap; margin:38px 0 4px;}
.stat{ flex:1; min-width:150px; max-width:230px; padding:20px 18px; border-radius:18px;
  background:var(--glass); border:1px solid var(--glass-line); backdrop-filter:blur(10px); -webkit-backdrop-filter:blur(10px);
  box-shadow:var(--shadow); opacity:0; animation:rise .7s forwards; }
.stats .stat:nth-child(1){animation-delay:.34s;}
.stats .stat:nth-child(2){animation-delay:.42s;}
.stats .stat:nth-child(3){animation-delay:.5s;}
.stat .num{font-family:'JetBrains Mono',monospace; font-weight:700; font-size:2.05rem; color:var(--ink); letter-spacing:-1px;}
.stat .lab{font-size:.7rem; color:var(--muted); text-transform:uppercase; letter-spacing:1.5px; margin-top:6px;}
.grid{display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:16px; margin:46px 0 0;}
.feat{ position:relative; padding:24px 22px; border-radius:20px; overflow:hidden;
  background:var(--glass); border:1px solid var(--glass-line); backdrop-filter:blur(10px); -webkit-backdrop-filter:blur(10px);
  box-shadow:var(--shadow); transition:transform .25s cubic-bezier(.2,.8,.2,1), border-color .25s, box-shadow .25s;
  opacity:0; animation:rise .7s forwards; }
.grid .feat:nth-child(1){animation-delay:.56s;}
.grid .feat:nth-child(2){animation-delay:.63s;}
.grid .feat:nth-child(3){animation-delay:.7s;}
.feat::before{content:""; position:absolute; inset:0 0 auto 0; height:2px;
  background:linear-gradient(90deg, transparent, var(--brand), transparent); opacity:.6;}
.feat:hover{transform:translateY(-5px); border-color:var(--brand-soft); box-shadow:0 24px 60px rgba(20,40,90,.16);}
.feat .ico{font-size:1.5rem; display:inline-block; margin-bottom:12px;}
.feat h3{font-family:'Sora',sans-serif; font-weight:700; font-size:1.02rem; color:var(--brand-deep); margin-bottom:8px;}
.feat p{color:var(--ink2); font-size:.85rem; line-height:1.55;}
.cta{display:flex; flex-wrap:wrap; gap:12px; justify-content:center; margin:44px 0 0; opacity:0; animation:rise .7s .8s forwards;}
.btn{ display:inline-flex; align-items:center; gap:8px; text-decoration:none; font-weight:700;
  font-size:.92rem; padding:15px 28px; border-radius:14px; transition:transform .2s, box-shadow .2s, background .2s; }
.btn.primary{color:#fff; background:linear-gradient(135deg, var(--brand), var(--brand2)); box-shadow:0 10px 30px rgba(47,107,214,.32);}
.btn.primary:hover{transform:translateY(-2px); box-shadow:0 16px 40px rgba(47,107,214,.45);}
.btn.ghost{color:var(--brand-deep); background:var(--surface); border:1px solid var(--line);}
.btn.ghost:hover{transform:translateY(-2px); background:var(--brand-soft);}
.foot{text-align:center; margin:54px 0 10px; color:var(--muted); font-size:.74rem; font-family:'JetBrains Mono',monospace; letter-spacing:.5px;}
</style></head><body>
<div class="shell">
  <div class="bar">
    <div class="brand">CMVNG<b>BOT</b></div>
    <div class="bar-right"><div class="live"><span class="dot"></span>Engine Live</div>
    <button class="theme-toggle" id="cmvngThemeBtn" onclick="cmvngToggleTheme()" aria-label="Toggle theme">🌙</button></div>
  </div>

  <div class="hero">
    <div class="kicker">Confirmation Trading Engine</div>
    <h1>Cmvng Bot<sup>v2</sup></h1>
    <p class="lede">Not prediction — confirmation. Wait for the candle to form, confirm the move won't reverse, then enter late at high odds.</p>

    <div class="curve">
      <svg viewBox="0 0 620 90" preserveAspectRatio="none">
        <defs><linearGradient id="grad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="rgba(47,107,214,0.26)"/>
          <stop offset="100%" stop-color="rgba(47,107,214,0)"/>
        </linearGradient></defs>
        <path class="fill" d="M0,74 L40,70 L90,72 L140,58 L190,62 L240,46 L300,50 L350,34 L410,40 L470,22 L530,26 L580,12 L620,16 L620,90 L0,90 Z"/>
        <path class="line" d="M0,74 L40,70 L90,72 L140,58 L190,62 L240,46 L300,50 L350,34 L410,40 L470,22 L530,26 L580,12 L620,16"/>
      </svg>
    </div>

    <div class="stats">
      <div class="stat"><div class="num" data-target="{{ paper_total }}" data-dec="0">0</div><div class="lab">Paper Trades</div></div>
      <div class="stat"><div class="num" data-target="{{ wr }}" data-dec="1" data-suffix="%">0</div><div class="lab">Win Rate</div></div>
      <div class="stat"><div class="num" data-target="{{ balance }}" data-dec="2" data-prefix="$">0</div><div class="lab">Paper Balance</div></div>
    </div>
  </div>

  <div class="grid">
    <div class="feat"><span class="ico">🕐</span><h3>Hourly</h3><p>5-minute intra-hour candles. HH/HL structure across Polymarket + Limitless, flat staking per entry.</p></div>
    <div class="feat"><span class="ico">⚡</span><h3>15-Minute</h3><p>1-minute candle structure with stricter confirmation. Both venues — enter only when the move is obvious.</p></div>
    <div class="feat"><span class="ico">⚽</span><h3>Football</h3><p>Full-board market scoring across 20+ fixtures. Safest legs surfaced into ready-to-play SportyBet codes.</p></div>
  </div>

  <div class="cta">
    <a class="btn primary" href="/app/codes">View Today's Codes →</a>
    <a class="btn ghost" href="/app/picks">⚽ Football Picks</a>
    <a class="btn ghost" href="/app/paper-poly">💰 Crypto Dashboard</a>
  </div>

  <div class="foot">CMVNG BOT · accuracy over coverage · auto-generated, always verify before staking</div>
</div>
<script>
window.addEventListener('load',function(){
  document.querySelectorAll('.num').forEach(function(el){
    var t=parseFloat(el.dataset.target)||0, dec=parseInt(el.dataset.dec||'0',10),
        pre=el.dataset.prefix||'', suf=el.dataset.suffix||'', start=null, dur=1200;
    function step(ts){ if(!start)start=ts; var p=Math.min((ts-start)/dur,1);
      var e=0.5-Math.cos(Math.PI*p)/2; var v=t*e;
      el.textContent=pre+v.toFixed(dec)+suf;
      if(p<1)requestAnimationFrame(step); }
    requestAnimationFrame(step);
  });
});
</script>
</body></html>"""


# ═══════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════

@app.route("/")
def landing():
    try:
        conn = get_db()
        rows = conn.run("SELECT COUNT(*), COALESCE(SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN outcome IN ('WIN','LOSS') THEN 1 ELSE 0 END), 0) FROM v2_paper_trades")
        row = list(rows)[0] if rows else (0, 0, 0)
        paper_total = int(row[0] or 0)
        wins = int(row[1] or 0)
        resolved = int(row[2] or 0)
        conn.close()
    except:
        paper_total = 0; wins = 0; resolved = 0

    wr = round(wins / resolved * 100, 1) if resolved > 0 else 0
    balance = _v2_balances.get("polymarket", {}).get("balance", 100)

    html = render_template_string(LANDING_HTML,
        paper_total=paper_total, wr=wr, balance="{:.2f}".format(balance))
    return html.replace("</body>", _tabbar_dark("home") + "</body>")


@app.route("/app/paper-poly")
def paper_poly():
    try:
        conn = get_db()
        rows = conn.run("SELECT * FROM v2_paper_trades WHERE platform = 'polymarket' ORDER BY id DESC LIMIT 200")
        cols = [c['name'] for c in conn.columns]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()
    except:
        trades = []
    bal = _v2_balances.get("polymarket", {"balance": 100, "peak_balance": 100})
    return _v2_dashboard_html("polymarket", trades, bal)


@app.route("/app/paper-limitless")
def paper_limitless():
    try:
        conn = get_db()
        rows = conn.run("SELECT * FROM v2_paper_trades WHERE platform = 'limitless' ORDER BY id DESC LIMIT 200")
        cols = [c['name'] for c in conn.columns]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()
    except:
        trades = []
    bal = _v2_balances.get("limitless", {"balance": 100, "peak_balance": 100})
    return _v2_dashboard_html("limitless", trades, bal)


# ═══════════════════════════════════════════════════════════
# LIMITLESS LIVE — in-app controls (toggle + approve + records)
# ═══════════════════════════════════════════════════════════

def _lmts_get_approval_state():
    """Read-only on-chain state for the wallet: USDC allowance + balance + ETH
    for gas. Returns dict with 'error' on any failure so the UI can show it."""
    if not LIMITLESS_PRIV_KEY:
        return {"error": "LIMITLESS_PRIVATE_KEY not set"}
    try:
        pass  # limitless_sdk inlined above
        from web3 import Web3
        from eth_account import Account
        w3 = Web3(Web3.HTTPProvider(LIMITLESS_BASE_RPC))
        addr = Account.from_key(LIMITLESS_PRIV_KEY).address
        allowance_raw = check_usdc_allowance(w3, addr, CLOB_ADDRESS)
        balance_raw = get_usdc_balance(w3, addr)
        eth_wei = w3.eth.get_balance(Web3.to_checksum_address(addr))
        return {
            "wallet": addr,
            "allowance_usdc": allowance_raw / (10 ** USDC_DECIMALS),
            "balance_usdc": balance_raw / (10 ** USDC_DECIMALS),
            "eth_balance": float(eth_wei) / 1e18,
            "needs_approval": allowance_raw < (10 ** USDC_DECIMALS),  # < $1 allowance
        }
    except ImportError:
        return {"error": "py-limitless not installed — add to requirements.txt"}
    except Exception as e:
        return {"error": str(e)[:200]}


def _lmts_run_approval():
    """Sends the USDC approval transaction to Base. One-time on-chain step
    that lets the Limitless exchange contract spend the wallet's USDC. Needs
    a small amount of ETH on Base for gas."""
    if not LIMITLESS_PRIV_KEY:
        return {"ok": False, "error": "LIMITLESS_PRIVATE_KEY not set"}
    try:
        pass  # limitless_sdk inlined above
        from web3 import Web3
        w3 = Web3(Web3.HTTPProvider(LIMITLESS_BASE_RPC))
        result = ensure_usdc_approved(w3, LIMITLESS_PRIV_KEY, CLOB_ADDRESS, min_amount=0)
        return {"ok": True,
                "already_approved": result.get("already_approved", False),
                "tx_hash": result.get("tx_hash"),
                "allowance": result.get("allowance", 0)}
    except ImportError:
        return {"ok": False, "error": "py-limitless not installed"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:500]}


@app.route("/app/limitless-toggle", methods=["POST", "GET"])
def limitless_toggle():
    """Flip the live trading switch (DB-backed). POST changes state; GET is
    treated the same so a simple link works on mobile. The toggle takes
    effect on the next signal — no redeploy needed."""
    cur = _lmts_live_enabled()
    target = request.values.get("to", "").strip().lower()
    if target in ("1", "live", "on", "yes", "true"):
        new = True
    elif target in ("0", "paper", "off", "no", "false"):
        new = False
    else:
        new = not cur
    ok = _lmts_live_set(new)
    print("[LMTS-LIVE] toggled {} -> {} (ok={})".format(
        "live" if cur else "paper", "live" if new else "paper", ok))
    return redirect("/app/paper-limitless")


@app.route("/app/limitless-approve", methods=["POST", "GET"])
def limitless_approve():
    """Trigger the one-time on-chain USDC approval. Result is shown back on
    the paper bot page via a flash query param so the UX is one click + one
    confirmation message."""
    if _FB_CACHE.get("running"):
        pass  # not relevant; placeholder to keep style consistent
    res = _lmts_run_approval()
    if res.get("ok"):
        if res.get("already_approved"):
            msg = "Already approved (allowance OK)"
        else:
            tx = res.get("tx_hash") or ""
            msg = "Approval submitted: {}{}".format(tx[:14], "…" if len(tx) > 14 else "")
        print("[LMTS-LIVE] approve: {}".format(msg))
        return redirect("/app/paper-limitless?approved=1&msg=" + msg.replace(" ", "+"))
    err = res.get("error") or "unknown error"
    print("[LMTS-LIVE] approve error: {}".format(err))
    return redirect("/app/paper-limitless?approved=0&msg=" + err.replace(" ", "+")[:120])


@app.route("/app/live-limitless")
def live_limitless():
    """Live trades records page — mirrors the paper dashboard's UI/UX but
    reads from v2_live_trades. Live status badge + toggle live."""
    try:
        conn = get_db()
        rows = conn.run("SELECT * FROM v2_live_trades ORDER BY id DESC LIMIT 200")
        cols = [c['name'] for c in conn.columns]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()
    except Exception:
        trades = []
    return _v2_live_dashboard_html(trades)


@app.route("/app/live-redemptions")
def live_redemptions_page():
    """Diagnostic page showing the FULL lifecycle of every v2_live_trades
    row, so you can see exactly where the chain is breaking.

    Five buckets, top to bottom:
      1. ERROR / CAPPED       — never even submitted
      2. CANCELLED            — submitted but didn't fill
      3. FILLED, unresolved   — waiting on Limitless to settle the market
      4. RESOLVED, no win     — LOSS or PUSH, nothing to redeem
      5. RESOLVED, WIN        — should auto-redeem; shows redeem_status,
                                 attempts, last attempt time, tx hash, error
    """
    try:
        conn = get_db()
        rows = list(conn.run("""
            SELECT id, asset, timeframe, direction, fill_status,
                   outcome, pnl, stake_usdc, filled_size,
                   fill_price_cents, condition_id, market_slug,
                   redeem_status, redeem_attempts, redeem_tx_hash,
                   error_message, fired_at, resolved_at,
                   raw_response, order_id, size_shares, limit_price_cents
            FROM v2_live_trades
            ORDER BY id DESC LIMIT 100
        """))
        conn.close()
    except Exception as e:
        return "<pre>DB error: {}</pre>".format(str(e)[:600]), 500

    cols = ["id","asset","tf","dir","fill_status","outcome","pnl","stake",
            "filled","fill_px","cid","slug","redeem_status","redeem_attempts",
            "redeem_tx","err","fired_at","resolved_at","raw_response",
            "order_id","shares","limit_cents"]
    trades = [dict(zip(cols, r)) for r in rows]

    # bucket
    errored, cancelled, filled_unresolved, resolved_no_win, wins = [], [], [], [], []
    for t in trades:
        fs = (t.get("fill_status") or "").upper()
        oc = (t.get("outcome") or "").upper()
        if fs in ("ERROR", "CAPPED"):
            errored.append(t)
        elif fs == "CANCELLED":
            cancelled.append(t)
        elif fs == "FILLED" and not oc:
            filled_unresolved.append(t)
        elif fs == "FILLED" and oc in ("LOSS", "PUSH"):
            resolved_no_win.append(t)
        elif fs == "FILLED" and oc == "WIN":
            wins.append(t)

    def _fmt_raw(raw):
        if not raw: return "—"
        # raw_response is stored as JSON-encoded text. Try to pretty-print
        # or fall back to the raw string.
        try:
            import json as _j
            obj = _j.loads(raw) if isinstance(raw, str) else raw
            return "<details><summary style='cursor:pointer;color:#2f6bd6'>view raw</summary><pre style='font:11px ui-monospace;background:#f8f8f8;padding:8px;border-radius:6px;white-space:pre-wrap;max-height:280px;overflow:auto'>" + _j.dumps(obj, indent=2)[:3000] + "</pre></details>"
        except Exception:
            return "<details><summary style='cursor:pointer;color:#2f6bd6'>view raw</summary><pre style='font:11px ui-monospace;background:#f8f8f8;padding:8px;border-radius:6px;white-space:pre-wrap;max-height:280px;overflow:auto'>" + (str(raw)[:3000]) + "</pre></details>"

    def _fmt_row(t, include_redeem=True, include_raw=False):
        cid = (t.get("cid") or "")[:18]
        cid_html = ('<a href="/app/redeem-now?id={}">{}</a>'.format(t["id"], cid)
                    if include_redeem and cid else (cid or "—"))
        tx = (t.get("redeem_tx") or "")
        tx_html = ('<a href="https://basescan.org/tx/{}" target="_blank">{}…</a>'.format(
                    tx, tx[:14]) if tx else "—")
        raw_html = _fmt_raw(t.get("raw_response")) if include_raw else ""
        order_html = ""
        if include_raw:
            order_html = ('<div style="font:11px ui-monospace;color:#666">'
                          'order_id={oid} · {shares}sh @ {lim}c</div>'.format(
                          oid=(t.get("order_id") or "—")[:18],
                          shares=t.get("shares") or "?",
                          lim=t.get("limit_cents") or "?"))
        err_cell = (t.get("err") or "")[:250]
        return (
            "<tr>"
            "<td>{id}</td><td>{tf} {dir} {asset}{order}</td><td>{fs}</td>"
            "<td>{oc}</td><td>${pnl}</td><td>{cid}</td>"
            "<td>{rs} ({ra})</td><td>{tx}</td>"
            "<td style='max-width:340px;color:#a00;font-size:11px'>{err}{raw}</td>"
            "</tr>".format(
            id=t["id"], tf=t.get("tf",""), dir=t.get("dir",""),
            asset=t.get("asset",""), order=order_html,
            fs=t.get("fill_status",""),
            oc=t.get("outcome","") or "—", pnl=t.get("pnl") or "—",
            cid=cid_html, rs=t.get("redeem_status") or "—",
            ra=t.get("redeem_attempts") or 0, tx=tx_html,
            err=err_cell, raw=("<br>" + raw_html if include_raw else "")))

    def _section(title, items, hint, include_redeem=False, include_raw=False):
        if not items:
            body = "<p style='color:#888;font-size:13px;margin:8px 0 0'>(none)</p>"
        else:
            rows_html = "".join(_fmt_row(t, include_redeem, include_raw) for t in items)
            body = (
                "<table style='border-collapse:collapse;width:100%;font:13px system-ui'>"
                "<thead><tr style='background:#f0f0f0;text-align:left'>"
                "<th>id</th><th>trade</th><th>fill</th><th>outcome</th>"
                "<th>pnl</th><th>condition_id</th><th>redeem (attempts)</th>"
                "<th>tx</th><th>error / raw response</th></tr></thead>"
                "<tbody>" + rows_html + "</tbody></table>")
        return ("<h2 style='font:600 17px system-ui;margin-top:24px'>{} <span style='font-weight:400;font-size:13px;color:#888'>· {} row(s)</span></h2>"
                "<p style='font:12px system-ui;color:#888;margin:0 0 8px'>{}</p>{}".format(
                title, len(items), hint, body))

    html = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Live Redemptions — Cmvng Bot</title>"
        "<style>body{font:14px system-ui;max-width:1280px;margin:24px auto;padding:0 18px}"
        "table td,table th{padding:6px 8px;border-bottom:1px solid #eee;vertical-align:top}"
        "a{color:#2f6bd6;text-decoration:none}a:hover{text-decoration:underline}"
        "details summary{user-select:none}</style>"
        "</head><body>"
        "<h1 style='font:600 24px system-ui'>Live Redemption Diagnostics</h1>"
        "<p>Trace the full lifecycle of every live trade. <b>Click 'view raw'</b> "
        "on CANCELLED or ERROR rows to see exactly what Limitless's API returned "
        "— that's how we figure out which response field the parser is missing.</p>")
    html += _section("1. ERROR / CAPPED — never reached the exchange",
                     errored, "Order didn't even submit. Usual causes: token-id missing, "
                     "tick violation, balance below floor, signature format.",
                     include_raw=True)
    html += _section("2. CANCELLED — submitted but didn't fill",
                     cancelled, "Marketable-limit didn't match a maker, OR Limitless's "
                     "response shape isn't recognized by the parser. The raw response "
                     "is the smoking gun — expand 'view raw' on any row to see what "
                     "Limitless actually returned.",
                     include_raw=True)
    html += _section("3. FILLED, unresolved — waiting for market to settle",
                     filled_unresolved, "Order filled on-chain. The resolver polls the "
                     "market every 60s; once Limitless returns winningOutcome / status=resolved, "
                     "the row moves to bucket 4 or 5.")
    html += _section("4. FILLED, resolved as LOSS/PUSH — nothing to redeem",
                     resolved_no_win, "Market resolved against us (or pushed). No on-chain "
                     "redemption needed.")
    html += _section("5. FILLED + WIN — should auto-redeem",
                     wins, "redeem_status PENDING = scheduler will retry. DONE = on-chain "
                     "redemption tx submitted (click tx for BaseScan). FAILED = exceeded "
                     "max attempts (default 10); click the condition_id to retry manually.",
                     include_redeem=True, include_raw=True)
    html += ("<p style='margin-top:32px;font:12px system-ui;color:#666'>"
             "<a href='/app/live-limitless'>← live-limitless dashboard</a> · "
             "<a href='/app/codes'>codes</a></p></body></html>")
    return html


@app.route("/app/resolve-now")
def force_resolve_one():
    """Force a market-resolution check for a specific FILLED row, bypassing
    the min_age gate. Tells you exactly what Limitless is reporting for the
    market right now — `status`, `winningOutcome`, `winningOutcomeIndex`,
    `prices`. Use this when a row has been sitting in bucket 3 longer than
    the candle period and you want to know whether the resolver is failing
    or whether Limitless just hasn't settled the market yet.

    Usage: /app/resolve-now?id=49"""
    back = ("<p style='font:14px system-ui;margin-top:18px'>"
            "<a href='/app/live-redemptions'>← back to diagnostics</a></p>")
    try:
        row_id = int(request.args.get("id") or "0")
    except Exception:
        row_id = 0
    if not row_id:
        return ("<h3>Missing or invalid <code>?id=</code> param.</h3>" + back), 400

    try:
        conn = get_db()
        r = list(conn.run(
            "SELECT id, market_slug, condition_id, timeframe, asset, direction, "
            "filled_size, fill_price_cents, limit_price_cents, outcome, fill_status, fired_at "
            "FROM v2_live_trades WHERE id = :i", i=row_id))
        conn.close()
    except Exception as e:
        return "<pre>DB error: {}</pre>".format(e), 500
    if not r:
        return ("<h3>Row {} not found.</h3>".format(row_id) + back), 404

    (_, slug, cid, tf, asset, direction, fsize, fpx, lpx, outcome, fstat, fired) = r[0]
    if fstat != "FILLED":
        return ("<h3>Row {} status = {}, not FILLED.</h3>".format(row_id, fstat) + back), 400

    import requests as req
    out_lines = []
    out_lines.append("<h2 style='font:600 20px system-ui'>Force resolve · row {}</h2>".format(row_id))
    out_lines.append("<p>{} {} {} · filled {} shares @ {}c · fired_at {}</p>".format(
        tf, direction, asset, fsize, fpx or lpx, fired))
    try:
        r = req.get("{}/markets/{}".format(LIMITLESS_API, slug), timeout=8)
        if r.status_code != 200:
            return "<h3>Limitless API returned {}</h3><pre>{}</pre>".format(
                r.status_code, (r.text or "")[:1000]) + back, 200
        market = r.json()
    except Exception as e:
        return ("<h3 style='color:#b91c1c'>Limitless fetch failed</h3><pre>{}</pre>".format(e) + back), 500

    status = str(market.get("status", "")).lower()
    is_done = (status in ("resolved", "closed", "settled", "expired", "ended")
               or market.get("closed") is True
               or market.get("resolved") is True
               or market.get("expired") is True
               or market.get("winningOutcomeIndex") is not None
               or market.get("winningOutcome") not in (None, ""))
    winner = market.get("winningOutcome")
    widx = market.get("winningOutcomeIndex")
    prices = market.get("prices") or market.get("outcomePrices")

    out_lines.append("<h3 style='font:600 16px system-ui;margin-top:16px'>Limitless market state</h3>")
    out_lines.append(
        "<table style='font:13px system-ui;border-collapse:collapse'>"
        "<tr><td style='padding:4px 12px;color:#888'>status</td><td style='padding:4px'><code>{}</code></td></tr>"
        "<tr><td style='padding:4px 12px;color:#888'>is_done</td><td style='padding:4px'><b>{}</b></td></tr>"
        "<tr><td style='padding:4px 12px;color:#888'>winningOutcome</td><td style='padding:4px'><code>{}</code></td></tr>"
        "<tr><td style='padding:4px 12px;color:#888'>winningOutcomeIndex</td><td style='padding:4px'><code>{}</code></td></tr>"
        "<tr><td style='padding:4px 12px;color:#888'>prices</td><td style='padding:4px'><code>{}</code></td></tr>"
        "<tr><td style='padding:4px 12px;color:#888'>expirationTimestamp</td><td style='padding:4px'><code>{}</code></td></tr>"
        "</table>".format(status or "(empty)", is_done, winner, widx, prices,
                          market.get("expirationTimestamp")))

    if not is_done:
        out_lines.append(
            "<h3 style='color:#a16207;margin-top:18px'>Market not yet resolved on Limitless</h3>"
            "<p>This is why the row hasn't moved out of bucket 3. The resolver checks every "
            "60s but Limitless still considers this market open. Wait for Limitless to flip "
            "it to resolved (usually a few minutes after candle close, sometimes longer for "
            "low-volume markets).</p>")
        return "".join(out_lines) + back, 200

    # Determine actual outcome
    actual = None
    if str(winner).lower() in ("yes", "up", "above"): actual = "UP"
    elif str(winner).lower() in ("no", "down", "below"): actual = "DOWN"
    elif widx is not None: actual = "UP" if int(widx) == 0 else "DOWN"
    elif isinstance(prices, list) and len(prices) >= 2:
        if float(prices[0]) > 0.9: actual = "UP"
        elif float(prices[0]) < 0.1: actual = "DOWN"

    if not actual:
        out_lines.append(
            "<h3 style='color:#a16207'>Market is done but we couldn't determine the winner</h3>"
            "<p>The market is closed but none of the recognized fields tell us the outcome. "
            "Send a screenshot of this page and I'll patch the parser.</p>")
        return "".join(out_lines) + back, 200

    out_lines.append("<h3 style='font:600 16px system-ui;margin-top:18px'>Outcome determined: {}</h3>".format(actual))

    # Compute P&L and write back
    cost = float(fsize or 0) * (float(fpx or lpx or 0) / 100.0)
    if actual == direction:
        oc = "WIN"; pnl = float(fsize or 0) - cost; rs = "PENDING"
    else:
        oc = "LOSS"; pnl = -cost; rs = "SKIPPED"

    try:
        conn = get_db()
        if cid:
            conn.run("UPDATE v2_live_trades SET actual_result=:a, outcome=:o, pnl=:p, "
                     "resolved_at=NOW(), redeem_status=:r WHERE id=:i",
                     a=actual, o=oc, p=round(pnl, 4), r=rs, i=row_id)
        else:
            captured_cid = market.get("conditionId")
            conn.run("UPDATE v2_live_trades SET actual_result=:a, outcome=:o, pnl=:p, "
                     "resolved_at=NOW(), redeem_status=:r, condition_id=:c WHERE id=:i",
                     a=actual, o=oc, p=round(pnl, 4), r=rs, c=captured_cid, i=row_id)
        conn.close()
    except Exception as e:
        return "".join(out_lines) + "<pre style='color:red'>DB update failed: {}</pre>".format(e) + back, 500

    if oc == "WIN":
        out_lines.append(
            "<p style='color:#15803d;font-weight:600;margin-top:10px'>WIN — P&L ${:.4f} · "
            "redeem_status=PENDING · the redeemer will fire on the next 60s tick "
            '(or click here to redeem now: <a href="/app/redeem-now?id={}">force redeem</a>).</p>'.format(pnl, row_id))
    else:
        out_lines.append("<p style='color:#b91c1c;font-weight:600'>LOSS — P&L ${:.4f}</p>".format(pnl))
    return "".join(out_lines) + back, 200


@app.route("/app/redeem-now")
def force_redeem_one():
    """Force a redemption attempt for a specific v2_live_trades row.
    Bypasses the scheduler's max-attempts cap and runs synchronously so you
    see the tx hash (or the exact error) on the response page.

    Usage: /app/redeem-now?id=42  (use the row id from /app/live-redemptions)
    """
    back = ("<p style='font:14px system-ui;margin-top:18px'>"
            "<a href='/app/live-redemptions'>← back to diagnostics</a></p>")
    try:
        row_id = int(request.args.get("id") or "0")
    except Exception:
        row_id = 0
    if not row_id:
        return ("<h3>Missing or invalid <code>?id=</code> param.</h3>" + back), 400

    try:
        conn = get_db()
        r = list(conn.run(
            "SELECT id, condition_id, outcome, redeem_status, fill_status "
            "FROM v2_live_trades WHERE id = :i", i=row_id))
        conn.close()
    except Exception as e:
        return "<pre>DB error: {}</pre>".format(e), 500
    if not r:
        return ("<h3>Row {} not found.</h3>".format(row_id) + back), 404

    (_, cid, outcome, redeem_status, fill_status) = r[0]
    cid = cid or ""

    if not cid:
        return ("<h3>Row {} has no condition_id.</h3>"
                "<p>The resolver needs to capture it first, OR the order was "
                "placed before the lazy-fetch landed. You can fetch it manually "
                "with <a href='/app/debug-lmts-market'>/app/debug-lmts-market</a>"
                " and update the row by hand.</p>".format(row_id) + back), 400

    if fill_status != "FILLED":
        return ("<h3>Row {} status = {}, not FILLED.</h3>"
                "<p>Only filled positions can be redeemed.</p>".format(row_id, fill_status) + back), 400

    res = _lmts_redeem_winnings(cid)
    if res.get("ok"):
        tx_hash = res.get("tx_hash") or ""
        try:
            conn = get_db()
            conn.run("""
                UPDATE v2_live_trades SET
                    redeem_status       = 'DONE',
                    redeem_tx_hash      = :tx,
                    redeem_last_attempt = NOW()
                WHERE id = :tid
            """, tx=tx_hash, tid=row_id)
            conn.close()
        except Exception as e:
            print("[LMTS-LIVE] manual redeem update error: {}".format(e))
        return ("<h2 style='color:#15803d'>✓ Redemption submitted</h2>"
                "<p>Tx: <a href='https://basescan.org/tx/{tx}' target='_blank'>{tx}</a></p>"
                "<p style='font-size:13px;color:#666'>Wait ~10s for it to be mined, "
                "then check BaseScan to confirm USDC was credited to your wallet.</p>".format(
                tx=tx_hash) + back), 200
    else:
        err = res.get("error") or "unknown"
        return ("<h2 style='color:#b91c1c'>✗ Redemption failed</h2>"
                "<pre style='background:#fef2f2;padding:14px;border-radius:10px;"
                "white-space:pre-wrap;font:13px ui-monospace'>{}</pre>".format(
                err) + back), 200


@app.route("/app/debug-lmts-market")
def debug_lmts_market():
    """Hit GET /markets/{slug} on Limitless and dump the raw response. Used
    once-off to confirm the schema for _lmts_extract_tokens after a live ERROR.
    Pass ?slug=foo-bar (defaults to the most recent live-attempted slug).
    Sends X-API-Key if LIMITLESS_API_KEY env var is set. Returns plain JSON."""
    import requests as _req
    slug = (request.args.get("slug") or "").strip()
    if not slug:
        # Fall back to the most recently attempted slug from v2_live_trades.
        try:
            conn = get_db()
            row = list(conn.run(
                "SELECT market_slug FROM v2_live_trades "
                "WHERE market_slug IS NOT NULL AND market_slug <> '' "
                "ORDER BY id DESC LIMIT 1"))
            conn.close()
            if row:
                slug = row[0][0]
        except Exception:
            pass
    if not slug:
        return jsonify({"error": "no slug provided and no v2_live_trades row to fall back to"}), 400
    hdrs = {}
    api_key = os.environ.get("LIMITLESS_API_KEY", "").strip()
    if api_key:
        hdrs["X-API-Key"] = api_key
    out = {"slug": slug,
           "url": "{}/markets/{}".format(LIMITLESS_API, slug),
           "auth_header_sent": bool(api_key)}
    try:
        r = _req.get(out["url"], headers=hdrs, timeout=8)
        out["status"] = r.status_code
        try:
            out["body"] = r.json()
        except Exception:
            out["body_text"] = (r.text or "")[:4000]
        if isinstance(out.get("body"), dict):
            out["top_level_keys"] = sorted(list(out["body"].keys()))
            # Surface the keys the extractor checks for, so the schema gap is
            # obvious at a glance.
            b = out["body"]
            out["extractor_probe"] = {
                "clobTokenIds": b.get("clobTokenIds"),
                "tokens_len": len(b.get("tokens") or []) if isinstance(b.get("tokens"), list) else None,
                "positionIds": b.get("positionIds"),
                "venue": b.get("venue"),
                "conditionId": b.get("conditionId") or b.get("condition_id"),
            }
    except Exception as e:
        out["exception"] = str(e)[:400]
    return jsonify(out)


def _v2_live_dashboard_html(trades):
    """Live trades dashboard. Same theme/structure as _v2_dashboard_html — uses
    the shared DASHBOARD_CSS, header, and nav so paper and live feel like two
    tabs of one product. Stats and table columns are adapted to live data."""
    import html as _html

    total = len(trades)
    filled = sum(1 for t in trades if t.get("fill_status") == "FILLED")
    cancelled = sum(1 for t in trades if t.get("fill_status") == "CANCELLED")
    errored = sum(1 for t in trades if t.get("fill_status") == "ERROR")
    capped = sum(1 for t in trades if t.get("fill_status") == "CAPPED")
    fill_rate = round(filled / max(filled + cancelled, 1) * 100, 1)
    spend = sum(float(t.get("stake_usdc") or 0) for t in trades if t.get("fill_status") == "FILLED")
    today_spend = _lmts_today_spend_usdc()
    balance_usdc = _lmts_get_balance_usdc()      # None if RPC failed
    paused_low = (balance_usdc is not None and balance_usdc < LIMITLESS_MIN_BALANCE_USDC)

    # Resolution + redemption tallies (FILLED rows only count toward W/L stats).
    wins = sum(1 for t in trades if t.get("outcome") == "WIN")
    losses = sum(1 for t in trades if t.get("outcome") == "LOSS")
    pushes = sum(1 for t in trades if t.get("outcome") == "PUSH")
    total_pnl = sum(float(t.get("pnl") or 0) for t in trades if t.get("outcome"))
    open_filled = sum(1 for t in trades
                      if t.get("fill_status") == "FILLED" and not t.get("outcome"))
    redeem_done = sum(1 for t in trades if t.get("redeem_status") == "DONE")
    redeem_pending = sum(1 for t in trades if t.get("redeem_status") == "PENDING")
    redeem_failed = sum(1 for t in trades if t.get("redeem_status") == "FAILED")
    win_rate = round(wins / max(wins + losses, 1) * 100, 1) if (wins + losses) else 0.0

    appr = _lmts_get_approval_state()
    addr = appr.get("wallet", "—")
    live = _lmts_live_enabled()

    h = '<!DOCTYPE html><html><head><meta charset="utf-8">'
    h += '<meta name="viewport" content="width=device-width, initial-scale=1">'
    h += '<title>Cmvng Bot v2 — Limitless Live</title>'
    h += DASHBOARD_CSS
    h += '</head><body><div class="container">'

    # Header
    h += '<div class="header"><div><h1>CMVNG BOT v2</h1>'
    h += '<div class="subtitle">Confirmation Trading — Limitless <b>LIVE</b></div></div>'
    h += '<div class="hd-right"><span class="rtds-dot {}"></span><span>{}</span>'.format(
        "on" if _chainlink_connected else "off",
        "RTDS Live" if _chainlink_connected else "RTDS Off")
    h += '<button class="theme-toggle" id="cmvngThemeBtn" onclick="cmvngToggleTheme()" aria-label="Toggle theme">🌙</button></div></div>'

    # Nav (same as paper, with active state on Limitless live)
    h += '<div class="nav">'
    h += '<a href="/app/paper-poly">Polymarket</a>'
    h += '<a href="/app/paper-limitless">Limitless Paper</a>'
    h += '<a href="/app/live-limitless" class="active">Limitless Live</a>'
    h += '<a href="/app/picks">⚽ Picks</a>'
    h += '<a href="/app/codes">🎫 Codes</a>'
    h += '<a href="/app/results">📈 Results</a>'
    h += '</div>'

    # Mode banner: shows current state + lets you flip back to paper from here too
    mode_bg = "#16a34a" if live else "#475569"
    mode_txt = "LIVE TRADING" if live else "PAPER MODE"
    flip_to = "0" if live else "1"
    flip_label = "Switch to Paper" if live else "Go Live →"
    h += ('<div style="display:flex;flex-wrap:wrap;align-items:center;gap:12px;'
          'background:var(--card-bg,#f8fafc);border:1px solid var(--border,#e5e7eb);'
          'border-radius:12px;padding:14px 16px;margin:12px 0 16px">'
          '<span style="background:{bg};color:#fff;padding:6px 14px;border-radius:8px;'
          'font:700 13px system-ui">{txt}</span>'
          '<span style="color:var(--muted,#64748b);font:13px ui-monospace,monospace">{addr}</span>'
          '<form action="/app/limitless-toggle" method="post" style="margin-left:auto">'
          '<input type="hidden" name="to" value="{to}">'
          '<button type="submit" onclick="return confirm({conf})" '
          'style="background:{bbg};color:#fff;border:0;padding:8px 16px;border-radius:8px;'
          'font:600 13px system-ui;cursor:pointer">{lbl}</button>'
          '</form></div>'.format(
              bg=mode_bg, txt=mode_txt, addr=addr, to=flip_to,
              conf=("'Switch to PAPER mode? Live trading will stop until you turn it back on.'"
                    if live else
                    "'Activate LIVE trading? Real USDC on Base will be used. ${:.2f} per trade, pauses below ${:.2f} balance.'".format(
                        LIMITLESS_MAX_TRADE_USDC, LIMITLESS_MIN_BALANCE_USDC)),
              bbg="#475569" if live else "#16a34a", lbl=flip_label))

    # Stats — entry side
    h += '<div class="stats-grid">'
    h += '<div class="stat-card"><div class="label">Total attempts</div><div class="value">{}</div></div>'.format(total)
    h += '<div class="stat-card"><div class="label">Filled</div><div class="value green">{}</div></div>'.format(filled)
    h += '<div class="stat-card"><div class="label">Cancelled</div><div class="value">{}</div></div>'.format(cancelled)
    h += '<div class="stat-card"><div class="label">Errors</div><div class="value {}">{}</div></div>'.format("red" if errored else "", errored)
    h += '<div class="stat-card"><div class="label">Fill rate</div><div class="value {}">{:.1f}%</div></div>'.format(
        "green" if fill_rate >= 60 else "red" if fill_rate < 30 else "", fill_rate)
    # Wallet balance vs floor — the live circuit breaker. RED if paused, GREEN
    # if trading, GREY if balance read failed.
    if balance_usdc is None:
        bal_val_html = '<div class="value">RPC ✕</div>'
    elif paused_low:
        bal_val_html = '<div class="value red">${:.2f} ⏸</div>'.format(balance_usdc)
    else:
        bal_val_html = '<div class="value green">${:.2f}</div>'.format(balance_usdc)
    h += '<div class="stat-card"><div class="label">Wallet / Floor ${:.2f}</div>{}</div>'.format(
        LIMITLESS_MIN_BALANCE_USDC, bal_val_html)
    h += '<div class="stat-card"><div class="label">24h volume</div><div class="value blue">${:.2f}</div></div>'.format(today_spend)
    h += '</div>'

    # Stats — resolution + redemption (only shown once any trade has resolved
    # so an empty dashboard isn't cluttered with zeros)
    if (wins + losses + pushes + open_filled) > 0:
        h += '<div class="stats-grid" style="margin-top:8px">'
        h += '<div class="stat-card"><div class="label">Open (awaiting resolve)</div><div class="value">{}</div></div>'.format(open_filled)
        h += '<div class="stat-card"><div class="label">Wins</div><div class="value green">{}</div></div>'.format(wins)
        h += '<div class="stat-card"><div class="label">Losses</div><div class="value red">{}</div></div>'.format(losses)
        h += '<div class="stat-card"><div class="label">Win rate</div><div class="value {}">{:.1f}%</div></div>'.format(
            "green" if win_rate >= 55 else "red" if win_rate < 45 else "", win_rate)
        h += '<div class="stat-card"><div class="label">Realized P&L</div><div class="value {}">${:+.2f}</div></div>'.format(
            "green" if total_pnl > 0 else "red" if total_pnl < 0 else "", total_pnl)
        h += '<div class="stat-card"><div class="label">Redeemed on-chain</div><div class="value {}">{} done · {} pending{}</div></div>'.format(
            "green" if redeem_done and not redeem_failed else "red" if redeem_failed else "",
            redeem_done, redeem_pending,
            " · {} failed".format(redeem_failed) if redeem_failed else "")
        h += '</div>'

    # Trade table
    h += '<div class="table-wrap"><table><thead><tr>'
    h += '<th>#</th><th>Time</th><th>TF</th><th>Asset</th><th>Dir</th>'
    h += '<th>Limit</th><th>Fill px</th><th>Stake</th><th>Shares</th>'
    h += '<th>Status</th><th>Outcome</th><th>P&L</th><th>Redeem</th><th>Order id</th>'
    h += '</tr></thead><tbody>'

    for i, t in enumerate(trades):
        st = t.get("fill_status") or "—"
        cls = {"FILLED": "green", "CANCELLED": "", "CAPPED": "",
               "ERROR": "red"}.get(st, "")
        ts_val = t.get("fired_at")
        ts_txt = ts_val.strftime("%m-%d %H:%M:%S") if ts_val else "—"
        lim = t.get("limit_price_cents") or 0
        stake = t.get("stake_usdc") or 0
        shares = t.get("size_shares") or 0
        fp = t.get("fill_price_cents")
        fs = t.get("filled_size")
        oid = (t.get("order_id") or "")[:14]
        err_msg = t.get("error_message") or ""

        outcome = t.get("outcome") or ""
        if outcome == "WIN":
            oc_html = '<span class="badge green">WIN</span>'
        elif outcome == "LOSS":
            oc_html = '<span class="badge red">LOSS</span>'
        elif outcome == "PUSH":
            oc_html = '<span class="badge">PUSH</span>'
        elif st == "FILLED":
            oc_html = '<span class="badge" style="background:#fef3c7;color:#92400e">OPEN</span>'
        else:
            oc_html = '—'

        pnl_val = t.get("pnl")
        if pnl_val is None or outcome == "":
            pnl_html = '—'
        else:
            pnl_html = '<span style="color:{}">${:+.2f}</span>'.format(
                "#16a34a" if pnl_val > 0 else ("#dc2626" if pnl_val < 0 else "#475569"),
                float(pnl_val))

        rs = t.get("redeem_status") or ""
        rtx = t.get("redeem_tx_hash") or ""
        if rs == "DONE" and rtx:
            short_tx = rtx[:10] + "…"
            redeem_html = ('<a href="https://basescan.org/tx/{tx}" target="_blank" '
                          'style="color:#16a34a;text-decoration:none;font:600 11px ui-monospace">'
                          '✓ {short}</a>').format(tx=_html.escape(rtx), short=_html.escape(short_tx))
        elif rs == "PENDING":
            redeem_html = '<span class="badge" style="background:#fef3c7;color:#92400e">PENDING</span>'
        elif rs == "FAILED":
            redeem_html = '<span class="badge red">FAILED</span>'
        elif rs == "SKIPPED":
            redeem_html = '<span style="color:#94a3b8;font-size:11px">—</span>'
        else:
            redeem_html = '—'

        h += '<tr><td>{}</td><td>{}</td><td>{}</td><td><b>{}</b></td><td><b>{}</b></td>'.format(
            i + 1, ts_txt, t.get("timeframe") or "—",
            t.get("asset") or "—", t.get("direction") or "—")
        h += '<td>{:.0f}c</td><td>{}</td><td>${:.2f}</td><td>{:.4f}</td>'.format(
            lim, "{:.1f}c".format(fp) if fp else "—", stake, shares)
        h += '<td><span class="badge {}">{}</span>'.format(cls, st)
        if err_msg:
            h += '<div class="errmsg">{}</div>'.format(_html.escape(err_msg[:120]))
        h += '</td>'
        h += '<td>{}</td><td>{}</td><td>{}</td>'.format(oc_html, pnl_html, redeem_html)
        h += '<td class="mono">{}</td></tr>'.format(oid or "—")
    if not trades:
        h += '<tr><td colspan="14" class="muted">No live trades yet — flip the toggle when ready.</td></tr>'
    h += '</tbody></table></div>'

    h += '<style>.badge{padding:3px 9px;border-radius:6px;font:600 11px system-ui;background:#e2e8f0;color:#475569}'
    h += '.badge.green{background:#16a34a;color:#fff}.badge.red{background:#dc2626;color:#fff}'
    h += '.errmsg{font:11px ui-monospace,monospace;color:#b91c1c;margin-top:3px;max-width:280px;white-space:normal}'
    h += '.mono{font:12px ui-monospace,monospace;color:#64748b}.muted{color:#94a3b8;text-align:center;padding:24px}</style>'
    h += '</div></body></html>'
    return h



@app.route("/v2/status")
def v2_status():
    now = datetime.now(timezone.utc)
    session_label, session_safe = _v2_session_filter(now.hour)

    status = {
        "engine": "CMVNG BOT v2",
        "mode": "PAPER",
        "utc_time": now.isoformat(),
        "lagos_time": now.astimezone(LAGOS_TZ).strftime("%Y-%m-%d %H:%M"),
        "session": {"label": session_label, "safe": session_safe},
        "rtds": {"connected": _chainlink_connected, "prices": dict(_chainlink_prices)},
        "balances": dict(_v2_balances),
        "active_boundaries": len(_v2_active_boundaries),
        "threads": {
            "hourly_watcher": "scanning every 2min",
            "fifteen_min_watcher": "scanning every 1min",
            "daily_watcher": "scanning every 10min",
            "monitor": "hedge check every 30s",
            "resolver": "resolve every 60s",
        },
    }
    return jsonify(status)


@app.route("/v2/trades")
def v2_trades_api():
    """API endpoint for trade data."""
    platform = request.args.get("platform", "polymarket")
    timeframe = request.args.get("timeframe", "")
    limit = int(request.args.get("limit", 100))

    try:
        conn = get_db()
        query = "SELECT * FROM v2_paper_trades WHERE platform = :p"
        params = {"p": platform}
        if timeframe:
            query += " AND timeframe = :tf"
            params["tf"] = timeframe
        query += " ORDER BY id DESC LIMIT :lim"
        params["lim"] = limit
        rows = conn.run(query, **params)
        cols = [c['name'] for c in conn.columns]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()

        # Serialize datetimes
        for t in trades:
            for k, v in t.items():
                if isinstance(v, datetime):
                    t[k] = v.isoformat()

        return jsonify({"trades": trades, "balance": _v2_balances.get(platform, {})})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/v2/prices")
def v2_prices():
    """Current Chainlink prices."""
    return jsonify({
        "connected": _chainlink_connected,
        "prices": dict(_chainlink_prices),
        "ptb": {k: {"ts": v[0], "price": v[1]} for k, v in _chainlink_ptb.items()},
    })


# ═══════════════════════════════════════════════════════════
# SPORTS PREDICTION MODULE — Football Consensus Scanner v2
# ═══════════════════════════════════════════════════════════
# Scrapes prediction sites (league-specific pages for overlap),
# finds consensus, matches against Polymarket sports markets,
# scores picks, sends Telegram alerts.
# v2 fixes: correct Polymarket API (/public-search, /sports metadata,
#   /markets?tag_id + sports_market_types), league-targeted scraping,
#   robust score/probability parsing.
# ═══════════════════════════════════════════════════════════

import requests as _sports_req
from bs4 import BeautifulSoup
import re as _sports_re

SPORTS_SCAN_INTERVAL = 21600  # 6 hours between full scans
SPORTS_MIN_SCORE = 30         # Lower for testing — raise to 70 once validated
SPORTS_SOURCES = [
    "footballpredictions.com",
    "forebet.com",
    "footballpredictions.net",
]

# League pages that BOTH FP.com and Forebet cover — ensures overlap
_SPORTS_LEAGUES = {
    "epl": {
        "fp": "https://footballpredictions.com/footballpredictions/premierleaguepredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/england/premier-league",
    },
    "la_liga": {
        "fp": "https://footballpredictions.com/footballpredictions/primeradivisionpredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/spain/la-liga",
    },
    "serie_a": {
        "fp": "https://footballpredictions.com/footballpredictions/serieapredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/italy/serie-a",
    },
    "bundesliga": {
        "fp": "https://footballpredictions.com/footballpredictions/bundesligapredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/germany/bundesliga",
    },
    "ligue_1": {
        "fp": "https://footballpredictions.com/footballpredictions/ligue1predictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/france/ligue-1",
    },
    "ucl": {
        "fp": "https://footballpredictions.com/footballpredictions/championsleaguepredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/champions-league",
    },
    "uel": {
        "fp": "https://footballpredictions.com/footballpredictions/europaleaguepredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/europa-league",
    },
}

_sports_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
}

# Cache Polymarket soccer tag_id so we don't refetch every scan
_sports_poly_soccer_tag_id = None


def _sports_scrape_footballpredictions_com():
    """Scrape footballpredictions.com — both tip pages AND league-specific pages."""
    predictions = []

    # Tip pages (broad coverage — friendlies, playoffs, etc.)
    tip_pages = [
        ("correct-score", "https://footballpredictions.com/betting-tips/correct-score/"),
        ("over-2-5", "https://footballpredictions.com/betting-tips/over-2-5-goals/"),
        ("btts", "https://footballpredictions.com/betting-tips/btts/"),
        ("predictions", "https://footballpredictions.com/footballpredictions/"),
    ]
    # League pages (targeted — same leagues as Forebet for overlap)
    for league_key, urls in _SPORTS_LEAGUES.items():
        fp_url = urls.get("fp")
        if fp_url:
            tip_pages.append((league_key, fp_url))

    for tip_type, url in tip_pages:
        try:
            r = _sports_req.get(url, headers=_sports_headers, timeout=15)
            if r.status_code != 200:
                print("[SPORTS] FP.com {} — HTTP {}".format(tip_type, r.status_code))
                continue
            soup = BeautifulSoup(r.text, "html.parser")

            # Find links with "-vs-" in href
            links = soup.find_all("a", href=True)
            for link in links:
                href = link.get("href", "")
                text = link.get_text(" ", strip=True)
                if "-vs-" in href.lower():
                    for part in href.split("/"):
                        if "-vs-" in part.lower():
                            teams = _sports_re.sub(r'-prediction.*|-tips.*|-betting.*|-odds.*|-preview.*', '', part)
                            team_parts = teams.split("-vs-")
                            if len(team_parts) == 2:
                                home = team_parts[0].replace("-", " ").strip().title()
                                away = team_parts[1].replace("-", " ").strip().title()
                                if len(home) > 2 and len(away) > 2:
                                    # Get parent context for score extraction
                                    parent = link.parent
                                    if parent:
                                        grandparent = parent.parent
                                    else:
                                        grandparent = None
                                    # Search in widening context
                                    contexts = [text]
                                    if parent:
                                        contexts.append(parent.get_text(" ", strip=True))
                                    if grandparent:
                                        contexts.append(grandparent.get_text(" ", strip=True))

                                    score = None
                                    for ctx in contexts:
                                        # Pattern 1: "Prediction: 2-1" or "Score: 1-0"
                                        sm = _sports_re.search(
                                            r'(?:score|prediction|tip|result)[:\s]*(\d)\s*[-–:]\s*(\d)',
                                            ctx.lower())
                                        if sm:
                                            h, a = int(sm.group(1)), int(sm.group(2))
                                            if h <= 6 and a <= 6:
                                                score = "{}-{}".format(h, a)
                                                break
                                        # Pattern 2: standalone low-digit score NOT inside large numbers
                                        sms = _sports_re.findall(
                                            r'(?<![0-9/])([0-5])\s*[-]\s*([0-5])(?![0-9/])', ctx)
                                        for sm2 in sms:
                                            candidate = "{}-{}".format(sm2[0], sm2[1])
                                            # Reject common false positives
                                            if candidate not in ("0-0",) and candidate not in ctx[:3]:
                                                score = candidate
                                                break
                                        if score:
                                            break

                                    predictions.append({
                                        "source": "footballpredictions.com",
                                        "type": tip_type,
                                        "home": home, "away": away,
                                        "score": score,
                                        "text": (contexts[1] if len(contexts) > 1 else text)[:200],
                                    })
                            break
                # Also try text-based matching for links without -vs- in href
                elif " vs " in text.lower() or " v " in text.lower():
                    vs_match = _sports_re.search(r'(.+?)\s+(?:vs?\.?)\s+(.+?)$', text)
                    if vs_match:
                        home = vs_match.group(1).strip()[:40]
                        away = vs_match.group(2).strip()[:40]
                        if len(home) > 2 and len(away) > 2:
                            predictions.append({
                                "source": "footballpredictions.com",
                                "type": tip_type,
                                "home": home, "away": away,
                                "score": None,
                                "text": text[:200],
                            })

            count = len([p for p in predictions if p["type"] == tip_type])
            print("[SPORTS] FP.com {}: {} predictions".format(tip_type, count))
        except Exception as e:
            print("[SPORTS] FP.com {} error: {}".format(tip_type, e))

    # Deduplicate
    seen = set()
    unique = []
    for p in predictions:
        key = (_sports_normalize_team(p["home"]), _sports_normalize_team(p["away"]), p["type"])
        if key not in seen:
            seen.add(key)
            unique.append(p)

    print("[SPORTS] FP.com total: {} unique predictions".format(len(unique)))
    for p in unique[:3]:
        print("[SPORTS] FP.com sample: {} vs {} ({}) → {}".format(
            p["home"], p["away"], p["type"], p.get("score", "?")))
    return unique


def _sports_scrape_footballpredictions_net():
    """Scrape footballpredictions.net for correct score predictions."""
    predictions = []
    url = "https://footballpredictions.net/correct-score-predictions-betting-tips"
    try:
        r = _sports_req.get(url, headers=_sports_headers, timeout=15)
        if r.status_code != 200:
            print("[SPORTS] FP.net — HTTP {}".format(r.status_code))
            return predictions
        soup = BeautifulSoup(r.text, "html.parser")

        # FP.net uses table rows with match data
        # Look for links to individual match pages — they contain team names
        links = soup.find_all("a", href=True)
        for link in links:
            href = link.get("href", "")
            text = link.get_text(" ", strip=True)
            # Match pages have format: /team-a-vs-team-b-prediction/
            if "-vs-" in href and "prediction" in href:
                # Extract teams from the href
                parts = href.split("/")
                for part in parts:
                    if "-vs-" in part:
                        teams = part.replace("-prediction", "").replace("-tips", "")
                        team_parts = teams.split("-vs-")
                        if len(team_parts) == 2:
                            home = team_parts[0].replace("-", " ").strip().title()
                            away = team_parts[1].replace("-", " ").strip().title()
                            if len(home) > 2 and len(away) > 2:
                                # Look for score in nearby text
                                parent = link.parent
                                parent_text = parent.get_text(" ", strip=True) if parent else text
                                score_match = _sports_re.search(r'(\d)\s*[-–:]\s*(\d)', parent_text)
                                score = "{}-{}".format(score_match.group(1), score_match.group(2)) if score_match else None
                                predictions.append({
                                    "source": "footballpredictions.net",
                                    "type": "correct-score",
                                    "home": home, "away": away,
                                    "score": score,
                                    "text": parent_text[:200] if parent_text else text[:200],
                                })

        # Deduplicate
        seen = set()
        unique = []
        for p in predictions:
            key = (_sports_normalize_team(p["home"]), _sports_normalize_team(p["away"]))
            if key not in seen:
                seen.add(key)
                unique.append(p)
        predictions = unique

        print("[SPORTS] FP.net: {} predictions".format(len(predictions)))
        # Debug: show first 3
        for p in predictions[:3]:
            print("[SPORTS] FP.net sample: {} vs {} → {}".format(p["home"], p["away"], p.get("score", "?")))
    except Exception as e:
        print("[SPORTS] FP.net error: {}".format(e))
    return predictions


def _sports_scrape_forebet():
    """Scrape Forebet for mathematical predictions — 1X2, over/under, BTTS, correct score.
    Scrapes both the main today page AND league-specific pages for overlap with FP.com."""
    predictions = []

    # Main today page + all league-specific pages
    urls_to_scrape = [
        ("today", "https://www.forebet.com/en/football-tips-and-predictions-for-today"),
    ]
    for league_key, league_urls in _SPORTS_LEAGUES.items():
        forebet_url = league_urls.get("forebet")
        if forebet_url:
            urls_to_scrape.append((league_key, forebet_url))

    for page_name, url in urls_to_scrape:
        try:
            r = _sports_req.get(url, headers=_sports_headers, timeout=15)
            if r.status_code != 200:
                print("[SPORTS] Forebet {} — HTTP {}".format(page_name, r.status_code))
                continue
            soup = BeautifulSoup(r.text, "html.parser")

            # Forebet match rows: div.rcnt or tr with class starting with "tr_"
            rows = soup.find_all("div", class_="rcnt")
            if not rows:
                rows = soup.find_all("tr", class_=_sports_re.compile(r"tr_|pred"))
            # Fallback: look inside contentmiddle container
            if not rows:
                container = soup.find("div", id="contentmiddle") or soup.find("section", class_="schema")
                if container:
                    rows = container.find_all("div", class_=_sports_re.compile(r"rcnt|predictionRow"))

            for row in rows:
                text = row.get_text(" ", strip=True)
                if len(text) < 10:
                    continue

                # Extract teams — multiple selector strategies
                home = away = None

                # Strategy 1: homeTeam/awayTeam spans
                home_el = row.find("span", class_=_sports_re.compile(r"homeTeam|home_team"))
                away_el = row.find("span", class_=_sports_re.compile(r"awayTeam|away_team"))
                if home_el and away_el:
                    home = home_el.get_text(strip=True)[:40]
                    away = away_el.get_text(strip=True)[:40]

                # Strategy 2: tnms container with two spans/anchors
                if not home or not away:
                    tnms = row.find("span", class_="tnms") or row.find("div", class_="tnms")
                    if tnms:
                        team_els = tnms.find_all("a") or tnms.find_all("span")
                        if len(team_els) >= 2:
                            home = team_els[0].get_text(strip=True)[:40]
                            away = team_els[1].get_text(strip=True)[:40]
                        else:
                            # Single element with "vs" or "-" separator
                            tnms_text = tnms.get_text(" ", strip=True)
                            vs_m = _sports_re.search(r'(.+?)\s+(?:vs?\.?|[-–])\s+(.+)', tnms_text)
                            if vs_m:
                                home = vs_m.group(1).strip()[:40]
                                away = vs_m.group(2).strip()[:40]

                # Strategy 3: href-based extraction
                if not home or not away:
                    match_link = row.find("a", href=_sports_re.compile(r"-vs-|-against-"))
                    if match_link:
                        href = match_link.get("href", "")
                        for part in href.split("/"):
                            if "-vs-" in part:
                                team_parts = part.split("-vs-")
                                if len(team_parts) == 2:
                                    home = team_parts[0].replace("-", " ").strip().title()[:40]
                                    away = team_parts[1].replace("-", " ").strip().title()[:40]
                                break

                # Strategy 4: regex on row text
                if not home or not away:
                    vs_match = _sports_re.search(r'(.{3,30}?)\s+(?:vs?\.?|[-–])\s+(.{3,30}?)(?:\s+\d|$)', text)
                    if vs_match:
                        home = vs_match.group(1).strip()[:40]
                        away = vs_match.group(2).strip()[:40]

                if not home or not away or len(home) < 3 or len(away) < 3:
                    continue

                # Extract 1X2 probabilities — try multiple CSS class patterns
                prob_1 = prob_x = prob_2 = None
                for prob_class in [r"fpr\b", r"fprc\b", r"predict", r"prob"]:
                    probs = row.find_all("span", class_=_sports_re.compile(prob_class))
                    if len(probs) >= 3:
                        try:
                            prob_1 = int(probs[0].get_text(strip=True).replace("%", ""))
                            prob_x = int(probs[1].get_text(strip=True).replace("%", ""))
                            prob_2 = int(probs[2].get_text(strip=True).replace("%", ""))
                            if prob_1 + prob_x + prob_2 > 50:  # Sanity: should sum near 100
                                break
                            else:
                                prob_1 = prob_x = prob_2 = None
                        except (ValueError, IndexError):
                            prob_1 = prob_x = prob_2 = None

                # Fallback: look for percentage values in text
                if not prob_1:
                    pct_matches = _sports_re.findall(r'(\d{1,2})%', text)
                    if len(pct_matches) >= 3:
                        try:
                            vals = [int(x) for x in pct_matches[:3]]
                            if 80 < sum(vals) < 120:
                                prob_1, prob_x, prob_2 = vals
                        except:
                            pass

                # Extract correct score prediction
                score = None
                for sc_class in [r"ex_sc", r"foremark", r"scorePred", r"correct.?score"]:
                    score_el = row.find("span", class_=_sports_re.compile(sc_class))
                    if score_el:
                        sc_text = score_el.get_text(strip=True)
                        if _sports_re.match(r'\d+-\d+$', sc_text):
                            score = sc_text
                            break

                # Fallback: look for score pattern in specific containers
                if not score:
                    for tag in row.find_all(["span", "td", "div"]):
                        t = tag.get_text(strip=True)
                        if _sports_re.match(r'^[0-5]-[0-5]$', t):
                            score = t
                            break

                # Extract over/under average goals
                avg_goals = None
                for ou_class in [r"ou_", r"avg_goals", r"total"]:
                    ou_el = row.find("span", class_=_sports_re.compile(ou_class))
                    if ou_el:
                        try:
                            avg_goals = float(ou_el.get_text(strip=True))
                            break
                        except:
                            pass

                predictions.append({
                    "source": "forebet.com",
                    "type": "full",
                    "league": page_name,
                    "home": home, "away": away,
                    "score": score,
                    "prob_home": prob_1, "prob_draw": prob_x, "prob_away": prob_2,
                    "avg_goals": avg_goals,
                    "text": text[:200],
                })

            page_count = len([p for p in predictions if p.get("league") == page_name])
            print("[SPORTS] Forebet {}: {} predictions".format(page_name, page_count))
        except Exception as e:
            print("[SPORTS] Forebet {} error: {}".format(page_name, e))

    # Deduplicate across all pages
    seen = set()
    unique = []
    for p in predictions:
        key = (_sports_normalize_team(p["home"]), _sports_normalize_team(p["away"]))
        if key not in seen:
            seen.add(key)
            unique.append(p)

    print("[SPORTS] Forebet total: {} unique predictions".format(len(unique)))
    for p in unique[:3]:
        print("[SPORTS] Forebet sample: {} vs {} → {} ({}% / {}% / {}%)".format(
            p["home"], p["away"], p.get("score", "?"),
            p.get("prob_home", "?"), p.get("prob_draw", "?"), p.get("prob_away", "?")))
    return unique


def _sports_scrape_predictz():
    """Scrape PredictZ — often blocked (403). Gracefully skip if so."""
    predictions = []
    url = "https://www.predictz.com/predictions/today/"
    try:
        r = _sports_req.get(url, headers=_sports_headers, timeout=10)
        if r.status_code != 200:
            print("[SPORTS] PredictZ — HTTP {} (blocked, skipping)".format(r.status_code))
            return predictions
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.find_all("tr", class_=_sports_re.compile(r"pointed|pttr"))
        if not rows:
            rows = soup.find_all("div", class_=_sports_re.compile(r"match|fixture"))
        for row in rows:
            text = row.get_text(" ", strip=True)
            if not text or len(text) < 10:
                continue
            vs_match = _sports_re.search(r'(.+?)\s+(?:vs?\.?|[-–])\s+(.+?)(?:\s+\d|$)', text)
            if vs_match:
                home = vs_match.group(1).strip()[:40]
                away = vs_match.group(2).strip()[:40]
                score_match = _sports_re.search(r'(\d)\s*[-–:]\s*(\d)', text)
                score = "{}-{}".format(score_match.group(1), score_match.group(2)) if score_match else None
                predictions.append({
                    "source": "predictz.com",
                    "type": "correct-score",
                    "home": home, "away": away,
                    "score": score,
                    "text": text[:200],
                })
        print("[SPORTS] PredictZ: {} predictions".format(len(predictions)))
    except Exception as e:
        print("[SPORTS] PredictZ error: {}".format(e))
    return predictions


def _sports_normalize_team(name):
    """Normalize team names for matching across sources."""
    if not name:
        return ""
    n = name.lower().strip()
    # Remove common suffixes/prefixes
    for suffix in [" fc", " cf", " sc", " ac", " afc", " united", " city",
                   " town", " rovers", " wanderers", " athletic", " sporting",
                   " de ", " del "]:
        n = n.replace(suffix, " ")
    # Common abbreviations
    abbrevs = {
        "psg": "paris saint germain",
        "man utd": "manchester united", "man united": "manchester united",
        "man city": "manchester city",
        "spurs": "tottenham", "tottenham hotspur": "tottenham",
        "wolves": "wolverhampton",
        "newcastle utd": "newcastle",
        "west ham utd": "west ham",
        "real madrid": "real madrid", "r madrid": "real madrid",
        "atletico madrid": "atletico",
        "atletico": "atletico",
        "inter milan": "inter", "internazionale": "inter",
        "ac milan": "milan",
        "bayern munich": "bayern", "bayern munchen": "bayern",
        "borussia dortmund": "dortmund", "bvb": "dortmund",
        "rb leipzig": "leipzig",
        "st etienne": "saint etienne",
    }
    for abbr, full in abbrevs.items():
        if abbr in n:
            n = n.replace(abbr, full)
    # Remove special chars
    n = _sports_re.sub(r'[^a-z0-9 ]', '', n)
    n = _sports_re.sub(r'\s+', ' ', n).strip()
    return n


def _sports_match_teams(pred_home, pred_away, market_text):
    """Check if a prediction's teams match a market title.
    Uses fuzzy matching — any significant word from BOTH teams must appear."""
    mt = _sports_normalize_team(market_text)

    ph = _sports_normalize_team(pred_home)
    pa = _sports_normalize_team(pred_away)

    # Get significant words (>2 chars) from each team
    ph_words = [w for w in ph.split() if len(w) > 2]
    pa_words = [w for w in pa.split() if len(w) > 2]

    if not ph_words or not pa_words:
        return False

    # At least one significant word from each team must be in the market text
    home_match = any(w in mt for w in ph_words)
    away_match = any(w in mt for w in pa_words)

    return home_match and away_match


def _sports_fetch_polymarket_sports(match_pairs=None):
    """Fetch soccer/football markets from Polymarket using correct API endpoints.

    Strategy:
    1. GET /sports → get soccer tag_id and series info
    2. GET /markets?tag_id=X&closed=false → all active soccer markets
    3. GET /public-search?q=<team names> → match-specific markets
    4. GET /markets?sports_market_types=moneyline,total → match-day markets

    The /search endpoint doesn't exist — use /public-search (documented)."""
    global _sports_poly_soccer_tag_id
    markets = []
    seen_ids = set()

    # Step 1: Get soccer-specific tag_ids from /sports metadata (cached)
    # tag_id=1 is shared across ALL sports — useless for filtering.
    # We need sport-specific tags like EPL=82, UCL=306, etc.
    if not _sports_poly_soccer_tag_id:
        try:
            r = _sports_req.get("{}/sports".format(POLY_GAMMA_API), timeout=10)
            if r.status_code == 200:
                sports_list = r.json() if isinstance(r.json(), list) else []
                print("[SPORTS] Poly /sports returned {} sports".format(len(sports_list)))
                soccer_tag_ids = set()
                soccer_sport_codes = ["epl", "ucl", "uel", "ser", "bun", "lig",  # leagues
                                       "mls", "lcu", "acn", "fif", "es2", "cdr",
                                       "ucf", "soc", "football"]
                for sport in sports_list:
                    sport_code = (sport.get("sport", "") or "").lower()
                    if any(sc in sport_code for sc in soccer_sport_codes):
                        tags_str = str(sport.get("tags", ""))
                        for t in tags_str.split(","):
                            t = t.strip()
                            if t and t != "1":  # Skip tag_id=1 (shared by all)
                                soccer_tag_ids.add(t)
                if soccer_tag_ids:
                    _sports_poly_soccer_tag_id = ",".join(list(soccer_tag_ids)[:5])
                    print("[SPORTS] Poly soccer tag_ids: {} (from {} soccer sports)".format(
                        _sports_poly_soccer_tag_id,
                        sum(1 for s in sports_list
                            if any(sc in (s.get("sport","") or "").lower() for sc in soccer_sport_codes))))
                else:
                    _sports_poly_soccer_tag_id = "NONE"
                    print("[SPORTS] No soccer-specific tags found, will rely on search + smt only")
        except Exception as e:
            print("[SPORTS] Poly /sports error: {}".format(e))

    def _parse_market(m, event_title=""):
        """Parse a market object into our standard format.
        Rejects stale markets (dates more than 3 days from today)."""
        mid = str(m.get("id", "") or m.get("conditionId", ""))
        if not mid or mid in seen_ids:
            return None
        seen_ids.add(mid)
        q = m.get("question", "") or ""
        slug = m.get("slug", "") or ""

        # Date freshness check — reject markets with dates > 3 days from today
        # Slugs often contain dates like "2026-05-30" or "2026-05-28"
        import datetime as _dt
        today = _dt.date.today()
        date_match = _sports_re.search(r'(\d{4})-(\d{2})-(\d{2})', slug)
        if date_match:
            try:
                market_date = _dt.date(int(date_match.group(1)),
                                        int(date_match.group(2)),
                                        int(date_match.group(3)))
                days_diff = abs((market_date - today).days)
                if days_diff > 3:
                    return None  # Stale or too far future
            except:
                pass

        op = m.get("outcomePrices", "")
        if isinstance(op, str):
            try:
                op = json.loads(op)
            except:
                op = []
        outcomes = m.get("outcomes", "")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except:
                outcomes = []
        # Build URL — sports match markets use /sports/{sport}/{game_slug}
        # Non-sports events use /event/{slug}
        # The game slug is the slug without trailing market type suffix
        # Sport code is the first segment of the slug (e.g. "ucl", "fif", "epl", "es2")
        market_url = ""
        game_id_val = m.get("gameId", "") or ""
        smt_val = m.get("sportsMarketType", "") or ""
        if slug:
            if game_id_val or smt_val or _sports_re.match(r'^[a-z]{2,5}-[a-z]{2,5}-[a-z]{2,5}-\d{4}', slug):
                # Sports market — extract sport code and game slug
                parts = slug.split("-")
                sport_code = parts[0] if parts else ""
                # Game slug is the date-based portion: {sport}-{team1}-{team2}-{date}
                # Strip trailing market type suffixes
                game_slug = _sports_re.sub(
                    r'-(moneyline|totals?|btts|spread|both-teams-to-score|'
                    r'corners?|total-corners|draw|soccer-halftime-result|'
                    r'will-[a-z-]+win[a-z-]*|o-u-\d+.*|handicap.*)$',
                    '', slug)
                # If stripping didn't change it, use slug up to date portion
                if game_slug == slug:
                    date_m = _sports_re.search(r'(\d{4}-\d{2}-\d{2})', slug)
                    if date_m:
                        end_idx = date_m.end()
                        game_slug = slug[:end_idx]
                market_url = "https://polymarket.com/sports/{}/{}".format(
                    sport_code, game_slug)
            else:
                # Non-sports event
                market_url = "https://polymarket.com/event/{}".format(slug)
        return {
            "platform": "polymarket",
            "title": event_title or q,
            "question": q,
            "slug": slug,
            "market_id": mid,
            "condition_id": m.get("conditionId", ""),
            "outcome_prices": op,
            "outcomes": outcomes,
            "volume": float(m.get("volume", 0) or 0),
            "url": market_url,
            "best_ask": float(m.get("bestAsk", 0) or 0),
            "last_price": float(m.get("lastTradePrice", 0) or 0),
            "game_id": m.get("gameId", "") or "",
            "sports_market_type": m.get("sportsMarketType", "") or "",
        }

    # Step 2: Fetch active soccer markets by soccer-specific tag_ids
    if _sports_poly_soccer_tag_id and _sports_poly_soccer_tag_id != "NONE":
        for tag_id in _sports_poly_soccer_tag_id.split(",")[:3]:  # Query top 3 tags
            try:
                r = _sports_req.get("{}/markets".format(POLY_GAMMA_API),
                                   params={"tag_id": tag_id,
                                           "closed": False, "limit": 50,
                                           "order": "volume", "ascending": False},
                                   timeout=15)
                if r.status_code == 200:
                    data = r.json() if isinstance(r.json(), list) else []
                    tag_count = 0
                    for m in data:
                        parsed = _parse_market(m)
                        if parsed:
                            markets.append(parsed)
                            tag_count += 1
                    if tag_count:
                        print("[SPORTS] Poly tag={}: {} markets".format(tag_id, tag_count))
                time.sleep(0.2)
            except Exception as e:
                print("[SPORTS] Poly tag={} error: {}".format(tag_id, e))

    # Step 3: Fetch match-day sports markets by type (moneyline, total, btts)
    for smt in ["moneyline", "total", "btts", "spread"]:
        try:
            r = _sports_req.get("{}/markets".format(POLY_GAMMA_API),
                               params={"sports_market_types": smt,
                                       "closed": False, "limit": 50},
                               timeout=10)
            if r.status_code == 200:
                data = r.json() if isinstance(r.json(), list) else []
                count = 0
                for m in data:
                    q = (m.get("question", "") or "").lower()
                    smt_val = (m.get("sportsMarketType", "") or "").lower()
                    # EXCLUDE non-soccer sports market types
                    if any(x in smt_val for x in ["tennis", "map_handicap", "esport",
                                                    "round_handicap", "nba", "nfl",
                                                    "nhl", "mlb", "mma", "ufc"]):
                        continue
                    # Filter for soccer — team names, league names, or soccer-specific market types
                    soccer_market_types = ["moneyline", "total", "btts", "spread",
                                           "total_corners", "both_teams_to_score",
                                           "correct_score", "first_goal", "anytime_goal"]
                    is_soccer_smt = any(x in smt_val for x in soccer_market_types)
                    is_soccer_q = any(kw in q for kw in [
                        "goal", "soccer", " fc", "united",
                        "arsenal", "chelsea", "liverpool",
                        "barcelona", "real madrid", "bayern",
                        "psg", "juventus", "dortmund", "inter",
                        "milan", "atletico", "napoli", "benfica",
                        "porto", "ajax", "celtic", "rangers",
                        "premier league", "la liga", "serie a",
                        "bundesliga", "ligue 1", "champions league",
                        "ucl", "europa", "mls", "corner",
                        "both teams to score", "clean sheet",
                    ])
                    if is_soccer_smt or is_soccer_q:
                        parsed = _parse_market(m)
                        if parsed:
                            markets.append(parsed)
                            count += 1
                if count:
                    print("[SPORTS] Poly smt={}: {} soccer markets".format(smt, count))
        except Exception as e:
            print("[SPORTS] Poly smt={} error: {}".format(smt, e))
        time.sleep(0.3)

    # Step 4: Search for specific matches using /public-search
    if match_pairs:
        searched = 0
        for home, away in match_pairs[:15]:  # Limit API calls
            # Use shortened team names for better search results
            home_short = home.split()[-1] if home else ""  # Last word (e.g. "United")
            away_short = away.split()[-1] if away else ""
            query = "{} {}".format(home_short, away_short)
            if len(query.strip()) < 5:
                query = "{} {}".format(home, away)
            try:
                r = _sports_req.get("{}/public-search".format(POLY_GAMMA_API),
                                   params={"q": query, "limit_per_type": 5},
                                   timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    # /public-search returns {events: [...], tags: [...], profiles: [...]}
                    events = []
                    if isinstance(data, dict):
                        events = data.get("events", []) or []
                    elif isinstance(data, list):
                        events = data  # Fallback if format differs

                    for event in events:
                        event_title = event.get("title", "")
                        event_markets = event.get("markets", []) or []
                        if event_markets:
                            for m in event_markets:
                                parsed = _parse_market(m, event_title=event_title)
                                if parsed:
                                    markets.append(parsed)
                        else:
                            # Event itself might be a market
                            parsed = _parse_market(event)
                            if parsed:
                                markets.append(parsed)
                searched += 1
                time.sleep(0.3)
            except Exception as e:
                print("[SPORTS] Poly search '{}' error: {}".format(query[:30], e))
                searched += 1

    # Step 5: Also broad soccer searches for futures/props
    for broad_q in ["world cup", "champions league", "premier league",
                    "ballon d'or", "golden boot"]:
        try:
            r = _sports_req.get("{}/public-search".format(POLY_GAMMA_API),
                               params={"q": broad_q, "limit_per_type": 5},
                               timeout=10)
            if r.status_code == 200:
                data = r.json()
                events = data.get("events", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for event in (events or []):
                    event_title = event.get("title", "")
                    for m in (event.get("markets", []) or [event]):
                        parsed = _parse_market(m, event_title=event_title)
                        if parsed:
                            markets.append(parsed)
            time.sleep(0.3)
        except:
            pass

    print("[SPORTS] Polymarket total: {} soccer/football markets".format(len(markets)))
    # Show breakdown
    match_markets = [m for m in markets if m.get("game_id") or m.get("sports_market_type")]
    futures = [m for m in markets if not m.get("game_id") and not m.get("sports_market_type")]
    print("[SPORTS] Poly breakdown: {} match-level, {} futures/other".format(
        len(match_markets), len(futures)))
    # Show soccer-relevant samples (skip generic ones)
    shown = 0
    for m in markets:
        q = (m.get("question", "") or "").lower()
        if shown < 3 and any(kw in q for kw in ["vs", "goal", "corner", "win",
                                                   "fc", "united", "arsenal"]):
            print("[SPORTS] Poly sample: '{}' | smt={} | ask={}".format(
                m.get("question", "")[:60], m.get("sports_market_type", "?"),
                m.get("best_ask", "?")))
            shown += 1
    return markets


def _sports_fetch_limitless_sports(match_pairs=None):
    """Fetch sports markets from Limitless using documented API endpoints.

    Strategy (from docs.limitless.exchange):
    1. GET /markets/active?automationType=sports — returns all sports markets directly
    2. GET /markets/search?query=<team names> — semantic search for specific matches
    3. GET /markets/categories/count — discover what categories exist
    """
    markets = []
    seen_ids = set()

    def _parse_limitless(m):
        """Parse a Limitless market object."""
        mid = str(m.get("id", "") or m.get("address", "") or m.get("slug", ""))
        if not mid or mid in seen_ids:
            return None
        seen_ids.add(mid)
        title = m.get("title", "") or ""
        slug = m.get("slug", "") or ""
        address = m.get("address", "") or ""
        prices = m.get("prices", [])
        # Build URL — Limitless uses /markets/{slug} or /markets/{address}
        market_url = ""
        if slug:
            market_url = "https://limitless.exchange/markets/{}".format(slug)
        elif address:
            market_url = "https://limitless.exchange/markets/{}".format(address)
        return {
            "platform": "limitless",
            "title": title,
            "question": title,
            "slug": slug,
            "market_id": mid,
            "condition_id": m.get("conditionId", ""),
            "outcome_prices": [str(p/100) for p in prices] if prices else [],
            "outcomes": ["Yes", "No"],
            "volume": float(m.get("volumeFormatted", 0) or 0),
            "url": market_url,
            "best_ask": 0,
            "last_price": float(prices[0]/100) if prices else 0,
            "game_id": "",
            "sports_market_type": "",
        }

    # Step 1: Try automationType=sports (may 400 if Limitless doesn't support it yet)
    try:
        r = _sports_req.get("{}/markets/active".format(LIMITLESS_API),
                           params={"automationType": "sports", "page": 1, "limit": 50},
                           timeout=15)
        if r.status_code == 200:
            data = r.json()
            items = data.get("data", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for m in items:
                title_lower = (m.get("title", "") or "").lower()
                if any(kw in title_lower for kw in [
                    "goal", "soccer", "football", " fc", "united",
                    "arsenal", "chelsea", "liverpool", "barcelona",
                    "real madrid", "bayern", "psg", "juventus",
                    "premier league", "la liga", "serie a",
                    "bundesliga", "champions league", "ucl",
                    "europa", "mls", "cup", "corner", "btts",
                    "both teams", "clean sheet", " vs ", " v ",
                ]):
                    parsed = _parse_limitless(m)
                    if parsed:
                        markets.append(parsed)
            if items:
                print("[SPORTS] Limitless automationType=sports: {} soccer markets".format(len(markets)))
        else:
            print("[SPORTS] Limitless automationType=sports — HTTP {} (trying alternatives)".format(r.status_code))
    except Exception as e:
        print("[SPORTS] Limitless sports browse error: {}".format(e))

    # Step 2: Discover categories first, then browse sports category if it exists
    try:
        r = _sports_req.get("{}/markets/categories/count".format(LIMITLESS_API), timeout=10)
        if r.status_code == 200:
            cat_data = r.json()
            cat_counts = cat_data.get("category", {}) if isinstance(cat_data, dict) else {}
            print("[SPORTS] Limitless categories: {}".format(
                ", ".join("{}={}".format(k, v) for k, v in list(cat_counts.items())[:8])))
            # Try each category to find sports-related ones
            for cat_id, count in cat_counts.items():
                if int(count) > 0:
                    try:
                        cr = _sports_req.get("{}/markets/active/{}".format(LIMITLESS_API, cat_id),
                                           params={"page": 1, "limit": 20}, timeout=10)
                        if cr.status_code == 200:
                            cdata = cr.json()
                            citems = cdata.get("data", []) if isinstance(cdata, dict) else []
                            for m in citems[:3]:  # Sample first 3
                                title = (m.get("title", "") or "").lower()
                                tags = " ".join(str(t).lower() for t in (m.get("tags", []) or []))
                                cats = " ".join(str(c).lower() for c in (m.get("categories", []) or []))
                                if any(kw in (title + tags + cats) for kw in [
                                    "soccer", "football", "goal", " fc ", "match",
                                    "premier", "champions", "world cup", "btts", " vs "
                                ]):
                                    # This category has sports — fetch all
                                    print("[SPORTS] Limitless cat={} has sports markets, fetching...".format(cat_id))
                                    for fm in citems:
                                        parsed = _parse_limitless(fm)
                                        if parsed:
                                            markets.append(parsed)
                                    break
                        time.sleep(0.2)
                    except:
                        pass
    except Exception as e:
        print("[SPORTS] Limitless categories error: {}".format(e))

    # Step 3: Semantic search for specific matches
    if match_pairs:
        searched = 0
        for home, away in match_pairs[:10]:
            query = "{} {}".format(home.split()[-1] if home else "", away.split()[-1] if away else "").strip()
            if len(query) < 4:
                query = "{} {}".format(home, away)
            try:
                r = _sports_req.get("{}/markets/search".format(LIMITLESS_API),
                                   params={"query": query, "limit": 5},
                                   timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    items = data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []
                    for m in items:
                        parsed = _parse_limitless(m)
                        if parsed:
                            markets.append(parsed)
                searched += 1
                time.sleep(0.3)
            except:
                searched += 1

    print("[SPORTS] Limitless total: {} sports markets".format(len(markets)))
    for m in markets[:3]:
        print("[SPORTS] Limitless sample: '{}' | {}".format(
            m.get("title", "")[:60], m.get("url", "")))
    return markets


def _sports_extract_insights(predictions, home, away):
    """From all predictions for a match, extract actionable insights."""
    insights = {
        "match": "{} vs {}".format(home, away),
        "home": home, "away": away,
        "sources": [],
        "scores": [],
        "home_wins": 0, "draws": 0, "away_wins": 0,
        "total_goals_predicted": [],
        "over_25": 0, "under_25": 0,
        "btts_yes": 0, "btts_no": 0,
        "consensus_winner": None,
        "consensus_goals": None,
        "consensus_btts": None,
    }

    for p in predictions:
        insights["sources"].append(p["source"])
        score = p.get("score")
        if score and _sports_re.match(r'\d+-\d+', score):
            insights["scores"].append({"source": p["source"], "score": score})
            parts = score.split("-")
            try:
                h_goals = int(parts[0])
                a_goals = int(parts[1])
                total = h_goals + a_goals
                insights["total_goals_predicted"].append(total)
                if h_goals > a_goals:
                    insights["home_wins"] += 1
                elif a_goals > h_goals:
                    insights["away_wins"] += 1
                else:
                    insights["draws"] += 1
                if total > 2.5:
                    insights["over_25"] += 1
                else:
                    insights["under_25"] += 1
                if h_goals > 0 and a_goals > 0:
                    insights["btts_yes"] += 1
                else:
                    insights["btts_no"] += 1
            except:
                pass

        # Check tip type
        if p.get("type") == "over-2-5":
            insights["over_25"] += 1
        elif p.get("type") == "btts":
            insights["btts_yes"] += 1

        # Forebet probabilities
        if p.get("prob_home") and p.get("prob_away"):
            if p["prob_home"] > p["prob_away"] and p["prob_home"] > (p.get("prob_draw") or 0):
                insights["home_wins"] += 1
            elif p["prob_away"] > p["prob_home"]:
                insights["away_wins"] += 1
            else:
                insights["draws"] += 1

    n = len(insights["sources"])
    if n > 0:
        if insights["home_wins"] > n / 2:
            insights["consensus_winner"] = home
        elif insights["away_wins"] > n / 2:
            insights["consensus_winner"] = away
        elif insights["draws"] > n / 2:
            insights["consensus_winner"] = "DRAW"

        if insights["over_25"] > n / 2:
            insights["consensus_goals"] = "OVER"
        elif insights["under_25"] > n / 2:
            insights["consensus_goals"] = "UNDER"

        if insights["btts_yes"] > n / 2:
            insights["consensus_btts"] = "YES"
        elif insights["btts_no"] > n / 2:
            insights["consensus_btts"] = "NO"

    return insights


def _sports_score_pick(insights, market):
    """Score a potential pick from 0-100.
    Scoring philosophy: a specific score prediction (e.g. PSG 2-1 Arsenal)
    from even ONE site is useful if it aligns with the market type."""
    score = 0
    reasons = []
    n_preds = len(insights["sources"])
    unique_sources = len(set(insights["sources"]))

    # 1. Multi-source consensus bonus (max 30)
    if unique_sources >= 4:
        score += 30
        reasons.append("4+ sites agree")
    elif unique_sources >= 3:
        score += 20
        reasons.append("3+ sites agree")
    elif unique_sources >= 2:
        score += 10
        reasons.append("2 sites agree")

    # 2. Score predictions (max 20) — specific score predictions are high-value signals
    n_scores = len(insights["scores"])
    if n_scores >= 3:
        score += 20
        reasons.append("{} score predictions".format(n_scores))
    elif n_scores >= 1:
        score += 15
        reasons.append("{} score prediction{}".format(n_scores, "s" if n_scores > 1 else ""))

    # 3. Goals alignment with market (max 15)
    mq = market.get("question", "").lower()
    if insights["total_goals_predicted"]:
        avg = sum(insights["total_goals_predicted"]) / len(insights["total_goals_predicted"])
        if avg > 2.5 and ("over" in mq or "o/u" in mq):
            score += 15
            reasons.append("Goals avg {:.1f} (over)".format(avg))
        elif avg <= 2.5 and "under" in mq:
            score += 15
            reasons.append("Goals avg {:.1f} (under)".format(avg))
        elif avg >= 2.0:
            score += 8
            reasons.append("Goals avg {:.1f}".format(avg))

    # 4. Winner/draw consensus matches market (max 15)
    if insights["consensus_winner"] == "DRAW":
        if "draw" in mq or "end in a draw" in mq:
            score += 15
            reasons.append("DRAW consensus")
        # Also boost BTTS if draw predicted with goals
        if insights["total_goals_predicted"]:
            avg_g = sum(insights["total_goals_predicted"]) / len(insights["total_goals_predicted"])
            if avg_g >= 2 and ("both" in mq and "score" in mq):
                score += 10
                reasons.append("Draw {:.0f}-{:.0f} → BTTS likely".format(avg_g/2, avg_g/2))
    elif insights["consensus_winner"]:
        winner_norm = _sports_normalize_team(insights["consensus_winner"])
        winner_words = [w for w in winner_norm.split() if len(w) > 3]
        if any(w in mq for w in winner_words):
            score += 15
            reasons.append("{} predicted winner".format(insights["consensus_winner"]))

    # 5. BTTS consensus (max 10)
    if insights["consensus_btts"] == "YES" and ("both" in mq and "score" in mq):
        score += 10
        reasons.append("BTTS YES consensus")
    elif insights["consensus_btts"] == "NO" and ("both" in mq and "score" in mq):
        score += 5
        reasons.append("BTTS NO consensus")

    # 6. Forebet probability (max 15)
    for p in [pred for pred in insights.get("_raw_preds", []) if pred.get("prob_home")]:
        max_prob = max(p.get("prob_home", 0) or 0, p.get("prob_away", 0) or 0)
        if max_prob > 70:
            score += 15
            reasons.append("{}% win probability".format(max_prob))
            break
        elif max_prob > 55:
            score += 8
            reasons.append("{}% win probability".format(max_prob))
            break

    return score, reasons


# Football v3: cache of recent sports alerts per platform (for /sports menu)
_sports_market_cache = {"polymarket": [], "limitless": []}


def _sports_model_cc_pick(insights, market, is_card):
    """If the proven club model covers this match, price the corner/card market's
    line from the model and return a Yes/No pick ONLY when worst-case-safe
    (model >=75% on the side) AND positive edge (>=5pp vs the market's own price).
    Otherwise None (the market stays list-only). Domestic-league matches only."""
    try:
        import math as _m
        home = insights.get("home")
        away = insights.get("away")
        if not home or not away:
            return None
        season = _model_season()
        code = _model_detect_league(home, away, season)
        if not code:
            return None  # model covers domestic-league matches only
        m = model_club_match(home, away, code, season)
        if not m:
            return None
        lam = m.get("cards_exp" if is_card else "corners_exp")
        if not lam:
            return None
        q = ((market.get("question", "") or "") + " "
             + (market.get("title", "") or "")).lower()
        # Only price genuine TEAM-TOTAL corner/card markets. Reject player props
        # ("most cards", named players, bookings, scorer markets) — the model
        # prices team totals, not individuals.
        if any(w in q for w in ("most ", "record", "player", "carded",
                                "booked", "anytime", "top ", "to score",
                                "scorer", "sent off")):
            return None
        mnum = (_sports_re.search(r'(\d+)\s*(?:\+|or more)', q)
                or _sports_re.search(r'over\s*(\d+\.?\d*)', q)
                or _sports_re.search(r'(\d+\.?\d*)\s*(?:total\s+)?(?:corner|card)', q))
        if not mnum:
            return None
        thr = float(mnum.group(1))
        # Sane team-total line only — kills years (e.g. 2025) and junk numbers.
        if not (0.0 < thr <= 20.0):
            return None
        # "11+"/"11 or more" -> need >=11 ; "over 10.5" -> need >=11
        need = int(thr) if thr == int(thr) else int(_m.floor(thr)) + 1
        p_over = 1.0 - sum(_poisson_pmf(k, lam) for k in range(0, need))
        op = market.get("outcome_prices") or []
        yes_imp = None
        try:
            yes_imp = float(op[0])            # Limitless/Poly: [Yes, No]
        except (IndexError, ValueError, TypeError):
            ba = market.get("best_ask")
            yes_imp = float(ba) if ba else None
        noun = "cards" if is_card else "corners"
        if p_over >= 0.75 and yes_imp is not None and (p_over - yes_imp) >= 0.05:
            return "Yes — {:g}+ {} (model {:.0f}%)".format(thr, noun, p_over * 100)
        p_under = 1.0 - p_over
        no_imp = (1.0 - yes_imp) if yes_imp is not None else None
        if p_under >= 0.75 and no_imp is not None and (p_under - no_imp) >= 0.05:
            return "No — under {:g} {} (model {:.0f}%)".format(thr, noun, p_under * 100)
        return None
    except Exception:
        return None


def _sports_pick_outcome(insights, market):
    """Return the EXPLICIT recommended pick for THIS specific market, phrased so
    the user knows exactly what to back — 'Burgos to Win', 'Under 2.5 Goals',
    'Both Teams to Score — Yes', 'Draw'. Returns None when there's no confident,
    specific recommendation (so we never surface a vague pick)."""
    q = ((market.get("question", "") or "") + " " +
         (market.get("title", "") or "")).lower()
    smt = (market.get("sports_market_type", "") or "").lower()

    # Corners/cards: pickable ONLY when the proven club model covers the match
    # AND there's worst-case-safe edge vs the market's price (references the real
    # Limitless/Polymarket market). Player props stay list-only. Must come before
    # the totals branch — 'total_corners' contains 'total'.
    _is_corner = ("corner" in smt or "corner" in q)
    _is_card = (("card" in smt or "cards" in q or "booking" in q
                 or "yellow card" in q or "red card" in q) and not _is_corner)
    if (_is_corner or _is_card
            or any(w in smt for w in ("first_goal", "anytime_goal",
                                      "goalscorer", "player"))
            or any(w in q for w in ("sent off", "to start"))):
        if _is_corner or _is_card:
            _mp = _sports_model_cc_pick(insights, market, _is_card)
            if _mp:
                return _mp
        return None

    win = insights.get("consensus_winner")
    btts = insights.get("consensus_btts")
    avg = None
    if insights.get("total_goals_predicted"):
        vals = insights["total_goals_predicted"]
        avg = sum(vals) / len(vals)

    # ── Totals / Over-Under goals: state the side AND the line ──
    if "total" in smt or (("over" in q or "under" in q) and "goal" in q):
        ln = _sports_re.search(r'(\d+\.?\d*)\s*goal', q) or _sports_re.search(r'(\d+\.\d+)', q)
        line = ln.group(1) if ln else "2.5"
        if avg is None:
            return None  # no goals signal — don't guess a direction
        try:
            return ("Over {} Goals".format(line) if avg > float(line)
                    else "Under {} Goals".format(line))
        except ValueError:
            return None

    # ── Both Teams To Score ──
    if "btts" in smt or "both_teams" in smt or ("both" in q and "score" in q):
        if btts == "YES":
            return "Both Teams to Score — Yes"
        if btts == "NO":
            return "Both Teams to Score — No"
        if avg is not None and avg >= 2.4:
            return "Both Teams to Score — Yes"
        return None

    # ── Correct score ──
    if "correct" in smt or "correct score" in q:
        if insights.get("scores"):
            sc = insights["scores"][0].get("score")
            if sc:
                return "Correct Score {}".format(sc)
        return None

    # ── Moneyline / match winner ──
    # Require an explicit winner signal — don't treat every blank/unknown-type
    # market as a winner pick (that would mis-pick props as 'Team to Win').
    if ("moneyline" in smt or "win" in q or "winner" in q
            or "vs" in q or " or " in q or "outcome" in q):
        if win == "DRAW":
            return "Draw"
        if win:
            return "{} to Win".format(win)
        return None

    return None


def _sports_scan_and_alert():
    """Main sports scanning function. Scrapes all sites, finds consensus, matches markets, sends alerts."""
    print("[SPORTS] Starting scan...")

    # 1. Scrape all prediction sites
    all_predictions = []
    all_predictions.extend(_sports_scrape_footballpredictions_com())
    all_predictions.extend(_sports_scrape_footballpredictions_net())
    all_predictions.extend(_sports_scrape_forebet())
    all_predictions.extend(_sports_scrape_predictz())
    print("[SPORTS] Total predictions scraped: {}".format(len(all_predictions)))

    if not all_predictions:
        print("[SPORTS] No predictions found — skipping")
        return

    # 2. Group predictions by match (normalize team names)
    # Also handle home/away swaps between prediction sites
    matches = {}
    for p in all_predictions:
        key = (_sports_normalize_team(p["home"]), _sports_normalize_team(p["away"]))
        rev_key = (key[1], key[0])
        if key[0] and key[1]:
            if key in matches:
                matches[key]["predictions"].append(p)
            elif rev_key in matches:
                matches[rev_key]["predictions"].append(p)
            else:
                matches[key] = {"home": p["home"], "away": p["away"], "predictions": []}
                matches[key]["predictions"].append(p)

    print("[SPORTS] Unique matches found: {}".format(len(matches)))

    # Debug: show first 3 matches and their sources
    for i, (key, md) in enumerate(list(matches.items())[:5]):
        sources = set(p["source"] for p in md["predictions"])
        print("[SPORTS] Match {}: '{}' vs '{}' — {} sources: {}".format(
            i+1, key[0], key[1], len(sources), ", ".join(sources)))

    # 3. Fetch markets — search for matches with predictions
    # Count unique SITES per match (not prediction count)
    match_pairs = []
    multi_source_count = 0
    for key, md in matches.items():
        unique_sites = set(p["source"] for p in md["predictions"])
        md["unique_sites"] = len(unique_sites)
        if len(unique_sites) >= 2:
            multi_source_count += 1
        # Include all matches that have at least 1 prediction source
        # (lower threshold for testing — raise to 2 once we have more working scrapers)
        match_pairs.append((md["home"], md["away"]))

    print("[SPORTS] {} matches with 2+ sites, {} total to search".format(
        multi_source_count, len(match_pairs)))
    # Put multi-source matches first in the search queue
    multi_pairs = [(md["home"], md["away"]) for md in matches.values() if md.get("unique_sites", 0) >= 2]
    single_pairs = [(md["home"], md["away"]) for md in matches.values() if md.get("unique_sites", 0) < 2]
    search_pairs = multi_pairs + single_pairs
    poly_markets = _sports_fetch_polymarket_sports(match_pairs=search_pairs[:30])
    lmts_markets = _sports_fetch_limitless_sports(match_pairs=search_pairs[:10])
    all_markets = poly_markets + lmts_markets
    print("[SPORTS] Total sports markets: {} (Poly: {}, Limitless: {})".format(
        len(all_markets), len(poly_markets), len(lmts_markets)))

    # Debug: show first 3 markets
    for i, m in enumerate(all_markets[:3]):
        print("[SPORTS] Sample market {}: '{}' | '{}'".format(
            i+1, m.get("title", "")[:50], m.get("question", "")[:50]))

    if not all_markets:
        print("[SPORTS] No sports markets found — skipping")
        return

    # 4. Match predictions to markets and score
    print("[SPORTS] Starting prediction↔market matching: {} matches × {} markets...".format(
        len(matches), len(all_markets)))

    # Debug: log details for high-profile matches
    for key, md in matches.items():
        if "paris" in key[0] or "arsenal" in key[1] or "psg" in key[0]:
            preds = md["predictions"]
            scores = [p.get("score") for p in preds if p.get("score")]
            types = [p.get("type", "?") for p in preds]
            print("[SPORTS] DEBUG PSG: {} preds, types={}, scores={}, home='{}' away='{}'".format(
                len(preds), types, scores, md["home"], md["away"]))

    alerts_sent = 0
    matched_count = 0
    MAX_ALERTS_PER_MATCH = 5  # Cap alerts per match to avoid Telegram spam
    for key, match_data in matches.items():
        home = match_data["home"]
        away = match_data["away"]
        preds = match_data["predictions"]
        sources = set(p["source"] for p in preds)
        match_alerts = 0  # Track alerts for this match
        seen_alert_keys = set()  # Deduplicate same market appearing twice
        game_candidates = []  # collect all scored markets, then surface the best
        other_markets = []    # markets that matched but the bot can't judge (corners,
        #                       cards, player props) — listed for the user, no fake pick

        # Extract insights
        insights = _sports_extract_insights(preds, home, away)
        insights["_raw_preds"] = preds

        # Find matching markets
        for market in all_markets:
            mq = (market.get("question", "") + " " + market.get("title", "")).lower()
            if _sports_match_teams(home, away, mq):
                matched_count += 1

                # Deduplicate — same match + same market type shouldn't alert twice
                # e.g. "Will Switzerland win?" and "Will Jordan win?" are both moneyline for same game
                mslug = market.get("slug", "")
                smt = market.get("sports_market_type", "") or "general"
                # Extract game identifier from slug: typically "league-team1-team2-date"
                # e.g. "fif-che-jor-2026-05-30-will-switzerland-win" → game="2026-05-30"
                date_match = _sports_re.search(r'(\d{4}-\d{2}-\d{2})', mslug)
                game_date = date_match.group(1) if date_match else ""
                # Dedup key: (match_key, market_type, game_date)
                # match_key (home/away) is already the outer loop key
                alert_key = (key, smt, game_date)
                if alert_key in seen_alert_keys:
                    continue
                seen_alert_keys.add(alert_key)

                # Single source without any useful signal is too weak — skip
                if len(sources) == 1:
                    has_score = any(p.get("score") for p in preds)
                    has_consensus = insights.get("consensus_winner") is not None
                    if not has_score and not has_consensus:
                        continue

                # Score this pick
                pick_score, reasons = _sports_score_pick(insights, market)

                # Source count bonus
                if len(sources) >= 2:
                    pick_score += 15
                    reasons.append("{} prediction sites".format(len(sources)))
                elif len(sources) == 1:
                    has_score = any(p.get("score") for p in preds)
                    if has_score:
                        pick_score += 5
                        reasons.append("1 site with score prediction")
                    else:
                        pick_score += 3
                        reasons.append("1 site prediction")

                # Log first 5 matches for debugging
                if matched_count <= 5:
                    print("[SPORTS] MATCH: {} vs {} ↔ '{}' — score={} reasons={}".format(
                        home, away, market.get("question", "")[:50],
                        pick_score, ", ".join(reasons[:3])))

                if pick_score >= SPORTS_MIN_SCORE:
                    pick_outcome = _sports_pick_outcome(insights, market)
                    if not pick_outcome:
                        # Bot has no basis to pick this market (corners, cards, player
                        # props). Record it as an available option — never fake a pick.
                        other_markets.append({"market": market, "smt": smt})
                        continue
                    game_candidates.append({
                        "score": pick_score, "market": market, "reasons": reasons,
                        "pick_outcome": pick_outcome, "smt": smt,
                    })

        # ── Surface the BEST market(s) for this game, each with an EXPLICIT pick ──
        # Dedup by the explicit pick first, so a game can't fire the same call
        # twice (e.g. moneyline→Draw and general→Draw).
        game_candidates.sort(key=lambda c: -c["score"])
        _seen_picks = set()
        _unique_cands = []
        for c in game_candidates:
            pk = (c.get("pick_outcome") or "").strip().lower()
            if pk in _seen_picks:
                continue
            _seen_picks.add(pk)
            _unique_cands.append(c)
        # Surface a pick for EVERY distinct market we have a confident read on
        # (moneyline + over/under + BTTS + correct score where the platform lists
        # them), not just the single highest-scoring one. Capped per match below.
        smt_labels = {
            "moneyline": "🏆 Match Winner", "total": "⚽ Over/Under Goals",
            "totals": "⚽ Over/Under Goals", "btts": "🎯 Both Teams To Score",
            "both_teams_to_score": "🎯 Both Teams To Score", "spread": "📊 Handicap/Spread",
            "total_corners": "🔲 Total Corners", "correct_score": "🎯 Correct Score",
            "first_goal": "1️⃣ First Goal Scorer", "anytime_goal": "⚽ Anytime Goal Scorer",
        }
        for cand in _unique_cands[:MAX_ALERTS_PER_MATCH]:
            if match_alerts >= MAX_ALERTS_PER_MATCH:
                break
            market = cand["market"]; pick_score = cand["score"]
            reasons = cand["reasons"]; pick_outcome = cand["pick_outcome"]; smt = cand["smt"]
            market_label = smt_labels.get(smt, "📊 {}".format(
                smt.replace("_", " ").title() if smt else "Market"))
            mq = market.get("question", "") or market.get("title", "")
            url = market.get("url", "")

            odds_str = ""
            op = market.get("outcome_prices", []); oc = market.get("outcomes", [])
            if op and oc and len(op) == len(oc):
                parts = []
                for outcome, price in zip(oc, op):
                    try:
                        parts.append("{}: {:.0f}%".format(outcome, float(price) * 100))
                    except (ValueError, TypeError):
                        pass
                odds_str = " | ".join(parts)

            _seen_sc = []
            for s in insights["scores"][:5]:
                sc = s.get("score")
                if sc and sc not in _seen_sc:
                    _seen_sc.append(sc)
            scores_str = ", ".join(_seen_sc[:3])
            msg = (
                "⚽ <b>SPORTS PICK</b>\n"
                "🏟 <b>{home} vs {away}</b>\n\n"
                "✅ <b>BEST PICK: {pick}</b>\n"
                "{market_label}\n"
                "{odds_line}"
                '🔗 <a href="{url}">Place this bet</a>\n\n'
                "📈 From {n_sites} prediction site{s}\n"
                "{scores_line}"
                "💡 {reasons}\n"
                "⭐ Confidence: {score}/100"
            ).format(
                home=home, away=away, pick=pick_outcome, market_label=market_label,
                odds_line="💰 Market odds: {}\n".format(odds_str) if odds_str else "",
                url=url, n_sites=len(sources), s="" if len(sources) == 1 else "s",
                scores_line="🎯 Score reads: {}\n".format(scores_str) if scores_str else "",
                reasons=" | ".join(reasons[:4]), score=pick_score,
            )
            # ── Once-per-pick dedup gate ──
            # The sports scanner now runs once a day at 08:00 Lagos. Without
            # dedup, the model's prediction for "Mexico vs Serbia → Mexico"
            # would recur every morning until Mexico-vs-Serbia is played,
            # so the user would receive the same Telegram alert N days in
            # a row. The dedup log makes each (home, away, pick) fire
            # exactly once across the entire lifetime of the deployment.
            if _sports_alert_seen(home, away, pick_outcome):
                print("[SPORTS] DEDUP: skip {} vs {} → {} (already alerted)".format(
                    home, away, pick_outcome))
                continue
            send_telegram(msg)
            _sports_alert_record(home, away, pick_outcome, market_label, url, pick_score)
            try:
                _plat = 'limitless' if 'limitless' in url else 'polymarket'
                _sports_market_cache.setdefault(_plat, [])
                _sports_market_cache[_plat].insert(0, {
                    'home': home, 'away': away, 'pick': pick_outcome,
                    'market': market_label, 'question': (mq or '')[:90],
                    'url': url, 'score': pick_score, 'odds': odds_str,
                })
                _seen = set(); _clean = []
                for _it in _sports_market_cache[_plat]:
                    _k = (_it.get('home'), _it.get('away'), _it.get('pick'))
                    if _k in _seen:
                        continue
                    _seen.add(_k); _clean.append(_it)
                _sports_market_cache[_plat] = _clean[:30]
            except Exception:
                pass
            alerts_sent += 1
            match_alerts += 1
            print("[SPORTS] ALERT: {} vs {} — {} → {} — {}/100".format(
                home, away, smt or "general", pick_outcome, pick_score))

        # ── Other markets the bot can't judge — list them so the user sees every
        # option (corners, cards, player props), clearly marked as no-pick. Only
        # when we already alerted a real pick for this match, to avoid noise.
        if other_markets and match_alerts > 0:
            seen_lbl = set()
            om_lines = []
            for om in other_markets:
                smt2 = om.get("smt") or "general"
                lbl = smt_labels.get(smt2, "📊 {}".format(
                    smt2.replace("_", " ").title() if smt2 else "Market"))
                mq2 = (om["market"].get("question", "") or
                       om["market"].get("title", "") or "")
                key2 = (lbl, mq2[:50])
                if key2 in seen_lbl:
                    continue
                seen_lbl.add(key2)
                om_url = om["market"].get("url", "")
                if om_url:
                    om_lines.append('• {} — <a href="{}">{}</a>'.format(
                        lbl, om_url, mq2[:60]))
                else:
                    om_lines.append("• {} — {}".format(lbl, mq2[:60]))
            if om_lines:
                send_telegram(
                    "📋 <b>Other markets for {} vs {}</b>\n"
                    "<i>No bot pick — your call</i>\n\n".format(home, away)
                    + "\n".join(om_lines[:6]))

    print("[SPORTS] Scan complete — {} predictions matched to markets, {} alerts sent".format(
        matched_count, alerts_sent))


def _sports_scanner_thread():
    """Background thread for the sports-picks scanner. Fires ONCE PER DAY
    at 08:00 Lagos time (07:00 UTC). On boot any already-passed slot for
    today is marked complete without firing — we wait for the next slot.
    Picks are deduped against sports_alerts_log so the same prediction
    never alerts twice across days."""
    print("[SPORTS] Scanner thread started — schedule: 08:00 Lagos / 07:00 UTC daily")
    time.sleep(45)  # let the DB/migrations finish on cold boot
    try:
        _init_scheduler_no_catchup((7,), "sports_last_slot", "SPORTS")
    except Exception as e:
        print("[SPORTS] scheduler init error: {}".format(e))
    while True:
        try:
            _scheduler_run_due_slot((7,), "sports_last_slot",
                                    _sports_scan_and_alert, "SPORTS")
        except Exception as e:
            print("[SPORTS] Scheduler tick error: {}".format(e))
            import traceback; traceback.print_exc()
        time.sleep(60)  # tick every minute, fires when the 07:00 UTC slot is due


# Sports dashboard page
@app.route("/app/sports")
def sports_dashboard():
    def _cards(platform):
        items = []
        try:
            items = list(_sports_market_cache.get(platform, []))
        except Exception:
            items = []
        if not items:
            return ('<div class="glass empty"><div class="big">📊</div>'
                    '<div>No {} picks cached yet. The scanner runs every few hours.</div></div>'.format(
                        platform.title()))
        blocks = []
        for m in items[:20]:
            if not isinstance(m, dict):
                continue
            winner = m.get("winner", "")
            winner_line = ""
            if winner and winner.upper() != "DRAW":
                winner_line = '<div class="pick">🏆 Pick: <b>{}</b></div>'.format(winner)
            elif winner:
                winner_line = '<div class="pick">🏆 Pick: <b>Draw</b></div>'
            market = (m.get("market", "") or "").replace("🏆 ", "").replace("⚽ ", "")
            url = m.get("url", "")
            link = ('<a href="{}" target="_blank">Place bet →</a>'.format(url)) if url else ""
            blocks.append(
                '<div class="glass match-card"><div class="mhead">'
                '<div class="teams">{} vs {}</div>'
                '<div class="league">{}/100</div></div>'
                '{}'
                '<div class="meta">📍 {}</div>'
                '<div class="code-box" style="margin-top:10px">'
                '<div><div class="label">{}</div></div>{}</div></div>'.format(
                    m.get("home", ""), m.get("away", ""), m.get("score", ""),
                    winner_line, market or "Market", platform.title(), link))
        return "".join(blocks)

    poly_cards = _cards("polymarket")
    lmts_cards = _cards("limitless")

    html = """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sports Markets — Cmvng Bot</title><style>{css}</style></head><body>
{nav}<div class="wrap">
<div class="page-head"><h1>Sports Markets</h1>
<div class="sub">Live picks matched to Polymarket & Limitless</div></div>
<h2 style="font-size:1rem;font-weight:900;color:#15803d;margin:6px 4px 12px">📊 Polymarket</h2>
{poly}
<h2 style="font-size:1rem;font-weight:900;color:#15803d;margin:22px 4px 12px">📊 Limitless</h2>
{lmts}
<div class="disclaimer">Picks are cross-validated across prediction sites then matched to live
prediction-market questions. Tap "Place bet" to open the market.</div>
</div></body></html>""".format(css=FB_CSS, nav=_nav("sports"), poly=poly_cards, lmts=lmts_cards)
    return html




# ═══════════════════════════════════════════════════════════════════════════
# CMVNG BOT v3 — FOOTBALL ENGINE (auto-assembled from modules)
# ═══════════════════════════════════════════════════════════════════════════
"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — FOOTBALL ANALYSIS ENGINE
═══════════════════════════════════════════════════════════════════
Pure-logic core: scoring + accumulator building.
No network calls here — fully testable in isolation.

Flow:
  1. analyze_fixture(data) -> scores every market type for one match
  2. build_accumulator(picks, tier) -> packs best picks into an odds tier
═══════════════════════════════════════════════════════════════════
"""

import math


# ═══════════════════════════════════════════════════════════════════
# ODDS ESTIMATION
# Convert a win probability into fair decimal odds, then shade it to
# look like a real bookmaker price (bookmakers add ~5-8% margin).
# ═══════════════════════════════════════════════════════════════════

def prob_to_odds(prob_pct, margin=0.06):
    """Convert probability % to realistic decimal odds with bookmaker margin."""
    p = max(0.01, min(0.99, prob_pct / 100.0))
    fair = 1.0 / p
    # Bookmaker shortens odds (adds margin) -> divide by (1+margin)
    shaded = fair / (1.0 + margin)
    return round(max(1.01, shaded), 2)


# ═══════════════════════════════════════════════════════════════════
# MARKET SCORING
# Each function returns a confidence percentage (0-100) for one market,
# using the 6 criteria the user specified.
# ═══════════════════════════════════════════════════════════════════

def _form_points(form_str):
    """Convert form string 'WWDLW' to avg points per game (0-3)."""
    if not form_str:
        return 1.5
    pts = {"W": 3, "D": 1, "L": 0}
    vals = [pts.get(c.upper(), 1) for c in form_str if c.upper() in pts]
    return sum(vals) / len(vals) if vals else 1.5


def _safe(d, key, default=0.0):
    """Safely pull a numeric value from a dict."""
    v = d.get(key, default)
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def analyze_fixture(fx):
    """
    Score every market type for a single fixture.
    `fx` is a dict with all scraped data (form, xg, stats, h2h, injuries).
    Returns a list of scored picks (each a dict).

    All fields are optional — missing data degrades the relevant market's
    confidence rather than crashing.
    """
    home = fx.get("home_team", "Home")
    away = fx.get("away_team", "Away")
    league = fx.get("league", "")
    kickoff = fx.get("kickoff_time", "")
    match_label = "{} vs {}".format(home, away)

    # ── Pull the inputs (all safe) ──
    home_form_pts = _form_points(fx.get("home_form", ""))
    away_form_pts = _form_points(fx.get("away_form", ""))
    home_xg_for = _safe(fx, "home_xg_for", 1.3)
    home_xg_against = _safe(fx, "home_xg_against", 1.3)
    away_xg_for = _safe(fx, "away_xg_for", 1.3)
    away_xg_against = _safe(fx, "away_xg_against", 1.3)
    home_gf = _safe(fx, "home_goals_scored_avg", home_xg_for)
    home_ga = _safe(fx, "home_goals_conceded_avg", home_xg_against)
    away_gf = _safe(fx, "away_goals_scored_avg", away_xg_for)
    away_ga = _safe(fx, "away_goals_conceded_avg", away_xg_against)
    home_corners_for = _safe(fx, "home_corners_for_avg", 5.0)
    home_corners_against = _safe(fx, "home_corners_against_avg", 5.0)
    away_corners_for = _safe(fx, "away_corners_for_avg", 5.0)
    away_corners_against = _safe(fx, "away_corners_against_avg", 5.0)
    home_cards = _safe(fx, "home_cards_avg", 2.0)
    away_cards = _safe(fx, "away_cards_avg", 2.0)
    # Only emit corner/card picks when the fixture actually carries that data.
    # No current feed populates these averages, so without this guard every game
    # defaults to a fixed "10 corners / 4 cards" guess and surfaces false
    # 60-88% picks (same number for every match). Club corners/cards still come
    # from the model path in board-explore, which is mdl-gated and edge-checked.
    _has_corner_data = any(k in fx for k in (
        "home_corners_for_avg", "home_corners_against_avg",
        "away_corners_for_avg", "away_corners_against_avg"))
    _has_card_data = ("home_cards_avg" in fx) or ("away_cards_avg" in fx)
    home_btts = _safe(fx, "home_btts_pct", 50.0)
    away_btts = _safe(fx, "away_btts_pct", 50.0)
    home_cs = _safe(fx, "home_clean_sheet_pct", 30.0)
    away_cs = _safe(fx, "away_clean_sheet_pct", 30.0)
    home_pos = int(_safe(fx, "home_position", 10))
    away_pos = int(_safe(fx, "away_position", 10))

    # Injuries: count of key players out (passed as int)
    home_inj = int(_safe(fx, "home_key_injuries", 0))
    away_inj = int(_safe(fx, "away_key_injuries", 0))

    # ── CRITERION 1+2: Relative strength (form + home advantage + table) ──
    HOME_ADV = 0.35  # home advantage boost in "strength points"
    home_strength = home_form_pts + HOME_ADV
    away_strength = away_form_pts

    # Table context adjustment: higher team (lower position number) gets boost
    pos_gap = away_pos - home_pos  # positive = home ranked higher
    home_strength += pos_gap * 0.05
    away_strength -= pos_gap * 0.05

    # Injury penalty
    home_strength -= home_inj * 0.20
    away_strength -= away_inj * 0.20

    total_strength = home_strength + away_strength
    if total_strength <= 0:
        total_strength = 1.0
    home_win_raw = home_strength / total_strength
    away_win_raw = away_strength / total_strength

    # ── Expected goals for THIS match (blend attack vs defense) ──
    exp_home_goals = (home_gf + away_ga) / 2.0
    exp_away_goals = (away_gf + home_ga) / 2.0
    exp_total_goals = exp_home_goals + exp_away_goals

    # ── How the match PROJECTS: tight or clear? ──
    # A predicted scoreline like 2-1 is really a "tight game" signal — it does
    # NOT mean Over 2.5 is safe (the match could land 1-0, 1-1, 0-0). Only a
    # clear margin (2-0, 3-1) justifies backing a winner + goals. So we read the
    # margin and total, and temper the risky reads on close games — worst-case
    # thinking rather than taking the headline scoreline at face value.
    goal_margin = abs(exp_home_goals - exp_away_goals)
    tight_game = goal_margin < 0.65 and exp_total_goals < 3.0
    clear_game = goal_margin >= 1.0
    low_scoring = exp_total_goals < 2.6

    # ── USER METHODOLOGY: last-10 granular form rules ──
    hfs = fx.get("home_form_stats") or {}
    afs = fx.get("away_form_stats") or {}
    # Over 1.5: safe when BOTH teams hit it in >=75% of recent games, OR the
    # stronger side is a high-scoring team.
    both_over15 = (hfs.get("over15_pct", 0) >= 0.75 and afs.get("over15_pct", 0) >= 0.75)
    stronger_high_scoring = max(hfs.get("avg_gf", 0), afs.get("avg_gf", 0)) >= 1.8
    over15_safe = both_over15 or stronger_high_scoring
    # Under 3.5 / unders: safe when both sides keep recent games under it.
    both_under35 = (hfs.get("under35_pct", 0) >= 0.70 and afs.get("under35_pct", 0) >= 0.70)
    # TACTICAL STYLE (Understat PPDA): low PPDA = aggressive high press, high =
    # passive low block. Two high-press sides => transitions/space => more goals;
    # two low blocks => controlled, fewer goals.
    h_ppda = _safe(fx, "home_ppda", 0) or 0
    a_ppda = _safe(fx, "away_ppda", 0) or 0
    both_high_press = 0 < h_ppda <= 9.0 and 0 < a_ppda <= 9.0
    both_low_block = h_ppda >= 13.0 and a_ppda >= 13.0
    if both_high_press:
        over15_safe = True
    if both_low_block:
        both_under35 = True
    # Universal fallback (works for ALL leagues, incl. those Understat doesn't
    # cover): read game tempo from each side's last-10 total goals/game. Two
    # high-tempo sides => open game => overs; two low-tempo => closed => unders.
    if not (h_ppda and a_ppda):
        h_tempo = hfs.get("avg_gf", 0) + hfs.get("avg_ga", 0)
        a_tempo = afs.get("avg_gf", 0) + afs.get("avg_ga", 0)
        if h_tempo and a_tempo:
            if h_tempo >= 3.0 and a_tempo >= 3.0:
                over15_safe = True
            elif h_tempo <= 2.2 and a_tempo <= 2.2:
                both_under35 = True
    # Universal fallback (ALL leagues): when PPDA is unavailable, read open-vs-
    # closed from recent goal volume — Sofascore form covers every league.
    if not (h_ppda or a_ppda) and hfs.get("played") and afs.get("played"):
        h_tot = hfs.get("avg_gf", 0) + hfs.get("avg_ga", 0)
        a_tot = afs.get("avg_gf", 0) + afs.get("avg_ga", 0)
        if h_tot >= 3.0 and a_tot >= 3.0:      # both play open, high-volume games
            over15_safe = True
        elif h_tot <= 2.2 and a_tot <= 2.2:    # both play tight, low-volume games
            both_under35 = True
    # Straight win only for the clearly stronger side; away win needs to be
    # OVERWHELMING. Otherwise the safe pick is the double chance.
    home_clearly_stronger = (home_win_raw >= 0.56 and hfs.get("ppg", 0) >= 1.6
                             and home_inj <= away_inj + 1)
    away_overwhelming = (away_win_raw >= 0.62 and afs.get("ppg", 0) >= 1.8)
    # FBref (Opta) possession: a big season possession edge is a control/strength
    # signal — it reinforces an already-favoured side's win, but never creates a
    # favourite on its own (dominating the ball isn't the same as winning).
    h_poss = _safe(fx, "home_possession", 0) or 0
    a_poss = _safe(fx, "away_possession", 0) or 0
    if h_poss and a_poss:
        if (h_poss - a_poss) >= 12 and home_win_raw >= 0.50:
            home_clearly_stronger = True
        elif (a_poss - h_poss) >= 12 and away_win_raw >= 0.58:
            away_overwhelming = True
    form_note = ""
    if hfs.get("played") and afs.get("played"):
        form_note = " | last{}: {} o1.5 {:.0f}%/{:.0f}%".format(
            hfs.get("played"), afs.get("played", 0),
            hfs.get("over15_pct", 0) * 100, afs.get("over15_pct", 0) * 100)

    picks = []

    def add(market_type, pick_label, confidence, reasoning):
        confidence = max(1.0, min(99.0, confidence))
        picks.append({
            "match": match_label,
            "home": home,
            "away": away,
            "league": league,
            "kickoff": kickoff,
            "market_type": market_type,
            "pick": pick_label,
            "confidence": round(confidence, 1),
            "odds": prob_to_odds(confidence),
            "reasoning": reasoning,
            "kickoff_ts": fx.get("kickoff_ts", 0),
            # SportyBet IDs (event id pre-resolved during enrichment)
            "sb_event_id": fx.get("sb_event_id", ""),
            "sb_market_id": "",
            "sb_specifier": None,
            "sb_outcome_id": "",
            "result": "pending",
        })

    # ── MATCH RESULT ──
    # If we have prediction-site probabilities (Forebet etc.), use them directly —
    # they're a stronger signal than our form heuristic. Recompute raw shares too,
    # so double-chance and combos downstream stay consistent.
    pred_ph = fx.get("pred_prob_home")
    pred_pd = fx.get("pred_prob_draw")
    pred_pa = fx.get("pred_prob_away")
    if pred_ph is not None:
        ph = max(1.0, float(pred_ph))
        pd = max(1.0, float(pred_pd)) if pred_pd is not None else 26.0
        pa = max(1.0, float(pred_pa)) if pred_pa is not None else max(1.0, 100 - ph - pd)
        tot_p = ph + pd + pa
        home_win_raw = ph / tot_p
        away_win_raw = pa / tot_p
        home_win_conf = ph
        away_win_conf = pa
        draw_conf = pd
        _win_reason = "prediction sites: {:.0f}% / {:.0f}% / {:.0f}%".format(ph, pd, pa)
    else:
        home_win_conf = home_win_raw * 100 * 0.85  # temper raw probability
        away_win_conf = away_win_raw * 100 * 0.80
        draw_conf = (1 - abs(home_win_raw - away_win_raw)) * 35  # draws ~25-32% typically
        _win_reason = "form {:.1f} vs {:.1f}, table {} vs {}".format(
            home_form_pts, away_form_pts, home_pos, away_pos)

    # On a genuinely tight projection, a clean win is less safe than the raw
    # probability suggests — shade outright wins down and double chance up.
    if tight_game:
        home_win_conf *= 0.85
        away_win_conf *= 0.85

    # USER RULE: a straight win is only a real pick for the clearly stronger
    # side; an away win must be OVERWHELMING. Otherwise it stays low so the
    # double chance (home/away or draw) is what surfaces.
    if not home_clearly_stronger:
        home_win_conf *= 0.80
    if not away_overwhelming:
        away_win_conf *= 0.72
    _win_reason += form_note

    add("home_win", "{} to Win".format(home), home_win_conf,
        "{}: {}".format(home, _win_reason))
    add("away_win", "{} to Win".format(away), away_win_conf,
        "{}: {}".format(away, _win_reason))
    add("draw", "Draw", draw_conf,
        "Draw probability: {:.0f}%".format(draw_conf))

    # ── DOUBLE CHANCE (much safer than straight win) ──
    dc_1x = (home_win_raw + (draw_conf/100)) * 100 * 0.92
    dc_x2 = (away_win_raw + (draw_conf/100)) * 100 * 0.92
    if tight_game:
        # safest market in a close game — nudge up
        dc_1x = min(96, dc_1x * 1.05)
        dc_x2 = min(96, dc_x2 * 1.05)
    add("double_chance_1X", "{} or Draw".format(home), dc_1x,
        "{} home + draw cover, form {:.1f}pts".format(home, home_form_pts))
    add("double_chance_X2", "{} or Draw".format(away), dc_x2,
        "{} + draw cover".format(away))

    # ── DRAW NO BET (win, stake refunded on a draw — safer than a straight win) ──
    dnb_base = home_win_raw + away_win_raw
    if dnb_base > 0:
        dnb_home = (home_win_raw / dnb_base) * 100 * 0.95
        dnb_away = (away_win_raw / dnb_base) * 100 * 0.95
        add("dnb_home", "{} Draw No Bet".format(home), dnb_home,
            "{} to win, stake back on draw".format(home))
        add("dnb_away", "{} Draw No Bet".format(away), dnb_away,
            "{} to win, stake back on draw".format(away))

    # ── OVER/UNDER GOALS ──
    # Poisson-ish heuristic from expected total goals
    over_05 = min(98, 70 + exp_total_goals * 10)
    over_15 = min(96, 45 + exp_total_goals * 13)
    over_25 = min(90, 20 + exp_total_goals * 16)
    over_35 = min(80, exp_total_goals * 15)
    over_45 = min(68, exp_total_goals * 11)
    over_55 = min(52, exp_total_goals * 8)
    # A tight/low-scoring projection should NOT look like a confident Over 2.5 —
    # the headline scoreline (e.g. 2-1) means "close", not "3 goals are coming".
    if tight_game or low_scoring:
        over_25 *= 0.70
        over_35 *= 0.60
        over_45 *= 0.55
        over_55 *= 0.50
        over_15 = min(over_15, 82)  # 1.5 still usually fine, but don't overstate
    # USER RULE: recent-form override for the goals ladder
    if over15_safe:
        over_15 = max(over_15, 88)   # both sides reliably clear 1.5 (or one is high-scoring)
    if both_under35:
        under_35 = max(100 - over_35, 85)
        under_45 = max(100 - over_45, 92)
    under_15 = 100 - over_15
    under_25 = 100 - over_25
    under_35 = 100 - over_35 if not both_under35 else max(100 - over_35, 85)
    under_45 = 100 - over_45 if not both_under35 else max(100 - over_45, 92)

    add("over_0.5", "Over 0.5 Goals", over_05,
        "Expected {:.1f} total goals".format(exp_total_goals))
    add("over_1.5", "Over 1.5 Goals", over_15,
        "Expected {:.1f} goals ({} {:.1f}xG, {} {:.1f}xG)".format(
            exp_total_goals, home, home_xg_for, away, away_xg_for))
    add("over_2.5", "Over 2.5 Goals", over_25,
        "Expected {:.1f} goals, both attacks active".format(exp_total_goals))
    add("over_3.5", "Over 3.5 Goals", over_35,
        "High-scoring projection {:.1f}".format(exp_total_goals))
    add("over_4.5", "Over 4.5 Goals", over_45,
        "Very high-scoring projection {:.1f}".format(exp_total_goals))
    add("over_5.5", "Over 5.5 Goals", over_55,
        "Goal-fest projection {:.1f}".format(exp_total_goals))
    add("under_1.5", "Under 1.5 Goals", under_15,
        "Low-scoring projection {:.1f}".format(exp_total_goals))
    add("under_2.5", "Under 2.5 Goals", under_25,
        "Lower-scoring projection {:.1f}".format(exp_total_goals))
    add("under_3.5", "Under 3.5 Goals", under_35,
        "Defensive projection {:.1f}".format(exp_total_goals))
    add("under_4.5", "Under 4.5 Goals", under_45,
        "Unlikely to see 5+ goals (proj {:.1f})".format(exp_total_goals))

    # ── BTTS ──
    btts_yes = (home_btts + away_btts) / 2.0
    # Adjust by clean sheet tendency
    btts_yes -= (home_cs + away_cs) / 4.0
    btts_yes = max(15, min(88, btts_yes + (exp_total_goals - 2.5) * 8))
    btts_no = 100 - btts_yes
    add("btts_yes", "Both Teams to Score - Yes", btts_yes,
        "{} BTTS {:.0f}%, {} BTTS {:.0f}%, exp {:.1f} goals".format(
            home, home_btts, away, away_btts, exp_total_goals))
    add("btts_no", "Both Teams to Score - No", btts_no,
        "Clean sheet tendency: {} {:.0f}%, {} {:.0f}%".format(home, home_cs, away, away_cs))

    # ── CORNERS ──
    if _has_corner_data:
        exp_corners = (home_corners_for + away_corners_against) / 2.0 + \
                      (away_corners_for + home_corners_against) / 2.0
        over_75c = min(92, exp_corners * 8)
        over_85c = min(85, exp_corners * 7)
        over_95c = min(75, exp_corners * 6)
        add("corners_over_7.5", "Over 7.5 Corners", over_75c,
            "Expected {:.1f} corners ({} {:.1f}, {} {:.1f})".format(
                exp_corners, home, home_corners_for, away, away_corners_for))
        add("corners_over_8.5", "Over 8.5 Corners", over_85c,
            "Expected {:.1f} corners".format(exp_corners))
        add("corners_over_9.5", "Over 9.5 Corners", over_95c,
            "Expected {:.1f} corners, both teams attack wide".format(exp_corners))

    # ── CARDS ──
    if _has_card_data:
        exp_cards = home_cards + away_cards
        over_25cards = min(88, exp_cards * 22)
        over_35cards = min(75, exp_cards * 17)
        add("cards_over_2.5", "Over 2.5 Cards", over_25cards,
            "Expected {:.1f} cards combined".format(exp_cards))
        add("cards_over_3.5", "Over 3.5 Cards", over_35cards,
            "Expected {:.1f} cards, physical matchup".format(exp_cards))

    # ── COMBOS ──
    home_win_btts = (home_win_conf/100) * (btts_yes/100) * 100 * 1.05
    add("home_win_btts", "{} Win & BTTS".format(home), home_win_btts,
        "{} favored + both score".format(home))
    home_win_over25 = (home_win_conf/100) * (over_25/100) * 100 * 1.05
    add("home_win_over_2.5", "{} Win & Over 2.5".format(home), home_win_over25,
        "{} win in high-scoring game".format(home))
    wd_over15 = (dc_1x/100) * (over_15/100) * 100
    add("dc_over_1.5", "{} or Draw & Over 1.5".format(home), wd_over15,
        "Safe double chance + goals")

    # ── HANDICAP ──
    if home_win_raw > 0.55:
        hcp = home_win_conf * 0.65
        add("handicap_home_-1.5", "{} -1.5".format(home), hcp,
            "{} strongly favored to win by 2+".format(home))
    if away_win_raw > 0.55:
        hcp = away_win_conf * 0.65
        add("handicap_away_-1.5", "{} -1.5".format(away), hcp,
            "{} strongly favored to win by 2+".format(away))

    # ── CORRECT SCORE (top likely scorelines) ──
    h = max(0, round(exp_home_goals))
    a = max(0, round(exp_away_goals))
    cs_conf = 12 + (10 if home_win_raw > 0.5 else 5)  # correct scores are low prob
    add("correct_score", "{} {}-{} {}".format(home, h, a, away), cs_conf,
        "Most likely scoreline from xG ({:.1f}-{:.1f})".format(exp_home_goals, exp_away_goals))

    # ── MARKET-DRIVEN EXPLORATION ──
    # Walk SportyBet's ACTUAL market board for this game and score the extra
    # families our fixed list doesn't cover (team totals, multigoal ranges,
    # 1st-half goals) straight from the team analysis. These come pre-mapped
    # with REAL SportyBet odds + a comment, and feed the exploratory tiers.
    try:
        picks.extend(_sb_board_explore(
            fx, home, away, exp_home_goals, exp_away_goals, exp_total_goals))
    except Exception as e:
        print("[FB] board explore error: {}".format(e))

    return picks


def _sb_board_explore(fx, home, away, exp_home, exp_away, exp_total):
    """
    Market-driven scorer. Reads the game's real SportyBet market board (cached
    during enrichment) and estimates the probability of outcomes in the
    families our fixed list doesn't model — team totals, multigoal ranges, and
    1st-half goals — using a Poisson model on the expected goals. Each returned
    pick carries the REAL SportyBet odds, the exact marketId/specifier/outcomeId
    (so it bypasses name-matching entirely — no decoy risk) and a comment.
    """
    import math as _math
    import re as _re
    eid = fx.get("sb_event_id")
    if not eid:
        return []
    markets = _SB_MARKET_CACHE.get(eid) or []
    if not markets:
        return []
    mdl = fx.get("_model") or {}   # proven club model (corners/cards), if covered

    def pmf(k, lam):
        try:
            return _math.exp(-lam) * (lam ** k) / _math.factorial(k)
        except Exception:
            return 0.0

    def p_range(lo, hi, lam):
        return sum(pmf(k, lam) for k in range(lo, hi + 1)) * 100.0

    def p_atleast(n, lam):
        return (1.0 - sum(pmf(k, lam) for k in range(0, n))) * 100.0

    # ── Joint full-time scoreline grid (independent Poisson per side) ──
    # Lets us derive win/margin/clean-sheet/win-to-nil probabilities that the
    # single-lambda totals model can't express.
    MAXG = 9
    grid = [[pmf(i, exp_home) * pmf(j, exp_away)
             for j in range(MAXG + 1)] for i in range(MAXG + 1)]

    def _sum_grid(cond):
        return sum(grid[i][j] for i in range(MAXG + 1)
                   for j in range(MAXG + 1) if cond(i, j)) * 100.0

    p_margin_home = lambda n: _sum_grid(lambda i, j: i - j >= n)   # home wins by n+
    p_margin_away = lambda n: _sum_grid(lambda i, j: j - i >= n)
    p_exact_home = lambda n: _sum_grid(lambda i, j: i - j == n)    # win by exactly n
    p_exact_away = lambda n: _sum_grid(lambda i, j: j - i == n)
    p_home_to_nil = _sum_grid(lambda i, j: i > j and j == 0)       # win, opp 0
    p_away_to_nil = _sum_grid(lambda i, j: j > i and i == 0)
    p_home_cs = _sum_grid(lambda i, j: j == 0)                     # clean sheet
    p_away_cs = _sum_grid(lambda i, j: i == 0)
    # 1UP (early payout the moment your team leads by 1). "Ever leads" is
    # path-dependent and not exactly derivable from the final-score grid, so we
    # use a sound proxy: always a win, plus most draws (likely led at some point)
    # and a slice of narrow losses (led then conceded). Strictly safer than a
    # straight win — which is exactly why 1UP is the preferred win market.
    p_home_win_ft = _sum_grid(lambda i, j: i > j)
    p_away_win_ft = _sum_grid(lambda i, j: j > i)
    p_draw_goals = _sum_grid(lambda i, j: i == j and i >= 1)
    p_home_loss1 = _sum_grid(lambda i, j: j - i == 1)
    p_away_loss1 = _sum_grid(lambda i, j: i - j == 1)
    p_home_1up = min(96.0, p_home_win_ft + 0.55 * p_draw_goals + 0.22 * p_home_loss1)
    p_away_1up = min(96.0, p_away_win_ft + 0.55 * p_draw_goals + 0.22 * p_away_loss1)
    # Double-Chance 1UP (marketId 60110): 1X/X2 with early payout. A DC-1UP side
    # wins whenever the plain DC wins PLUS when its team led at some point (the
    # early payout), so it's strictly >= the plain double chance.
    p_1x = _sum_grid(lambda i, j: i >= j)
    p_x2 = _sum_grid(lambda i, j: j >= i)
    p_00 = grid[0][0] * 100.0
    p_dc1up_1x = min(98.0, p_1x + 0.30 * p_home_loss1)   # +edge: home led then lost
    p_dc1up_x2 = min(98.0, p_x2 + 0.30 * p_away_loss1)
    p_dc1up_12 = min(99.0, 100.0 - p_00)                 # loses only on 0-0

    # ── Per-half model: goals split ~45% 1st half / 55% 2nd, halves independent ──
    def _win_half(lh, la):
        return sum(pmf(i, lh) * pmf(j, la)
                   for i in range(MAXG + 1) for j in range(MAXG + 1) if i > j) * 100.0
    h1h, h1a = exp_home * 0.45, exp_away * 0.45
    h2h, h2a = exp_home * 0.55, exp_away * 0.55
    p_home_win_h1, p_home_win_h2 = _win_half(h1h, h1a), _win_half(h2h, h2a)
    p_away_win_h1, p_away_win_h2 = _win_half(h1a, h1h), _win_half(h2a, h2h)
    p_home_either = (1 - (1 - p_home_win_h1 / 100) * (1 - p_home_win_h2 / 100)) * 100
    p_away_either = (1 - (1 - p_away_win_h1 / 100) * (1 - p_away_win_h2 / 100)) * 100
    p_home_both = (p_home_win_h1 / 100) * (p_home_win_h2 / 100) * 100
    p_away_both = (p_away_win_h1 / 100) * (p_away_win_h2 / 100) * 100

    def first_num(*texts):
        for t in texts:
            m = _re.search(r'(\d+(?:\.\d+)?)', str(t))
            if m:
                return float(m.group(1))
        return None

    hl = home.lower()
    al = away.lower()
    h_tok = hl.split()[0] if hl.split() else hl
    a_tok = al.split()[0] if al.split() else al
    match_label = "{} vs {}".format(home, away)
    out = []
    seen = set()

    CORE = ("1x2", "double chance", "gg/ng", "both teams to score",
            "over/under", "draw no bet", "1x2 1up", "1x2 2up")

    for mk in markets:
        desc = (mk.get("desc") or mk.get("name")
                or mk.get("marketName") or "").lower().strip()
        if not desc or desc in CORE:
            continue
        spec = str(mk.get("specifier") or "")
        mid = str(mk.get("id", ""))
        ocs = mk.get("outcomes") or mk.get("outcome") or []

        # classify the family
        is_range = any(_re.match(r'^\s*\d+\s*-\s*\d+\s*$',
                       (o.get("desc") or o.get("name") or "")) for o in ocs)
        is_multigoal = is_range and ("goal" in desc or "multigoal" in desc
                                     or "total" in desc)
        is_fh = ("1st half" in desc or "first half" in desc) and \
                ("over" in desc or "under" in desc or "total" in desc)
        team_for = None
        if not is_multigoal and ("over" in desc or "under" in desc or "total" in desc):
            if h_tok and h_tok in desc:
                team_for = "home"
            elif a_tok and a_tok in desc:
                team_for = "away"

        # result-derived families (use the joint grid, not the totals model)
        is_winmargin = ("winning margin" in desc or "win by" in desc
                        or "goals ahead" in desc or "to lead by" in desc)
        is_winhalf = ("win" in desc and "half" in desc) and not is_fh
        is_tonil = "to nil" in desc or "win to nil" in desc
        is_cleansheet = "clean sheet" in desc
        is_1up = "1up" in desc or "1 up" in desc

        def _side(*texts):
            for t in texts:
                t = (t or "").lower()
                if "home" in t or (h_tok and h_tok in t):
                    return "home"
                if "away" in t or (a_tok and a_tok in t):
                    return "away"
            return None

        for o in ocs:
            od = (o.get("desc") or o.get("name") or "").strip()
            odl = od.lower()
            try:
                odds = float(o.get("odds"))
            except (TypeError, ValueError):
                continue
            if odds <= 1.0:
                continue

            prob = label = comment = mtype = None

            # ── Model-driven corners/cards (market 166 etc.) ──
            # STRATEGY: only the best picks — worst-case-safe (model >=75% on the
            # side) AND positive edge (model beats SportyBet's implied by >=5pp).
            # Priced from the model's expected corners/cards via Poisson, so it
            # works for whatever line SportyBet offers. Only fires for covered
            # club fixtures (mdl present); internationals have no mdl -> skipped.
            if mdl and ("corner" in desc or "card" in desc or "booking" in desc):
                _is_card = ("card" in desc or "booking" in desc)
                _exp = mdl.get("cards_exp" if _is_card else "corners_exp")
                _noun = "Cards" if _is_card else "Corners"
                _line = first_num(spec, od)
                if _exp and _line is not None:
                    _over_p = p_atleast(int(_math.floor(_line)) + 1, _exp)
                    if "over" in odl or odl.startswith("o"):
                        _cand, _sd = _over_p, "Over"
                    elif "under" in odl or odl.startswith("u"):
                        _cand, _sd = 100.0 - _over_p, "Under"
                    else:
                        _cand, _sd = None, None
                    if _cand is not None:
                        _implied = 100.0 / odds
                        _edge = _cand - _implied
                        if _cand >= 75.0 and _edge >= 5.0:
                            prob = _cand
                            label = "{} {:g} {}".format(_sd, _line, _noun)
                            mtype = "{}_{}_{:g}".format(_noun.lower(), _sd.lower(), _line)
                            comment = ("Model {} ~{:.1f}; {} {:.0f}% vs market {:.0f}% "
                                       "(edge +{:.0f}%)").format(_noun.lower(), _exp,
                                                                 label, prob, _implied, _edge)
                            print("[MODEL-PICK] {} | {} — model {:.0f}% vs mkt {:.0f}% "
                                  "(edge +{:.0f}%) @ {:.2f}".format(match_label, label,
                                                                    prob, _implied, _edge, odds))
            elif is_multigoal:
                m = _re.match(r'^\s*(\d+)\s*-\s*(\d+)\s*$', od)
                if m:
                    lo, hi = int(m.group(1)), int(m.group(2))
                    prob = p_range(lo, hi, exp_total)
                    label = "{}-{} Total Goals".format(lo, hi)
                    mtype = "multigoal_{}_{}".format(lo, hi)
                    comment = "Proj {:.1f} goals; band {}-{} ~{:.0f}%".format(
                        exp_total, lo, hi, prob)
            elif team_for:
                line = first_num(spec, od)
                if line is not None:
                    lam = exp_home if team_for == "home" else exp_away
                    tname = home if team_for == "home" else away
                    need = int(line) + 1
                    if "over" in odl or odl.startswith("o"):
                        prob = p_atleast(need, lam)
                        label = "{} Over {:g} Goals".format(tname, line)
                        mtype = "team_{}_over_{:g}".format(team_for, line)
                        comment = "{} proj {:.1f} goals; Over {:g} ~{:.0f}%".format(
                            tname, lam, line, prob)
                    elif "under" in odl or odl.startswith("u"):
                        prob = 100.0 - p_atleast(need, lam)
                        label = "{} Under {:g} Goals".format(tname, line)
                        mtype = "team_{}_under_{:g}".format(team_for, line)
                        comment = "{} proj {:.1f} goals; Under {:g} ~{:.0f}%".format(
                            tname, lam, line, prob)
            elif is_fh:
                line = first_num(spec, od)
                if line is not None:
                    lam1h = exp_total * 0.42  # ~42% of goals fall in the 1st half
                    need = int(line) + 1
                    if "over" in odl or odl.startswith("o"):
                        prob = p_atleast(need, lam1h)
                        label = "1st Half Over {:g} Goals".format(line)
                        mtype = "fh_over_{:g}".format(line)
                        comment = "1H proj {:.1f} goals; Over {:g} ~{:.0f}%".format(
                            lam1h, line, prob)
                    elif "under" in odl or odl.startswith("u"):
                        prob = 100.0 - p_atleast(need, lam1h)
                        label = "1st Half Under {:g} Goals".format(line)
                        mtype = "fh_under_{:g}".format(line)
                        comment = "1H proj {:.1f} goals; Under {:g} ~{:.0f}%".format(
                            lam1h, line, prob)
            elif is_winmargin:
                side = _side(od) or _side(desc)
                num = first_num(od)
                yn = "yes" if odl in ("yes", "gg") else ("no" if odl == "no" else None)
                if side and num is not None:
                    n = int(num)
                    plus = ("+" in od or "more" in odl or "least" in odl)
                    tname = home if side == "home" else away
                    if plus:
                        prob = (p_margin_home if side == "home" else p_margin_away)(n)
                        label = "{} to win by {}+".format(tname, n)
                    else:
                        prob = (p_exact_home if side == "home" else p_exact_away)(n)
                        label = "{} to win by exactly {}".format(tname, n)
                    prob *= 0.95  # model-uncertainty haircut on margin estimates
                    mtype = "winmargin_{}_{}{}".format(side, n, "p" if plus else "e")
                    comment = "{} proj {:.1f}-{:.1f}; {} ~{:.0f}%".format(
                        tname, exp_home, exp_away, label, prob)
                elif yn is not None and ("goals ahead" in desc or "lead by" in desc):
                    side2 = _side(desc)
                    num2 = first_num(desc)
                    if side2 and num2 is not None:
                        n = int(num2)
                        base = (p_margin_home if side2 == "home" else p_margin_away)(n) * 0.95
                        prob = base if yn == "yes" else 100.0 - base
                        tname = home if side2 == "home" else away
                        label = "{} {} goals ahead: {}".format(tname, n, yn.upper())
                        mtype = "leadby_{}_{}_{}".format(side2, n, yn)
                        comment = "{} proj {:.1f}-{:.1f}; lead by {} ~{:.0f}%".format(
                            tname, exp_home, exp_away, n, prob)
            elif is_winhalf:
                side = _side(od) or _side(desc)
                yn = "yes" if odl in ("yes", "gg") else ("no" if odl == "no" else None)
                if side:
                    if "either" in desc:
                        base, kind = (p_home_either if side == "home" else p_away_either), "either half"
                    elif "both" in desc:
                        base, kind = (p_home_both if side == "home" else p_away_both), "both halves"
                    elif "1st" in desc or "first" in desc:
                        base, kind = (p_home_win_h1 if side == "home" else p_away_win_h1), "1st half"
                    elif "2nd" in desc or "second" in desc:
                        base, kind = (p_home_win_h2 if side == "home" else p_away_win_h2), "2nd half"
                    else:
                        base, kind = None, None
                    if base is not None:
                        base *= 0.95
                        prob = base if (yn != "no") else 100.0 - base
                        tname = home if side == "home" else away
                        label = "{} to win {}{}".format(
                            tname, kind, "" if yn != "no" else " — No")
                        mtype = "winhalf_{}_{}_{}".format(
                            side, kind.replace(" ", ""), yn or "y")
                        comment = "{} win {} ~{:.0f}% (proj {:.1f}-{:.1f})".format(
                            tname, kind, prob, exp_home, exp_away)
            elif is_tonil:
                side = _side(od) or _side(desc)
                yn = "yes" if odl in ("yes", "gg") else ("no" if odl == "no" else None)
                if side:
                    base = (p_home_to_nil if side == "home" else p_away_to_nil)
                    prob = base if (yn != "no") else 100.0 - base
                    tname = home if side == "home" else away
                    label = "{} to win to nil{}".format(
                        tname, "" if yn != "no" else " — No")
                    mtype = "tonil_{}_{}".format(side, yn or "y")
                    comment = "{} win-to-nil ~{:.0f}% (proj {:.1f}-{:.1f})".format(
                        tname, base, exp_home, exp_away)
            elif is_cleansheet:
                side = _side(od) or _side(desc)
                yn = "yes" if odl in ("yes", "gg") else ("no" if odl == "no" else None)
                if side:
                    base = (p_home_cs if side == "home" else p_away_cs)
                    prob = base if (yn != "no") else 100.0 - base
                    tname = home if side == "home" else away
                    label = "{} clean sheet{}".format(
                        tname, "" if yn != "no" else " — No")
                    mtype = "cleansheet_{}_{}".format(side, yn or "y")
                    comment = "{} clean sheet ~{:.0f}% (opp proj {:.1f})".format(
                        tname, base, exp_away if side == "home" else exp_home)
            elif is_1up:
                # 1UP — early payout the instant a side leads by one. SportyBet
                # ships this as BOTH single-team (1X2-1UP) and Double-Chance-1UP
                # (marketId 60110, outcomes 'Home or Draw' etc.) — they settle
                # differently, so they must NOT be conflated.
                if " or " in odl or "/" in odl:        # Double-Chance 1UP
                    has_home = "home" in odl or (h_tok and h_tok in odl)
                    has_away = "away" in odl or (a_tok and a_tok in odl)
                    has_draw = "draw" in odl
                    if has_home and has_draw:
                        prob, mtype = p_dc1up_1x, "dc1up_1x"
                        label = "{} or Draw (1UP)".format(home)
                        comment = "1X with early payout — wins if {} ever leads or it's 1X (~{:.0f}%)".format(home, prob)
                    elif has_away and has_draw:
                        prob, mtype = p_dc1up_x2, "dc1up_x2"
                        label = "{} or Draw (1UP)".format(away)
                        comment = "X2 with early payout — wins if {} ever leads or it's X2 (~{:.0f}%)".format(away, prob)
                    elif has_home and has_away:
                        prob, mtype = p_dc1up_12, "dc1up_12"
                        label = "{} or {} (1UP)".format(home, away)
                        comment = "12 with early payout — loses only on 0-0 (~{:.0f}%)".format(prob)
                else:                                   # single-team 1UP
                    side = _side(od) or _side(desc)
                    if side:
                        prob = p_home_1up if side == "home" else p_away_1up
                        tname = home if side == "home" else away
                        label = "{} 1UP".format(tname)
                        mtype = "oneup_{}".format(side)
                        comment = "{} 1UP (ever leads ~{:.0f}%, safer than a straight win)".format(
                            tname, prob)

            if prob is None or not label:
                continue
            if not (1.0 < prob < 99.5):
                continue
            key = (mtype, mid, str(o.get("id", "")))
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "match": match_label, "home": home, "away": away,
                "league": fx.get("league", ""), "kickoff": fx.get("kickoff", ""),
                "kickoff_ts": fx.get("kickoff_ts", 0),
                "market_type": mtype, "pick": label,
                "confidence": round(max(1.0, min(99.0, prob)), 1),
                "odds": round(odds, 2),          # REAL SportyBet odds
                "reasoning": comment,
                "result": "pending",
                "explore": True,                 # board-explored: prefer in 5/10/1000
                "sb_event_id": eid,
                "sb_market_id": mid,
                "sb_specifier": mk.get("specifier") or None,
                "sb_outcome_id": str(o.get("id", "")),
            })
    if out:
        print("[FB] board-explore: +{} extra markets for {}".format(
            len(out), match_label))
    return out


# ═══════════════════════════════════════════════════════════════════
# ACCUMULATOR BUILDER
# Pack the best picks into each odds tier.
# ═══════════════════════════════════════════════════════════════════

# Tier configuration: per-selection odds band + packing rules
TIER_CONFIG = {
    "2_odds": {
        "target": 2.0, "min_conf": 90, "min_sel": 4, "max_sel": 8,
        "odds_lo": 1.04, "odds_hi": 1.32,
        # BANKER: only the lowest-variance markets. No BTTS, no corners, no
        # outright win — those are coin-flips on a 2-odds slip.
        "allow": ["double_chance_1X", "double_chance_X2", "over_0.5",
                  "over_1.5", "under_3.5", "under_4.5", "oneup_home", "oneup_away",
                  "dc1up_1x", "dc1up_x2", "dc1up_12"],
        "prefer": ["dc1up_1x", "dc1up_x2", "oneup_home", "oneup_away",
                   "double_chance_1X", "double_chance_X2",
                   "over_1.5", "under_4.5", "under_3.5", "over_0.5"],
        "label": "2 ODDS — BANKER", "emoji": "🟢",
    },
    "3_odds": {
        "target": 3.0, "min_conf": 83, "min_sel": 4, "max_sel": 7,
        "odds_lo": 1.12, "odds_hi": 1.48,
        # SAFE: double chance + goals lines + outright wins ONLY when the match
        # projects clear (tight games shade wins below the 70 floor). Still no
        # BTTS or corners here — too unreliable for a "safe" slip.
        "allow": ["double_chance_1X", "double_chance_X2", "over_1.5",
                  "under_3.5", "under_2.5", "under_4.5", "over_2.5", "home_win", "away_win",
                  "dnb_home", "dnb_away", "oneup_home", "oneup_away",
                  "dc1up_1x", "dc1up_x2", "dc1up_12"],
        "prefer": ["dc1up_1x", "dc1up_x2", "oneup_home", "oneup_away",
                   "double_chance_1X", "double_chance_X2",
                   "over_1.5", "under_3.5", "home_win", "away_win", "under_2.5"],
        "label": "3 ODDS — SAFE", "emoji": "🟢",
    },
    "5_odds": {
        "target": 5.0, "min_conf": 73, "min_sel": 3, "max_sel": 6,
        "odds_lo": 1.22, "odds_hi": 1.80,
        # VALUE: this is where BTTS / corners are allowed to enter.
        "exclude": ["correct_score"],
        "prefer": ["home_win", "away_win", "over_2.5", "btts_yes",
                   "dc_over_1.5", "corners_over_7.5", "home_win_btts"],
        "label": "5 ODDS — VALUE", "emoji": "🟡",
    },
    "10_odds": {
        "target": 10.0, "min_conf": 63, "min_sel": 4, "max_sel": 6,
        "odds_lo": 1.38, "odds_hi": 2.40,
        "prefer": ["home_win", "away_win", "home_win_btts", "corners_over_8.5",
                   "handicap_home_-1.5", "cards_over_3.5", "over_2.5",
                   "home_win_over_2.5", "over_3.5"],
        "label": "10 ODDS — RISK", "emoji": "🟠",
    },
    "1000_odds": {
        "target": 1000.0, "min_conf": 58, "min_sel": 4, "max_sel": 20,
        "odds_lo": 1.28, "odds_hi": 15.0,   # real legs only — no 1.01 padding
        "rank": "odds",                      # build toward big odds, best longshots first
        "prefer": ["correct_score", "home_win_btts", "home_win_over_2.5",
                   "handicap_home_-1.5", "handicap_away_-1.5", "over_3.5",
                   "cards_over_3.5", "away_win", "corners_over_9.5"],
        "label": "1000+ ODDS — MOONSHOT", "emoji": "🔴",
    },
}


def _mkt_family(mt):
    """Group a market_type into a broad bet FAMILY so the accumulator diversity
    cap treats e.g. 'fh_under_1', 'fh_under_1.5', 'fh_under_2' as ONE kind of
    bet (first-half goals) instead of three different ones — which is what let
    a slip stack five near-identical 'under' legs and look repetitive."""
    mt = mt or ""
    if mt.startswith("double_chance") or mt.startswith("dc1up"):
        return "double_chance"
    if mt.startswith("oneup"):
        return "oneup"
    if mt in ("home_win", "away_win") or mt.startswith("dnb"):
        return "match_winner"
    if mt.startswith("fh_"):
        return "first_half_goals"
    if mt.startswith("team_"):
        return "team_total"
    if mt.startswith("multigoal"):
        return "goal_range"
    if mt.startswith("over_") or mt.startswith("under_"):
        return "match_goals"
    if mt.startswith("winmargin") or mt.startswith("leadby"):
        return "winning_margin"
    if mt.startswith("winhalf"):
        return "win_a_half"
    if mt.startswith("tonil"):
        return "win_to_nil"
    if mt.startswith("cleansheet"):
        return "clean_sheet"
    if "btts" in mt:
        return "btts"
    if mt.startswith("corners"):
        return "corners"
    if mt.startswith("cards"):
        return "cards"
    if mt.startswith("correct_score"):
        return "correct_score"
    return mt


def build_accumulator(all_picks, tier_key, used_selections=None, match_count=None, match_families=None, rotation_seed=0, max_reuse=2, avoid_games=None):
    """
    Build one accumulator tier with a DIVERSE mix of market types.
    Strategy:
      1. Filter picks to this tier's confidence floor + odds band, and drop any
         exact selection already placed in an earlier tier (used_selections), so
         no two slips repeat the identical leg
      2. Greedily pack (max 1 per match) but cap how many of each market
         type can appear, so a slip is a genuine mix (not all "win or draw"
         or all "over 8.5 corners")
      3. Stop when total odds reaches the tier target
    Returns dict {selections, total_odds, ...} or None if not buildable.
    """
    cfg = TIER_CONFIG[tier_key]
    target = cfg["target"]
    prefer_set = set(cfg["prefer"])
    allow_set = set(cfg.get("allow", []))      # if set, ONLY these market types
    exclude_set = set(cfg.get("exclude", []))  # never these market types
    used_selections = used_selections or set()   # (match, market_type) already used
    match_count = match_count or {}              # match -> how many tiers it's in already
    match_families = match_families or {}        # match -> bet families already used for it
    avoid_games = avoid_games or set()           # games used in the PREVIOUS session

    eligible = [
        p for p in all_picks
        if p["confidence"] >= cfg["min_conf"]
        and cfg["odds_lo"] <= p["odds"] <= cfg["odds_hi"]
        and (not allow_set or p["market_type"] in allow_set)
        and p["market_type"] not in exclude_set
        and (p["match"], p["market_type"]) not in used_selections
        # A game may appear in at most `max_reuse` slips, and any repeat must be a
        # TOTALLY different bet family — so one result can't sink the board, and a
        # repeated game is never the same kind of bet twice. max_reuse defaults to
        # 1 (no repeat) and is only relaxed by the caller when a tier can't build.
        and match_count.get(p["match"], 0) < max_reuse
        and _mkt_family(p["market_type"]) not in match_families.get(p["match"], set())
    ]
    if not eligible:
        return None

    # 5/10/1000 have no banker whitelist — those are the exploratory tiers.
    # They cap diversity by bet FAMILY (so "1st Half Under 1/1.5/2" count as ONE
    # kind of bet) and spread hard, to reach the full range of SportyBet markets.
    # The 2/3 banker tiers keep their ORIGINAL behaviour: cap by exact market
    # type with a +1 slack, so they stay safe and unchanged.
    explore_tier = not allow_set

    def _div_key(p):
        return _mkt_family(p["market_type"]) if explore_tier else p["market_type"]

    import math as _m
    distinct_types = len(set(_div_key(p) for p in eligible))
    slack = 0 if explore_tier else 1
    max_per_type = max(1, _m.ceil(cfg["max_sel"] / max(1, distinct_types)) + slack)
    if distinct_types >= cfg["max_sel"]:
        max_per_type = 1  # plenty of variety -> force every leg a different type

    rank_mode = cfg.get("rank", "conf")   # "odds" = moonshot builds toward big odds

    def base_rank(p):
        # 1) games from the PREVIOUS session sink to the bottom (only used if the
        #    slate is too thin to avoid them) — session 2 doesn't repeat session 1;
        # 2) games NOT already used in an earlier tier this run come next;
        # 3) preferred market types; 4) the MAIN sort:
        #      - normal tiers: confidence BAND (nearest 10), best picks first;
        #      - moonshot: ODDS band (highest first), so it actually climbs toward
        #        the big number using the best available longshots, not 1.01 padding;
        # 5) a per-run rotation jitter, so 6am and 6pm aren't identical.
        avoided = p["match"] in avoid_games
        fresh = match_count.get(p["match"], 0) == 0
        preferred = (p["market_type"] in prefer_set
                     or (explore_tier and p.get("explore")))
        if rank_mode == "odds":
            band = -int(round(p["odds"] * 2))         # longer odds first (best longshots)
        else:
            band = -(int(p["confidence"]) // 10)      # 90-99 together, 80-89 together...
        jitter = hash((rotation_seed, p["match"], p["market_type"])) % 100000
        return (1 if avoided else 0, 0 if fresh else 1,
                0 if preferred else 1, band, jitter)

    remaining = sorted(eligible, key=base_rank)

    slip = []
    used_matches = set()
    type_count = {}
    running = 1.0

    def try_pack(per_type_cap):
        nonlocal running
        # Greedy with diversity: each step, pick the highest-ranked candidate
        # whose market type is still under the cap and that doesn't overshoot.
        progressed = True
        while progressed and len(slip) < cfg["max_sel"]:
            progressed = False
            for p in remaining:
                if p in slip or p["match"] in used_matches:
                    continue
                key = _div_key(p)
                if type_count.get(key, 0) >= per_type_cap:
                    continue
                new_running = running * p["odds"]
                if rank_mode != "odds" and new_running > target * 1.18 and len(slip) >= cfg["min_sel"]:
                    continue
                slip.append(p)
                used_matches.add(p["match"])
                type_count[key] = type_count.get(key, 0) + 1
                running = new_running
                progressed = True
                if running >= target * 0.92 and len(slip) >= cfg["min_sel"]:
                    return True
                break  # restart scan from the top for best-ranked next pick
        return running >= target * 0.92 and len(slip) >= cfg["min_sel"]

    # First pass: strict diversity cap
    done = try_pack(max_per_type)
    # If we couldn't reach the target band, relax the cap and keep packing
    if not done and len(slip) < cfg["min_sel"]:
        try_pack(max_per_type + 2)
    if not done and len(slip) < cfg["min_sel"]:
        try_pack(cfg["max_sel"])  # last resort: allow repeats to hit min legs

    if len(slip) < cfg["min_sel"]:
        return None

    return {
        "tier": tier_key,
        "label": cfg["label"],
        "emoji": cfg["emoji"],
        "target_odds": target,
        "total_odds": round(running, 2),
        "num_selections": len(slip),
        "selections": slip,
    }


def build_all_accumulators(all_picks, avoid_games=None):
    """Build all 5 tiers, each distinct. Returns {tier_key: accumulator|None}.
    Tiers are built safest-first, with de-duplication that controls correlation:
      - the identical leg (match, market_type) is never repeated across tiers;
      - by DEFAULT a game appears in only ONE slip (no repeat). A tier that can't
        build under that rule relaxes to allow a game in 2 then 3 slips — but any
        repeat must be a TOTALLY different bet family, so no one result sinks the
        board. This keeps thin days from losing the bigger tiers (flexible
        moonshot) while busy days stay fully spread;
      - games used in the PREVIOUS session are pushed to the bottom, so the 12h
        session 2 doesn't repeat session 1 (best-effort: on a very thin slate it
        may have to reuse some, since the same few games are all that's on)."""
    result = {}
    used = set()            # (match, market_type) legs already placed
    match_count = {}        # match -> how many slips it appears in
    match_families = {}     # match -> set of bet families already used for it
    avoid_games = avoid_games or set()   # games from the previous session
    # Per-run rotation: changes each hour so consecutive runs (e.g. 6am vs 6pm)
    # pick DIFFERENT expressions of the same games rather than the identical slip.
    rotation_seed = int(time.time() // 3600)
    for tier_key in ["2_odds", "3_odds", "5_odds", "10_odds", "1000_odds"]:
        tgt = TIER_CONFIG[tier_key]["target"]
        if tier_key in ("10_odds", "1000_odds"):
            # The two highest tiers need the scarce high-odds legs, and on a thin
            # slate the lower tiers eat them first under no-repeat — which is what
            # stranded the moonshot on 4 legs. Each of these is a STANDALONE slip
            # the user plays on its own, so build each from the FULL board (highest
            # available legs, one per match) rather than the leftovers. They may
            # share a game with another slip — that's fine across separate slips.
            result[tier_key] = build_accumulator(
                all_picks, tier_key, set(), {}, {}, rotation_seed,
                max_reuse=1, avoid_games=avoid_games)
            continue
        # 2/3/5: kept distinct from each other (no-repeat). Relax the cap 1->2->3
        # so a tier can still REACH ITS TARGET on a thin slate by reusing a game
        # with a DIFFERENT bet family. Keep the best build across caps.
        best = None
        for cap in (1, 2, 3):
            acc = build_accumulator(all_picks, tier_key, used, match_count,
                                    match_families, rotation_seed,
                                    max_reuse=cap, avoid_games=avoid_games)
            if acc and (best is None or acc["total_odds"] > best["total_odds"]):
                best = acc
            if best and best["total_odds"] >= tgt * 0.92:
                break
        result[tier_key] = best
        if best:
            for s in best["selections"]:
                used.add((s["match"], s["market_type"]))
                m = s["match"]
                match_count[m] = match_count.get(m, 0) + 1
                match_families.setdefault(m, set()).add(_mkt_family(s["market_type"]))
    return result


def top_picks_per_match(all_picks, n=3):
    """Group picks by match, return top N per match by confidence."""
    by_match = {}
    for p in all_picks:
        by_match.setdefault(p["match"], []).append(p)
    out = {}
    for match, picks in by_match.items():
        picks.sort(key=lambda x: x["confidence"], reverse=True)
        out[match] = picks[:n]
    return out


"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — FOOTBALL DATA SCRAPERS
═══════════════════════════════════════════════════════════════════
Scrapes Sofascore (fixtures/form/h2h/injuries/standings),
Understat (xG), and FootyStats (corners/cards/btts).

EVERY function is wrapped so a failure returns safe defaults instead
of crashing. The engine fills gaps with league-average assumptions.

NOTE: These endpoints cannot be tested from the build sandbox
(network restricted). They are written from documented API shapes
and need one round of Railway validation. Failures degrade gracefully.
═══════════════════════════════════════════════════════════════════
"""

import time
import json
import datetime as _dt
import os as _os

try:
    import requests as _req
except ImportError:
    _req = None

# Cloudflare bypass: curl_cffi sends a real Chrome TLS/JA3 fingerprint, which
# defeats the fingerprint layer that auto-blocks plain `requests` from a server.
# That's the layer blocking Sofascore/FBref from Railway. If IP-reputation
# blocking ALSO persists (datacenter ASNs are pre-scored as bots), set the
# SCRAPE_PROXY env var to a residential/ISP proxy URL and every scrape routes
# through it — covering both Cloudflare layers.
try:
    from curl_cffi import requests as _cf
except ImportError:
    _cf = None

# Cloudscraper solves Cloudflare's JS/IUAM challenge in pure Python (no browser).
# This is what ScraperFC uses for FBref; it's the light fix for the 'challenge'
# 403 that TLS impersonation alone can't clear.
try:
    import cloudscraper as _cloudscraper
except ImportError:
    _cloudscraper = None

try:
    from bs4 import BeautifulSoup as _BS
except ImportError:
    _BS = None

def _sanitize_proxy(raw):
    """Pull a clean proxy URL out of whatever was set — tolerates a pasted
    `curl --proxy "..."` command, surrounding quotes, whitespace, or a trailing
    slash. Returns scheme://[user:pass@]host:port or '' if nothing usable."""
    if not raw:
        return ""
    raw = raw.strip().strip('"').strip("'")
    m = re.search(r'(socks5h|socks5|socks4|https?)://[^\s"\']+', raw)
    if not m:
        return ""
    url = m.group(0).rstrip("/")
    return url


_SCRAPE_PROXY = _sanitize_proxy(_os.environ.get("SCRAPE_PROXY", "")
                                or _os.environ.get("POLY_PROXY_URL", ""))
_CF_IMPERSONATE = _os.environ.get("CF_IMPERSONATE", "chrome131").strip() or "chrome131"
print("[SCRAPE] curl_cffi={} cloudscraper={} impersonate={} proxy={}".format(
    "yes" if _cf else "NO", "yes" if _cloudscraper else "NO", _CF_IMPERSONATE,
    "set" if _SCRAPE_PROXY else "none"))

# Browser-like headers to avoid trivial blocks
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

SOFA = "https://api.sofascore.com/api/v1"


def _scrape_referer(url):
    """Pick a believable Referer/Origin for the host (Cloudflare checks these)."""
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower()
    except Exception:
        return None
    if "sofascore" in host:
        return "https://www.sofascore.com/"
    if "fbref" in host:
        return "https://fbref.com/"
    if "understat" in host:
        return "https://understat.com/"
    try:
        p = urlparse(url)
        return "{}://{}/".format(p.scheme, p.netloc)
    except Exception:
        return None


_CF_SESSION = None
_CS_SESSION = None
_WARMED = set()


def _cs_session():
    """Cloudscraper session (solves Cloudflare JS challenge in pure Python),
    routed through the proxy if set."""
    global _CS_SESSION
    if _CS_SESSION is None and _cloudscraper is not None:
        try:
            _CS_SESSION = _cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False})
            if _SCRAPE_PROXY:
                _CS_SESSION.proxies = {"http": _SCRAPE_PROXY, "https": _SCRAPE_PROXY}
        except Exception:
            _CS_SESSION = None
    return _CS_SESSION


def _cf_session():
    """Persistent curl_cffi session so Cloudflare clearance cookies (set after a
    homepage visit) carry into subsequent API calls."""
    global _CF_SESSION
    if _CF_SESSION is None and _cf is not None:
        try:
            kw = {"impersonate": _CF_IMPERSONATE}
            if _SCRAPE_PROXY:
                kw["proxies"] = {"http": _SCRAPE_PROXY, "https": _SCRAPE_PROXY}
            _CF_SESSION = _cf.Session(**kw)
        except Exception:
            _CF_SESSION = None
    return _CF_SESSION


def _warmup(api_url):
    """Visit the site's homepage once to collect the cf_clearance cookie, then
    reuse the same session for the protected API endpoint. This clears the
    cookie-based 'challenge' that a cold request trips."""
    ref = _scrape_referer(api_url)  # e.g. https://www.sofascore.com/
    if not ref:
        return
    try:
        from urllib.parse import urlparse
        host = urlparse(ref).netloc
    except Exception:
        host = ref
    if host in _WARMED:
        return
    _WARMED.add(host)
    s = _cf_session()
    if s is None:
        return
    try:
        s.get(ref, headers=_HEADERS, timeout=15)
        time.sleep(1.2)  # let the clearance cookie settle
    except Exception:
        pass


def _scrape_get(url, timeout=15):
    """One GET that beats Cloudflare via curl_cffi Chrome impersonation + a warmed
    session (homepage visit → clearance cookie → API call), routing through
    SCRAPE_PROXY when set. Falls back to plain requests if curl_cffi is absent."""
    headers = dict(_HEADERS)
    ref = _scrape_referer(url)
    if ref:
        headers["Referer"] = ref
    proxies = {"http": _SCRAPE_PROXY, "https": _SCRAPE_PROXY} if _SCRAPE_PROXY else None
    # Cloudflare-challenged hosts: try cloudscraper first (solves the JS challenge)
    challenged = any(h in url for h in ("sofascore", "fbref", "understat"))
    if challenged and _cloudscraper is not None:
        cs = _cs_session()
        if cs is not None:
            try:
                r = cs.get(url, headers=headers, timeout=timeout)
                if r is not None and r.status_code == 200:
                    return r
            except Exception:
                pass
    if _cf is not None:
        s = _cf_session()
        if s is not None:
            try:
                _warmup(url)  # one homepage hit per host to pick up cf cookies
                return s.get(url, headers=headers, timeout=timeout)
            except Exception:
                pass
        try:
            kw = {"headers": headers, "timeout": timeout, "impersonate": _CF_IMPERSONATE}
            if proxies:
                kw["proxies"] = proxies
            return _cf.get(url, **kw)
        except Exception:
            pass
    if _req is not None:
        try:
            kw = {"headers": headers, "timeout": timeout}
            if proxies:
                kw["proxies"] = proxies
            return _req.get(url, **kw)
        except Exception:
            pass
    return None


def _get_json(url, timeout=12, retries=2):
    """GET a URL and parse JSON. Returns None on any failure."""
    if _req is None and _cf is None:
        return None
    for attempt in range(retries):
        try:
            r = _scrape_get(url, timeout=timeout)
            if r is not None and r.status_code == 200:
                return r.json()
            if r is not None and r.status_code == 429:
                time.sleep(3)  # rate limited, back off
        except Exception:
            pass
        time.sleep(1)
    return None


def _get_html(url, timeout=12):
    """GET a URL and return text. Returns None on any failure."""
    if _req is None and _cf is None:
        return None
    try:
        r = _scrape_get(url, timeout=timeout)
        if r is not None and r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════
# SOFASCORE — fixtures, form, H2H, injuries, standings
# ═══════════════════════════════════════════════════════════════════

# Soccer leagues we care about (Sofascore uniqueTournament IDs)
# These are stable IDs from Sofascore.
SOFA_LEAGUES = {
    "EPL": 17, "La Liga": 8, "Bundesliga": 35, "Serie A": 23,
    "Ligue 1": 34, "Eredivisie": 37, "Primeira Liga": 238,
    "Champions League": 7, "Europa League": 679, "Championship": 18,
}


def sofa_todays_fixtures(date_str=None, max_leagues=None):
    """
    Get today's football fixtures from Sofascore.
    Returns list of fixture dicts with event_id, teams, league, kickoff.
    """
    if date_str is None:
        date_str = _dt.date.today().isoformat()

    fixtures = []
    data = _get_json("{}/sport/football/scheduled-events/{}".format(SOFA, date_str))
    if not data or "events" not in data:
        return fixtures

    wanted_league_ids = set(SOFA_LEAGUES.values())
    for ev in data.get("events", []):
        try:
            tournament = ev.get("tournament", {})
            unique_t = tournament.get("uniqueTournament", {}) or {}
            league_id = unique_t.get("id")
            league_name = unique_t.get("name", tournament.get("name", ""))

            # Only keep leagues we track (or all if not restricting)
            if wanted_league_ids and league_id not in wanted_league_ids:
                continue

            status = ev.get("status", {}).get("type", "")
            if status not in ("notstarted", "inprogress"):
                continue  # skip finished games

            home = ev.get("homeTeam", {})
            away = ev.get("awayTeam", {})
            start_ts = ev.get("startTimestamp", 0)

            fixtures.append({
                "event_id": str(ev.get("id", "")),
                "home_team": home.get("name", ""),
                "away_team": away.get("name", ""),
                "home_id": home.get("id"),
                "away_id": away.get("id"),
                "league": league_name,
                "league_id": league_id,
                "season_id": unique_t.get("id"),  # resolved later
                "kickoff_time": _dt.datetime.fromtimestamp(start_ts).isoformat() if start_ts else "",
                "kickoff_ts": start_ts,
            })
        except Exception:
            continue

    return fixtures


def sofa_search_team(name):
    """Resolve a team name to its Sofascore ID via the search endpoint. Returns
    (team_id, reachable): reachable distinguishes 'Sofascore answered but no
    match' from 'Sofascore blocked/unreachable' so we can diagnose Cloudflare."""
    if not name:
        return None, False
    try:
        from urllib.parse import quote
        q = quote(name)
    except Exception:
        q = name.replace(" ", "%20")
    data = _get_json("{}/search/all?q={}".format(SOFA, q))
    if data is None:
        return None, False  # unreachable / blocked
    results = data.get("results", []) if isinstance(data, dict) else []
    for r in results:
        if r.get("type") == "team":
            ent = r.get("entity", {}) or {}
            if ent.get("id"):
                return ent["id"], True
    return None, True  # reachable, just no team match


def sofa_team_form_stats(team_id, limit=10):
    """Last-N granular form for a team, computed from the same events feed as
    sofa_team_form (which already has each match's full score). Returns the
    per-match breakdowns the pick rules need:
      over15_pct / over25_pct / under35_pct : share of recent games hitting it
      scored_pct / cs_pct / btts_pct        : attack/defence reliability
      avg_gf / avg_ga / ppg                 : strength signals
      form (WWDLW), played
    Returns {} on no data so callers fall back gracefully."""
    if not team_id:
        return {}
    data = _get_json("{}/team/{}/events/last/0".format(SOFA, team_id))
    if not data or "events" not in data:
        return {}
    events = [e for e in data.get("events", [])
              if e.get("homeScore", {}).get("current") is not None
              and e.get("awayScore", {}).get("current") is not None][-limit:]
    n = len(events)
    if n == 0:
        return {}
    over15 = over25 = under35 = scored = cs = btts = pts = 0
    gf = ga = 0
    form = []
    for ev in reversed(events):  # most recent first
        hs = ev["homeScore"]["current"]
        as_ = ev["awayScore"]["current"]
        is_home = ev.get("homeTeam", {}).get("id") == team_id
        my, opp = (hs, as_) if is_home else (as_, hs)
        tot = hs + as_
        gf += my; ga += opp
        if tot > 1.5: over15 += 1
        if tot > 2.5: over25 += 1
        if tot < 3.5: under35 += 1
        if my > 0: scored += 1
        if opp == 0: cs += 1
        if hs > 0 and as_ > 0: btts += 1
        if my > opp: form.append("W"); pts += 3
        elif my < opp: form.append("L")
        else: form.append("D"); pts += 1
    return {
        "played": n, "form": "".join(form),
        "over15_pct": over15 / n, "over25_pct": over25 / n, "under35_pct": under35 / n,
        "scored_pct": scored / n, "cs_pct": cs / n, "btts_pct": btts / n,
        "avg_gf": round(gf / n, 2), "avg_ga": round(ga / n, 2), "ppg": round(pts / n, 2),
    }


def sofa_team_form(team_id, limit=5):
    """Get last N results for a team as a form string like 'WWDLW'."""
    if not team_id:
        return ""
    data = _get_json("{}/team/{}/events/last/0".format(SOFA, team_id))
    if not data or "events" not in data:
        return ""
    events = data.get("events", [])[-limit:]
    form = []
    for ev in reversed(events):  # most recent first
        try:
            home_id = ev.get("homeTeam", {}).get("id")
            hs = ev.get("homeScore", {}).get("current")
            as_ = ev.get("awayScore", {}).get("current")
            if hs is None or as_ is None:
                continue
            is_home = (home_id == team_id)
            my_score = hs if is_home else as_
            opp_score = as_ if is_home else hs
            if my_score > opp_score:
                form.append("W")
            elif my_score < opp_score:
                form.append("L")
            else:
                form.append("D")
        except Exception:
            continue
    return "".join(form)


def sofa_h2h(event_id):
    """Get head-to-head summary for a match. Returns dict with last meetings."""
    if not event_id:
        return {}
    data = _get_json("{}/event/{}/h2h".format(SOFA, event_id))
    if not data:
        return {}
    return data.get("teamDuel", {}) or {}


def sofa_injuries(team_id):
    """Team-news signal weighted by player IMPORTANCE, not a raw count — a
    missing star hurts far more than a missing squad player. Sofascore's injury
    feed carries each absent player; we weight by how central they are (rating /
    market value when present), so 'key injuries' reflects real squad damage."""
    if not team_id:
        return 0
    data = _get_json("{}/team/{}/player/injuries".format(SOFA, team_id))
    if not data:
        return 0
    injuries = data.get("playerInjuries", data.get("injuries", []))
    if not isinstance(injuries, list):
        return 0
    weight = 0.0
    for inj in injuries:
        p = inj.get("player", inj) if isinstance(inj, dict) else {}
        # importance proxy: market value (€) or recent rating; fall back to 1
        mv = p.get("proposedMarketValue") or p.get("marketValue") or 0
        rating = _to_num(p.get("rating") or p.get("avgRating") or 0)
        if mv and mv >= 40_000_000:
            weight += 2.0          # genuine star
        elif mv and mv >= 12_000_000:
            weight += 1.3
        elif rating and rating >= 7.3:
            weight += 1.5          # high performer
        else:
            weight += 0.6          # squad/fringe player
    # Return an integer "key-injury equivalent" the strength calc already uses
    return int(round(weight))


def sofa_injuries_count(team_id):
    """Raw count of absences (kept for any callers that want the plain number)."""
    if not team_id:
        return 0
    data = _get_json("{}/team/{}/player/injuries".format(SOFA, team_id))
    if not data:
        return 0
    inj = data.get("playerInjuries", data.get("injuries", []))
    return len(inj) if isinstance(inj, list) else 0


def sofa_match_stats_summary(event_id):
    """Get corners/cards from a finished match (for averages). Used in aggregation."""
    if not event_id:
        return {}
    data = _get_json("{}/event/{}/statistics".format(SOFA, event_id))
    if not data:
        return {}
    out = {}
    try:
        for period in data.get("statistics", []):
            if period.get("period") != "ALL":
                continue
            for group in period.get("groups", []):
                for item in group.get("statisticsItems", []):
                    name = item.get("name", "").lower()
                    if "corner" in name:
                        out["home_corners"] = _to_num(item.get("home"))
                        out["away_corners"] = _to_num(item.get("away"))
                    if "yellow" in name:
                        out["home_cards"] = _to_num(item.get("home"))
                        out["away_cards"] = _to_num(item.get("away"))
    except Exception:
        pass
    return out


def _to_num(v):
    try:
        return float(str(v).split()[0])
    except (ValueError, TypeError, IndexError, AttributeError):
        return 0.0


# ═══════════════════════════════════════════════════════════════════
# UNDERSTAT — xG data (JSON embedded in <script> tags)
# ═══════════════════════════════════════════════════════════════════

UNDERSTAT_LEAGUES = {
    "EPL": "EPL", "La Liga": "La_liga", "Bundesliga": "Bundesliga",
    "Serie A": "Serie_A", "Ligue 1": "Ligue_1",
}


def understat_team_xg(league_name, season="2025"):
    """
    Scrape Understat for team xG data.
    Returns dict: {team_name: {xg_for, xg_against, played}}
    Understat embeds data as JSON.parse('...') inside <script> tags.
    """
    out = {}
    us_league = UNDERSTAT_LEAGUES.get(league_name)
    if not us_league:
        return out

    html = _get_html("https://understat.com/league/{}/{}".format(us_league, season))
    if not html:
        return out

    try:
        # Find the teamsData script — format: var teamsData = JSON.parse('...')
        import re
        m = re.search(r"teamsData\s*=\s*JSON\.parse\('([^']+)'\)", html)
        if not m:
            return out
        # Decode the hex-escaped JSON
        raw = m.group(1).encode().decode("unicode_escape")
        teams_data = json.loads(raw)

        for team_id, tdata in teams_data.items():
            name = tdata.get("title", "")
            history = tdata.get("history", [])
            if not history:
                continue
            xg_for = sum(_to_num(h.get("xG")) for h in history)
            xg_against = sum(_to_num(h.get("xGA")) for h in history)
            played = len(history)
            # PPDA = opponent passes allowed per defensive action. LOW = aggressive
            # high press; HIGH = passive low block. This is the tactical-style signal.
            ppda_att = ppda_def = 0.0
            for h in history:
                pp = h.get("ppda") or {}
                ppda_att += _to_num(pp.get("att"))
                ppda_def += _to_num(pp.get("def"))
            ppda = round(ppda_att / ppda_def, 2) if ppda_def else None
            if played > 0:
                out[name] = {
                    "xg_for": round(xg_for / played, 2),
                    "xg_against": round(xg_against / played, 2),
                    "ppda": ppda,
                    "played": played,
                }
    except Exception:
        pass

    return out


# ═══════════════════════════════════════════════════════════════════
# FBREF — Opta-sourced team style (possession, progressive play). One
# Big-5 page covers every top-5-league team; cached for the whole run.
# ═══════════════════════════════════════════════════════════════════

_FBREF_CACHE = {}


def fbref_big5_possession():
    """Scrape FBref's Big-5 squad possession table once (Opta-sourced data):
    season possession %, progressive passes/carries per team. FBref hides some
    tables inside HTML comments, so we strip comment markers before parsing.
    Returns {normalized_team: {possession, prog_passes, prog_carries}}; {} on
    any failure (engine degrades gracefully — these are bonus signals)."""
    if _FBREF_CACHE.get("loaded"):
        return _FBREF_CACHE.get("data", {})
    out = {}
    _FBREF_CACHE["loaded"] = True  # only attempt once per process
    if _BS is None:
        _FBREF_CACHE["data"] = out
        return out
    html = _get_html(
        "https://fbref.com/en/comps/Big5/possession/squads/Big-5-European-Leagues-Stats")
    if not html:
        _FBREF_CACHE["data"] = out
        return out
    try:
        # un-hide comment-wrapped tables
        html = html.replace("<!--", "").replace("-->", "")
        soup = _BS(html, "html.parser")
        table = soup.find("table", id=lambda x: x and "possession" in x) \
            or soup.find("table", id=lambda x: x and "stats_squads" in x)
        if not table:
            _FBREF_CACHE["data"] = out
            return out
        for row in table.select("tbody tr"):
            tcell = row.find(attrs={"data-stat": "team"})
            if not tcell:
                continue
            name = tcell.get_text(strip=True)
            if not name or name.lower() == "squad":
                continue

            def g(stat):
                c = row.find(attrs={"data-stat": stat})
                if not c:
                    return None
                try:
                    return float(c.get_text(strip=True))
                except (ValueError, TypeError):
                    return None
            out[_fb_norm_team(name)] = {
                "possession": g("possession"),
                "prog_passes": g("progressive_passes"),
                "prog_carries": g("progressive_carries"),
            }
        print("[FBREF] loaded {} teams (possession/progressive)".format(len(out)))
    except Exception as e:
        print("[FBREF] parse error: {}".format(e))
    _FBREF_CACHE["data"] = out
    return out


def fbref_lookup(stats, team_name):
    """Name-match a fixture team into the FBref table (uses the same canon/alias
    logic as score matching, so 'Man Utd' -> 'Manchester Utd' etc.)."""
    if not stats or not team_name:
        return None
    key = _fb_norm_team(team_name)
    if key in stats:
        return stats[key]
    for k, v in stats.items():
        if _fb_teams_match(team_name, k):
            return v
    return None


# ═══════════════════════════════════════════════════════════════════
# API-FOOTBALL (api-sports.io) — real last-N form/goals/over-under, from any
# IP, no Cloudflare. Uses a key already in the user's Railway variables. Auto-
# detects the var name AND the access method (direct api-sports vs RapidAPI).
# ═══════════════════════════════════════════════════════════════════

_APIFB = {"checked": False, "base": None, "headers": None, "calls": 0,
          "day": "", "today": 0}
_APIFB_TEAM_IDS = {}
# Hard daily ceiling so the free tier can never be blown — fallback only.
APIFB_DAILY_CAP = int(_os.environ.get("APIFB_DAILY_CAP", "60") or 60)


def _apifootball_key():
    for name in ("API_FOOTBALL_KEY", "APIFOOTBALL_KEY", "API_FOOTBALL",
                 "API_SPORTS_KEY", "APISPORTS_KEY", "FOOTBALL_API_KEY",
                 "RAPIDAPI_KEY", "X_RAPIDAPI_KEY"):
        v = _os.environ.get(name, "").strip()
        if v:
            return name, v
    return None, None


def _apifootball_setup():
    """Probe both access methods once; remember whichever returns a valid body."""
    if _APIFB["checked"]:
        return _APIFB["base"] is not None
    _APIFB["checked"] = True
    name, key = _apifootball_key()
    if not key:
        print("[APIFB] no API-Football key found in env")
        return False
    candidates = [
        ("https://v3.football.api-sports.io", {"x-apisports-key": key}),
        ("https://api-football-v1.p.rapidapi.com/v3",
         {"x-rapidapi-key": key, "x-rapidapi-host": "api-football-v1.p.rapidapi.com"}),
    ]
    for base, headers in candidates:
        try:
            r = _req.get(base + "/status", headers=headers, timeout=15)
            if r.status_code == 200 and isinstance(r.json(), dict) and "response" in r.json():
                _APIFB["base"], _APIFB["headers"] = base, headers
                print("[APIFB] connected via {} (key var: {})".format(
                    "rapidapi" if "rapidapi" in base else "api-sports", name))
                return True
        except Exception:
            pass
    print("[APIFB] key found ({}) but neither access method validated".format(name))
    return False


def _apifootball_get(path, params):
    if not _apifootball_setup():
        return None
    # daily budget guard — never exceed the free-tier ceiling
    today = _dt.date.today().isoformat()
    if _APIFB["day"] != today:
        _APIFB["day"] = today
        _APIFB["today"] = 0
    if _APIFB["today"] >= APIFB_DAILY_CAP:
        return None
    try:
        _APIFB["calls"] += 1
        _APIFB["today"] += 1
        r = _req.get(_APIFB["base"] + path, headers=_APIFB["headers"],
                     params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def _apifootball_team_id(name):
    if not name:
        return None
    key = name.lower().strip()
    if key in _APIFB_TEAM_IDS:
        return _APIFB_TEAM_IDS[key]
    data = _apifootball_get("/teams", {"search": name})
    tid = None
    if data and data.get("response"):
        tid = (data["response"][0].get("team") or {}).get("id")
    _APIFB_TEAM_IDS[key] = tid
    return tid


def apifootball_form_stats(team_name):
    """Real last-10 form computed from API-Football fixtures — same shape the
    methodology rules read (over15_pct, under35_pct, avg_gf/ga, ppg, form...)."""
    tid = _apifootball_team_id(team_name)
    if not tid:
        return {}
    data = _apifootball_get("/fixtures", {"team": tid, "last": 10})
    if not data or not data.get("response"):
        return {}
    evs = []
    for ev in data["response"]:
        g = ev.get("goals") or {}
        hs, as_ = g.get("home"), g.get("away")
        st = ((ev.get("fixture") or {}).get("status") or {}).get("short", "")
        if hs is None or as_ is None or st not in ("FT", "AET", "PEN"):
            continue
        home_id = ((ev.get("teams") or {}).get("home") or {}).get("id")
        evs.append((hs, as_, home_id))
    n = len(evs)
    if n == 0:
        return {}
    over15 = over25 = under35 = scored = cs = btts = pts = gf = ga = 0
    form = []
    for hs, as_, home_id in evs:
        is_home = (home_id == tid)
        my, opp = (hs, as_) if is_home else (as_, hs)
        tot = hs + as_
        gf += my; ga += opp
        if tot > 1.5: over15 += 1
        if tot > 2.5: over25 += 1
        if tot < 3.5: under35 += 1
        if my > 0: scored += 1
        if opp == 0: cs += 1
        if hs > 0 and as_ > 0: btts += 1
        if my > opp: form.append("W"); pts += 3
        elif my < opp: form.append("L")
        else: form.append("D"); pts += 1
    return {
        "played": n, "form": "".join(form),
        "over15_pct": over15 / n, "over25_pct": over25 / n, "under35_pct": under35 / n,
        "scored_pct": scored / n, "cs_pct": cs / n, "btts_pct": btts / n,
        "avg_gf": round(gf / n, 2), "avg_ga": round(ga / n, 2), "ppg": round(pts / n, 2),
    }


# ═══════════════════════════════════════════════════════════════════
# NEW MODEL (additive, isolated) — ClubElo + football-data.co.uk + Dixon-Coles.
# Produces EXTRA options (corners, cards, correct score, value flags) for CLUB
# matches only. Fully wrapped: if any source is down it returns {} and the
# existing engine is untouched. Proven sources: football-data.co.uk (Joseph
# Buchdahl, since 2001) and the Dixon-Coles 1997 model (Journal of the Royal
# Statistical Society).
# ═══════════════════════════════════════════════════════════════════

_FD_CACHE = {}      # football-data.co.uk league CSVs, per (code, season)
_CE_CACHE = {}      # ClubElo day ratings


def _poisson_pmf(k, lam):
    import math
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def _dc_tau(x, y, lh, la, rho):
    # Dixon-Coles low-score dependency adjustment
    if x == 0 and y == 0:
        return 1.0 - lh * la * rho
    if x == 0 and y == 1:
        return 1.0 + lh * rho
    if x == 1 and y == 0:
        return 1.0 + la * rho
    if x == 1 and y == 1:
        return 1.0 - rho
    return 1.0


def _dc_matrix(lh, la, rho=-0.13, max_goals=8):
    m = [[0.0] * (max_goals + 1) for _ in range(max_goals + 1)]
    s = 0.0
    for x in range(max_goals + 1):
        for y in range(max_goals + 1):
            p = _poisson_pmf(x, lh) * _poisson_pmf(y, la) * _dc_tau(x, y, lh, la, rho)
            if p < 0:
                p = 0.0
            m[x][y] = p
            s += p
    if s > 0:
        for x in range(max_goals + 1):
            for y in range(max_goals + 1):
                m[x][y] /= s
    return m


def _dc_markets(m):
    """Derive 1X2, over/under, BTTS and most-likely correct score from the matrix."""
    mg = len(m)
    home = draw = away = btts = 0.0
    over = {1.5: 0.0, 2.5: 0.0, 3.5: 0.0}
    best = (0, 0, 0.0)
    for x in range(mg):
        for y in range(mg):
            p = m[x][y]
            if x > y:
                home += p
            elif x == y:
                draw += p
            else:
                away += p
            if x > 0 and y > 0:
                btts += p
            tot = x + y
            for ln in over:
                if tot > ln:
                    over[ln] += p
            if p > best[2]:
                best = (x, y, p)
    return {
        "home": round(home, 3), "draw": round(draw, 3), "away": round(away, 3),
        "btts_yes": round(btts, 3), "btts_no": round(1 - btts, 3),
        "over": {k: round(v, 3) for k, v in over.items()},
        "under": {k: round(1 - v, 3) for k, v in over.items()},
        "correct_score": "{}-{}".format(best[0], best[1]),
        "cs_prob": round(best[2], 3),
    }


def _poisson_over(lam, line):
    """P(total > line) for a Poisson count (corners, cards)."""
    import math
    cum = sum(_poisson_pmf(k, lam) for k in range(0, int(math.floor(line)) + 1))
    return max(0.0, min(1.0, 1.0 - cum))


def _fd_norm(name):
    return "".join(c for c in (name or "").lower() if c.isalnum())


def _fd_match_name(a, b):
    na, nb = _fd_norm(a), _fd_norm(b)
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na


def _fd_league(code, season):
    """Fetch + cache one football-data.co.uk league CSV (historical results with
    goals, corners HC/AC, cards HY/AY/HR/AR and bookmaker odds)."""
    key = (code, season)
    if key in _FD_CACHE:
        return _FD_CACHE[key]
    rows = []
    try:
        import csv as _csv
        import io as _io
        url = "https://www.football-data.co.uk/mmz4281/{}/{}.csv".format(season, code)
        r = _req.get(url, timeout=25)
        if r.status_code == 200 and r.text:
            rd = _csv.DictReader(_io.StringIO(r.text))
            for row in rd:
                if row.get("HomeTeam") and row.get("FTHG") not in (None, ""):
                    rows.append(row)
    except Exception as e:
        print("[MODEL] football-data fetch error {}/{}: {}".format(code, season, e))
    _FD_CACHE[key] = rows
    return rows


def _fd_team_stats(rows, team, last=12):
    """Recent averages for one team: goals for/against, corners for/against, cards."""
    def _f(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0
    games = []
    for row in rows:
        h, a = row.get("HomeTeam", ""), row.get("AwayTeam", "")
        if _fd_match_name(team, h):
            games.append(("H", row))
        elif _fd_match_name(team, a):
            games.append(("A", row))
    games = games[-last:]
    if not games:
        return {}
    gf = ga = cf = ca = cards = 0.0
    for side, row in games:
        if side == "H":
            gf += _f(row.get("FTHG")); ga += _f(row.get("FTAG"))
            cf += _f(row.get("HC")); ca += _f(row.get("AC"))
            cards += _f(row.get("HY")) + _f(row.get("HR"))
        else:
            gf += _f(row.get("FTAG")); ga += _f(row.get("FTHG"))
            cf += _f(row.get("AC")); ca += _f(row.get("HC"))
            cards += _f(row.get("AY")) + _f(row.get("AR"))
    n = len(games)
    return {
        "games": n, "gf": gf / n, "ga": ga / n,
        "cf": cf / n, "ca": ca / n, "cards": cards / n,
    }


def _fd_league_avgs(rows):
    def _f(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0
    if not rows:
        return None
    h = sum(_f(r.get("FTHG")) for r in rows) / len(rows)
    a = sum(_f(r.get("FTAG")) for r in rows) / len(rows)
    return {"home_goals": h or 1.4, "away_goals": a or 1.1}


def model_club_match(home, away, code, season, odds=None):
    """Build the EXTRA-options model for one club match. Returns {} if no data
    (so callers degrade silently). `odds` optional dict for value flags:
    {'home','draw','away'} decimal odds."""
    rows = _fd_league(code, season)
    if not rows:
        return {}
    lg = _fd_league_avgs(rows)
    hs = _fd_team_stats(rows, home)
    as_ = _fd_team_stats(rows, away)
    if not hs or not as_ or not lg:
        return {}

    # Attack/defense strengths relative to league average → goal expectancies
    lhg, lag = lg["home_goals"] or 1.4, lg["away_goals"] or 1.1
    h_att = hs["gf"] / lhg if lhg else 1.0
    h_def = hs["ga"] / lag if lag else 1.0
    a_att = as_["gf"] / lag if lag else 1.0
    a_def = as_["ga"] / lhg if lhg else 1.0
    lh = max(0.2, lhg * h_att * a_def)
    la = max(0.2, lag * a_att * h_def)

    matrix = _dc_matrix(lh, la)
    goals = _dc_markets(matrix)

    # Corners + cards via Poisson on combined recent averages
    corners_lam = hs["cf"] + as_["cf"]
    cards_lam = hs["cards"] + as_["cards"]
    corners = {ln: round(_poisson_over(corners_lam, ln), 3) for ln in (8.5, 9.5, 10.5)}
    cards = {ln: round(_poisson_over(cards_lam, ln), 3) for ln in (3.5, 4.5, 5.5)}

    out = {
        "home": home, "away": away, "code": code, "season": season,
        "exp_goals": {"home": round(lh, 2), "away": round(la, 2)},
        "result": {"home": goals["home"], "draw": goals["draw"], "away": goals["away"]},
        "over": goals["over"], "under": goals["under"],
        "btts": {"yes": goals["btts_yes"], "no": goals["btts_no"]},
        "correct_score": {"score": goals["correct_score"], "prob": goals["cs_prob"]},
        "corners_over": corners, "corners_exp": round(corners_lam, 1),
        "cards_over": cards, "cards_exp": round(cards_lam, 1),
        "games_used": {"home": hs["games"], "away": as_["games"]},
    }

    # Value flags vs supplied odds (model thinks the price is generous)
    if odds:
        try:
            oh, od, oa = float(odds["home"]), float(odds["draw"]), float(odds["away"])
            ph, pd, pa = 1 / oh, 1 / od, 1 / oa
            s = ph + pd + pa
            fair = {"home": ph / s, "draw": pd / s, "away": pa / s}
            val = {}
            for k in ("home", "draw", "away"):
                edge = out["result"][k] - fair[k]
                if edge > 0.05:   # model 5pp+ above fair → flag value
                    val[k] = {"edge": round(edge, 3), "odds": odds[k]}
            out["value"] = val
        except Exception:
            pass
    return out


# Spelling-tolerant ClubElo strength cross-check (optional, supplementary)
def _ce_day_ratings(date_str=None):
    import datetime as _dtm
    d = date_str or _dtm.date.today().isoformat()
    if d in _CE_CACHE:
        return _CE_CACHE[d]
    table = {}
    try:
        import csv as _csv
        import io as _io
        r = _req.get("http://api.clubelo.com/{}".format(d), timeout=20)
        if r.status_code == 200 and r.text:
            rd = _csv.DictReader(_io.StringIO(r.text))
            for row in rd:
                club = row.get("Club")
                elo = row.get("Elo")
                if club and elo:
                    try:
                        table[_fd_norm(club)] = float(elo)
                    except ValueError:
                        pass
    except Exception as e:
        print("[MODEL] ClubElo fetch error: {}".format(e))
    _CE_CACHE[d] = table
    return table


def _ce_elo(name, table=None):
    table = table if table is not None else _ce_day_ratings()
    if not table or not name:
        return None
    k = _fd_norm(name)
    if k in table:
        return table[k]
    for tk, v in table.items():
        if k and (k in tk or tk in k):
            return v
    return None


# ── Model extras wiring (additive, isolated) ──────────────────────────────
_MODEL_LEAGUES = ["E0", "E1", "SP1", "SP2", "I1", "I2", "D1", "D2",
                  "F1", "F2", "B1", "N1", "P1", "SC0", "T1", "G1"]


def _model_season():
    """football-data season code for today, e.g. '2526' for 2025/26."""
    n = _dt.date.today()
    sy = n.year if n.month >= 8 else n.year - 1
    return "{:02d}{:02d}".format(sy % 100, (sy + 1) % 100)


def _model_detect_league(home, away, season):
    """Find which covered league CSV contains BOTH teams. Auto-detect — no
    manual league mapping. Returns code, or None when not a covered club
    league (e.g. internationals)."""
    for code in _MODEL_LEAGUES:
        rows = _fd_league(code, season)
        if not rows:
            continue
        has_h = any(_fd_match_name(home, r.get("HomeTeam", "")) or
                    _fd_match_name(home, r.get("AwayTeam", "")) for r in rows)
        if not has_h:
            continue
        has_a = any(_fd_match_name(away, r.get("HomeTeam", "")) or
                    _fd_match_name(away, r.get("AwayTeam", "")) for r in rows)
        if has_a:
            return code
    return None


def _best_line(over_dict):
    """Pick the highest-confidence Over/Under side across all lines.
    Returns (line_str, side, prob) or None."""
    best = None
    for ln, op in (over_dict or {}).items():
        try:
            op = float(op)
        except (TypeError, ValueError):
            continue
        for side, p in (("Over", op), ("Under", 1.0 - op)):
            if best is None or p > best[2]:
                best = (ln, side, p)
    return best


def _model_card(m):
    """Concise Telegram card for one club match's model extras."""
    home, away = m.get("home", "Home"), m.get("away", "Away")
    res = m.get("result", {})
    fav = max((("home", res.get("home", 0)), ("draw", res.get("draw", 0)),
               ("away", res.get("away", 0))), key=lambda x: x[1])
    favlbl = {"home": home + " win", "draw": "Draw",
              "away": away + " win"}[fav[0]]
    cs = m.get("correct_score", {})
    ov = m.get("over", {})
    btts = m.get("btts", {})
    cob = _best_line(m.get("corners_over", {}))
    cab = _best_line(m.get("cards_over", {}))
    gu = m.get("games_used", {})
    lines = ["📊 MODEL EXTRAS — {} vs {}".format(home, away),
             "(model estimate · your call)", ""]
    lines.append("Result: {} ({:.0f}%)".format(favlbl, fav[1] * 100))
    if cs.get("score"):
        lines.append("Likely score: {} ({:.0f}%)".format(
            cs["score"], cs.get("prob", 0) * 100))
    if ov.get("2.5") is not None:
        lines.append("Over 2.5 goals: {:.0f}%".format(ov["2.5"] * 100))
    if btts.get("yes") is not None:
        lines.append("BTTS: Yes {:.0f}%".format(btts["yes"] * 100))
    if cob:
        lines.append("Corners: {} {} ({:.0f}%)".format(cob[1], cob[0],
                                                        cob[2] * 100))
    if cab:
        lines.append("Cards: {} {} ({:.0f}%)".format(cab[1], cab[0],
                                                      cab[2] * 100))
    lines += ["", "Form: {} games each".format(
        min(gu.get("home", 0) or 0, gu.get("away", 0) or 0))]
    return "\n".join(lines)


def _model_code_legs(fx, m, threshold=0.70):
    """Read a fixture's cached SportyBet board, find the corners/cards markets,
    compute the model's win chance for each offered line, and return the single
    safest qualifying leg for this match (one leg per event keeps the
    accumulator clean). Each leg carries the exact SportyBet IDs.
    Returns a list of (prob, selection, description) — 0 or 1 item."""
    import re as _re
    eid = fx.get("sb_event_id")
    if not eid:
        return []
    markets = _SB_MARKET_CACHE.get(eid) or []
    if not markets:
        return []
    corners_lam = m.get("corners_exp")
    cards_lam = m.get("cards_exp")
    best = None  # (prob, selection, desc)
    for market in markets:
        name = (market.get("desc") or market.get("name")
                or market.get("marketName") or "").lower().strip()
        spec = str(market.get("specifier") or "")
        is_corner = "corner" in name
        is_card = (("card" in name or "booking" in name) and not is_corner)
        if not (is_corner or is_card):
            continue
        lam = corners_lam if is_corner else cards_lam
        if not lam:
            continue
        mm = _re.search(r"total=(\d+\.?\d*)", spec) or _re.search(r"(\d+\.5)", name)
        if not mm:
            continue
        try:
            line = float(mm.group(1))
        except (ValueError, TypeError):
            continue
        p_over = _poisson_over(lam, line)
        if p_over >= 0.5:
            side, prob, want = "Over", p_over, "over"
        else:
            side, prob, want = "Under", 1.0 - p_over, "under"
        if prob < threshold:
            continue
        oid = None
        for oc in (market.get("outcomes") or market.get("outcome") or []):
            od = (oc.get("desc") or oc.get("name") or "").strip().lower()
            if want == "over" and ("over" in od or od.startswith("o ") or od == "o"):
                oid = str(oc.get("id", "")); break
            if want == "under" and ("under" in od or od.startswith("u ") or od == "u"):
                oid = str(oc.get("id", "")); break
        if not oid:
            continue
        sel = {"eventId": eid, "marketId": str(market.get("id", "")),
               "specifier": spec or None, "outcomeId": oid}
        desc = "{} v {} — {} {} {} ({:.0f}%)".format(
            m.get("home", "?"), m.get("away", "?"),
            "corners" if is_corner else "cards", side, line, prob * 100)
        if best is None or prob > best[0]:
            best = (prob, sel, desc)
    return [best] if best else []


def _model_attach(fixtures):
    """Pre-scoring: detect each club fixture's league, run the proven model and
    attach it as fx['_model'] so board-explore can price corners/cards from it.
    No Telegram, no codes — just attaches. Internationals/cross-league get no
    match and are skipped. Fully isolated."""
    season = _model_season()
    n = 0
    for fx in fixtures:
        try:
            if fx.get("_model"):
                continue
            home = fx.get("home_team", "")
            away = fx.get("away_team", "")
            if not home or not away:
                continue
            code = _model_detect_league(home, away, season)
            if not code:
                continue
            m = model_club_match(home, away, code, season)
            if m:
                fx["_model"] = m
                n += 1
        except Exception as e:
            print("[MODEL] attach error for {}: {}".format(
                fx.get("home_team", "?"), e))
    print("[MODEL] attached model to {} club fixtures (pre-scoring)".format(n))
    return n


def _model_extras_run(fixtures, announce=True):
    """Additive: for each covered club fixture, send a MODEL EXTRAS Telegram
    card from the already-attached model. Isolated — never affects picks/codes."""
    season = _model_season()
    extras = []
    for fx in fixtures:
        try:
            home = fx.get("home_team", "")
            away = fx.get("away_team", "")
            if not home or not away:
                continue
            m = fx.get("_model")
            if not m:
                code = _model_detect_league(home, away, season)
                if not code:
                    continue  # not a covered club league (e.g. international)
                m = model_club_match(home, away, code, season)
                if not m:
                    continue
                fx["_model"] = m
            extras.append(m)
            if announce:
                try:
                    send_telegram(_model_card(m))
                except Exception as e:
                    print("[MODEL] telegram error: {}".format(e))
        except Exception as e:
            print("[MODEL] extras error for {}: {}".format(
                fx.get("home_team", "?"), e))
            continue
    _FB_CACHE["model_extras"] = extras
    print("[MODEL] extras: {} club matches enriched (season {})".format(
        len(extras), season))
    return extras


def footystats_team(team_slug):
    """
    Scrape FootyStats team page for corners/cards/btts stats.
    Returns dict of stats or empty dict on failure.
    NOTE: FootyStats slugs are unpredictable; this is best-effort.
    """
    out = {}
    if _BS is None:
        return out
    html = _get_html("https://footystats.org/clubs/{}".format(team_slug))
    if not html:
        return out
    try:
        soup = _BS(html, "html.parser")
        text = soup.get_text().lower()
        # Best-effort extraction — FootyStats layout varies
        # This is a placeholder structure; refined after Railway inspection
        import re
        btts_m = re.search(r"btts[^\d]*(\d+)%", text)
        if btts_m:
            out["btts_pct"] = float(btts_m.group(1))
    except Exception:
        pass
    return out


# ═══════════════════════════════════════════════════════════════════
# AGGREGATOR — combine all sources into one fixture record
# ═══════════════════════════════════════════════════════════════════

def build_fixture_dataset(date_str=None, rate_limit=1.5, max_fixtures=30):
    """
    Master function: scrape everything and return enriched fixture dicts
    ready for the analysis engine.

    Rate-limited to be respectful to Sofascore (Cloudflare).
    """
    fixtures = sofa_todays_fixtures(date_str)
    if not fixtures:
        print("[FB] No fixtures found for {}".format(date_str or "today"))
        return []

    fixtures = fixtures[:max_fixtures]
    print("[FB] Found {} fixtures, enriching...".format(len(fixtures)))

    # Pre-fetch Understat xG per league (one call per league)
    xg_cache = {}
    leagues_present = set(f["league"] for f in fixtures)
    for lg in leagues_present:
        if lg in UNDERSTAT_LEAGUES:
            xg_cache[lg] = understat_team_xg(lg)
            time.sleep(rate_limit)

    # Pre-fetch FBref Big-5 possession/progressive once (covers all top-5 teams)
    fbref_stats = fbref_big5_possession()

    enriched = []
    for fx in fixtures:
        try:
            # Form (2 calls)
            fx["home_form"] = sofa_team_form(fx.get("home_id"))
            time.sleep(rate_limit)
            fx["away_form"] = sofa_team_form(fx.get("away_id"))
            time.sleep(rate_limit)

            # Granular last-10 stats (over1.5%, under3.5%, clean-sheet%, avg goals,
            # ppg) — same events feed, drives the over/under + ranking rules.
            fx["home_form_stats"] = sofa_team_form_stats(fx.get("home_id"))
            time.sleep(rate_limit)
            fx["away_form_stats"] = sofa_team_form_stats(fx.get("away_id"))
            time.sleep(rate_limit)

            # Injuries (2 calls)
            fx["home_key_injuries"] = sofa_injuries(fx.get("home_id"))
            fx["away_key_injuries"] = sofa_injuries(fx.get("away_id"))
            time.sleep(rate_limit)

            # xG from Understat cache (name matching)
            lg_xg = xg_cache.get(fx["league"], {})
            home_xg = _match_team_xg(lg_xg, fx["home_team"])
            away_xg = _match_team_xg(lg_xg, fx["away_team"])
            if home_xg:
                fx["home_xg_for"] = home_xg["xg_for"]
                fx["home_xg_against"] = home_xg["xg_against"]
                if home_xg.get("ppda") is not None:
                    fx["home_ppda"] = home_xg["ppda"]
            if away_xg:
                fx["away_xg_for"] = away_xg["xg_for"]
                fx["away_xg_against"] = away_xg["xg_against"]
                if away_xg.get("ppda") is not None:
                    fx["away_ppda"] = away_xg["ppda"]

            # FBref possession / progressive play (Opta-sourced style signal)
            hf = fbref_lookup(fbref_stats, fx["home_team"])
            af = fbref_lookup(fbref_stats, fx["away_team"])
            if hf and hf.get("possession") is not None:
                fx["home_possession"] = hf["possession"]
                fx["home_prog"] = hf.get("prog_passes")
            if af and af.get("possession") is not None:
                fx["away_possession"] = af["possession"]
                fx["away_prog"] = af.get("prog_passes")

            enriched.append(fx)
        except Exception as e:
            print("[FB] enrich error for {}: {}".format(fx.get("home_team"), e))
            enriched.append(fx)  # keep it with whatever data we have

    print("[FB] Enriched {} fixtures".format(len(enriched)))
    return enriched


def _match_team_xg(xg_dict, team_name):
    """Fuzzy-match a team name to Understat data (names differ slightly)."""
    if not xg_dict or not team_name:
        return None
    # Exact
    if team_name in xg_dict:
        return xg_dict[team_name]
    # Partial — match on last word or substring
    tn_lower = team_name.lower()
    for name, data in xg_dict.items():
        nl = name.lower()
        if nl in tn_lower or tn_lower in nl:
            return data
        # Match on significant word overlap
        tn_words = set(tn_lower.replace("fc", "").replace("afc", "").split())
        n_words = set(nl.replace("fc", "").replace("afc", "").split())
        if tn_words & n_words:
            return data
    return None


"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — SPORTYBET BOOKING CODE GENERATOR
═══════════════════════════════════════════════════════════════════
Confirmed endpoints (from sacsbrainz/betconverter source):

  READ a code:
    GET https://www.sportybet.com/api/ng/orders/share/{CODE}
    -> {message:"success", data:{outcomes:[{eventId, markets:[{id, specifier, outcomes:[{id}]}]}]}}

  CREATE a code:
    POST https://www.sportybet.com/api/ng/orders/share
    body: {"selections":[{eventId, marketId, specifier, outcomeId}, ...]}
    -> {message:"success", data:{code:"A7K2M9"}}

To map analysis picks -> SportyBet IDs, we:
  1. Search SportyBet fixtures for the match (by team name)
  2. Pull that event's markets
  3. Match our pick to the right market+outcome by description
  4. Build selections and POST

NOTE: cannot be tested from build sandbox (network restricted).
Written from confirmed endpoint shapes. Needs Railway validation.
Every step degrades gracefully — an unmappable pick is skipped, not fatal.
═══════════════════════════════════════════════════════════════════
"""

import time
import json

try:
    import requests as _req
except ImportError:
    _req = None

SB_BASE = "https://www.sportybet.com/api/ng"

_SB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Referer": "https://www.sportybet.com/ng/sport/football",
}


_SB_DIAG_COUNT = [0]
_SB_DIAG_MAX = 14


def _sb_get(url, timeout=12, diag=False):
    if _req is None:
        return None
    try:
        r = _req.get(url, headers=_SB_HEADERS, timeout=timeout)
        if diag and _SB_DIAG_COUNT[0] < _SB_DIAG_MAX:
            _SB_DIAG_COUNT[0] += 1
            body = (r.text or "")[:280].replace("\n", " ").replace("\r", "")
            print("[SB-DIAG] GET {} -> HTTP {} | {}".format(url[:95], r.status_code, body))
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return None
    except Exception as e:
        if diag and _SB_DIAG_COUNT[0] < _SB_DIAG_MAX:
            _SB_DIAG_COUNT[0] += 1
            print("[SB-DIAG] GET {} -> EXCEPTION {}".format(url[:95], e))
    return None


def _sb_post(url, payload, timeout=15, diag=False):
    if _req is None:
        return None
    try:
        r = _req.post(url, headers=_SB_HEADERS, json=payload, timeout=timeout)
        if diag:
            body = (r.text or "")[:300].replace("\n", " ").replace("\r", "")
            print("[SB-DIAG] POST {} -> HTTP {} | payload={} | resp={}".format(
                url[:60], r.status_code, json.dumps(payload)[:200], body))
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        if diag:
            print("[SB-DIAG] POST {} -> EXCEPTION {}".format(url[:60], e))
    return None


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Find SportyBet event for a fixture
# ═══════════════════════════════════════════════════════════════════

_SB_MARKET_CACHE = {}  # eventId -> markets (captured from search results)
_SB_STRUCT_LOGGED = [False]
_SB_EVENT_INFO = {}     # eventId -> {kickoff_ts, match_status, scores, teams}


def sb_search_event(home_team, away_team):
    """
    Search SportyBet for a match by team name. Returns eventId or None.
    Uses the confirmed football.com/SportyBet shared backend path
    'factsCenter/event/firstSearch'. Captures inline markets when present.
    """
    import urllib.parse as _up
    ts = int(time.time() * 1000)
    kwq = _up.quote(home_team.strip())
    candidates = [
        "{}/factsCenter/event/firstSearch?keyword={}&offset=0&pageSize=20&_t={}".format(SB_BASE, kwq, ts),
        "{}/factsCenter/query/frontend/search/event/page/v2?keyword={}&sportId=sr:sport:1&_t={}".format(SB_BASE, kwq, ts),
    ]
    for url in candidates:
        data = _sb_get(url, diag=True)
        if not data:
            continue
        try:
            events = _extract_events_from_search(data)
            for ev in events:
                ev_home = (ev.get("homeTeamName") or ev.get("home")
                           or ev.get("homeTeam") or "").lower()
                ev_away = (ev.get("awayTeamName") or ev.get("away")
                           or ev.get("awayTeam") or "").lower()
                if _team_match(home_team, ev_home) and _team_match(away_team, ev_away):
                    if not _sb_is_real_soccer(ev):
                        _sp = ev.get("sport")
                        _sid = (ev.get("sportId") or (_sp.get("id") if isinstance(_sp, dict) else _sp) or "?")
                        print("[SB] skip non-football event for {} vs {} (sport={})".format(
                            home_team, away_team, _sid))
                        continue
                    eid = ev.get("eventId") or ev.get("id")
                    # Capture kickoff time + status for display and settlement
                    try:
                        _SB_EVENT_INFO[eid] = {
                            "kickoff_ts": ev.get("estimateStartTime") or ev.get("startTime") or 0,
                            "match_status": ev.get("matchStatus") or "",
                            "status": ev.get("status"),
                            "home_score": ev.get("homeScore") or ev.get("setScore"),
                            "away_score": ev.get("awayScore"),
                            "home": ev.get("homeTeamName"), "away": ev.get("awayTeamName"),
                        }
                    except Exception:
                        pass
                    # Capture inline markets so we don't need a second call
                    mkts = ev.get("markets") or ev.get("marketList") or []
                    if mkts:
                        _SB_MARKET_CACHE[eid] = mkts
                        # One-time dump of UNIQUE market types (to map the
                        # ones still missing: double chance, corners, cards)
                        if not _SB_STRUCT_LOGGED[0]:
                            _SB_STRUCT_LOGGED[0] = True
                            seen_desc = set()
                            for mk in mkts:
                                desc = (mk.get("desc") or mk.get("name") or mk.get("marketName") or "")
                                if desc in seen_desc:
                                    continue
                                seen_desc.add(desc)
                                ocs = mk.get("outcomes") or mk.get("outcome") or []
                                oc_str = " ; ".join("{}={}".format(
                                    o.get("id"), (o.get("desc") or o.get("name") or "")) for o in ocs[:4])
                                print("[SB-STRUCT] id={} desc='{}' spec='{}' | {}".format(
                                    mk.get("id"), desc, mk.get("specifier", ""), oc_str))
                    print("[SB] matched {} vs {} -> {} ({} inline markets)".format(
                        home_team, away_team, eid, len(mkts)))
                    return eid
            if events:
                break  # endpoint works, team just not listed
        except Exception as e:
            print("[SB] search parse error: {}".format(e))
    return None


def _extract_events_from_search(data):
    """Recursively pull event dicts (with eventId + team names) from any shape.
    Propagates sport/tournament context DOWN so a nested event that lacks its own
    sportId/tournament still carries its parent's — essential for filtering SRL /
    eSoccer, whose markers often sit on a parent grouping, not the event itself."""
    found = []
    _ctx_keys = ("sportId", "sport", "sportName", "tournamentName",
                 "categoryName", "category", "tournament", "leagueName")

    def walk(obj, depth=0, ctx=None):
        if depth > 7:
            return
        ctx = ctx or {}
        if isinstance(obj, dict):
            newctx = dict(ctx)
            for k in _ctx_keys:
                v = obj.get(k)
                if v and not isinstance(v, (dict, list)):
                    newctx[k] = v
            if (obj.get("eventId") or obj.get("id")) and \
               (obj.get("homeTeamName") or obj.get("home") or obj.get("homeTeam")):
                merged = dict(obj)
                for k, v in newctx.items():
                    merged.setdefault(k, v)  # event's own fields win; inherit gaps
                found.append(merged)
            for v in obj.values():
                walk(v, depth + 1, newctx)
        elif isinstance(obj, list):
            for it in obj:
                walk(it, depth + 1, ctx)

    walk(data.get("data", data))
    return found


def _sb_is_real_soccer(ev):
    """Reject simulated / eSoccer / non-football events that SportyBet's search
    returns alongside real fixtures. Two distinct fakes use real club names:

      • eSoccer (FIFA-style): sportId sr:sport:202120001, long 15-digit match IDs.
      • SRL (Simulated Reality League): Sportradar virtual matches that run during
        off-seasons under the NORMAL sr:sport:1 sport with 8-digit IDs — the tell
        is 'Simulated Reality' in the competition name or an 'SRL' suffix on team
        names ('Manchester United SRL', 'Premier League SRL').

    SportyBet nests sport/category/tournament as DICTS (sport.category.tournament),
    so we descend into them rather than stringifying — and we FAIL OPEN: anything
    that isn't positively identified as fake is treated as real, so a missing field
    never blocks a genuine fixture."""
    # ── resolve sportId + competition name from the (possibly nested) structure ──
    sid = str(ev.get("sportId") or "")
    comp = ""              # competition/tournament/category names
    sport = ev.get("sport")
    if isinstance(sport, dict):
        sid = sid or str(sport.get("id") or sport.get("sportId") or "")
        cat = sport.get("category") if isinstance(sport.get("category"), dict) else {}
        comp += " " + str(cat.get("name") or "")
        tour = cat.get("tournament") if isinstance(cat.get("tournament"), dict) else {}
        comp += " " + str(tour.get("name") or "")
    elif isinstance(sport, str):
        sid = sid or sport
    for k in ("tournamentName", "categoryName", "tournament", "category",
              "leagueName", "name"):
        v = ev.get(k)
        if isinstance(v, str):
            comp += " " + v
        elif isinstance(v, dict):
            comp += " " + str(v.get("name") or "")

    # ── sportId checks (only reject KNOWN-bad; unknown/blank => keep) ──
    if "202120001" in sid:                       # eSoccer / virtual
        return False
    if sid.startswith("sr:sport:") and not sid.endswith(":1"):
        return False                             # a different real sport entirely

    # ── name markers (string team names + resolved competition name) ──
    names = []
    for k in ("homeTeamName", "awayTeamName"):
        v = ev.get(k)
        if isinstance(v, str):
            names.append(v)
        elif isinstance(v, dict):
            names.append(str(v.get("name") or ""))
    blob = (" ".join(names) + " " + comp).lower()
    toks = set(blob.replace(".", " ").replace("-", " ").split())
    if "srl" in toks:                            # 'SRL' suffix on team/league name
        return False
    markers = ("esoccer", "e-soccer", "esport", "cyber", "simulated reality",
               "simulated", "mins play", "min play", "gg league", "ggleague",
               "(srl)")
    return not any(m in blob for m in markers)


def _team_match(name, candidate):
    """Fuzzy team name match."""
    if not name or not candidate:
        return False
    n = name.lower().replace("fc", "").replace("afc", "").strip()
    c = candidate.lower().replace("fc", "").replace("afc", "").strip()
    if n in c or c in n:
        return True
    n_words = set(n.split())
    c_words = set(c.split())
    # Guard against same-city different-club matches (United vs City, etc.)
    _suffix = {"united", "city", "town", "rovers", "wanderers", "albion",
               "hotspur", "county", "athletic", "wednesday"}
    n_suf, c_suf = n_words & _suffix, c_words & _suffix
    if n_suf and c_suf and not (n_suf & c_suf):
        return False
    return bool(n_words & c_words)


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Get markets for an event, match our pick
# ═══════════════════════════════════════════════════════════════════

def sb_get_event_markets(event_id):
    """Return markets for a SportyBet event — inline-cached first, else fetch."""
    if not event_id:
        return []
    # Markets captured inline during search?
    if event_id in _SB_MARKET_CACHE:
        return _SB_MARKET_CACHE[event_id]
    ts = int(time.time() * 1000)
    candidates = [
        "{}/factsCenter/event?eventId={}&productId=3&_t={}".format(SB_BASE, event_id, ts),
        "{}/factsCenter/query/frontend/match/detail?eventId={}&_t={}".format(SB_BASE, event_id, ts),
        "{}/factsCenter/wapEvent?eventId={}&_t={}".format(SB_BASE, event_id, ts),
    ]
    for url in candidates:
        data = _sb_get(url, diag=True)
        if not data:
            continue
        try:
            d = data.get("data", data)
            markets = []
            if isinstance(d, dict):
                markets = d.get("markets") or d.get("marketList") or []
                # Sometimes nested under event
                if not markets and isinstance(d.get("event"), dict):
                    markets = d["event"].get("markets", [])
            if markets:
                return markets
        except Exception:
            pass
    return []


# Map engine market_type -> matching logic against SportyBet market descriptions
# Each entry: (market_name_keywords, outcome_matcher_function)
def _outcome_matches_home(desc, home, away):
    return _team_match(home, desc) or desc.strip() in ("1", "home")

def _outcome_matches_away(desc, home, away):
    return _team_match(away, desc) or desc.strip() in ("2", "away")

def _outcome_matches_draw(desc, home, away):
    return "draw" in desc.lower() or desc.strip().upper() == "X"


# Mapping: engine market_type -> (sb_market_name_keywords, specifier_value, outcome_desc_matcher)
SB_MARKET_MAP = {
    "home_win":            (["1x2"], None, "home"),
    "away_win":            (["1x2"], None, "away"),
    "draw":                (["1x2"], None, "draw"),
    "double_chance_1X":    (["double chance"], None, "1X"),
    "double_chance_X2":    (["double chance"], None, "X2"),
    "dnb_home":            (["draw no bet"], None, "home"),
    "dnb_away":            (["draw no bet"], None, "away"),
    "over_0.5":            (["over/under"], "0.5", "over"),
    "over_1.5":            (["over/under"], "1.5", "over"),
    "over_2.5":            (["over/under"], "2.5", "over"),
    "over_3.5":            (["over/under"], "3.5", "over"),
    "over_4.5":            (["over/under"], "4.5", "over"),
    "over_5.5":            (["over/under"], "5.5", "over"),
    "under_1.5":           (["over/under"], "1.5", "under"),
    "under_2.5":           (["over/under"], "2.5", "under"),
    "under_3.5":           (["over/under"], "3.5", "under"),
    "under_4.5":           (["over/under"], "4.5", "under"),
    "btts_yes":            (["gg/ng", "both teams to score"], None, "yes"),
    "btts_no":             (["gg/ng", "both teams to score"], None, "no"),
    "corners_over_7.5":    (["corner"], "7.5", "over"),
    "corners_over_8.5":    (["corner"], "8.5", "over"),
    "corners_over_9.5":    (["corner"], "9.5", "over"),
    "cards_over_2.5":      (["card", "booking"], "2.5", "over"),
    "cards_over_3.5":      (["card", "booking"], "3.5", "over"),
}


def sb_map_pick_to_selection(pick, markets):
    """
    Given an engine pick and the event's markets, find the matching
    SportyBet marketId + specifier + outcomeId.
    Handles SportyBet's standard outcome labels (Home/Draw/Away, 1/X/2,
    Over/Under, Yes/No) as well as full team names.
    """
    mt = pick["market_type"]
    home = pick["home"]
    away = pick["away"]

    # Board-explored picks already carry the exact SportyBet IDs they were
    # read from (no name-matching needed, no decoy risk). Trust them directly.
    if pick.get("sb_market_id") and pick.get("sb_outcome_id"):
        return {
            "eventId": pick.get("sb_event_id", ""),
            "marketId": str(pick["sb_market_id"]),
            "specifier": pick.get("sb_specifier") or None,
            "outcomeId": str(pick["sb_outcome_id"]),
        }

    mapping = SB_MARKET_MAP.get(mt)
    if not mapping:
        return None
    name_keywords, want_specifier, outcome_kind = mapping

    def is_home(d):
        d = d.strip().lower()
        return _team_match(home, d) or d in ("home", "1", "{} (home)".format(home.lower()))
    def is_away(d):
        d = d.strip().lower()
        return _team_match(away, d) or d in ("away", "2", "{} (away)".format(away.lower()))
    def is_draw(d):
        d = d.strip().lower()
        return d in ("draw", "x", "tie") or "draw" in d

    # Corners/cards markets use substring matching (names vary, e.g. "Total
    # Corners"); the core markets require an EXACT name to avoid grabbing
    # variant markets like "Monza Over/Under" or "1st Half - 1X2".
    use_substring = mt.startswith("corners") or mt.startswith("cards")

    for market in markets:
        m_name = (market.get("desc") or market.get("name")
                  or market.get("marketName") or "").lower().strip()
        m_specifier = market.get("specifier") or ""

        if use_substring:
            if not any(kw in m_name for kw in name_keywords):
                continue
        else:
            if m_name not in name_keywords:
                continue
        if want_specifier and want_specifier not in str(m_specifier) \
           and want_specifier not in m_name:
            continue

        outcomes = market.get("outcomes") or market.get("outcome") or []
        for oc in outcomes:
            oc_desc = (oc.get("desc") or oc.get("name") or "").strip()
            od = oc_desc.lower()
            matched = False
            if outcome_kind == "home":
                matched = is_home(od)
            elif outcome_kind == "away":
                matched = is_away(od)
            elif outcome_kind == "draw":
                matched = is_draw(od)
            elif outcome_kind == "1X":
                # SportyBet: "Home or Draw" (id 9)
                nd = od.replace(" ", "").replace("/", "")
                matched = ("draw" in od and ("home" in od or is_home(od))) or \
                          nd in ("1x", "1ordraw", "homeordraw")
            elif outcome_kind == "X2":
                # SportyBet: "Draw or Away" (id 11)
                nd = od.replace(" ", "").replace("/", "")
                matched = ("draw" in od and ("away" in od or is_away(od))) or \
                          nd in ("x2", "2ordraw", "awayordraw", "draworaway")
            elif outcome_kind == "over":
                matched = "over" in od or od.startswith("o ") or od == "o"
            elif outcome_kind == "under":
                matched = "under" in od or od.startswith("u ") or od == "u"
            elif outcome_kind == "yes":
                matched = od in ("yes", "gg") or "yes" in od
            elif outcome_kind == "no":
                matched = od in ("no", "ng") or od == "no"

            if matched:
                return {
                    "eventId": pick.get("sb_event_id", ""),
                    "marketId": str(market.get("id", "")),
                    "specifier": m_specifier if m_specifier else None,
                    "outcomeId": str(oc.get("id", "")),
                }
    return None


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Create a booking code from selections
# ═══════════════════════════════════════════════════════════════════

def sb_create_code(selections):
    """
    POST selections to SportyBet, return booking code or None.
    selections = [{eventId, marketId, specifier, outcomeId}, ...]
    """
    if not selections:
        return None
    url = "{}/orders/share".format(SB_BASE)
    resp = _sb_post(url, {"selections": selections}, diag=True)
    if not resp:
        return None
    # SportyBet success: message="Success" / bizCode=10000, code lives in data.shareCode
    ok = str(resp.get("message", "")).lower() == "success" or resp.get("bizCode") == 10000
    if ok:
        data = resp.get("data", {}) or {}
        return data.get("shareCode") or data.get("code") or data.get("shareURL", "").split("shareCode=")[-1] or None
    return None


def sb_create_bet_builder(event_id, selections):
    """Attempt to create a SAME-MATCH bet-builder code on SportyBet and learn the
    real mechanism empirically (their bet-builder endpoint isn't documented).

    A bet builder differs from an accumulator: all legs are on ONE event and the
    combined price is correlation-adjusted server-side. We try the known share
    endpoint first (it may auto-detect same-event legs as a builder), then a small
    set of likely bet-builder endpoints, logging each raw response verbatim so a
    live deploy reveals exactly what SportyBet expects. Returns
    (code, combined_odds) on success, else (None, None)."""
    if not event_id or len(selections) < 2:
        return None, None
    # ensure every leg is the same event (bet builders are single-match)
    sels = [dict(s, eventId=event_id) for s in selections]

    # strategy 1: plain share with same-event legs (does it auto-build?)
    attempts = [
        ("share", "{}/orders/share".format(SB_BASE), {"selections": sels}),
        # strategy 2: share with an explicit bet-builder grouping flag
        ("share+flag", "{}/orders/share".format(SB_BASE),
         {"selections": [dict(s, parentBetBuilderMarketId="1") for s in sels],
          "betBuilder": True}),
        # strategy 3: a dedicated bet-builder calc endpoint (guess, may 404)
        ("bb-calc", "{}/factsCenter/calculateOdds".format(SB_BASE),
         {"selections": sels, "betBuilder": True}),
    ]
    for name, url, payload in attempts:
        resp = _sb_post(url, payload, diag=True)
        if not resp:
            print("[SB-BB] {} -> no response".format(name))
            continue
        # log the raw shape so we can see what SportyBet actually returns
        print("[SB-BB] {} -> bizCode={} msg={} keys={}".format(
            name, resp.get("bizCode"), str(resp.get("message"))[:40],
            list((resp.get("data") or {}).keys())[:8]))
        ok = str(resp.get("message", "")).lower() == "success" or resp.get("bizCode") == 10000
        if ok:
            data = resp.get("data", {}) or {}
            code = (data.get("shareCode") or data.get("code")
                    or data.get("shareURL", "").split("shareCode=")[-1] or None)
            odds = data.get("totalOdds") or data.get("odds") or data.get("price")
            if code:
                # CONFIRM: decode the code and verify every leg is the SAME event
                # (that's what makes it a bet builder, not a normal accumulator).
                confirmed = None
                try:
                    decoded = sb_decode_code(code)
                    if isinstance(decoded, list) and decoded:
                        eids = {str(s.get("eventId") or s.get("event_id") or "")
                                for s in decoded}
                        confirmed = (len(eids) == 1 and event_id in eids)
                        print("[SB-BB] decode {} -> {} legs, {} event(s) -> {}".format(
                            code, len(decoded), len(eids),
                            "CONFIRMED bet builder" if confirmed else "NOT same-match"))
                except Exception as e:
                    print("[SB-BB] decode check failed: {}".format(e))
                print("[SB-BB] SUCCESS via {} -> code={} odds={} confirmed={}".format(
                    name, code, odds, confirmed))
                if confirmed is not False:   # accept True or unknown; reject only proven-wrong
                    return code, odds
    return None, None


def _sb_build_bet_builders(build_picks, max_matches=3, min_board=38):
    """Premium-match bet builders: for the richest-board matches, let the engine
    pick the 3 safest correlated legs (highest-confidence, one per market family)
    and attempt a single SportyBet bet-builder code for each."""
    def _fam(mt):
        if mt.startswith("double_chance") or mt.startswith("dc1up"):
            return "dc"
        if mt in ("home_win", "away_win", "dnb_home", "dnb_away") or mt.startswith("oneup"):
            return "win"
        if mt.startswith("over_") or mt.startswith("under_"):
            return "ou"
        if mt.startswith("corners"):
            return "corners"
        if mt.startswith("btts"):
            return "btts"
        if mt.startswith("tt_") or "team" in mt:
            return "tt"
        return mt

    by_event = {}
    for p in build_picks:
        eid = p.get("sb_event_id")
        if eid:
            by_event.setdefault(eid, []).append(p)

    ranked = []
    for eid, ps in by_event.items():
        n = len(_SB_MARKET_CACHE.get(eid, []))
        if n >= min_board:
            ranked.append((n, eid, ps))
    ranked.sort(key=lambda t: -t[0])

    builders = []
    for n, eid, ps in ranked[:max_matches]:
        mkts = _SB_MARKET_CACHE.get(eid, [])
        legs, seen = [], set()
        for p in sorted(ps, key=lambda x: -x.get("confidence", 0)):
            f = _fam(p.get("market_type", ""))
            if f in seen:
                continue
            sel = sb_map_pick_to_selection(p, mkts)
            if not sel:
                continue
            seen.add(f)
            legs.append({"pick": p.get("pick"), "conf": p.get("confidence"),
                         "odds": p.get("odds"), "sel": sel,
                         "mt": p.get("market_type")})
            if len(legs) >= 3:
                break
        if len(legs) < 3:
            continue
        est = 1.0
        for l in legs:
            try:
                est *= float(l["odds"] or 1)
            except (ValueError, TypeError):
                pass
        code, sb_odds = sb_create_bet_builder(eid, [l["sel"] for l in legs])
        builders.append({
            "event_id": eid, "markets": n,
            "home": ps[0].get("home", ""), "away": ps[0].get("away", ""),
            "legs": legs, "est_odds": round(est, 2),
            "code": code, "sb_odds": sb_odds,
        })
    return builders


def fmt_bet_builders(builders):
    """Telegram section for premium-match bet builders."""
    if not builders:
        return ""
    lines = ["⭐ <b>PREMIUM BET BUILDERS</b>", "<i>Correlated same-match picks</i>", ""]
    for b in builders:
        lines.append("⚽ <b>{} vs {}</b>".format(b["home"], b["away"]))
        for l in b["legs"]:
            lines.append("  ✅ {} <i>({:.0f}%)</i>".format(l["pick"], l["conf"]))
        odds = b.get("sb_odds") or b.get("est_odds")
        approx = "" if b.get("sb_odds") else " (est.)"
        lines.append("  💰 ~{}{}".format(odds, approx))
        if b.get("code"):
            lines.append('  🎟 Code: <b>{}</b>'.format(b["code"]))
            lines.append('  🔗 <a href="https://www.sportybet.com/ng/sport/football?shareCode={}">Open in SportyBet</a>'.format(b["code"]))
        else:
            lines.append('  🔗 <a href="https://www.sportybet.com/ng/sport/football/sr:match:{}">Build on SportyBet</a>'.format(
                b["event_id"].split(":")[-1] if ":" in str(b["event_id"]) else b["event_id"]))
        lines.append("")
    return "\n".join(lines)


def sb_decode_code(code):
    """
    Decode an existing SportyBet code (for testing / validation).
    Returns the selections list or None.
    """
    if not code:
        return None
    url = "{}/orders/share/{}".format(SB_BASE, code)
    resp = _sb_get(url)
    if not resp:
        return None
    if str(resp.get("message", "")).lower() == "success":
        return resp.get("data", {}).get("outcomes", [])
    return None


# ═══════════════════════════════════════════════════════════════════
# ORCHESTRATOR: accumulator -> SportyBet code
# ═══════════════════════════════════════════════════════════════════

def generate_code_for_accumulator(accumulator, event_id_cache=None):
    """
    Take an accumulator (from football_engine.build_accumulator) and
    generate a SportyBet booking code.

    Returns dict: {code, mapped, total, missing} where:
      code   = the booking code (or None)
      mapped = number of picks successfully mapped
      total  = total picks in the accumulator
      missing = list of picks that couldn't be mapped
    """
    if event_id_cache is None:
        event_id_cache = {}

    selections = []
    missing = []

    for pick in accumulator["selections"]:
        match_key = pick["match"]

        # Resolve event ID (cached per match)
        if match_key in event_id_cache:
            event_id = event_id_cache[match_key]
        else:
            event_id = sb_search_event(pick["home"], pick["away"])
            event_id_cache[match_key] = event_id
            time.sleep(0.4)

        if not event_id:
            missing.append("{} ({})".format(pick["match"], pick["pick"]))
            continue

        pick["sb_event_id"] = event_id
        # Attach kickoff time from the SportyBet event info we captured
        info = _SB_EVENT_INFO.get(event_id, {})
        if info.get("kickoff_ts"):
            pick["kickoff_ts"] = info["kickoff_ts"]

        # Get markets and map the pick
        markets = sb_get_event_markets(event_id)
        time.sleep(0.3)
        selection = sb_map_pick_to_selection(pick, markets)

        if selection and selection.get("outcomeId"):
            selections.append(selection)
        else:
            missing.append("{} ({})".format(pick["match"], pick["pick"]))

    code = sb_create_code(selections) if selections else None

    return {
        "code": code,
        "mapped": len(selections),
        "total": len(accumulator["selections"]),
        "missing": missing,
        "selections": selections,
    }


"""
CMVNG BOT v3 — DASHBOARD TEMPLATES
Glassmorphism, light-green theme, DM Sans + JetBrains Mono.
Matches the arcaprotocol aesthetic (frosted cards, bold display type).
"""

# Shared CSS for all v3 football pages
FB_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700;800&family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@500;700&display=swap');
:root{
  --brand:#2f6bd6; --brand2:#4f86ee; --brand-deep:#1f54b0; --brand-soft:#e8f0fd;
  --bg1:#eef3fa; --bg2:#e3eaf5;
  --ink:#0c1320; --ink2:#3a465c; --muted:#73819b;
  --surface:#ffffff; --surface2:#f6f9fd;
  --line:rgba(12,19,32,0.09); --line2:rgba(12,19,32,0.06);
  --glass:rgba(255,255,255,0.72); --glass-line:rgba(12,19,32,0.07);
  --shadow:0 16px 44px rgba(20,40,90,0.10);
  --good:#1f9d6b; --good-soft:rgba(31,157,107,0.13);
  --red:#e1556a; --red-soft:rgba(225,85,106,0.13);
  --orange:#e08a3c; --grid:rgba(47,107,214,0.05);
}
[data-theme="dark"]{
  --brand:#5a8cf0; --brand2:#7aa2f5; --brand-deep:#3f78e6; --brand-soft:rgba(90,140,240,0.16);
  --bg1:#0a0e16; --bg2:#06090f;
  --ink:#eaf0fb; --ink2:#c3cde0; --muted:#8696b4;
  --surface:#121826; --surface2:#0f1420;
  --line:rgba(255,255,255,0.09); --line2:rgba(255,255,255,0.06);
  --glass:rgba(255,255,255,0.055); --glass-line:rgba(255,255,255,0.10);
  --shadow:0 20px 54px rgba(0,0,0,0.55);
  --good:#56d3a0; --good-soft:rgba(86,211,160,0.16);
  --red:#fca5a5; --red-soft:rgba(248,113,113,0.16);
  --orange:#fb923c; --grid:rgba(120,160,255,0.05);
}
* { margin:0; padding:0; box-sizing:border-box; }
body {
  font-family:'DM Sans',sans-serif; color:var(--ink);
  background:radial-gradient(130% 90% at 50% -10%, var(--bg1), var(--bg2));
  min-height:100vh; padding-bottom:90px; position:relative; overflow-x:hidden;
  transition:background .35s ease, color .35s ease;
}
body::before{ content:""; position:fixed; inset:0; z-index:0; pointer-events:none; opacity:.9;
  background:
    radial-gradient(900px 520px at 12% -8%, var(--brand-soft), transparent 60%),
    radial-gradient(760px 520px at 90% 2%, var(--brand-soft), transparent 58%);
}
body::after{ content:""; position:fixed; inset:0; z-index:0; pointer-events:none; opacity:.6;
  background-image:linear-gradient(var(--grid) 1px,transparent 1px),
    linear-gradient(90deg,var(--grid) 1px,transparent 1px);
  background-size:48px 48px;
  mask-image:radial-gradient(circle at 50% 0%,black,transparent 75%);
  -webkit-mask-image:radial-gradient(circle at 50% 0%,black,transparent 75%); }
.nav {
  position:sticky; top:0; z-index:50;
  display:flex; align-items:center; justify-content:space-between; padding:14px 20px;
  background:color-mix(in srgb,var(--surface) 80%, transparent);
  backdrop-filter:blur(18px); -webkit-backdrop-filter:blur(18px);
  border-bottom:1px solid var(--line);
}
.nav .logo { font-family:'Sora',sans-serif; font-weight:800; font-size:1.1rem; color:var(--brand); letter-spacing:-0.5px; text-decoration:none; }
.nav .logo span { color:var(--ink); }
.nav-right { display:flex; align-items:center; gap:12px; }
.nav .nav-page { font-size:0.8rem; font-weight:700; color:var(--muted); font-family:'JetBrains Mono',monospace; letter-spacing:1px; text-transform:uppercase; }
.theme-toggle { width:34px; height:34px; border-radius:10px; border:1px solid var(--line); background:var(--surface2);
  color:var(--ink); font-size:15px; cursor:pointer; line-height:1; display:flex; align-items:center; justify-content:center; transition:.18s; }
.theme-toggle:hover { border-color:var(--brand); }
.theme-toggle:active { transform:scale(.94); }
.tabbar {
  position:fixed; bottom:0; left:0; right:0; z-index:60;
  display:flex; justify-content:space-around; align-items:stretch;
  background:color-mix(in srgb,var(--surface) 90%, transparent);
  backdrop-filter:blur(22px); -webkit-backdrop-filter:blur(22px);
  border-top:1px solid var(--line);
  padding:6px 4px calc(6px + env(safe-area-inset-bottom));
  box-shadow:0 -6px 26px rgba(20,40,90,0.10);
}
.tabbar a { flex:1; display:flex; flex-direction:column; align-items:center; gap:3px;
  text-decoration:none; color:var(--muted); padding:6px 2px; border-radius:12px; transition:color .15s, background .15s; }
.tabbar a .ic { font-size:1.3rem; line-height:1; }
.tabbar a .tl { font-size:0.62rem; font-weight:700; letter-spacing:0.2px; }
.tabbar a.active { color:var(--brand); }
.tabbar a.active .tl { font-weight:900; }
.tabbar a:active { background:var(--brand-soft); }
.wrap { position:relative; z-index:1; max-width:980px; margin:0 auto; padding:26px 18px 0; }
.page-head { margin:14px 4px 22px; }
.page-head h1 { font-family:'Sora',sans-serif; font-size:2.2rem; font-weight:800; letter-spacing:-1.5px; color:var(--ink);
  background:linear-gradient(180deg,var(--ink),var(--brand-deep)); -webkit-background-clip:text; background-clip:text; -webkit-text-fill-color:transparent; }
.page-head .sub { color:var(--muted); font-size:0.95rem; margin-top:4px; font-weight:500; }
.page-head .date { font-family:'JetBrains Mono',monospace; font-size:0.78rem; color:var(--brand); margin-top:6px; letter-spacing:0.5px; }

/* Glass card */
.glass {
  position:relative; background:var(--glass);
  backdrop-filter:blur(16px); -webkit-backdrop-filter:blur(16px);
  border:1px solid var(--glass-line); border-radius:20px;
  box-shadow:var(--shadow); padding:22px; margin-bottom:18px;
  animation:fb-rise .55s ease both;
}
@keyframes fb-rise{from{opacity:0; transform:translateY(14px);} to{opacity:1; transform:translateY(0);}}

/* Accumulator tier card */
.tier { position:relative; overflow:hidden; transition:transform .2s, box-shadow .2s, border-color .2s; }
.tier::before{ content:""; position:absolute; inset:0 0 auto 0; height:2px;
  background:linear-gradient(90deg,transparent,var(--brand),transparent); opacity:.6; }
.tier:hover{ transform:translateY(-3px); border-color:var(--brand-soft); box-shadow:0 24px 60px rgba(20,40,90,0.16); }
.tier-head { display:flex; align-items:center; justify-content:space-between; margin-bottom:16px; }
.tier-title { display:flex; align-items:center; gap:10px; }
.tier-title .dot { width:11px; height:11px; border-radius:50%; box-shadow:0 0 12px currentColor; }
.tier-title h2 { font-family:'Sora',sans-serif; font-size:1.12rem; font-weight:800; letter-spacing:-0.3px; color:var(--ink); }
.tier-odds { font-family:'JetBrains Mono',monospace; font-weight:700; font-size:1.45rem; color:var(--brand); }
.tier-odds .lbl { font-size:0.62rem; color:var(--muted); display:block; text-align:right; font-weight:500; letter-spacing:1.5px; }
.section-head { display:flex; align-items:baseline; gap:10px; margin:6px 2px 14px; font-family:'Sora',sans-serif;
  font-size:1.15rem; font-weight:800; letter-spacing:-0.4px; color:var(--ink); }
.section-head span { font-family:'DM Sans',sans-serif; font-size:0.72rem; font-weight:500; color:var(--muted);
  letter-spacing:0.3px; }
.sel { display:flex; align-items:flex-start; gap:12px; padding:12px 0; border-top:1px solid var(--line2); }
.sel:first-of-type { border-top:none; }
.sel .ico { font-size:1.05rem; margin-top:1px; opacity:.9; }
.sel .body { flex:1; min-width:0; }
.sel .match { font-weight:700; font-size:0.88rem; color:var(--ink); }
.sel .pick { font-size:0.82rem; color:var(--brand-deep); margin-top:1px; }
.sel .why { font-size:0.68rem; color:var(--muted); margin-top:2px; font-style:italic; line-height:1.3; }
.sel .reason { font-size:0.72rem; color:var(--muted); margin-top:3px; }
.sel .odds { font-family:'JetBrains Mono',monospace; font-weight:700; font-size:0.95rem; color:var(--brand); white-space:nowrap; }
.sel .conf { font-family:'JetBrains Mono',monospace; font-size:0.66rem; color:var(--muted); text-align:right; }
.code-box {
  margin-top:16px; padding:15px 16px; border-radius:14px;
  background:var(--brand-soft); border:1px solid var(--glass-line);
  display:flex; align-items:center; justify-content:space-between; gap:10px;
}
.code-box .label { font-size:0.66rem; color:var(--muted); font-weight:700; text-transform:uppercase; letter-spacing:1.5px; }
.code-box .code { font-family:'JetBrains Mono',monospace; font-weight:700; font-size:1.4rem; color:var(--brand-deep); letter-spacing:3px; margin-top:2px; }
.code-box a { font-size:0.75rem; font-weight:700; color:#fff; background:linear-gradient(135deg,var(--brand),var(--brand2)); padding:10px 16px; border-radius:999px; text-decoration:none; white-space:nowrap; box-shadow:0 6px 18px rgba(47,107,214,0.32); transition:transform .15s; }
.code-box a:active { transform:scale(0.96); }
.code-box.pending { background:var(--surface2); border-color:var(--line); }
.code-box.pending .code { color:var(--muted); font-size:0.9rem; letter-spacing:0; }

/* Match pick card */
.match-card .mhead { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:6px; }
.match-card .teams { font-family:'Sora',sans-serif; font-weight:700; font-size:1.05rem; color:var(--ink); letter-spacing:-0.3px; }
.match-card .league { font-size:0.66rem; color:var(--brand-deep); font-weight:700; text-transform:uppercase; letter-spacing:1px; background:var(--brand-soft); padding:4px 10px; border-radius:999px; white-space:nowrap; }
.match-card .meta { font-family:'JetBrains Mono',monospace; font-size:0.72rem; color:var(--muted); margin-bottom:12px; }
.match-card .meta .inj { color:var(--orange); }
.match-card .status { display:inline-block; font-family:'JetBrains Mono',monospace; font-size:0.72rem; font-weight:700; padding:4px 10px; border-radius:8px; margin-bottom:10px; letter-spacing:0.3px; }
.match-card .status.live { background:var(--red-soft); color:var(--red); }
.match-card .status.live .det { font-weight:600; opacity:0.85; }
.match-card .status.ft { background:var(--good-soft); color:var(--good); }
.match-card .status.pre { background:var(--surface2); color:var(--muted); }
.pickrow { display:flex; align-items:center; gap:10px; padding:10px 0; border-top:1px solid var(--line2); }
.pickrow:first-of-type { border-top:none; }
.pickrow .rank { width:22px; height:22px; border-radius:50%; background:linear-gradient(135deg,var(--brand),var(--brand-deep)); color:#fff; font-size:0.7rem; font-weight:800; display:flex; align-items:center; justify-content:center; }
.pickrow .ptext { flex:1; font-weight:600; font-size:0.85rem; color:var(--ink); }
.pickrow .pct { font-family:'JetBrains Mono',monospace; font-weight:700; font-size:0.95rem; color:var(--brand); }
.bar { height:5px; background:var(--line); border-radius:999px; margin-top:5px; overflow:hidden; }
.bar > div { height:100%; background:linear-gradient(90deg,var(--brand),var(--brand-deep)); border-radius:999px; }
.empty { text-align:center; padding:54px 20px; color:var(--muted); }
.empty .big { font-size:2.4rem; margin-bottom:10px; }
.disclaimer { text-align:center; font-size:0.72rem; color:var(--muted); margin:24px 18px; line-height:1.5; }

/* Calendar */
.cal-head { display:flex; align-items:center; justify-content:space-between; margin-bottom:14px; }
.cal-head h2 { font-family:'Sora',sans-serif; font-size:1.2rem; font-weight:800; color:var(--ink); }
.cal-nav a { text-decoration:none; color:var(--brand-deep); font-weight:700; font-size:1.3rem; padding:4px 12px; border-radius:10px; background:var(--brand-soft); }
.cal-grid { display:grid; grid-template-columns:repeat(7,1fr); gap:6px; }
.cal-dow { text-align:center; font-size:0.65rem; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:0.5px; padding:4px 0; }
.cal-cell { aspect-ratio:1; border-radius:12px; background:var(--surface2); border:1px solid var(--line2); display:flex; flex-direction:column; align-items:center; justify-content:center; text-decoration:none; color:var(--ink); position:relative; transition:transform .12s; }
.cal-cell.has-data { background:var(--brand-soft); border-color:var(--brand-soft); font-weight:700; }
.cal-cell.has-data:active { transform:scale(0.95); }
.cal-cell.empty-cell { background:transparent; border:none; }
.cal-cell.today { outline:2px solid var(--brand); }
.cal-cell .dnum { font-size:0.85rem; }
.cal-cell .dcount { font-size:0.6rem; color:var(--brand-deep); font-family:'JetBrains Mono',monospace; margin-top:1px; }
.cal-cell .ddots { display:flex; gap:2px; margin-top:2px; }
.cal-cell .ddot { width:5px; height:5px; border-radius:50%; }
.legend { display:flex; gap:14px; justify-content:center; flex-wrap:wrap; margin-top:14px; font-size:0.72rem; color:var(--muted); }
.legend span { display:flex; align-items:center; gap:5px; }
.legend i { width:9px; height:9px; border-radius:50%; display:inline-block; }
.back-link { display:inline-block; margin-bottom:14px; color:var(--brand-deep); font-weight:700; text-decoration:none; font-size:0.85rem; }
.badge { font-size:0.62rem; font-weight:700; padding:3px 9px; border-radius:999px; text-transform:uppercase; letter-spacing:0.5px; }
.badge.won { background:var(--good-soft); color:var(--good); }
.badge.lost { background:var(--red-soft); color:var(--red); }
.badge.pending { background:var(--surface2); color:var(--muted); }
.badge.void { background:var(--surface2); color:var(--muted); }
.tier .sel.won .pick { color:var(--good); }
.tier .sel.lost .pick { color:var(--red); text-decoration:line-through; opacity:0.75; }
/* Pending-reason note — explains WHY a slip is still pending, so the user
   never has to guess. Subtle gray pill + smaller text. */
.pending-reason {
  font: 12px/1.45 system-ui;
  color: var(--muted);
  background: var(--surface2);
  border: 1px solid rgba(0,0,0,0.04);
  border-radius: 10px;
  padding: 8px 11px;
  margin: 10px 0 0;
}
.pending-reason .why-pill {
  display: inline-block;
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  background: var(--surface);
  color: var(--text);
  padding: 2px 7px;
  border-radius: 999px;
  margin-right: 7px;
  border: 1px solid rgba(0,0,0,0.06);
}

@media (max-width:600px){
  .page-head h1 { font-size:1.75rem; }
  .nav { padding:12px 16px; }
  .nav .logo { font-size:1rem; }
  .tier-odds { font-size:1.25rem; }
  .wrap { padding:18px 13px 0; }
  .glass { padding:17px; border-radius:18px; }
  .sel .match { font-size:0.82rem; }
  .cal-grid { gap:4px; }
  .cal-cell .dnum { font-size:0.78rem; }
}
"""


def _nav(active):
    page_names = {"home": "Home", "picks": "Picks", "codes": "Codes",
                  "crypto": "Crypto", "sports": "Markets", "results": "Results"}
    top = ('<div class="nav"><a href="/" class="logo">CMVNG<span>BOT</span></a>'
           '<div class="nav-right"><div class="nav-page">{}</div>'
           '<button class="theme-toggle" id="cmvngThemeBtn" onclick="cmvngToggleTheme()" aria-label="Toggle theme">🌙</button>'
           '</div></div>').format(page_names.get(active, ""))
    top += ('<script>(function(){var t=localStorage.getItem("cmvng-theme")||"light";document.documentElement.setAttribute("data-theme",t);})();'
            'function cmvngToggleTheme(){var d=document.documentElement,n=d.getAttribute("data-theme")==="dark"?"light":"dark";d.setAttribute("data-theme",n);localStorage.setItem("cmvng-theme",n);var b=document.getElementById("cmvngThemeBtn");if(b)b.textContent=n==="dark"?"☀️":"🌙";}'
            'document.addEventListener("DOMContentLoaded",function(){var t=localStorage.getItem("cmvng-theme")||"light";var b=document.getElementById("cmvngThemeBtn");if(b)b.textContent=t==="dark"?"☀️":"🌙";});</script>')
    tabs = [
        ("home", "/", "🏠", "Home"),
        ("picks", "/app/picks", "⚽", "Picks"),
        ("codes", "/app/codes", "🎫", "Codes"),
        ("builder", "/app/builder", "⭐", "Builder"),
        ("cards", "/app/cards", "🖼️", "Cards"),
        ("crypto", "/app/paper-poly", "💰", "Crypto"),
        ("results", "/app/results", "📈", "Results"),
    ]
    items = "".join(
        '<a href="{}" class="{}"><span class="ic">{}</span>'
        '<span class="tl">{}</span></a>'.format(
            url, "active" if key == active else "", _ICONS.get(key, ""), label)
        for key, url, ic, label in tabs)
    bottom = '<div class="tabbar">{}</div>'.format(items)
    return top + bottom


def _fb_fmt_kickoff(ts):
    """Epoch-ms -> 'Sat 15:00' (UTC+1 Lagos), or '' if unknown."""
    if not ts:
        return ""
    try:
        dt = _dt.datetime.fromtimestamp(int(ts) / 1000, _dt.timezone.utc) + _dt.timedelta(hours=1)
        return dt.strftime("%a %H:%M")
    except Exception:
        return ""


def _fb_fmt_when(ts):
    """Epoch-ms -> 'Sat 14 Jun · 15:00' (UTC+1 Lagos) for share-card sublines."""
    if not ts:
        return ""
    try:
        dt = _dt.datetime.fromtimestamp(int(ts) / 1000, _dt.timezone.utc) + _dt.timedelta(hours=1)
        return dt.strftime("%a %d %b · %H:%M")
    except Exception:
        return ""


def _fb_builder_blocks(builders):
    """Render premium bet-builder cards (3 correlated legs on one match, one code)."""
    if not builders:
        return ""
    cards = []
    for b in builders:
        legs = []
        for l in b.get("legs", []):
            conf = l.get("conf") or 0
            legs.append(
                '<div class="sel"><div class="ico">✅</div>'
                '<div class="body"><div class="pick">{}</div></div>'
                '<div><div class="conf">{:.0f}%</div></div></div>'.format(
                    l.get("pick", ""), conf))
        legs_html = "".join(legs)
        odds = b.get("sb_odds") or b.get("est_odds") or 0
        odds_lbl = "COMBINED ODDS" if b.get("sb_odds") else "EST. ODDS"
        if b.get("code"):
            code_box = (
                '<div class="code-box"><div><div class="label">Bet Builder Code</div>'
                '<div class="code">{}</div></div>'
                '<a href="https://www.sportybet.com/ng/sport/football?shareCode={}" '
                'target="_blank">Open →</a></div>').format(b["code"], b["code"])
        else:
            eid = str(b.get("event_id", ""))
            eid = eid.split(":")[-1] if ":" in eid else eid
            code_box = (
                '<div class="code-box pending"><div><div class="label">Bet Builder Code</div>'
                '<div class="code">build on SportyBet</div></div>'
                '<a href="https://www.sportybet.com/ng/sport/football/sr:match:{}" '
                'target="_blank">Open →</a></div>').format(eid)
        cards.append(
            '<div class="glass tier"><div class="tier-head"><div class="tier-title">'
            '<div class="dot" style="background:#7c3aed"></div><h2>⭐ {} vs {}</h2></div>'
            '<div class="tier-odds">{:.2f}<span class="lbl">{}</span></div></div>'
            '{}{}</div>'.format(
                b.get("home", ""), b.get("away", ""), odds, odds_lbl, legs_html, code_box))
    return ('<div class="section-head">⭐ Premium Bet Builders<span>3 correlated '
            'picks, one match</span></div>' + "".join(cards))


def render_builder_page(builders, date_str):
    """Dedicated Bet Builder page — premium single-match correlated slips."""
    if builders:
        body = _fb_builder_blocks(builders)
    else:
        body = ('<div class="glass empty"><div class="big">⭐</div>'
                '<div>No bet builders yet. The engine builds them for the '
                'richest-board matches each run — check back after the next scan.</div></div>')
    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bet Builders — Cmvng Bot</title><style>{css}</style></head><body>
{nav}
<div class="wrap">
<div class="page-head"><h1>Bet Builders</h1>
<div class="sub">3 correlated picks per premium match, one code</div>
<div class="date">{date}</div></div>
{body}
<div class="disclaimer">Bet builders combine correlated picks on a single match.
Combined odds shown are SportyBet's where available, otherwise an estimate. Always
review the slip in your SportyBet app before staking. No bet is guaranteed.</div>
</div></body></html>""".format(css=FB_CSS, nav=_nav("builder"), date=date_str, body=body)


def render_codes_page(accumulators, date_str, builders=None):
    """Render the SportyBet codes page. accumulators = list of dicts with code info."""
    blocks = []
    for acca in accumulators:
        if not acca:
            continue
        rows = []
        started = 0
        for s in acca["selections"]:
            # Live status so the page reflects games that have kicked off since
            # this code was built (codes are snapshots from the last engine run).
            _hm, _, _aw = (s.get("match", "") or "").partition(" vs ")
            _live = None
            try:
                _live = _fb_live_status(_hm.strip(), _aw.strip())
            except Exception:
                _live = None
            _st = (_live or {}).get("state")
            if _st == "in":
                started += 1
                _hs, _as = _live.get("hs"), _live.get("aw")
                status_html = ('<div class="reason" style="color:#dc2626;font-weight:600">'
                               '🔴 LIVE {}–{} {}</div>').format(
                    _hs if _hs is not None else 0, _as if _as is not None else 0,
                    _live.get("detail", "") or "")
            elif _st == "post":
                started += 1
                _hs, _as = _live.get("hs"), _live.get("aw")
                status_html = ('<div class="reason" style="color:var(--good);font-weight:600">'
                               '✅ FT {}–{}</div>').format(
                    _hs if _hs is not None else 0, _as if _as is not None else 0)
            else:
                ko = _fb_fmt_kickoff(s.get("kickoff_ts"))
                status_html = '<div class="reason">🕐 {}</div>'.format(ko) if ko else ""
            rows.append(
                '<div class="sel"><div class="ico">⚽</div>'
                '<div class="body"><div class="match">{}</div>'
                '<div class="pick">{}</div>{}</div>'
                '<div><div class="odds">{}</div><div class="conf">{:.0f}%</div></div></div>'.format(
                    s["match"], s["pick"], status_html, s["odds"], s["confidence"]))
        sels = "".join(rows)
        if started:
            sels = ('<div class="reason" style="color:#ea580c;font-weight:600;'
                    'padding:8px 0">⚠️ {} leg(s) already kicked off — this code '
                    'can no longer be placed in full.</div>'.format(started)) + sels
        if acca.get("code"):
            code_box = (
                '<div class="code-box"><div><div class="label">SportyBet Code</div>'
                '<div class="code">{}</div></div>'
                '<a href="https://www.sportybet.com/ng/sport/football?shareCode={}" target="_blank">Open →</a></div>'
            ).format(acca["code"], acca["code"])
        else:
            code_box = ('<div class="code-box pending"><div><div class="label">SportyBet Code</div>'
                        '<div class="code">build manually below</div></div></div>')

        dot_color = {"🟢": "#15803d", "🟡": "#ca8a04", "🟠": "#ea580c", "🔴": "#dc2626"}.get(acca["emoji"], "#15803d")
        blocks.append(
            '<div class="glass tier"><div class="tier-head"><div class="tier-title">'
            '<div class="dot" style="background:{}"></div><h2>{}</h2></div>'
            '<div class="tier-odds">{:.2f}<span class="lbl">TOTAL ODDS</span></div></div>'
            '{}{}</div>'.format(dot_color, acca["label"], acca["total_odds"], sels, code_box)
        )

    if not blocks:
        body = ('<div class="glass empty"><div class="big">🎫</div>'
                '<div>No codes generated yet. The engine runs every few hours — '
                'check back after the next scan.</div></div>')
    else:
        body = "".join(blocks)

    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SportyBet Codes — Cmvng Bot</title><style>{css}</style></head><body>
{nav}
<div class="wrap">
<div class="page-head"><h1>Today's Codes</h1>
<div class="sub">Accumulator booking codes for SportyBet</div>
<div class="date">{date}</div>
<a href="/app/fb-rescan" style="display:inline-block;margin-top:8px;font:600 13px system-ui;color:var(--brand,#2563eb);text-decoration:none;border:1px solid var(--line,#e5e7eb);border-radius:8px;padding:6px 12px">↻ Rescan now</a></div>
{body}
<div class="disclaimer">Codes are auto-generated from data analysis. Odds may shift before kickoff.
Always review selections in your SportyBet app before staking. No bet is guaranteed.</div>
</div></body></html>""".format(css=FB_CSS, nav=_nav("codes"), date=date_str, body=body)


def render_picks_page(match_picks, date_str):
    """Render the analyzed picks page. match_picks = dict {match: [top picks]}."""
    blocks = []
    for match, picks in match_picks.items():
        if not picks:
            continue
        first = picks[0]
        meta_bits = []
        if first.get("home_form_disp"):
            meta_bits.append("Form: {} {} | {} {}".format(
                first["home"], first.get("home_form_disp", "?"),
                first["away"], first.get("away_form_disp", "?")))
        meta = " · ".join(meta_bits) if meta_bits else first.get("reasoning", "")

        rows = "".join(
            '<div class="pickrow"><div class="rank">{}</div>'
            '<div class="ptext">{}<div class="bar"><div style="width:{:.0f}%"></div></div></div>'
            '<div class="pct" style="color:{}">{:.0f}%</div></div>'.format(
                i, p["pick"], p["confidence"],
                "#15803d" if p["confidence"] >= 70 else ("#ca8a04" if p["confidence"] >= 55 else "#ea580c"),
                p["confidence"])
            for i, p in enumerate(picks, 1)
        )

        # Live status + accurate league from ESPN (refreshed in background)
        league = first.get("league", "")
        status_html = ""
        try:
            live = _fb_live_status(first.get("home", ""), first.get("away", ""))
        except Exception:
            live = None
        if live:
            if live.get("league"):
                league = live["league"]
            st = live.get("state")
            hs, aw = live.get("hs"), live.get("aw")
            det = live.get("detail", "")
            if st == "in":
                status_html = ('<div class="status live">🔴 LIVE {}–{} '
                               '<span class="det">{}</span></div>').format(
                    hs if hs is not None else 0, aw if aw is not None else 0, det)
            elif st == "post":
                status_html = ('<div class="status ft">✅ FT {}–{}</div>').format(
                    hs if hs is not None else 0, aw if aw is not None else 0)
            elif st == "pre" and det:
                status_html = '<div class="status pre">⏳ {}</div>'.format(det)

        ko = first.get("kickoff_ts")
        if not status_html and ko:
            kt = _fb_fmt_kickoff(ko)
            if kt:
                status_html = '<div class="status pre">⏳ {}</div>'.format(kt)

        blocks.append(
            '<div class="glass match-card"><div class="mhead">'
            '<div class="teams">{}</div><div class="league">{}</div></div>'
            '{}<div class="meta">{}</div>{}</div>'.format(
                match, league, status_html, meta, rows)
        )

    if not blocks:
        body = ('<div class="glass empty"><div class="big">⚽</div>'
                '<div>No picks analyzed yet. The engine runs every few hours.</div></div>')
    else:
        body = "".join(blocks)

    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Football Picks — Cmvng Bot</title><style>{css}</style></head><body>
{nav}
<div class="wrap">
<div class="page-head"><h1>Football Picks</h1>
<div class="sub">Top 3 highest-probability picks per match</div>
<div class="date">{date}</div></div>
{body}
<div class="disclaimer">Picks generated from form, xG, head-to-head, injuries and team stats.
Percentages are model estimates, not guarantees.</div>
</div></body></html>""".format(css=FB_CSS, nav=_nav("picks"), date=date_str, body=body)


def render_results_page(stats, date_str):
    """Tier win-rate summary cards (used at top of the calendar page)."""
    blocks = []
    for st in stats:
        wr = (st["wins"] / st["settled"] * 100) if st.get("settled") else 0
        blocks.append(
            '<div class="glass"><div class="tier-head">'
            '<div class="tier-title"><h2>{}</h2></div>'
            '<div class="tier-odds">{:.0f}%<span class="lbl">WIN RATE</span></div></div>'
            '<div class="meta" style="font-family:JetBrains Mono,monospace;color:var(--muted);font-size:0.8rem">'
            '{} won / {} settled · {} pending</div></div>'.format(
                st["tier_label"], wr, st["wins"], st["settled"], st.get("pending", 0))
        )
    body = "".join(blocks) if blocks else (
        '<div class="glass empty"><div class="big">📈</div>'
        '<div>No settled results yet. Win rates appear after matches finish.</div></div>')
    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Results — Cmvng Bot</title><style>{css}</style></head><body>
{nav}<div class="wrap"><div class="page-head"><h1>Results</h1>
<div class="sub">Win-rate tracking per accumulator tier</div>
<div class="date">{date}</div></div>{body}</div></body></html>""".format(
        css=FB_CSS, nav=_nav("results"), date=date_str, body=body)


_MONTHS = ["", "January", "February", "March", "April", "May", "June",
           "July", "August", "September", "October", "November", "December"]


def render_results_calendar(year, month, day_data, tier_stats, today_iso):
    """
    Calendar view of results history.
    day_data = {day_int: {"slips":N, "won":N, "lost":N, "pending":N}}
    tier_stats = list of tier summary dicts (overall win rates)
    """
    import calendar as _cal
    cal = _cal.Calendar(firstweekday=0)  # Monday first
    weeks = cal.monthdayscalendar(year, month)

    # Prev / next month links
    pm, py = (12, year - 1) if month == 1 else (month - 1, year)
    nm, ny = (1, year + 1) if month == 12 else (month + 1, year)

    dow = "".join('<div class="cal-dow">{}</div>'.format(d)
                  for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])

    cells = []
    for week in weeks:
        for day in week:
            if day == 0:
                cells.append('<div class="cal-cell empty-cell"></div>')
                continue
            iso = "{:04d}-{:02d}-{:02d}".format(year, month, day)
            d = day_data.get(day)
            classes = "cal-cell"
            if iso == today_iso:
                classes += " today"
            if d and d["slips"] > 0:
                classes += " has-data"
                dots = ""
                if d["won"]:
                    dots += '<span class="ddot" style="background:#15803d"></span>'
                if d["lost"]:
                    dots += '<span class="ddot" style="background:#dc2626"></span>'
                if d["pending"]:
                    dots += '<span class="ddot" style="background:#9ca3af"></span>'
                cells.append(
                    '<a href="/app/results?date={}" class="{}">'
                    '<div class="dnum">{}</div>'
                    '<div class="dcount">{} slip{}</div>'
                    '<div class="ddots">{}</div></a>'.format(
                        iso, classes, day, d["slips"], "" if d["slips"] == 1 else "s", dots))
            else:
                cells.append('<div class="{}"><div class="dnum">{}</div></div>'.format(classes, day))

    grid = '<div class="cal-grid">{}{}</div>'.format(dow, "".join(cells))

    cal_card = (
        '<div class="glass"><div class="cal-head">'
        '<div class="cal-nav"><a href="/app/results?ym={:04d}-{:02d}">‹</a></div>'
        '<h2>{} {}</h2>'
        '<div class="cal-nav"><a href="/app/results?ym={:04d}-{:02d}">›</a></div>'
        '</div>{}'
        '<div class="legend">'
        '<span><i style="background:#15803d"></i>Won</span>'
        '<span><i style="background:#dc2626"></i>Lost</span>'
        '<span><i style="background:#9ca3af"></i>Pending</span></div></div>'
    ).format(py, pm, _MONTHS[month], year, ny, nm, grid)

    # Tier summary below
    summary = []
    for st in tier_stats:
        wr = (st["wins"] / st["settled"] * 100) if st.get("settled") else 0
        summary.append(
            '<div class="glass" style="padding:14px 18px"><div class="tier-head" style="margin:0">'
            '<div class="tier-title"><h2 style="font-size:0.95rem">{}</h2></div>'
            '<div class="tier-odds" style="font-size:1.1rem">{:.0f}%'
            '<span class="lbl">{} / {}</span></div></div></div>'.format(
                st["tier_label"], wr, st["wins"], st["settled"]))
    summary_html = "".join(summary)

    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Results — Cmvng Bot</title><style>{css}</style></head><body>
{nav}<div class="wrap"><div class="page-head"><h1>Results History</h1>
<div class="sub">Tap any highlighted day to see that day's slips</div></div>
{cal}{summary}</div></body></html>""".format(
        css=FB_CSS, nav=_nav("results"), cal=cal_card, summary=summary_html)


def render_results_day(date_iso, date_human, sets):
    """Detail view for one day's accumulators, grouped by engine run (newest first).
    `sets` = list of {run_id, created_at, accas:[...]} groups."""

    def _run_time(grp):
        raw = grp.get("run_id") or (str(grp.get("created_at")) if grp.get("created_at") else "")
        m = re.search(r'(\d{2}):(\d{2})', raw or "")
        return "{}:{}".format(m.group(1), m.group(2)) if m else ""

    def _acca_block(a):
        status = (a.get("result") or "pending").lower()
        badge = '<span class="badge {}">{}</span>'.format(
            status, {"won": "WON", "lost": "LOST", "void": "VOID"}.get(status, "PENDING"))
        # NEW: surface the settle thread's explanation for why this slip is
        # still PENDING, so the user doesn't have to guess.  Only shown for
        # pending slips; settled slips don't need this.
        pending_note = ""
        if status == "pending" and a.get("pending_reason"):
            reason_txt = (a.get("pending_reason") or "")[:400]
            # last-attempt freshness so the user knows the reason isn't stale
            sl = a.get("settle_last_attempt")
            age_txt = ""
            try:
                if sl:
                    age_s = max(0, int(time.time()) - int(sl))
                    if age_s < 60:
                        age_txt = " · checked {}s ago".format(age_s)
                    elif age_s < 3600:
                        age_txt = " · checked {}m ago".format(age_s // 60)
                    else:
                        age_txt = " · checked {}h ago".format(age_s // 3600)
            except Exception:
                age_txt = ""
            pending_note = (
                '<div class="pending-reason">'
                '<span class="why-pill">Why pending?</span> {}{}'
                '</div>'.format(reason_txt, age_txt))
        parts = []
        for s in a.get("selections", []):
            lr = (s.get("result") or "").lower()
            if lr == "won":
                ico, cls = "✅", "won"
            elif lr == "lost":
                ico, cls = "❌", "lost"
            else:
                ico, cls = "⚽", "pending"
            parts.append(
                '<div class="sel {}"><div class="ico">{}</div><div class="body">'
                '<div class="match">{}</div><div class="pick">{}</div>'
                '<div class="why">{}</div></div>'
                '<div class="odds">{}</div></div>'.format(
                    cls, ico, s.get("match", ""), s.get("pick", ""),
                    s.get("reasoning", ""), s.get("odds", "")))
        code = ""
        if a.get("sportybet_code"):
            code = ('<div class="code-box"><div><div class="label">SportyBet Code</div>'
                    '<div class="code">{}</div></div></div>'.format(a["sportybet_code"]))
        return ('<div class="glass tier"><div class="tier-head"><div class="tier-title">'
                '<h2>{}</h2>{}</div><div class="tier-odds">{:.2f}'
                '<span class="lbl">TOTAL ODDS</span></div></div>{}{}{}</div>'.format(
                    a.get("label", ""), badge, a.get("total_odds", 0),
                    pending_note, "".join(parts), code))

    set_blocks = []
    for i, grp in enumerate(sets or []):
        accas = grp.get("accas", [])
        if not accas:
            continue
        t = _run_time(grp)
        if i == 0:
            head_lbl = "🟢 LATEST SET" + (" · {}".format(t) if t else "")
            cls = "latest"
        else:
            head_lbl = "EARLIER SET" + (" · {}".format(t) if t else "")
            cls = "earlier"
        inner = "".join(_acca_block(a) for a in accas)
        set_blocks.append(
            '<div class="run-set {cls}"><div class="run-head"><span>{head}</span>'
            '<span class="run-count">{n} slips</span></div>{inner}</div>'.format(
                cls=cls, head=head_lbl, n=len(accas), inner=inner))

    body = "".join(set_blocks) if set_blocks else (
        '<div class="glass empty"><div class="big">📅</div>'
        '<div>No slips were generated on this day.</div></div>')

    extra = (
        ".run-set{margin-bottom:30px}"
        ".run-head{display:flex;align-items:center;justify-content:space-between;"
        "font-weight:700;font-size:13px;letter-spacing:.05em;margin:20px 0 12px;"
        "padding:10px 14px;border-radius:12px;background:var(--surface2);"
        "border:1px solid var(--line)}"
        ".run-set.latest .run-head{color:var(--brand-deep);border-color:var(--brand-soft);"
        "background:var(--brand-soft)}"
        ".run-set.earlier .run-head{opacity:.75}"
        ".run-count{font-weight:600;font-size:11px;opacity:.8}")

    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{date} Results — Cmvng Bot</title><style>{css}{extra}</style></head><body>
{nav}<div class="wrap">
<a href="/app/results" class="back-link">‹ Back to calendar</a>
<div class="page-head"><h1>{date}</h1><div class="sub">Each set is one engine run — newest first</div></div>
{body}</div></body></html>""".format(
        css=FB_CSS, extra=extra, nav=_nav("results"), date=date_human, body=body)


"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — TELEGRAM COMMAND SYSTEM
═══════════════════════════════════════════════════════════════════
Menu structure exactly as specified:

  /sports  -> [Polymarket Sports] [Limitless Sports] [Football Picks]
  /crypto  -> [Polymarket Crypto] [Limitless Crypto]
  /picks   -> today's football picks
  /codes   -> SportyBet booking codes (5 tiers)
  /live    -> all unresolved bets across platforms
  /results -> win rates per tier

Anyone can use / commands to browse. New picks/signals auto-send.

This module builds the message text + inline keyboards. The actual
send/answer happens via the Telegram Bot API. Wired in app.py.
═══════════════════════════════════════════════════════════════════
"""

import json

try:
    import requests as _req
except ImportError:
    _req = None


# ═══════════════════════════════════════════════════════════════════
# LOW-LEVEL TELEGRAM API
# ═══════════════════════════════════════════════════════════════════

def tg_send(token, chat_id, text, keyboard=None, parse_mode="HTML"):
    """Send a message, optionally with an inline keyboard."""
    if _req is None or not token or not chat_id:
        return None
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode,
               "disable_web_page_preview": True}
    if keyboard:
        payload["reply_markup"] = json.dumps({"inline_keyboard": keyboard})
    try:
        r = _req.post("https://api.telegram.org/bot{}/sendMessage".format(token),
                      json=payload, timeout=10)
        return r.json()
    except Exception as e:
        print("[TG] send error: {}".format(e))
        return None


def tg_answer_callback(token, callback_id, text=""):
    """Acknowledge a button tap so Telegram stops the loading spinner."""
    if _req is None or not token:
        return
    try:
        _req.post("https://api.telegram.org/bot{}/answerCallbackQuery".format(token),
                  json={"callback_query_id": callback_id, "text": text}, timeout=8)
    except Exception:
        pass


def tg_set_webhook(token, url):
    """Register the webhook URL with Telegram."""
    if _req is None or not token:
        return None
    try:
        r = _req.post("https://api.telegram.org/bot{}/setWebhook".format(token),
                      json={"url": url, "allowed_updates": ["message", "callback_query"]},
                      timeout=10)
        return r.json()
    except Exception as e:
        print("[TG] setWebhook error: {}".format(e))
        return None


def tg_set_commands(token):
    """Register the slash-command menu shown in the Telegram UI."""
    if _req is None or not token:
        return
    commands = [
        {"command": "picks", "description": "Today's football picks"},
        {"command": "codes", "description": "SportyBet booking codes"},
        {"command": "sports", "description": "Sports markets menu"},
        {"command": "crypto", "description": "Crypto signals menu"},
        {"command": "live", "description": "Unresolved bets (all platforms)"},
        {"command": "results", "description": "Win rates per tier"},
    ]
    try:
        _req.post("https://api.telegram.org/bot{}/setMyCommands".format(token),
                  json={"commands": commands}, timeout=8)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
# MENU KEYBOARDS
# ═══════════════════════════════════════════════════════════════════

def kb_sports_menu():
    return [
        [{"text": "📊 Polymarket Sports", "callback_data": "sports_poly"}],
        [{"text": "📊 Limitless Sports", "callback_data": "sports_limitless"}],
        [{"text": "⚽ Football Picks", "callback_data": "show_picks"}],
        [{"text": "🎫 SportyBet Codes", "callback_data": "show_codes"}],
    ]


def kb_crypto_menu():
    return [
        [{"text": "💰 Polymarket Crypto", "callback_data": "crypto_poly"}],
        [{"text": "💰 Limitless Crypto", "callback_data": "crypto_limitless"}],
    ]


def kb_main_menu():
    return [
        [{"text": "⚽ Football Picks", "callback_data": "show_picks"},
         {"text": "🎫 Codes", "callback_data": "show_codes"}],
        [{"text": "📊 Sports Markets", "callback_data": "menu_sports"},
         {"text": "💰 Crypto", "callback_data": "menu_crypto"}],
        [{"text": "📈 Results", "callback_data": "show_results"},
         {"text": "🔴 Live Bets", "callback_data": "show_live"}],
    ]


# ═══════════════════════════════════════════════════════════════════
# MESSAGE FORMATTERS
# ═══════════════════════════════════════════════════════════════════

def fmt_welcome():
    return (
        "🤖 <b>CMVNG BOT</b>\n\n"
        "Your automated football + crypto prediction engine.\n\n"
        "<b>Commands:</b>\n"
        "/picks — today's football picks\n"
        "/codes — SportyBet booking codes\n"
        "/sports — sports markets menu\n"
        "/crypto — crypto signals menu\n"
        "/live — unresolved bets\n"
        "/results — win rates\n\n"
        "Pick a section below 👇"
    )


def fmt_codes(accumulators, date_str):
    """Format the SportyBet codes message for all tiers — clean and scannable."""
    if not accumulators:
        return ("🎫 <b>SPORTYBET CODES</b>\n\nNo codes generated yet. "
                "The engine runs every few hours — check back soon.")

    lines = ["🎫 <b>SPORTYBET CODES</b>", "<i>{}</i>".format(date_str), ""]
    for acca in accumulators:
        if not acca:
            continue
        lines.append("━━━━━━━━━━━━━━━")
        lines.append("{} <b>{}</b>".format(acca["emoji"], acca["label"]))
        lines.append("💰 Total odds: <b>{:.2f}</b>  ·  {} legs".format(
            acca["total_odds"], acca.get("num_selections", len(acca["selections"]))))
        lines.append("")
        for s in acca["selections"]:
            ko = _fb_fmt_kickoff(s.get("kickoff_ts")) if "_fb_fmt_kickoff" in globals() else ""
            ko_str = "  🕐 {}".format(ko) if ko else ""
            lines.append("  ⚽ {}{}".format(_short(s["match"]), ko_str))
            lines.append("     → <b>{}</b>  @ {:.2f}".format(s["pick"], s["odds"]))
        lines.append("")
        if acca.get("code"):
            lines.append("🎫 Code: <code>{}</code>".format(acca["code"]))
            lines.append('🔗 <a href="https://www.sportybet.com/ng/sport/football?shareCode={}">Open in SportyBet</a>'.format(acca["code"]))
        else:
            lines.append("🎫 <i>Build manually from the picks above</i>")
        lines.append("")
    return "\n".join(lines)


def fmt_picks(match_picks, date_str, limit=12):
    """Format the football picks message — top 3 per match, clean layout."""
    if not match_picks:
        return ("⚽ <b>FOOTBALL PICKS</b>\n\nNo picks analyzed yet. "
                "The engine runs every few hours.")

    lines = ["⚽ <b>FOOTBALL PICKS</b>", "<i>{}</i>".format(date_str),
             "<i>Top 3 picks per match</i>", ""]
    count = 0
    for match, picks in match_picks.items():
        if not picks or count >= limit:
            continue
        count += 1
        lines.append("🏟 <b>{}</b>".format(match))
        for i, p in enumerate(picks, 1):
            bar = "🟢" if p["confidence"] >= 70 else ("🟡" if p["confidence"] >= 55 else "🟠")
            lines.append("  {} {} — <b>{:.0f}%</b>".format(bar, p["pick"], p["confidence"]))
        lines.append("")
    lines.append("🎫 /codes for ready-made SportyBet slips")
    return "\n".join(lines)


def fmt_results(stats, date_str):
    if not stats:
        return "📈 <b>RESULTS</b>\n\nNo settled results yet. Win rates appear after matches finish."
    lines = ["📈 <b>WIN RATES BY TIER</b>", ""]
    for st in stats:
        wr = (st["wins"] / st["settled"] * 100) if st.get("settled") else 0
        lines.append("{}".format(st["tier_label"]))
        lines.append("   ✅ {} won / {} settled ({:.0f}%)  ·  ⏳ {} pending".format(
            st["wins"], st["settled"], wr, st.get("pending", 0)))
        lines.append("")
    return "\n".join(lines)


def _short(match, n=34):
    return match if len(match) <= n else match[:n-1] + "…"


"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — FOOTBALL INTEGRATION GLUE
═══════════════════════════════════════════════════════════════════
This is the code that gets inlined into app.py. It assumes all the
engine/scraper/sportybet/web/telegram functions are in the same
namespace (they're concatenated above it in the final app.py).

Provides:
  - DB tables (football_picks, sportybet_accumulators, pick_results)
  - run_football_engine()  -> the daily scrape→analyze→build→codes→save→telegram
  - in-memory cache so /picks /codes serve instantly
  - background thread (every 6h)
  - Flask routes  /app/picks /app/codes /app/results
  - Telegram webhook  /api/telegram-webhook  + command router
═══════════════════════════════════════════════════════════════════
"""


import os
import json
import time
import threading
import datetime as _dt


# ═══════════════════════════════════════════════════════════════════
# IN-MEMORY CACHE — latest engine output (so commands respond instantly)
# ═══════════════════════════════════════════════════════════════════

_FB_CACHE = {
    "date": "",
    "match_picks": {},      # {match: [top picks]}
    "accumulators": [],     # [acca dict with code]
    "last_run": None,
    "last_run_ts": 0,       # epoch of last scan; anchors the scheduler + survives restarts via DB restore
    "running": False,
}


# ═══════════════════════════════════════════════════════════════════
# DB SETUP
# ═══════════════════════════════════════════════════════════════════

def fb_init_db(get_db):
    """Create football tables. Safe to call repeatedly."""
    try:
        conn = get_db()
        conn.run("""
            CREATE TABLE IF NOT EXISTS football_picks (
                id SERIAL PRIMARY KEY,
                match_date DATE,
                home TEXT, away TEXT, league TEXT,
                market_type TEXT, pick TEXT,
                confidence REAL, odds REAL,
                reasoning TEXT,
                kickoff_ts BIGINT,
                result TEXT DEFAULT 'pending',
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.run("""
            CREATE TABLE IF NOT EXISTS sportybet_accumulators (
                id SERIAL PRIMARY KEY,
                match_date DATE,
                tier TEXT, label TEXT,
                target_odds REAL, total_odds REAL,
                num_selections INT,
                selections_json TEXT,
                sportybet_code TEXT,
                status TEXT DEFAULT 'pending',
                result TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.run("""
            CREATE TABLE IF NOT EXISTS pick_results (
                id SERIAL PRIMARY KEY,
                match_date DATE,
                tier TEXT, total_picks INT, hits INT,
                won BOOLEAN, total_odds REAL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Migrations — in case an older football_picks/accumulators table exists
        # from a previous session with a different schema.
        _fp_cols = [
            ("match_date", "DATE"), ("home", "TEXT"), ("away", "TEXT"),
            ("league", "TEXT"), ("market_type", "TEXT"), ("pick", "TEXT"),
            ("confidence", "REAL"), ("odds", "REAL"), ("reasoning", "TEXT"),
            ("kickoff_ts", "BIGINT"),
            ("result", "TEXT DEFAULT 'pending'"), ("created_at", "TIMESTAMPTZ DEFAULT NOW()"),
        ]
        for col, typ in _fp_cols:
            try:
                conn.run("ALTER TABLE football_picks ADD COLUMN IF NOT EXISTS {} {}".format(col, typ))
            except Exception:
                pass
        _acc_cols = [
            ("match_date", "DATE"), ("tier", "TEXT"), ("label", "TEXT"),
            ("target_odds", "REAL"), ("total_odds", "REAL"), ("num_selections", "INT"),
            ("selections_json", "TEXT"), ("sportybet_code", "TEXT"),
            ("status", "TEXT DEFAULT 'pending'"), ("result", "TEXT"),
            ("run_id", "TEXT"),
            # pending_reason: structured human-readable explanation for WHY a slip is
            # still pending after a settle attempt. Updated on every settle tick.
            # Cleared when the slip is finally graded (won/lost/void).
            ("pending_reason", "TEXT"),
            ("settle_last_attempt", "BIGINT"),
            ("created_at", "TIMESTAMPTZ DEFAULT NOW()"),
        ]
        for col, typ in _acc_cols:
            try:
                conn.run("ALTER TABLE sportybet_accumulators ADD COLUMN IF NOT EXISTS {} {}".format(col, typ))
            except Exception:
                pass
        conn.close()
        print("[FB] DB tables ready")
    except Exception as e:
        print("[FB] DB init error: {}".format(e))


def _fb_recent_session_games(get_db):
    """Return the set of games used in the most recent run, so the next session
    can avoid repeating them. Reads the latest run_id's selections from the
    sportybet_accumulators table. Safe-returns an empty set on any error."""
    games = set()
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT selections_json FROM sportybet_accumulators "
            "WHERE run_id = (SELECT run_id FROM sportybet_accumulators "
            "ORDER BY created_at DESC LIMIT 1)")
        conn.close()
        for r in (rows or []):
            try:
                for s in json.loads(r[0] or "[]"):
                    m = s.get("match")
                    if m:
                        games.add(m)
            except Exception:
                continue
    except Exception as e:
        print("[FB] recent-session lookup error: {}".format(e))
    return games


def fb_save_run(get_db, date_str, all_picks, accumulators):
    """Persist the day's picks and accumulators."""
    try:
        conn = get_db()
        today = _dt.date.today()
        # Save top picks (limit to keep DB lean)
        for p in all_picks[:200]:
            conn.run("""INSERT INTO football_picks
                (match_date, home, away, league, market_type, pick, confidence, odds, reasoning, kickoff_ts)
                VALUES (:d,:h,:a,:l,:mt,:pk,:cf,:od,:rs,:kt)""",
                d=today, h=p["home"], a=p["away"], l=p["league"],
                mt=p["market_type"], pk=p["pick"], cf=p["confidence"],
                od=p["odds"], rs=p["reasoning"], kt=int(p.get("kickoff_ts") or 0))
        # Save accumulators — tag every slip in this run with one shared run_id
        # so the history view can group them as a single "set".
        run_id = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for acca in accumulators:
            if not acca:
                continue
            conn.run("""INSERT INTO sportybet_accumulators
                (match_date, tier, label, target_odds, total_odds, num_selections,
                 selections_json, sportybet_code, run_id)
                VALUES (:d,:t,:lb,:tg,:to,:ns,:sj,:cd,:ri)""",
                d=today, t=acca["tier"], lb=acca["label"],
                tg=acca["target_odds"], to=acca["total_odds"],
                ns=acca["num_selections"],
                sj=json.dumps([{"match": s["match"], "pick": s["pick"],
                                "odds": s["odds"], "confidence": s["confidence"],
                                "market_type": s.get("market_type", ""),
                                "home": s.get("home", ""), "away": s.get("away", ""),
                                "sb_event_id": s.get("sb_event_id", ""),
                                "kickoff_ts": s.get("kickoff_ts", 0),
                                "result": s.get("result", "pending")}
                               for s in acca["selections"]]),
                cd=acca.get("code"), ri=run_id)
        conn.close()
    except Exception as e:
        print("[FB] save error: {}".format(e))


def fb_load_latest(get_db):
    """Repopulate the in-memory cache from the most recent run already saved in
    the DB, so a redeploy shows the last scan immediately WITHOUT re-scraping.
    Returns True if a run was restored, else False (caller then scans). Fully
    defensive: any failure -> False -> normal boot scan, so it can't make a
    deploy worse than today's behaviour."""
    try:
        conn = get_db()
        # Accumulators from the latest run_id (ascending odds so the page lists
        # banker -> moonshot, matching a live run).
        accs = conn.run(
            "SELECT tier, label, target_odds, total_odds, num_selections, "
            "selections_json, sportybet_code, match_date, created_at "
            "FROM sportybet_accumulators "
            "WHERE run_id = (SELECT run_id FROM sportybet_accumulators "
            "ORDER BY created_at DESC LIMIT 1) ORDER BY total_odds ASC")
        accumulators = []
        latest_date = None
        latest_ts = None
        for r in (accs or []):
            tier, label, tgt, tot, ns, sj, code, mdate, created = r
            try:
                sels = json.loads(sj or "[]")
            except Exception:
                sels = []
            if not sels:
                continue
            accumulators.append({
                "tier": tier, "label": label,
                "emoji": TIER_CONFIG.get(tier, {}).get("emoji", "🟢"),
                "target_odds": tgt, "total_odds": tot,
                "num_selections": ns or len(sels),
                "selections": sels, "code": code or None,
            })
            if mdate is not None and latest_date is None:
                latest_date = mdate
            if created is not None and latest_ts is None:
                latest_ts = created

        # Per-match picks from the latest match_date (drives /app/picks + /app/cards).
        prows = conn.run(
            "SELECT home, away, league, market_type, pick, confidence, odds, "
            "reasoning, kickoff_ts FROM football_picks "
            "WHERE match_date = (SELECT MAX(match_date) FROM football_picks)")
        conn.close()
        by_match = {}
        for r in (prows or []):
            home, away, league, mt, pick, conf, odds, reason, kts = r
            label = "{} vs {}".format(home, away)
            by_match.setdefault(label, []).append({
                "match": label, "home": home, "away": away, "league": league or "",
                "market_type": mt or "", "pick": pick or "",
                "confidence": float(conf or 0), "odds": float(odds or 0),
                "reasoning": reason or "", "kickoff_ts": int(kts or 0),
                "result": "pending",
            })
        match_picks = {}
        for m, ps in by_match.items():
            ps.sort(key=lambda x: x["confidence"], reverse=True)
            match_picks[m] = ps[:3]

        if not accumulators and not match_picks:
            return False

        date_human = ""
        if latest_date is not None:
            try:
                date_human = latest_date.strftime("%A, %B %d, %Y")
            except Exception:
                date_human = str(latest_date)
        _FB_CACHE["date"] = date_human or _fb_today_human()
        _FB_CACHE["match_picks"] = match_picks
        _FB_CACHE["accumulators"] = accumulators
        if latest_ts is not None:
            try:
                _FB_CACHE["last_run_ts"] = latest_ts.timestamp()
                _FB_CACHE["last_run"] = (latest_ts.replace(tzinfo=None)
                                         if hasattr(latest_ts, "tzinfo") else latest_ts)
            except Exception:
                pass
        print("[FB] restored last run from DB: {} codes, {} matches, date={}".format(
            len(accumulators), len(match_picks), _FB_CACHE["date"]))
        return True
    except Exception as e:
        print("[FB] restore error: {}".format(e))
        return False


# ═══════════════════════════════════════════════════════════════════
# SETTLEMENT — mark accumulators won/lost from final scores
# ═══════════════════════════════════════════════════════════════════

_FB_STATS_CACHE = {}  # espn_id -> {"corners": int|None, "cards": int|None}


def _espn_match_stats(slug, espn_id):
    """Fetch corner-kick + card totals for a finished game from ESPN's summary
    endpoint (same free, keyless API as the scoreboard). Returns
    {"corners": int|None, "cards": int|None} or None. Cached per event."""
    if not slug or not espn_id or _req is None:
        return None
    key = str(espn_id)
    if key in _FB_STATS_CACHE:
        return _FB_STATS_CACHE[key]
    url = ("https://site.api.espn.com/apis/site/v2/sports/soccer/{}"
           "/summary?event={}".format(slug, espn_id))
    corners = cards = None
    try:
        r = _req.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            _FB_STATS_CACHE[key] = None
            return None
        data = r.json()
        # boxscore.teams[].statistics[] -> match by name OR label (defensive:
        # ESPN soccer uses name='wonCorners'/'yellowCards'/'redCards', but we
        # also match the human label so a field rename can't silently break us).
        c_tot = y_tot = r_tot = 0
        found_c = found_card = False
        for team in ((data.get("boxscore") or {}).get("teams") or []):
            for st in (team.get("statistics") or []):
                nm = str(st.get("name") or "").lower()
                lbl = str(st.get("label") or st.get("displayName") or "").lower()
                val = st.get("displayValue") or st.get("value")
                try:
                    n = int(float(str(val)))
                except (ValueError, TypeError):
                    continue
                if "woncorners" in nm or "corner" in lbl:
                    c_tot += n; found_c = True
                elif "yellowcard" in nm or "yellow card" in lbl:
                    y_tot += n; found_card = True
                elif "redcard" in nm or "red card" in lbl:
                    r_tot += n; found_card = True
        corners = c_tot if found_c else None
        cards = (y_tot + r_tot) if found_card else None
        # Goal timeline → did each side EVER lead? (for airtight 1UP settlement)
        # ESPN keyEvents carry the running homeScore/awayScore after each goal.
        esp_home_led = esp_away_led = None
        kev = data.get("keyEvents") or data.get("commentary") or []
        if kev:
            esp_home_led = esp_away_led = False
            for e in kev:
                is_goal = (e.get("scoringPlay") is True
                           or "goal" in str((e.get("type") or {}).get("text", "")).lower())
                if not is_goal:
                    continue
                try:
                    hsc = int(e.get("homeScore")); asc = int(e.get("awayScore"))
                except (TypeError, ValueError):
                    continue
                if hsc > asc:
                    esp_home_led = True
                if asc > hsc:
                    esp_away_led = True
    except Exception:
        _FB_STATS_CACHE[key] = None
        return None
    res = {"corners": corners, "cards": cards,
           "home_ever_led": esp_home_led, "away_ever_led": esp_away_led}
    _FB_STATS_CACHE[key] = res
    return res


def _fb_settle_stat_pick(market_type, stats):
    """Grade a corners_*/cards_* pick from ESPN match stats. True/False/None."""
    if not stats:
        return None
    mt = market_type
    m = _sports_re.match(r'^corners_(over|under)_(\d+(?:\.\d+)?)$', mt)
    if m:
        c = stats.get("corners")
        if c is None:
            return None
        return c > float(m.group(2)) if m.group(1) == "over" else c < float(m.group(2))
    m = _sports_re.match(r'^cards_(over|under)_(\d+(?:\.\d+)?)$', mt)
    if m:
        c = stats.get("cards")
        if c is None:
            return None
        return c > float(m.group(2)) if m.group(1) == "over" else c < float(m.group(2))
    # 1UP — settle from the goal timeline: did the side EVER lead by one?
    m = _sports_re.match(r'^oneup_(home|away)$', mt)
    if m:
        return (stats.get("home_ever_led") if m.group(1) == "home"
                else stats.get("away_ever_led"))
    # DC-1UP on a non-trivial result: 1X/X2 win via the team's early payout
    if mt == "dc1up_1x":
        return bool(stats.get("home_ever_led"))
    if mt == "dc1up_x2":
        return bool(stats.get("away_ever_led"))
    return None


def _fb_settle_pick(market_type, pick_text, hs, aw, h1h=None, h1a=None):
    """Evaluate a pick against a final score (and 1st-half score if available).
    Returns True/False/None (can't grade from available data).
    h1h/h1a = 1st-half home/away goals (optional; only needed for half markets)."""
    total = hs + aw
    margin = hs - aw  # home margin (negative => away ahead)
    mt = market_type
    _re = _sports_re

    # ── core result markets ──
    if mt == "home_win":            return hs > aw
    if mt == "away_win":            return aw > hs
    if mt == "draw":                return hs == aw
    if mt == "double_chance_1X":    return hs >= aw
    if mt == "double_chance_X2":    return aw >= hs
    if mt == "btts_yes":            return hs > 0 and aw > 0
    if mt == "btts_no":             return not (hs > 0 and aw > 0)
    if mt == "home_win_btts":       return hs > aw and hs > 0 and aw > 0
    if mt == "home_win_over_2.5":   return hs > aw and total > 2.5
    if mt == "dc_over_1.5":         return hs >= aw and total > 1.5

    # ── draw no bet (stake back on a draw -> VOID leg, removed from slip) ──
    if mt == "dnb_home":            return "void" if hs == aw else hs > aw
    if mt == "dnb_away":            return "void" if hs == aw else aw > hs

    # ── over/under goals, ANY line (over_0.5 ... over_5.5, under_1.5 ...) ──
    m = _re.match(r'^over_(\d+(?:\.\d+)?)$', mt)
    if m:                           return total > float(m.group(1))
    m = _re.match(r'^under_(\d+(?:\.\d+)?)$', mt)
    if m:                           return total < float(m.group(1))

    # ── handicap (-1.5 lines) ──
    if mt == "handicap_home_-1.5":  return margin > 1.5
    if mt == "handicap_away_-1.5":  return (-margin) > 1.5

    # ── multigoal range (board): "1-3 total goals" etc. ──
    m = _re.match(r'^multigoal_(\d+)_(\d+)$', mt)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return lo <= total <= hi

    # ── team totals (board): team_home_over_2.5 / team_away_under_1.5 ──
    m = _re.match(r'^team_(home|away)_(over|under)_(\d+(?:\.\d+)?)$', mt)
    if m:
        g = hs if m.group(1) == "home" else aw
        line = float(m.group(3))
        return g > line if m.group(2) == "over" else g < line

    # ── winning margin (board): winmargin_home_2p (by 2+), _2e (exactly 2) ──
    m = _re.match(r'^winmargin_(home|away)_(\d+)(p|e)$', mt)
    if m:
        mar = margin if m.group(1) == "home" else -margin
        n = int(m.group(2))
        return mar >= n if m.group(3) == "p" else mar == n

    # ── lead-by-N (board): final-margin proxy, leadby_home_2_yes ──
    m = _re.match(r'^leadby_(home|away)_(\d+)_(yes|no)$', mt)
    if m:
        mar = margin if m.group(1) == "home" else -margin
        hit = mar >= int(m.group(2))
        return hit if m.group(3) == "yes" else not hit

    # ── 1UP early-payout (board): oneup_home / oneup_away ──
    m = _re.match(r'^oneup_(home|away)$', mt)
    if m:
        won = (hs > aw) if m.group(1) == "home" else (aw > hs)
        # A final win guarantees they led; a draw/loss can't be confirmed either
        # way from the final score alone (they may have led then been pegged back).
        return True if won else None

    # ── Double-Chance 1UP (board): dc1up_1x / dc1up_x2 / dc1up_12 ──
    if mt == "dc1up_1x":
        return True if hs >= aw else None    # else: won iff home ever led (timeline)
    if mt == "dc1up_x2":
        return True if aw >= hs else None    # else: won iff away ever led (timeline)
    if mt == "dc1up_12":
        return (hs != aw) or (hs > 0)        # loses only on 0-0 (any goal => a side led)

    # ── win to nil (board): tonil_home_y / _n ──
    m = _re.match(r'^tonil_(home|away)_(y|n)$', mt)
    if m:
        hit = (hs > aw and aw == 0) if m.group(1) == "home" else (aw > hs and hs == 0)
        return hit if m.group(2) == "y" else not hit

    # ── clean sheet (board): cleansheet_home_y / _n ──
    m = _re.match(r'^cleansheet_(home|away)_(y|n)$', mt)
    if m:
        hit = (aw == 0) if m.group(1) == "home" else (hs == 0)
        return hit if m.group(2) == "y" else not hit

    # ── correct score ──
    if mt == "correct_score":
        m = _re.search(r'(\d+)-(\d+)', pick_text or "")
        if m:
            return hs == int(m.group(1)) and aw == int(m.group(2))
        return None

    # ── 1st-half markets: only gradeable when half-time score is known ──
    if h1h is not None and h1a is not None:
        h1total = h1h + h1a
        m = _re.match(r'^fh_over_(\d+(?:\.\d+)?)$', mt)
        if m:                       return h1total > float(m.group(1))
        m = _re.match(r'^fh_under_(\d+(?:\.\d+)?)$', mt)
        if m:                       return h1total < float(m.group(1))
        m = _re.match(r'^winhalf_(home|away)_(1sthalf|2ndhalf|eitherhalf|bothhalves)_(y|n)$', mt)
        if m:
            h2h, h2a = hs - h1h, aw - h1a            # 2nd half = full - 1st
            if m.group(1) == "home":
                w1, w2 = h1h > h1a, h2h > h2a
            else:
                w1, w2 = h1a > h1h, h2a > h2h
            kind = m.group(2)
            hit = (w1 if kind == "1sthalf" else w2 if kind == "2ndhalf"
                   else (w1 or w2) if kind == "eitherhalf" else (w1 and w2))
            return hit if m.group(3) == "y" else not hit

    # corners_* / cards_* (and half markets without HT data) can't be graded
    return None


def sb_get_event_result(event_id, home, away):
    """
    Fallback: fetch score for a SportyBet event that's still listed (live or
    just-finished). SportyBet drops games soon after they end, so this only
    catches in-play/just-ended games — ESPN is the primary settlement source.
    """
    try:
        sb_search_event(home, away)
    except Exception:
        pass
    info = _SB_EVENT_INFO.get(event_id, {})
    hs, aw = info.get("home_score"), info.get("away_score")
    status = str(info.get("match_status") or "").lower()
    st = info.get("status")
    finished = ("end" in status or "ft" in status or "finish" in status
                or st in (3, 4, 100))
    if hs is not None and aw is not None:
        try:
            return int(hs), int(aw), finished
        except (ValueError, TypeError):
            return None
    return None


# ── ESPN scoreboard: the match-status + score feed (free, no key, keeps
#    finished games — unlike SportyBet which drops them, and livescore.com
#    whose own API is body-encrypted). Powers league labels, live scores,
#    in-play status AND settlement. One scoreboard call per league per date. ──
ESPN_SOCCER_LEAGUES = [
    "eng.1", "eng.2", "esp.1", "esp.2", "ita.1", "ita.2", "ger.1", "ger.2",
    "fra.1", "fra.2", "ned.1", "por.1", "sco.1", "tur.1", "bel.1", "gre.1",
    "usa.1", "mex.1", "aut.1", "sui.1", "rus.1", "ukr.1",
    "uefa.champions", "uefa.europa", "uefa.europa_conf", "uefa.nations",
    "fifa.friendly", "fifa.friendly.w", "fifa.world",
    "fifa.worldq.uefa", "fifa.worldq.conmebol", "fifa.worldq.concacaf",
    "fifa.worldq.afc", "fifa.worldq.caf", "fifa.worldq.ofc",
    "conmebol.libertadores", "conmebol.sudamericana", "conmebol.america",
    "afc.cup", "aus.1", "jpn.1", "bra.1", "arg.1", "col.1", "chi.1",
]

ESPN_LEAGUE_NAMES = {
    "eng.1": "Premier League", "eng.2": "Championship",
    "esp.1": "LaLiga", "esp.2": "LaLiga 2", "ita.1": "Serie A", "ita.2": "Serie B",
    "ger.1": "Bundesliga", "ger.2": "Bundesliga 2", "fra.1": "Ligue 1", "fra.2": "Ligue 2",
    "ned.1": "Eredivisie", "por.1": "Primeira Liga", "sco.1": "Scottish Prem",
    "tur.1": "Süper Lig", "bel.1": "Belgian Pro", "gre.1": "Super League Greece",
    "usa.1": "MLS", "mex.1": "Liga MX", "aut.1": "Austrian Bundesliga",
    "sui.1": "Swiss Super League", "rus.1": "Russian Premier", "ukr.1": "Ukrainian Premier",
    "uefa.champions": "Champions League", "uefa.europa": "Europa League",
    "uefa.europa_conf": "Conference League", "uefa.nations": "Nations League",
    "fifa.friendly": "International Friendly", "fifa.friendly.w": "Int'l Friendly (W)",
    "fifa.world": "World Cup",
    "fifa.worldq.uefa": "WC Qualifier (UEFA)", "fifa.worldq.conmebol": "WC Qualifier (CONMEBOL)",
    "fifa.worldq.concacaf": "WC Qualifier (CONCACAF)", "fifa.worldq.afc": "WC Qualifier (AFC)",
    "fifa.worldq.caf": "WC Qualifier (CAF)", "fifa.worldq.ofc": "WC Qualifier (OFC)",
    "conmebol.libertadores": "Copa Libertadores", "conmebol.sudamericana": "Copa Sudamericana",
    "conmebol.america": "Copa América", "afc.cup": "AFC Cup", "aus.1": "A-League",
    "jpn.1": "J1 League", "bra.1": "Brasileirão", "arg.1": "Liga Argentina",
    "col.1": "Liga Colombiana", "chi.1": "Primera Chile",
}


def _espn_scoreboard(slug, yyyymmdd):
    """One ESPN league+date scoreboard. Returns rich game dicts:
    {home, away, hs, aw, state(pre|in|post), completed, detail, league}."""
    if _req is None:
        return []
    url = ("https://site.api.espn.com/apis/site/v2/sports/soccer/{}"
           "/scoreboard?dates={}".format(slug, yyyymmdd))
    out = []
    league_name = ESPN_LEAGUE_NAMES.get(slug, slug)
    try:
        r = _req.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return []
        data = r.json()
        # ESPN also reports a precise league name at the top level
        try:
            lg = (data.get("leagues") or [{}])[0]
            league_name = lg.get("name") or lg.get("abbreviation") or league_name
        except Exception:
            pass
        for ev in data.get("events", []):
            comp = (ev.get("competitions") or [{}])[0]
            stype = ((ev.get("status") or {}).get("type")) or {}
            state = stype.get("state") or ""          # pre | in | post
            completed = bool(stype.get("completed"))
            detail = stype.get("shortDetail") or stype.get("detail") or ""
            home = away = None
            hs = aw = None
            h1h = h1a = None
            for c in comp.get("competitors", []):
                team = c.get("team") or {}
                nm = team.get("displayName") or team.get("name") or team.get("shortDisplayName") or ""
                sc = c.get("score")
                # 1st-half goals from per-period linescores ([0] = 1st half)
                h1 = None
                ls = c.get("linescores") or []
                if ls:
                    try:
                        h1 = int(float(ls[0].get("value")))
                    except (ValueError, TypeError, AttributeError, IndexError):
                        h1 = None
                if c.get("homeAway") == "home":
                    home, hs, h1h = nm, sc, h1
                else:
                    away, aw, h1a = nm, sc, h1
            if not (home and away):
                continue
            try:
                hs_i = int(hs) if hs is not None else None
                aw_i = int(aw) if aw is not None else None
            except (ValueError, TypeError):
                hs_i = aw_i = None
            out.append({"home": home, "away": away, "hs": hs_i, "aw": aw_i,
                        "h1h": h1h, "h1a": h1a,
                        "state": state, "completed": completed,
                        "detail": detail, "league": league_name,
                        "espn_id": ev.get("id"), "espn_slug": slug})
    except Exception:
        return []
    return out


def _thesportsdb_day(yyyymmdd):
    """All soccer for a day from TheSportsDB (free, broad lower-league + friendly
    coverage, one call per day). Returns rich game dicts like _espn_scoreboard."""
    if _req is None:
        return []
    d = "{}-{}-{}".format(yyyymmdd[:4], yyyymmdd[4:6], yyyymmdd[6:8])
    url = "https://www.thesportsdb.com/api/v1/json/3/eventsday.php?d={}&s=Soccer".format(d)
    out = []
    try:
        r = _req.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return []
        for ev in (r.json().get("events") or []):
            home = ev.get("strHomeTeam")
            away = ev.get("strAwayTeam")
            if not (home and away):
                continue
            status = str(ev.get("strStatus") or "").lower().strip()
            hs, aw = ev.get("intHomeScore"), ev.get("intAwayScore")
            try:
                hs_i = int(hs) if hs not in (None, "") else None
                aw_i = int(aw) if aw not in (None, "") else None
            except (ValueError, TypeError):
                hs_i = aw_i = None
            completed = status in ("match finished", "ft", "finished", "aet", "pen",
                                   "after extra time", "after penalties")
            if status in ("1h", "2h", "ht", "live", "et", "bt", "p"):
                state = "in"
            elif completed:
                state = "post"
            else:
                state = "pre"
            out.append({"home": home, "away": away, "hs": hs_i, "aw": aw_i,
                        "state": state, "completed": completed,
                        "detail": ev.get("strStatus") or "",
                        "league": ev.get("strLeague") or ""})
    except Exception:
        return []
    return out


def _livescore_day(yyyymmdd):
    """All football for a day from Livescore.com's PUBLIC json endpoint (no key,
    not the encrypted CDN one) — broadest single-call coverage incl. lower
    leagues and friendlies. Endpoint pattern per the Simatwa/livescore-api repo.
    Returns rich game dicts like the other feeds."""
    if _req is None:
        return []
    url = ("https://prod-public-api.livescore.com/v1/api/app/date/soccer/{}/3?MD=1".format(yyyymmdd))
    out = []
    try:
        r = _req.get(url, timeout=12, headers={
            "User-Agent": "Mozilla/5.0", "Referer": "https://www.livescore.com/"})
        if r.status_code != 200:
            return []
        data = r.json()
        for stage in (data.get("Stages") or []):
            league = stage.get("Snm") or ""
            country = stage.get("Cnm") or ""
            if country and country.lower() not in league.lower():
                league_full = "{}: {}".format(country, league)
            else:
                league_full = league
            for ev in (stage.get("Events") or []):
                t1 = ev.get("T1") or [{}]
                t2 = ev.get("T2") or [{}]
                home = (t1[0].get("Nm") if t1 else "") or ""
                away = (t2[0].get("Nm") if t2 else "") or ""
                if not (home and away):
                    continue
                hs, aw = ev.get("Tr1"), ev.get("Tr2")
                try:
                    hs_i = int(hs) if hs not in (None, "") else None
                    aw_i = int(aw) if aw not in (None, "") else None
                except (ValueError, TypeError):
                    hs_i = aw_i = None
                eps = str(ev.get("Eps") or "").upper().strip()
                completed = eps in ("FT", "AET", "AP", "PEN", "FT_PEN", "AWD", "ABD")
                if eps in ("NS", "", "POSTP", "CANC", "TBD"):
                    state = "pre"
                elif completed:
                    state = "post"
                else:
                    state = "in"      # HT or a live minute like "63'"
                out.append({"home": home, "away": away, "hs": hs_i, "aw": aw_i,
                            "state": state, "completed": completed,
                            "detail": ("FT" if state == "post" else eps),
                            "league": league_full})
    except Exception:
        return []
    return out


def _fb_espn_index(dates):
    """Rich index of all games (pre/in/post) for given dates. Livescore.com is
    the primary feed (one broad call/day); TheSportsDB + ESPN fill any gaps."""
    index = []
    ls_n = tsdb_n = espn_n = 0
    for d in dates:
        ls = _livescore_day(d)
        if ls:
            index.extend(ls)
            ls_n += len(ls)
        time.sleep(0.1)
        rows = _thesportsdb_day(d)
        if rows:
            index.extend(rows)
            tsdb_n += len(rows)
        time.sleep(0.1)
        for slug in ESPN_SOCCER_LEAGUES:
            erows = _espn_scoreboard(slug, d)
            if erows:
                index.extend(erows)
                espn_n += len(erows)
            time.sleep(0.04)
    fin = sum(1 for r in index if r.get("completed"))
    live = sum(1 for r in index if r.get("state") == "in")
    print("[FB] score index: {} games ({} finished, {} live) "
          "[Livescore {}, TheSportsDB {}, ESPN {}] across {} dates".format(
              len(index), fin, live, ls_n, tsdb_n, espn_n, len(dates)))
    return index


# back-compat alias used by the settler
def _fb_build_score_index(dates):
    return _fb_espn_index(dates)


def _fb_norm_team(s):
    """Aggressive normalization to maximize fuzzy-match recall while leaving
    distinguishing words intact.  Strips:
      - common club tokens at either end (FC, CF, SC, AC, AFC, AS, CD, UD, EC,
        FK, SK, BK, CK, KS, VfB, VfL, SV, KV, SF, RB, OFK, NK, MFK, RC, SD …)
      - founding-year tokens (any 4-digit number 18XX–20XX)
      - parenthesized state/country codes ("(GO)", "(SP)", "(BR)" …)
      - filler words (and, de, of, the, do, da, la, le, el, l')
      - trailing reserve markers (B / II / 2 / Res / Reserves)
      - punctuation and accents (Latin, Slavic, Turkish)
    DOES NOT strip age-group / gender markers (U17/U21/W/Women) — those are
    *meaningful* distinctions: "Croatia" vs "Croatia U21" must not collide."""
    import re
    s = (s or "").lower().strip()
    # parenthesized regional codes — "(GO)", "(SP)", "( br )", etc.
    s = re.sub(r"\(\s*[a-z]{2,3}\s*\)", " ", s)
    # founding years inside the name.
    s = re.sub(r"\b(18\d\d|19\d\d|20\d\d)\b", " ", s)
    # Punctuation → space
    s = (s.replace("&", " and ")
           .replace("-", " ").replace("/", " ").replace(",", " ")
           .replace(":", " ").replace(".", " ").replace("'", "")
           .replace("\u2019", "").replace("`", ""))
    # Latin + Slavic + Turkish accent fold (do this BEFORE token filter so the
    # filter words match in their ASCII form).
    accent_map = {
        "á": "a", "à": "a", "â": "a", "ä": "a", "ã": "a", "å": "a",
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "í": "i", "ì": "i", "î": "i", "ï": "i", "ı": "i",
        "ó": "o", "ò": "o", "ô": "o", "ö": "o", "õ": "o", "ø": "o",
        "ú": "u", "ù": "u", "û": "u", "ü": "u",
        "ý": "y", "ÿ": "y",
        "ñ": "n", "ń": "n",
        "ç": "c", "ć": "c", "č": "c",
        "š": "s", "ş": "s",
        "ž": "z", "ż": "z", "ź": "z",
        "đ": "d", "ð": "d",
        "ğ": "g", "ł": "l",
    }
    for k, v in accent_map.items():
        if k in s:
            s = s.replace(k, v)
    # Token filter — drop standalone noise tokens at any position. This is more
    # robust than substring stripping because it handles BOTH prefixes
    # ("VfL Wolfsburg") AND suffixes ("Real Madrid CF") in one shot, and won't
    # accidentally cut into a real team-name substring.
    noise_tokens = {
        # Latin-script club abbreviations
        "fc", "cf", "sc", "ac", "afc", "as", "cd", "ud", "ec", "rc", "sd",
        # Nordic/Central European
        "fk", "sk", "bk", "ck", "gks", "ks", "nk", "mfk", "ofk", "hk", "ik",
        # German prefixes
        "vfb", "vfl", "sv", "kv", "sf", "rb", "tsv", "fsv",
        # Filler / decorative
        "club", "calcio", "sport", "athletic", "atletico",
        "and", "de", "do", "da", "of", "the",
        "la", "le", "el", "l",
    }
    tokens = [t for t in s.split() if t and t not in noise_tokens]
    s = " ".join(tokens)
    # strip trailing reserve markers (B-team) but only at the absolute end
    s = re.sub(r"\s+(b|ii|res|reserves|b\s*team)\s*$", "", s)
    return s.strip()


_FB_CANON = {}
for _canon, _variants in {
    "paris saint germain": ["psg", "paris sg", "paris s g", "paris"],
    "manchester united": ["man united", "man utd", "man u", "manchester utd"],
    "manchester city": ["man city"],
    "tottenham hotspur": ["tottenham", "spurs"],
    "internazionale": ["inter", "inter milan"],
    "wolverhampton wanderers": ["wolves", "wolverhampton"],
    "borussia monchengladbach": ["gladbach", "monchengladbach", "b monchengladbach"],
    "borussia dortmund": ["dortmund", "bvb"],
    "bayern munich": ["bayern", "bayern munchen"],
    "atletico madrid": ["atletico", "atletico de madrid", "atleti"],
    "real betis": ["betis"],
    "sporting cp": ["sporting", "sporting lisbon"],
    "saudi arabia": ["ksa"],
    "united states": ["usa", "usmnt"],
    "south korea": ["korea republic", "korea south"],
}.items():
    _FB_CANON[_canon] = _canon
    for _v in _variants:
        _FB_CANON[_v] = _canon


def _fb_canon(name):
    """Normalize + collapse known abbreviations to one canonical spelling so
    'Psg' matches 'Paris Saint-Germain', 'Man Utd' matches 'Manchester United'."""
    n = _fb_norm_team(name)
    return _FB_CANON.get(n, n)


_FB_AGE_GENDER_TOKENS = {
    # Youth categories (under-NN). Same age = compatible; different = NOT.
    "u15", "u16", "u17", "u18", "u19", "u20", "u21", "u23",
    # Gender markers — F/W is women's football; absence = men's/senior.
    "w", "women", "wom", "femenil", "femenino", "feminine", "ladies",
    # Other category markers
    "youth", "junior", "jr",
}


def _fb_age_gender_set(normalized_name):
    """Return the set of age/gender tokens present in a normalized team name.
    Empty set = senior men's team (the default).  Two team names are
    'age/gender compatible' if they have the same set."""
    return {t for t in normalized_name.split() if t in _FB_AGE_GENDER_TOKENS}


def _fb_teams_match(a, b):
    """Fuzzy team-name match tolerant of suffixes, accents, reserve sides,
    and common abbreviations (PSG, Man Utd, Spurs, ...).  Gated by an
    age/gender compatibility check: 'Brazil' never matches 'Brazil U17' or
    'Brazil Women' even though the substring is present."""
    ca, cb = _fb_canon(a), _fb_canon(b)
    if ca and cb and ca == cb:
        return True
    a, b = _fb_norm_team(a), _fb_norm_team(b)
    if not a or not b:
        return False
    # Age/gender disqualifier: if one name has a youth/women marker and the
    # other doesn't (or has a different one), they are different teams.
    ag_a, ag_b = _fb_age_gender_set(a), _fb_age_gender_set(b)
    if ag_a != ag_b:
        return False
    # Exact match.
    if a == b:
        return True
    # Substring match — only safe NOW that age/gender markers are equal on
    # both sides.  Without that gate, 'brazil' would be a substring of
    # 'brazil u17' and the matcher would return True for a totally different
    # game.
    if a in b or b in a:
        return True
    # Token overlap (≥4-char word) — last-resort fuzzy match.
    wa, wb = set(a.split()), set(b.split())
    return any(len(w) >= 4 and w in wb for w in wa)


def _fb_closest_team(name, index):
    """Best-guess feed spelling of a team, for diagnostics. Returns name or ''."""
    target = set(_fb_norm_team(name).split())
    best, best_score = "", 0
    seen = set()
    for g in index:
        for t in (g.get("home", ""), g.get("away", "")):
            if t in seen:
                continue
            seen.add(t)
            toks = set(_fb_norm_team(t).split())
            overlap = len(target & toks)
            if overlap > best_score:
                best, best_score = t, overlap
    return best if best_score > 0 else ""


def _fb_find_game(index, home, away):
    """Find a game in the index matching these teams. Returns the game dict
    (with scores possibly home/away-swapped to the caller's orientation)."""
    for g in index:
        if _fb_teams_match(home, g["home"]) and _fb_teams_match(away, g["away"]):
            return g
        if _fb_teams_match(home, g["away"]) and _fb_teams_match(away, g["home"]):
            sw = dict(g)
            sw["hs"], sw["aw"] = g["aw"], g["hs"]
            sw["home"], sw["away"] = g["away"], g["home"]
            sw["_swapped"] = True
            return sw
    return None


def _fb_lookup_score(index, home, away):
    """Final score for a FINISHED game matching these teams. -> (hs,aw) or None."""
    g = _fb_find_game(index, home, away)
    if g and g.get("completed") and g.get("hs") is not None and g.get("aw") is not None:
        return g["hs"], g["aw"]
    return None


# ── Live-status cache: refreshed in the background so the picks page can show
#    league + kickoff/LIVE/FT + current score without a slow API call per load.
_FB_LIVE = {"index": [], "ts": 0}


def _fb_live_status(home, away):
    """Return {league, state, detail, hs, aw} for a match, or None."""
    g = _fb_find_game(_FB_LIVE.get("index", []), home, away)
    return g


def _fb_live_refresh_thread():
    """Refresh the live match index (today ± 1) every ~3 minutes. Uses the cheap
    TheSportsDB feed (one call per day, all live/finished soccer) so the picks
    page can show league + LIVE/FT + scores without hammering ESPN."""
    def loop():
        time.sleep(45)
        while True:
            try:
                today = _dt.date.today()
                idx = []
                for i in (-1, 0, 1):
                    d = (today + _dt.timedelta(days=i)).strftime("%Y%m%d")
                    day = _livescore_day(d) or _thesportsdb_day(d)
                    idx.extend(day)
                    time.sleep(0.2)
                if idx:
                    _FB_LIVE["index"] = idx
                    _FB_LIVE["ts"] = int(time.time())
                    live = sum(1 for g in idx if g.get("state") == "in")
                    print("[FB] live index: {} games ({} live)".format(len(idx), live))
            except Exception as e:
                print("[FB] live refresh error: {}".format(e))
            time.sleep(180)
    threading.Thread(target=loop, daemon=True).start()
    print("[FB] live-status thread started (every 3min)")





def _fb_settle_accumulators(get_db):
    """Settle pending accumulators using ESPN final scores. Triggers off the
    slip's match_date (always stored) rather than per-leg kickoff timestamps."""
    today = _dt.date.today()
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT id, match_date, selections_json, result FROM sportybet_accumulators "
            "WHERE result IS NULL OR result = 'pending'")
        pend = [(r[0], r[1], r[2]) for r in rows]
        conn.close()
    except Exception as e:
        print("[FB] settle query error: {}".format(e))
        return

    if not pend:
        return

    # Which match-dates are due (game day is today or earlier)?
    def _as_date(v):
        if isinstance(v, _dt.date):
            return v
        try:
            return _dt.date.fromisoformat(str(v)[:10])
        except Exception:
            return None

    due_slips = []
    for acc_id, md, sj in pend:
        d = _as_date(md)
        if d and d <= today:
            due_slips.append((acc_id, d, sj))

    if not due_slips:
        return

    # Fetch scores for the dates the games are ACTUALLY on — derived from each
    # due slip's selection kickoff times — plus a band around today for live /
    # just-finished games and any legacy selection with no kickoff stored.
    # The old code only scanned today +/- 3, so a slip whose games were played
    # (or scheduled) 4+ days from today could never be found in the feed and sat
    # as "NOT IN FEED" forever. kickoff_ts is epoch-MILLISECONDS.
    date_set = set()
    floor_date = today - _dt.timedelta(days=10)  # older than this voids anyway
    ceil_date = today + _dt.timedelta(days=1)
    for _aid, _md, _sj in due_slips:
        try:
            for _s in (json.loads(_sj) if _sj else []):
                _ko = _s.get("kickoff_ts")
                if not _ko:
                    continue
                _gd = _dt.datetime.fromtimestamp(int(_ko) / 1000, _dt.timezone.utc).date()
                for _off in (-1, 0, 1):  # timezone + late-finish safety
                    _d = _gd + _dt.timedelta(days=_off)
                    if floor_date <= _d <= ceil_date:
                        date_set.add(_d.strftime("%Y%m%d"))
        except Exception:
            pass
    # Always include a band around today (live/just-finished + legacy slips).
    for _i in (-3, -2, -1, 0, 1):
        date_set.add((today + _dt.timedelta(days=_i)).strftime("%Y%m%d"))
    dates = sorted(date_set)
    print("[FB] settle: {} pending slips due, fetching scores for {} dates".format(
        len(due_slips), len(dates)))
    index = _fb_build_score_index(dates)
    if not index:
        print("[FB] settle: no finished games available yet")
        return

    settled_count = 0
    tally = {}
    for acc_id, md, sj in due_slips:
        try:
            sels = json.loads(sj) if sj else []
        except Exception:
            continue
        if not sels:
            continue

        any_lost = False
        all_known = True
        evaluable = 0
        ungradeable = 0  # match finished but this leg's market can't be graded
        voided = 0       # genuine void (e.g. draw-no-bet on a draw) — leg removed
        upcoming = []   # in the feed but not finished yet (game is future/live)
        absent = []     # not found in the feed at all (coverage / name mismatch)
        for s in sels:
            home, away = s.get("home", ""), s.get("away", "")
            score = _fb_lookup_score(index, home, away)
            if score is None:
                all_known = False
                g = _fb_find_game(index, home, away)
                label = s.get("match", "{} vs {}".format(home, away))
                if g:
                    st = g.get("state")
                    upcoming.append("{}[{}]".format(label, "live" if st == "in" else "upcoming"))
                else:
                    ch = _fb_closest_team(home, index)
                    ca = _fb_closest_team(away, index)
                    hint = ""
                    if ch or ca:
                        hint = " (closest in feed: {} / {})".format(ch or "?", ca or "?")
                    absent.append(label + hint)
                continue
            hs, aw = score
            g = _fb_find_game(index, home, away)
            h1h = g.get("h1h") if g else None
            h1a = g.get("h1a") if g else None
            mt = s.get("market_type", "")
            outcome = _fb_settle_pick(mt, s.get("pick", ""), hs, aw, h1h, h1a)
            if outcome is None and (mt.startswith("corners") or mt.startswith("cards")):
                # goal score can't grade these — pull corner/card stats from ESPN
                if g and g.get("espn_id"):
                    stats = _espn_match_stats(g.get("espn_slug"), g.get("espn_id"))
                    outcome = _fb_settle_stat_pick(mt, stats)
            if outcome is None and (mt.startswith("oneup") or mt.startswith("dc1up")):
                # 1UP / DC-1UP on a non-win: confirm from the goal timeline whether
                # the relevant side ever led (early payout would have triggered).
                if g and g.get("espn_id"):
                    stats = _espn_match_stats(g.get("espn_slug"), g.get("espn_id"))
                    if stats:
                        el_h, el_a = stats.get("home_ever_led"), stats.get("away_ever_led")
                        if g.get("_swapped"):
                            el_h, el_a = el_a, el_h
                            stats = dict(stats, home_ever_led=el_h, away_ever_led=el_a)
                        if mt == "oneup_home":
                            outcome = el_h
                        elif mt == "oneup_away":
                            outcome = el_a
                        else:  # dc1up_*
                            outcome = _fb_settle_stat_pick(mt, stats)
            if outcome == "void":
                # genuine void (stake-back) leg — remove it from the slip;
                # it neither wins nor loses and does not block a WON.
                s["result"] = "void"
                voided += 1
                continue
            if outcome is None:
                # match finished but this leg can't be graded (no stats /
                # half-no-HT). Outcome UNKNOWN — must block a WON so we never
                # call a slip won on legs we never actually checked.
                ungradeable += 1
                continue
            evaluable += 1
            s["result"] = "won" if outcome else "lost"
            if not outcome:
                any_lost = True

        new_result = None
        if any_lost:
            new_result = "lost"
        elif all_known and ungradeable == 0 and evaluable > 0:
            # WON only when every leg is accounted for: each leg either won or
            # was voided, all matches finished, no losses, >=1 real winning leg.
            new_result = "won"
        elif md < today - _dt.timedelta(days=7):
            # a week old and still unresolvable → stop showing pending
            new_result = "void"

        if new_result:
            try:
                conn = get_db()
                # Clear pending_reason on settlement — slip is now graded.
                conn.run("UPDATE sportybet_accumulators SET result=:r, selections_json=:sj, "
                         "pending_reason=NULL, settle_last_attempt=:ts WHERE id=:i",
                         r=new_result, sj=json.dumps(sels), i=acc_id,
                         ts=int(time.time()))
                conn.close()
                settled_count += 1
                tally[new_result] = tally.get(new_result, 0) + 1
                print("[FB] settle: slip {} -> {} ({} won, {} void, {} ungradeable of {} legs)".format(
                    acc_id, new_result.upper(), evaluable, voided, ungradeable, len(sels)))
            except Exception as e:
                print("[FB] settle update error: {}".format(e))
        else:
            bits = []
            if upcoming:
                bits.append("Not played yet: " + ", ".join(upcoming[:3]))
            if absent:
                bits.append("Not in score feed: " + ", ".join(absent[:3]))
            if ungradeable:
                bits.append("{} leg(s) finished but missing stats (corner/card/timeline data unavailable for the source ESPN feed)".format(ungradeable))
            reason_str = " · ".join(bits) if bits else None
            # Persist the reason so the codes page can show "why pending" to the
            # user without recomputing.  Also stamp settle_last_attempt so we
            # know how fresh the reason is.
            try:
                conn = get_db()
                conn.run("UPDATE sportybet_accumulators "
                         "SET pending_reason = :r, settle_last_attempt = :ts "
                         "WHERE id = :i",
                         r=reason_str, ts=int(time.time()), i=acc_id)
                conn.close()
            except Exception as e:
                # Persistence failure is non-fatal — log only.
                print("[FB] settle: pending_reason write failed for slip {}: {}".format(
                    acc_id, str(e)[:120]))
            if bits:
                print("[FB] settle: slip {} pending — {}".format(acc_id, " | ".join(bits)))

    if settled_count:
        print("[FB] Settled {} accumulators — {} won, {} lost, {} void".format(
            settled_count, tally.get("won", 0), tally.get("lost", 0), tally.get("void", 0)))
    else:
        print("[FB] settle: 0 newly settled (all due slips are for games not "
              "finished yet, or not found in the score feed)")


def fb_settle_thread(get_db):
    """Background thread: settle finished accumulators hourly."""
    def loop():
        _t = __import__("time")
        _t.sleep(180)
        while True:
            try:
                _fb_settle_accumulators(get_db)
            except Exception as e:
                print("[FB] settle thread error: {}".format(e))
            _t.sleep(3600)
    threading.Thread(target=loop, daemon=True).start()
    print("[FB] settle thread started (hourly)")


# ═══════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATION
# Assumes these are in namespace (inlined above in app.py):
#   build_fixture_dataset, analyze_fixture, build_all_accumulators,
#   top_picks_per_match, generate_code_for_accumulator,
#   tg_send (+ token/chat), fmt_codes, fmt_picks
# ═══════════════════════════════════════════════════════════════════

def _sofa_probe():
    """One diagnostic call so we KNOW the cause before buying anything:
    403/503 = Cloudflare IP-reputation block (needs residential proxy);
    200 = reachable (then any 0-hits is a parsing/coverage issue, not a block);
    exception = curl_cffi/transport problem."""
    url = "{}/search/all?q=arsenal".format(SOFA)
    try:
        r = _scrape_get(url, timeout=12)
        if r is None:
            print("[SOFA-PROBE] no response (curl_cffi+requests both failed)")
            return False
        body = ""
        try:
            body = (r.text or "")[:100].replace("\n", " ")
        except Exception:
            pass
        print("[SOFA-PROBE] status={} via={} body={}".format(
            r.status_code, "curl_cffi" if _cf else "requests", body))
        return r.status_code == 200
    except Exception as e:
        print("[SOFA-PROBE] exception: {}: {}".format(type(e).__name__, e))
        return False


def _fb_apply_methodology(fixtures):
    """Attach xG/PPDA (Understat) and possession (FBref) to the LIVE prediction
    fixtures — these were previously only added on the Sofascore path, which
    Railway can't reach, so the methodology never influenced real picks. Sources
    are name-matched (no IDs needed). Logs per-source hit counts so we can see
    what's actually reachable from the Railway IP. Sofascore last-10 form still
    needs a non-datacenter IP (proxy/VPS) and is handled separately."""
    if not fixtures:
        return
    sofa_ok = _sofa_probe()  # one diagnostic call; gates the form loop below

    # API-Football is a BUDGET-CAPPED FALLBACK (free tier): tips are primary, so
    # we only spend real-form calls on fixtures the tips couldn't read clearly
    # (neutral), and stop at the daily cap. Clear over/under matches cost 0 calls.
    apifb_hits = 0
    if _apifootball_setup():
        neutral = [fx for fx in fixtures if fx.get("_tip_neutral")]
        for fx in neutral:
            if _APIFB["today"] >= APIFB_DAILY_CAP:
                break
            try:
                hs = apifootball_form_stats(fx.get("home_team", ""))
                if hs:
                    fx["home_form_stats"] = hs
                    fx["_form_source"] = "api-football"
                as_ = apifootball_form_stats(fx.get("away_team", ""))
                if as_:
                    fx["away_form_stats"] = as_
                if hs or as_:
                    apifb_hits += 1
            except Exception:
                pass
        print("[APIFB] fallback: {}/{} neutral fixtures enriched ({} calls today, cap {})".format(
            apifb_hits, len(neutral), _APIFB["today"], APIFB_DAILY_CAP))

    def _us_key(lg):
        lg = (lg or "").lower()
        for k in UNDERSTAT_LEAGUES:
            if k.lower() in lg or k.lower().replace(" ", "") in lg.replace(" ", ""):
                return k
        return None

    fbref_stats = fbref_big5_possession()
    us_cache = {}
    for fx in fixtures:
        k = _us_key(fx.get("league", ""))
        if k and k not in us_cache:
            us_cache[k] = understat_team_xg(k)
            time.sleep(1.0)

    fb_hits = us_hits = 0
    for fx in fixtures:
        hf = fbref_lookup(fbref_stats, fx.get("home_team", ""))
        af = fbref_lookup(fbref_stats, fx.get("away_team", ""))
        if hf and hf.get("possession") is not None:
            fx["home_possession"] = hf["possession"]; fb_hits += 1
        if af and af.get("possession") is not None:
            fx["away_possession"] = af["possession"]
        lgxg = us_cache.get(_us_key(fx.get("league", "")), {})
        hx = _match_team_xg(lgxg, fx.get("home_team", ""))
        ax = _match_team_xg(lgxg, fx.get("away_team", ""))
        if hx:
            fx["home_xg_for"] = hx["xg_for"]; fx["home_xg_against"] = hx["xg_against"]
            if hx.get("ppda") is not None:
                fx["home_ppda"] = hx["ppda"]
            us_hits += 1
        if ax:
            fx["away_xg_for"] = ax["xg_for"]; fx["away_xg_against"] = ax["xg_against"]
            if ax.get("ppda") is not None:
                fx["away_ppda"] = ax["ppda"]

    # Sofascore last-10 form — the UNIVERSAL backbone (every league, incl.
    # internationals/lower divisions Understat & FBref don't cover). Needs a
    # team-ID search per side; track reachability separately from match count
    # so we can tell a Cloudflare block (sofa_reached=False) apart from a slate
    # with no Sofascore coverage.
    sofa_hits = 0
    sofa_reached = False
    sofa_attempts = 0
    # Only attempt the per-fixture Sofascore form pull if the probe got through.
    # It's confirmed Cloudflare-blocked from this IP, so skipping it saves ~30-60s
    # of guaranteed-fail calls per run; form_stats already come from predictions.
    for fx in (fixtures if sofa_ok else []):
        try:
            hid, hok = sofa_search_team(fx.get("home_team", ""))
            time.sleep(0.4)
            aid, aok = sofa_search_team(fx.get("away_team", ""))
            time.sleep(0.4)
            sofa_attempts += 1
            sofa_reached = sofa_reached or hok or aok
            if hid:
                hs = sofa_team_form_stats(hid)
                if hs:
                    fx["home_form_stats"] = hs
                fx["home_key_injuries"] = sofa_injuries(hid)
                time.sleep(0.4)
            if aid:
                as_ = sofa_team_form_stats(aid)
                if as_:
                    fx["away_form_stats"] = as_
                fx["away_key_injuries"] = sofa_injuries(aid)
                time.sleep(0.4)
            if fx.get("home_form_stats") or fx.get("away_form_stats"):
                sofa_hits += 1
        except Exception:
            pass

    print("[FB] methodology enrich: FBref {} hits, Understat {} hits, "
          "Sofascore {} (form_stats from predictions) across {} fixtures".format(
              fb_hits, us_hits,
              "{}/{} hits".format(sofa_hits, sofa_attempts) if sofa_ok else "skipped (blocked)",
              len(fixtures)))


def _fb_enrich_and_filter_upcoming(fixtures, max_lookup=150):
    """
    Look each fixture up on SportyBet and keep ONLY upcoming, not-started
    games that still have markets. SportyBet removes a game the moment it
    kicks off, so 'present on SportyBet with markets' == 'bettable'.

    Attaches sb_event_id + kickoff_ts to each surviving fixture.
    Returns (upcoming_fixtures, match->event_id cache for code-gen reuse).
    """
    import time as _t
    now_ms = int(_t.time() * 1000)
    upcoming = []
    cache = {}
    looked = 0
    for fx in fixtures:
        if looked >= max_lookup:
            break
        looked += 1
        try:
            eid = sb_search_event(fx["home_team"], fx["away_team"])
            _t.sleep(0.35)
        except Exception:
            eid = None
        if not eid:
            continue
        info = _SB_EVENT_INFO.get(eid, {})
        ko = info.get("kickoff_ts") or 0
        status = str(info.get("match_status") or "").lower()
        has_markets = bool(_SB_MARKET_CACHE.get(eid))
        # Keep only games that haven't started and still have markets
        not_started = ("not start" in status) or (status == "" and (not ko or now_ms < ko))
        future_ok = (not ko) or (now_ms < ko)
        # Reject far-future fixtures (e.g. next-season openers SportyBet lists
        # months out) — only keep games within the next ~4 days.
        near_term = (not ko) or (ko < now_ms + 4 * 24 * 3600 * 1000)
        if not has_markets or not not_started or not future_ok or not near_term:
            continue
        fx["sb_event_id"] = eid
        fx["kickoff_ts"] = ko
        cache["{} vs {}".format(fx["home_team"], fx["away_team"])] = eid
        upcoming.append(fx)
    print("[FB] {} upcoming bettable fixtures (looked up {} on SportyBet)".format(
        len(upcoming), looked))
    return upcoming, cache


def run_football_engine(get_db, tg_token, tg_chat, send_telegram,
                        generate_codes=True, announce=True):
    """
    The full daily pipeline. Designed to never crash — every stage is
    wrapped, and a failure in one stage doesn't kill the others.
    """
    if _FB_CACHE["running"]:
        print("[FB] engine already running, skip")
        return
    _FB_CACHE["running"] = True
    try:
        date_human = _dt.date.today().strftime("%A, %B %d, %Y")
        print("[FB] ═══ Engine run starting ({}) ═══".format(date_human))

        # 1. Scrape — PRIMARY: prediction scrapers (proven working on Railway).
        #    Sofascore is Cloudflare-blocked from datacenter IPs, so it's only a fallback.
        try:
            fixtures = _fb_fixtures_from_predictions()
            if not fixtures:
                print("[FB] prediction scrapers empty — trying Sofascore fallback")
                fixtures = build_fixture_dataset()
        except Exception as e:
            print("[FB] scrape failed: {}".format(e))
            fixtures = []

        if not fixtures:
            print("[FB] No fixtures — engine run aborted")
            _FB_CACHE["running"] = False
            return

        # 1b. Reset SportyBet caches, then enrich + filter to UPCOMING bettable
        #     games only. SportyBet drops games once they start, so this both
        #     gets kickoff times AND guarantees codes can map (markets exist).
        try:
            _SB_DIAG_COUNT[0] = 0
            _SB_MARKET_CACHE.clear()
            _SB_STRUCT_LOGGED[0] = False
            _SB_EVENT_INFO.clear()
        except Exception:
            pass

        event_cache = {}
        if generate_codes:
            try:
                fixtures, event_cache = _fb_enrich_and_filter_upcoming(fixtures)
            except Exception as e:
                print("[FB] enrich/filter error: {}".format(e))

        # 1c. Apply the methodology data (xG/PPDA/possession) to the LIVE fixtures
        #     — connects Understat + FBref to the picks that actually get bet.
        try:
            _fb_apply_methodology(fixtures)
        except Exception as e:
            print("[FB] methodology enrich error: {}".format(e))

        if not fixtures:
            print("[FB] No upcoming bettable fixtures right now — nothing to post")
            _FB_CACHE["running"] = False
            return

        # 1b. Attach the proven club model (corners/cards) BEFORE scoring, so
        #     board-explore can inject qualifying corner/card legs into the codes.
        try:
            _model_attach(fixtures)
        except Exception as e:
            print("[MODEL] attach run error: {}".format(e))

        # 2. Analyze
        all_picks = []
        for fx in fixtures:
            try:
                all_picks.extend(analyze_fixture(fx))
            except Exception as e:
                print("[FB] analyze error for {}: {}".format(fx.get("home_team"), e))
        print("[FB] Scored {} picks across {} fixtures".format(len(all_picks), len(fixtures)))

        if not all_picks:
            _FB_CACHE["running"] = False
            return

        # 2b. Keep only picks that map to a real SportyBet market on that event,
        #     so every accumulator leg produces a valid code (no dead legs from
        #     corners/cards/combos SportyBet doesn't offer for the game).
        build_picks = all_picks
        if generate_codes:
            mappable = []
            for p in all_picks:
                eid = p.get("sb_event_id")
                if not eid:
                    continue
                mkts = _SB_MARKET_CACHE.get(eid, [])
                if mkts and sb_map_pick_to_selection(p, mkts):
                    mappable.append(p)
            print("[FB] {} of {} picks are SportyBet-bettable".format(
                len(mappable), len(all_picks)))
            if mappable:
                build_picks = mappable

        # 3. Build accumulators (from bettable picks). Avoid the previous
        #    session's games so the 12h session 2 doesn't repeat session 1.
        try:
            avoid_prev = _fb_recent_session_games(get_db)
            if avoid_prev:
                print("[FB] avoiding {} games from previous session".format(len(avoid_prev)))
        except Exception:
            avoid_prev = set()
        try:
            acca_dict = build_all_accumulators(build_picks, avoid_games=avoid_prev)
        except Exception as e:
            print("[FB] accumulator build error: {}".format(e))
            acca_dict = {}

        accumulators = [acca_dict[k] for k in
                        ["2_odds", "3_odds", "5_odds", "10_odds", "1000_odds"]
                        if acca_dict.get(k)]

        # 4. Generate SportyBet codes (reusing the event cache from enrichment)
        if generate_codes:
            for acca in accumulators:
                try:
                    result = generate_code_for_accumulator(acca, event_cache)
                    acca["code"] = result.get("code")
                    acca["code_mapped"] = result.get("mapped", 0)
                    acca["code_total"] = result.get("total", 0)
                    if result.get("code"):
                        print("[FB] {} code: {} ({}/{} mapped)".format(
                            acca["tier"], result["code"], result["mapped"], result["total"]))
                    else:
                        print("[FB] {} code FAILED ({}/{} mapped, missing: {})".format(
                            acca["tier"], result.get("mapped", 0), result.get("total", 0),
                            result.get("missing", [])[:3]))
                except Exception as e:
                    print("[FB] code gen error for {}: {}".format(acca["tier"], e))
                    acca["code"] = None

        # 5. Cache + persist
        match_picks = top_picks_per_match(all_picks, 3)

        # 4b. Premium-match bet builders (top 2-3 rich-board matches, 3 correlated legs)
        bet_builders = []
        if generate_codes:
            try:
                bet_builders = _sb_build_bet_builders(build_picks, max_matches=3)
                print("[FB] bet builders: {} premium matches".format(len(bet_builders)))
                for b in bet_builders:
                    print("[FB] builder {} vs {} — {} legs, code={}, odds={}".format(
                        b["home"], b["away"], len(b["legs"]),
                        b.get("code") or "—", b.get("sb_odds") or b.get("est_odds")))
            except Exception as e:
                print("[FB] bet builder error: {}".format(e))

        _FB_CACHE["date"] = date_human
        _FB_CACHE["match_picks"] = match_picks
        _FB_CACHE["accumulators"] = accumulators
        _FB_CACHE["bet_builders"] = bet_builders
        _FB_CACHE["last_run"] = _dt.datetime.now()
        _FB_CACHE["last_run_ts"] = time.time()

        try:
            fb_save_run(get_db, date_human, all_picks, accumulators)
        except Exception as e:
            print("[FB] persist error: {}".format(e))

        # 6. Telegram announce
        if announce:
            try:
                msg = fmt_codes(accumulators, date_human)
                send_telegram(msg)
            except Exception as e:
                print("[FB] announce error: {}".format(e))
            if bet_builders:
                try:
                    bb_msg = fmt_bet_builders(bet_builders)
                    if bb_msg:
                        send_telegram(bb_msg)
                except Exception as e:
                    print("[FB] bet builder announce error: {}".format(e))

        # 7. Model extras (additive, isolated — proven club-data model)
        try:
            # Standalone MODEL EXTRAS cards are OFF — the model surfaces only via
            # SportyBet code legs, Limitless/Polymarket alerts, or the app on demand.
            _model_extras_run(fixtures, announce=False)
        except Exception as e:
            print("[MODEL] extras run error: {}".format(e))

        print("[FB] ═══ Engine run complete ═══")
    finally:
        _FB_CACHE["running"] = False


def fb_scanner_thread(get_db, tg_token, tg_chat, send_telegram, interval_hours=12):
    """Football accumulator scanner. Fires TWICE A DAY at fixed wall-clock
    times: 08:00 and 16:00 Lagos (07:00 and 15:00 UTC). The interval_hours
    arg is kept for backward-compat with the existing callsite but is now
    ignored — the scheduler is slot-driven.

    On boot we still load the last successful run from the DB so the UI
    isn't blank, but we DO NOT re-scrape on boot. If today's 07:00 UTC
    slot has already passed when the bot comes up, it is marked complete
    without firing and we wait for 15:00 UTC. Same applies if both slots
    have already passed — we wait until tomorrow's 07:00.

    Manual /app/fb-rescan still works the same and re-anchors the cache."""
    def loop():
        time.sleep(30)  # let the app boot
        # Restore last successful run so the pages aren't blank after a redeploy.
        try:
            fb_load_latest(get_db)
        except Exception as e:
            print("[FB] boot restore error: {}".format(e))
        # Mark today's already-passed slots as complete (no catch-up).
        try:
            _init_scheduler_no_catchup((7, 15), "fb_last_slot", "FB")
        except Exception as e:
            print("[FB] scheduler init error: {}".format(e))
        # Tick every minute and fire the engine when a slot is due.
        def _run():
            run_football_engine(get_db, tg_token, tg_chat, send_telegram)
        while True:
            time.sleep(60)
            try:
                if _FB_CACHE.get("running"):
                    continue
                _scheduler_run_due_slot((7, 15), "fb_last_slot", _run, "FB")
            except Exception as e:
                print("[FB] scheduler tick error: {}".format(e))
    threading.Thread(target=loop, daemon=True).start()
    print("[FB] scanner thread started — schedule: 08:00 + 16:00 Lagos (07:00 + 15:00 UTC) daily")


# ═══════════════════════════════════════════════════════════════════
# TELEGRAM WEBHOOK HANDLER
# Assumes in namespace: tg_send, tg_answer_callback, kb_*, fmt_*
# Plus crypto/sports market accessors from existing app.py
# ═══════════════════════════════════════════════════════════════════

def fb_handle_telegram_update(update, tg_token,
                              get_crypto_signals=None, get_sports_markets=None,
                              get_live_bets=None, get_results=None):
    """
    Process one Telegram update (message or callback).
    Returns nothing — sends replies directly.
    """
    try:
        # ── Slash commands ──
        if "message" in update:
            msg = update["message"]
            chat_id = msg["chat"]["id"]
            text = (msg.get("text") or "").strip().lower()

            if text in ("/start", "/menu", "/help"):
                tg_send(tg_token, chat_id, fmt_welcome(), kb_main_menu())
            elif text.startswith("/picks"):
                tg_send(tg_token, chat_id,
                        fmt_picks(_FB_CACHE["match_picks"], _FB_CACHE["date"] or "today"))
            elif text.startswith("/codes"):
                tg_send(tg_token, chat_id,
                        fmt_codes(_FB_CACHE["accumulators"], _FB_CACHE["date"] or "today"))
            elif text.startswith("/sports"):
                tg_send(tg_token, chat_id, "📊 <b>Sports Markets</b>\nChoose a source:", kb_sports_menu())
            elif text.startswith("/crypto"):
                tg_send(tg_token, chat_id, "💰 <b>Crypto Signals</b>\nChoose a source:", kb_crypto_menu())
            elif text.startswith("/live"):
                _send_live(tg_token, chat_id, get_live_bets)
            elif text.startswith("/results"):
                _send_results(tg_token, chat_id, get_results)
            return

        # ── Button taps ──
        if "callback_query" in update:
            cq = update["callback_query"]
            chat_id = cq["message"]["chat"]["id"]
            data = cq.get("data", "")
            tg_answer_callback(tg_token, cq["id"])

            if data == "show_picks":
                tg_send(tg_token, chat_id,
                        fmt_picks(_FB_CACHE["match_picks"], _FB_CACHE["date"] or "today"))
            elif data == "show_codes":
                tg_send(tg_token, chat_id,
                        fmt_codes(_FB_CACHE["accumulators"], _FB_CACHE["date"] or "today"))
            elif data == "menu_sports":
                tg_send(tg_token, chat_id, "📊 <b>Sports Markets</b>\nChoose a source:", kb_sports_menu())
            elif data == "menu_crypto":
                tg_send(tg_token, chat_id, "💰 <b>Crypto Signals</b>\nChoose a source:", kb_crypto_menu())
            elif data == "show_results":
                _send_results(tg_token, chat_id, get_results)
            elif data == "show_live":
                _send_live(tg_token, chat_id, get_live_bets)
            elif data in ("crypto_poly", "crypto_limitless"):
                platform = "polymarket" if data == "crypto_poly" else "limitless"
                _send_crypto(tg_token, chat_id, platform, get_crypto_signals)
            elif data in ("sports_poly", "sports_limitless"):
                platform = "polymarket" if data == "sports_poly" else "limitless"
                _send_sports(tg_token, chat_id, platform, get_sports_markets)
            return
    except Exception as e:
        print("[TG] handler error: {}".format(e))


def _send_crypto(tg_token, chat_id, platform, getter):
    if getter is None:
        tg_send(tg_token, chat_id, "💰 Crypto signals not available right now.")
        return
    try:
        signals = getter(platform)
        if not signals:
            tg_send(tg_token, chat_id,
                    "💰 <b>{} Crypto</b>\nNo open signals right now.".format(platform.title()))
            return
        lines = ["💰 <b>{} Crypto Signals</b>".format(platform.title()), ""]
        for s in signals[:15]:
            lines.append("• {}".format(s))
        tg_send(tg_token, chat_id, "\n".join(lines))
    except Exception as e:
        tg_send(tg_token, chat_id, "💰 Error loading crypto signals.")
        print("[TG] crypto error: {}".format(e))


def _send_sports(tg_token, chat_id, platform, getter):
    if getter is None:
        tg_send(tg_token, chat_id, "📊 Sports markets not available right now.")
        return
    try:
        markets = getter(platform)
        if not markets:
            tg_send(tg_token, chat_id,
                    "📊 <b>{} Sports</b>\n\nNo sports picks cached yet. "
                    "The scanner runs every few hours — check back soon.".format(platform.title()))
            return
        lines = ["📊 <b>{} — SPORTS PICKS</b>".format(platform.title()),
                 "<i>Verified across prediction sites</i>", ""]
        for m in markets[:12]:
            if isinstance(m, dict):
                home = m.get("home", "")
                away = m.get("away", "")
                pick = m.get("pick", "") or m.get("winner", "")
                market = m.get("market", "")
                url = m.get("url", "")
                score = m.get("score", "")
                lines.append("⚽ <b>{} vs {}</b>".format(home, away))
                if pick:
                    lines.append("✅ Pick: <b>{}</b>".format(
                        "Draw" if pick.upper() == "DRAW" else pick))
                if market:
                    lines.append("📍 {}".format(market.replace("🏆 ", "").replace("⚽ ", "")))
                if score:
                    lines.append("⭐ Confidence: {}/100".format(score))
                if url:
                    lines.append('🔗 <a href="{}">Place this bet</a>'.format(url))
                lines.append("")
            else:
                lines.append("• {}".format(m))
        tg_send(tg_token, chat_id, "\n".join(lines))
    except Exception as e:
        tg_send(tg_token, chat_id, "📊 Error loading sports markets.")
        print("[TG] sports error: {}".format(e))


def _send_live(tg_token, chat_id, getter):
    if getter is None:
        tg_send(tg_token, chat_id, "🔴 Live bets not available right now.")
        return
    try:
        bets = getter()
        if not bets:
            tg_send(tg_token, chat_id, "🔴 <b>Live Bets</b>\nNo unresolved bets right now.")
            return
        lines = ["🔴 <b>Unresolved Bets</b>", ""]
        for b in bets[:25]:
            lines.append("• {}".format(b))
        tg_send(tg_token, chat_id, "\n".join(lines))
    except Exception as e:
        tg_send(tg_token, chat_id, "🔴 Error loading live bets.")
        print("[TG] live error: {}".format(e))


def _send_results(tg_token, chat_id, getter):
    if getter is None:
        tg_send(tg_token, chat_id, fmt_results([], _FB_CACHE["date"] or "today"))
        return
    try:
        stats = getter()
        tg_send(tg_token, chat_id, fmt_results(stats, _FB_CACHE["date"] or "today"))
    except Exception as e:
        tg_send(tg_token, chat_id, "📈 Error loading results.")
        print("[TG] results error: {}".format(e))



# ═══════════════════════════════════════════════════════════════════
# FOOTBALL v3 — BRIDGE: data getters for Telegram + Flask routes
# ═══════════════════════════════════════════════════════════════════

def _fb_parse_score(score_str):
    """Parse a predicted score like '2-1' or '2:1' -> (2, 1), else None."""
    try:
        m = _sports_re.search(r'(\d+)\s*[-:]\s*(\d+)', str(score_str))
        if m:
            return int(m.group(1)), int(m.group(2))
    except Exception:
        pass
    return None


_FB_LEAGUE_NAMES = {
    "epl": "EPL", "la_liga": "La Liga", "serie_a": "Serie A",
    "bundesliga": "Bundesliga", "ligue_1": "Ligue 1",
    "ucl": "Champions League", "uel": "Europa League",
}


def _fb_estimate_goals_from_preds(preds):
    """When a row has no correct-score, estimate expected goals per side from the
    1X2 win probabilities + average-goals that Forebet publishes for EVERY match.
    Returns (avg_home, avg_away) or None if there's nothing usable. This is what
    stops other-league games (which often have probs but no score string) from
    being silently dropped."""
    def _nums(key):
        return [p.get(key) for p in preds
                if isinstance(p.get(key), (int, float)) and p.get(key) is not None]
    ph, pa, pd = _nums("prob_home"), _nums("prob_away"), _nums("prob_draw")
    totals = [t for t in _nums("avg_goals") if t]
    if not ph or not pa:
        return None  # no win probabilities -> genuinely no data to work with
    prob_home = sum(ph) / len(ph)
    prob_away = sum(pa) / len(pa)
    prob_draw = (sum(pd) / len(pd)) if pd else max(0.0, 100 - prob_home - prob_away)
    if totals:
        total = sum(totals) / len(totals)
    else:
        # Higher draw probability -> tighter, lower-scoring game.
        total = max(1.8, 3.4 - (prob_draw / 100.0) * 2.0)
    margin = (prob_home - prob_away) / 100.0 * 1.8   # win-prob gap -> goal margin
    avg_h = max(0.3, (total + margin) / 2.0)
    avg_a = max(0.3, (total - margin) / 2.0)
    return (round(avg_h, 2), round(avg_a, 2))


def _fb_fixtures_from_predictions(max_fixtures=200):
    """
    Build engine fixtures from the prediction scrapers that are PROVEN to
    work on Railway (footballpredictions.com + forebet + fp.net).

    Each prediction carries a predicted score (e.g. "2-1"). We derive the
    engine's goal/xG inputs and a form bias from that score, then optionally
    overlay real Understat xG when available. Markets with no real data
    (corners/cards) fall back to league averages inside the engine.
    """
    all_predictions = []
    for scraper, name in [
        (_sports_scrape_footballpredictions_com, "fp.com"),
        (_sports_scrape_forebet, "forebet"),
        (_sports_scrape_footballpredictions_net, "fp.net"),
    ]:
        try:
            got = scraper() or []
            all_predictions.extend(got)
            print("[FB] scraped {} predictions from {}".format(len(got), name))
        except Exception as e:
            print("[FB] {} scrape error: {}".format(name, e))

    if not all_predictions:
        print("[FB] No predictions from any scraper")
        return []

    # Group predictions by match (handle home/away swaps)
    matches = {}
    for p in all_predictions:
        h = _sports_normalize_team(p.get("home", ""))
        a = _sports_normalize_team(p.get("away", ""))
        if not h or not a:
            continue
        key, rev = (h, a), (a, h)
        if key in matches:
            matches[key]["preds"].append(p)
        elif rev in matches:
            matches[rev]["preds"].append(p)
        else:
            matches[key] = {"home": p.get("home"), "away": p.get("away"), "preds": [p]}

    # Merge fuzzy-duplicate matches (e.g. "Ireland" vs "Republic of Ireland").
    # Substring-only match — strict enough not to merge "Man Utd" with "Man City".
    def _same_team(n1, n2):
        a1 = (n1 or "").lower().replace(" fc", "").replace("afc ", "").strip()
        b1 = (n2 or "").lower().replace(" fc", "").replace("afc ", "").strip()
        if not a1 or not b1:
            return False
        if a1 == b1:
            return True
        short, lng = (a1, b1) if len(a1) <= len(b1) else (b1, a1)
        if len(short) >= 4 and short in lng:
            return True
        # Initials match: "psg" == initials of "paris saint germain"
        short_ns = short.replace(" ", "").replace(".", "")
        initials = "".join(w[0] for w in lng.split() if w)
        if len(short_ns) >= 3 and short_ns == initials:
            return True
        return False

    merged = []
    for md in matches.values():
        placed = False
        for m in merged:
            same = ((_same_team(md["home"], m["home"]) and _same_team(md["away"], m["away"])) or
                    (_same_team(md["home"], m["away"]) and _same_team(md["away"], m["home"])))
            if same:
                m["preds"].extend(md["preds"])
                # Keep the longer (more complete) team names
                if len(md["home"]) > len(m["home"]):
                    m["home"] = md["home"]
                if len(md["away"]) > len(m["away"]):
                    m["away"] = md["away"]
                placed = True
                break
        if not placed:
            merged.append(md)

    # ── Funnel diagnostics: show EXACTLY where games are lost ──
    # A match is kept if it has a score OR (now) usable win-probabilities. Only
    # rows with neither are dropped. This makes the pipeline fully visible.
    with_score, recoverable, truly_dropped = [], [], []
    for md in merged:
        if any(_fb_parse_score(p.get("score")) for p in md["preds"]):
            with_score.append(md)
        elif _fb_estimate_goals_from_preds(md["preds"]):
            recoverable.append(md)
        else:
            truly_dropped.append(md)
    src_seen = {}
    for p in all_predictions:
        s = p.get("source", "?")
        src_seen[s] = src_seen.get(s, 0) + 1
    print("[FB] FUNNEL: {} raw preds -> {} unique matches -> {} with score, "
          "{} recovered from probs, {} dropped (no data)".format(
              len(all_predictions), len(merged), len(with_score),
              len(recoverable), len(truly_dropped)))
    print("[FB] by source: {}".format(src_seen))
    if truly_dropped:
        sample = ", ".join("{} vs {}".format(m["home"], m["away"]) for m in truly_dropped[:15])
        print("[FB] dropped (no data) sample: {}".format(sample))

    # Optional: real xG overlay from Understat (best-effort, never fatal)
    xg_cache = {}

    fixtures = []
    recovered = 0
    for md in merged:
        scores = [s for s in (_fb_parse_score(p.get("score")) for p in md["preds"]) if s]
        if scores:
            avg_h = sum(s[0] for s in scores) / len(scores)
            avg_a = sum(s[1] for s in scores) / len(scores)
        else:
            # No predicted score on this row — derive expected goals from the
            # win-probabilities + average-goals Forebet publishes for every match,
            # so other-league games aren't silently dropped.
            est = _fb_estimate_goals_from_preds(md["preds"])
            if not est:
                continue  # genuinely no usable data (no score AND no probabilities)
            avg_h, avg_a = est
            recovered += 1

        # League from prediction type tags
        league = ""
        for p in md["preds"]:
            t = (p.get("type") or "").lower()
            if t in _FB_LEAGUE_NAMES:
                league = _FB_LEAGUE_NAMES[t]
                break

        # Derive a form bias from the predicted margin
        margin = avg_h - avg_a
        if margin >= 1.5:
            hf, af = "WWWWW", "LLLDL"
        elif margin >= 0.6:
            hf, af = "WWWDW", "LDLLL"
        elif margin <= -1.5:
            hf, af = "LLLDL", "WWWWW"
        elif margin <= -0.6:
            hf, af = "LDLLL", "WWWDW"
        else:
            hf, af = "WDWDW", "DWDWD"

        both_score = avg_h >= 0.5 and avg_a >= 0.5
        btts_pct = 65 if both_score else 35

        fx = {
            "home_team": md["home"], "away_team": md["away"], "league": league,
            "kickoff_time": "",
            "home_form": hf, "away_form": af,
            "home_xg_for": round(avg_h, 2), "home_xg_against": round(avg_a, 2),
            "away_xg_for": round(avg_a, 2), "away_xg_against": round(avg_h, 2),
            "home_goals_scored_avg": round(avg_h, 2), "home_goals_conceded_avg": round(avg_a, 2),
            "away_goals_scored_avg": round(avg_a, 2), "away_goals_conceded_avg": round(avg_h, 2),
            "home_btts_pct": btts_pct, "away_btts_pct": btts_pct,
            "home_clean_sheet_pct": 35 if not both_score else 22,
            "away_clean_sheet_pct": 35 if not both_score else 22,
            "home_position": 8, "away_position": 8,
            "home_key_injuries": 0, "away_key_injuries": 0,
            "_pred_score": "{:.0f}-{:.0f}".format(round(avg_h), round(avg_a)),
            "_n_sources": len(set(p.get("source") for p in md["preds"])),
        }

        # ── Path 2: activate the methodology from data we ALREADY scrape ──
        # The prediction sites tag each match with its tip type (over-2-5, btts,
        # correct-score). Combine that tip signal with the predicted goals to
        # build the form_stats dict the over/under + ranking rules read — so the
        # methodology fires without Sofascore (Cloudflare-blocked) or any API.
        pred_types = set((p.get("type") or "").lower() for p in md["preds"])
        over_tipped = any(("over-2" in t or "over2" in t or "over_2" in t)
                          for t in pred_types)
        btts_tipped = any("btts" in t for t in pred_types)
        total_goals = avg_h + avg_a
        if over_tipped or total_goals >= 2.8:
            o15, o25, u35 = 0.85, 0.72, 0.42      # goals-likely → Over 1.5 safe
            tip_neutral = False
        elif total_goals <= 2.0 and not btts_tipped:
            o15, o25, u35 = 0.55, 0.28, 0.82      # low-scoring → Under 3.5 safe
            tip_neutral = False
        else:
            o15, o25, u35 = 0.70, 0.50, 0.62      # neutral — uncertain read
            tip_neutral = True
        btts_p = 0.70 if (btts_tipped or both_score) else 0.38

        def _mk_form(gf, ga, won, drew=False):
            return {
                "played": 10,
                "form": hf if won else (af if not drew else "DDDDD"),
                "over15_pct": o15, "over25_pct": o25, "under35_pct": u35,
                "scored_pct": 0.80 if gf >= 1.2 else 0.55,
                "cs_pct": 0.35 if ga <= 0.9 else 0.20,
                "btts_pct": btts_p,
                "avg_gf": round(gf, 2), "avg_ga": round(ga, 2),
                "ppg": 2.0 if won else (1.0 if drew else 0.9),
            }
        fx["home_form_stats"] = _mk_form(avg_h, avg_a, margin > 0.3, abs(margin) <= 0.3)
        fx["away_form_stats"] = _mk_form(avg_a, avg_h, margin < -0.3, abs(margin) <= 0.3)
        fx["_form_source"] = "predictions"
        fx["_tip_neutral"] = tip_neutral

        # Overlay real xG from Understat where the league is supported
        try:
            if league in UNDERSTAT_LEAGUES:
                if league not in xg_cache:
                    xg_cache[league] = understat_team_xg(league)
                lg = xg_cache[league]
                hx = _match_team_xg(lg, md["home"])
                ax = _match_team_xg(lg, md["away"])
                if hx:
                    fx["home_xg_for"] = hx["xg_for"]
                    fx["home_xg_against"] = hx["xg_against"]
                if ax:
                    fx["away_xg_for"] = ax["xg_for"]
                    fx["away_xg_against"] = ax["xg_against"]
        except Exception:
            pass

        fixtures.append(fx)

    # Prefer matches confirmed by more sources
    fixtures.sort(key=lambda f: f.get("_n_sources", 0), reverse=True)
    print("[FB] Built {} fixtures from predictions ({} with multi-source, "
          "{} recovered from probs without a score)".format(
        len(fixtures), sum(1 for f in fixtures if f.get("_n_sources", 0) >= 2), recovered))
    return fixtures[:max_fixtures]


def _fb_today_human():
    return _dt.date.today().strftime("%A, %B %d, %Y")


def _fb_get_crypto_signals(platform):
    """Open crypto signals for a platform (from v2_paper_trades)."""
    out = []
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT asset, direction, timeframe, entry_odds, confidence "
            "FROM v2_paper_trades WHERE platform=:p AND status='OPEN' "
            "ORDER BY id DESC LIMIT 20", p=platform)
        conn.close()
        for r in rows:
            asset, direction, tf, odds, conf = r
            odds_disp = ""
            if odds:
                cents = odds * 100 if odds <= 1 else odds
                odds_disp = " @ {:.0f}c".format(cents)
            conf_disp = " ({})".format(conf) if conf else ""
            out.append("{} {} · {}{}{}".format(asset, direction, tf, odds_disp, conf_disp))
    except Exception as e:
        print("[FB] crypto getter error: {}".format(e))
    return out


def _fb_get_sports_markets(platform):
    """Recent sports market alerts for a platform (from in-memory cache)."""
    try:
        return list(_sports_market_cache.get(platform, []))
    except Exception:
        return []


def _fb_get_live_bets():
    """All unresolved bets across platforms (crypto open + football codes)."""
    out = []
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT platform, asset, direction, timeframe FROM v2_paper_trades "
            "WHERE status='OPEN' ORDER BY id DESC LIMIT 30")
        conn.close()
        for r in rows:
            out.append("{} · {} {} {}".format(r[0], r[1], r[2], r[3]))
    except Exception as e:
        print("[FB] live getter error: {}".format(e))
    for acca in _FB_CACHE.get("accumulators", []):
        if acca.get("code"):
            out.append("{} {} — code {} @ {:.2f}".format(
                acca.get("emoji", "⚽"), acca["label"], acca["code"], acca["total_odds"]))
    return out


def _fb_get_results():
    """Win-rate stats per accumulator tier."""
    tiers = [("2_odds", "🟢 2 ODDS — BANKER"), ("3_odds", "🟢 3 ODDS — SAFE"),
             ("5_odds", "🟡 5 ODDS — VALUE"), ("10_odds", "🟠 10 ODDS — RISK"),
             ("1000_odds", "🔴 1000+ ODDS — MOONSHOT")]
    stats = []
    agg = {}
    try:
        conn = get_db()
        rows = conn.run("SELECT tier, result FROM sportybet_accumulators")
        conn.close()
        for r in rows:
            tier, result = r[0], r[1]
            a = agg.setdefault(tier, {"wins": 0, "settled": 0, "pending": 0})
            if result == "won":
                a["wins"] += 1; a["settled"] += 1
            elif result == "lost":
                a["settled"] += 1
            else:
                a["pending"] += 1
    except Exception as e:
        print("[FB] results getter error: {}".format(e))
    for tier, label in tiers:
        a = agg.get(tier, {"wins": 0, "settled": 0, "pending": 0})
        stats.append({"tier_label": label, "wins": a["wins"],
                      "settled": a["settled"], "pending": a["pending"]})
    return stats


# ── Flask routes ──

import base64

SHARE_STUDIO_HTML = base64.b64decode(
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImVuIiBkYXRhLXRoZW1lPSJsaWdodCI+CjxoZWFkPgo8bWV0YSBjaGFyc2V0PSJ1dGYtOCI+CjxtZXRhIG5hbWU9InZpZXdwb3J0IiBjb250ZW50PSJ3aWR0aD1kZXZpY2Utd2lkdGgsIGluaXRpYWwtc2NhbGU9MSI+Cjx0aXRsZT5jbXZuZyDCtyBTaGFyZSBDYXJkIFN0dWRpbzwvdGl0bGU+CjxsaW5rIHJlbD0icHJlY29ubmVjdCIgaHJlZj0iaHR0cHM6Ly9mb250cy5nb29nbGVhcGlzLmNvbSI+CjxsaW5rIHJlbD0icHJlY29ubmVjdCIgaHJlZj0iaHR0cHM6Ly9mb250cy5nc3RhdGljLmNvbSIgY3Jvc3NvcmlnaW4+CjxsaW5rIGhyZWY9Imh0dHBzOi8vZm9udHMuZ29vZ2xlYXBpcy5jb20vY3NzMj9mYW1pbHk9U29yYTp3Z2h0QDUwMDs2MDA7NzAwOzgwMCZmYW1pbHk9RE0rU2Fuczp3Z2h0QDQwMDs1MDA7NjAwOzcwMCZkaXNwbGF5PXN3YXAiIHJlbD0ic3R5bGVzaGVldCI+CjxzdHlsZT4KOnJvb3R7CiAgLyogYnJhbmQgKi8KICAtLWJyYW5kOiMyZjZiZDY7IC0tYnJhbmQtZGVlcDojMWY1NGIwOyAtLWJyYW5kLXNvZnQ6I2U4ZjBmZDsKICAvKiBwYWdlIChidWlsZGVyIGNocm9tZSkg4oCUIExJR0hUICovCiAgLS1wYWdlMTojZWVmM2ZhOyAtLXBhZ2UyOiNlNGViZjY7CiAgLS1zdXJmYWNlOiNmZmZmZmY7IC0tc3VyZmFjZTI6I2Y1ZjhmZDsgLS1maWVsZDojZjFmNWZjOyAtLWZpZWxkLWxpbmU6cmdiYSgxMiwxOSwzMiwuMTApOwogIC0taW5rOiMwYzEzMjA7IC0taW5rLXNvZnQ6IzNhNDY1YzsgLS1tdXRlZDojNzM4MTliOyAtLWxpbmU6cmdiYSgxMiwxOSwzMiwuMDkpOwogIC8qIGNhcmQgKi8KICAtLWMxOiNmZmZmZmY7IC0tYzI6I2VhZjBmOTsgLS1jLWluazojMGMxMzIwOyAtLWMtaW5rLXNvZnQ6IzNhNDY1YzsgLS1jLW11dGVkOiM3YzhhYTQ7CiAgLS1jLWxpbmU6cmdiYSgxMiwxOSwzMiwuMDkpOyAtLWdsYXNzOnJnYmEoMjU1LDI1NSwyNTUsLjcyKTsgLS1nbGFzcy1saW5lOnJnYmEoMTIsMTksMzIsLjA3KTsKICAtLWdsb3cxOnJnYmEoNDcsMTA3LDIxNCwuMTYpOyAtLWdsb3cyOnJnYmEoMTIwLDE3MCwyNTUsLjEzKTsgLS12aWc6cmdiYSgyMCw0MCw5MCwuMDYpOwogIC0tcmluZy1hOiNmZmZmZmY7IC0tcmluZy1iOiNjZmRhZWU7IC0tYy1zaGFkb3c6MCAzMHB4IDcwcHggcmdiYSgyMCw0MCw5MCwuMTYpLDAgNnB4IDE4cHggcmdiYSgyMCw0MCw5MCwuMTApOwogIC0tYy1zb2Z0OiNlOGYwZmQ7Cn0KW2RhdGEtdGhlbWU9ImRhcmsiXXsKICAtLWJyYW5kOiM1YThjZjA7IC0tYnJhbmQtZGVlcDojMmY2YmQ2OyAtLWJyYW5kLXNvZnQ6cmdiYSg5MCwxNDAsMjQwLC4xNik7CiAgLS1wYWdlMTojMGEwZTE2OyAtLXBhZ2UyOiMwNzBhMTE7CiAgLS1zdXJmYWNlOiMxMjE4MjY7IC0tc3VyZmFjZTI6IzBmMTQyMDsgLS1maWVsZDojMGUxMzFlOyAtLWZpZWxkLWxpbmU6cmdiYSgyNTUsMjU1LDI1NSwuMTApOwogIC0taW5rOiNlYWYwZmI7IC0taW5rLXNvZnQ6I2M1Y2ZlMjsgLS1tdXRlZDojODY5NmI0OyAtLWxpbmU6cmdiYSgyNTUsMjU1LDI1NSwuMDkpOwogIC0tYzE6IzE0MWIyYTsgLS1jMjojMGIxMDFhOyAtLWMtaW5rOiNlZWYyZmI7IC0tYy1pbmstc29mdDojYzVjZmUyOyAtLWMtbXV0ZWQ6IzhiOWFiODsKICAtLWMtbGluZTpyZ2JhKDI1NSwyNTUsMjU1LC4wOSk7IC0tZ2xhc3M6cmdiYSgyNTUsMjU1LDI1NSwuMDU1KTsgLS1nbGFzcy1saW5lOnJnYmEoMjU1LDI1NSwyNTUsLjEwKTsKICAtLWdsb3cxOnJnYmEoOTAsMTQwLDI0MCwuMjgpOyAtLWdsb3cyOnJnYmEoMzQsMTIwLDIyMCwuMTgpOyAtLXZpZzpyZ2JhKDAsMCwwLC41KTsKICAtLXJpbmctYTojNDY1MzZlOyAtLXJpbmctYjojMTYxZTJkOyAtLWMtc2hhZG93OjAgNDBweCA5MHB4IHJnYmEoMCwwLDAsLjYpLDAgOHB4IDI0cHggcmdiYSgwLDAsMCwuNSk7CiAgLS1jLXNvZnQ6cmdiYSg5MCwxNDAsMjQwLC4xNSk7Cn0KKntib3gtc2l6aW5nOmJvcmRlci1ib3g7bWFyZ2luOjA7cGFkZGluZzowfQpodG1sLGJvZHl7aGVpZ2h0OjEwMCV9CmJvZHl7Zm9udC1mYW1pbHk6J0RNIFNhbnMnLHN5c3RlbS11aSxzYW5zLXNlcmlmO2NvbG9yOnZhcigtLWluayk7CiAgYmFja2dyb3VuZDpyYWRpYWwtZ3JhZGllbnQoMTIwJSA4MCUgYXQgNTAlIC0xMCUsIHZhcigtLXBhZ2UxKSwgdmFyKC0tcGFnZTIpKTsKICBtaW4taGVpZ2h0OjEwMHZoO3RyYW5zaXRpb246YmFja2dyb3VuZCAuMzVzIGVhc2UsY29sb3IgLjM1cyBlYXNlfQoKLyogLS0tLS0tLS0tLSB0b3AgYmFyIC0tLS0tLS0tLS0gKi8KLmJhcntwb3NpdGlvbjpzdGlja3k7dG9wOjA7ei1pbmRleDo0MDtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDoxOHB4O2ZsZXgtd3JhcDp3cmFwOwogIHBhZGRpbmc6MTZweCAyNnB4O2JhY2tncm91bmQ6Y29sb3ItbWl4KGluIHNyZ2IsdmFyKC0tc3VyZmFjZSkgNzglLCB0cmFuc3BhcmVudCk7CiAgLXdlYmtpdC1iYWNrZHJvcC1maWx0ZXI6Ymx1cigxNHB4KTtiYWNrZHJvcC1maWx0ZXI6Ymx1cigxNHB4KTtib3JkZXItYm90dG9tOjFweCBzb2xpZCB2YXIoLS1saW5lKX0KLmJhciAuYnJhbmR7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTJweDttYXJnaW4tcmlnaHQ6YXV0b30KLmJhciAuYnJhbmQgaW1ne2hlaWdodDoyNHB4O2Rpc3BsYXk6YmxvY2t9Ci5iYXIgLmJyYW5kIC50YWd7Zm9udDo2MDAgMTJweCAnRE0gU2Fucyc7bGV0dGVyLXNwYWNpbmc6LjIyZW07dGV4dC10cmFuc2Zvcm06dXBwZXJjYXNlO2NvbG9yOnZhcigtLW11dGVkKX0KLnNlZ3tkaXNwbGF5OmZsZXg7YmFja2dyb3VuZDp2YXIoLS1maWVsZCk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1maWVsZC1saW5lKTtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo0cHh9Ci5zZWcgYnV0dG9ue2JvcmRlcjowO2JhY2tncm91bmQ6dHJhbnNwYXJlbnQ7Y29sb3I6dmFyKC0tbXV0ZWQpO2ZvbnQ6NjAwIDEzcHggJ0RNIFNhbnMnO2xldHRlci1zcGFjaW5nOi4wNGVtOwogIHBhZGRpbmc6OHB4IDE2cHg7Ym9yZGVyLXJhZGl1czo5cHg7Y3Vyc29yOnBvaW50ZXI7dHJhbnNpdGlvbjouMThzfQouc2VnIGJ1dHRvbi5vbntiYWNrZ3JvdW5kOnZhcigtLWJyYW5kKTtjb2xvcjojZmZmO2JveC1zaGFkb3c6MCA0cHggMTJweCByZ2JhKDQ3LDEwNywyMTQsLjMpfQouaWNvbi1idG57d2lkdGg6NDJweDtoZWlnaHQ6NDJweDtib3JkZXItcmFkaXVzOjEycHg7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1maWVsZC1saW5lKTtiYWNrZ3JvdW5kOnZhcigtLWZpZWxkKTsKICBjb2xvcjp2YXIoLS1pbmspO2N1cnNvcjpwb2ludGVyO2ZvbnQtc2l6ZToxOHB4O2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OmNlbnRlcjt0cmFuc2l0aW9uOi4xOHN9Ci5pY29uLWJ0bjpob3Zlcntib3JkZXItY29sb3I6dmFyKC0tYnJhbmQpfQouZGx7Ym9yZGVyOjA7YmFja2dyb3VuZDp2YXIoLS1icmFuZCk7Y29sb3I6I2ZmZjtmb250OjcwMCAxNHB4ICdETSBTYW5zJztsZXR0ZXItc3BhY2luZzouMDNlbTsKICBwYWRkaW5nOjExcHggMjJweDtib3JkZXItcmFkaXVzOjEycHg7Y3Vyc29yOnBvaW50ZXI7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6OXB4OwogIGJveC1zaGFkb3c6MCA4cHggMjBweCByZ2JhKDQ3LDEwNywyMTQsLjMyKTt0cmFuc2l0aW9uOi4xOHN9Ci5kbDpob3ZlcntiYWNrZ3JvdW5kOnZhcigtLWJyYW5kLWRlZXApfQouZGw6YWN0aXZle3RyYW5zZm9ybTp0cmFuc2xhdGVZKDFweCl9CgovKiAtLS0tLS0tLS0tIGxheW91dCAtLS0tLS0tLS0tICovCi5hcHB7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczozODBweCAxZnI7Z2FwOjI2cHg7cGFkZGluZzoyNnB4O21heC13aWR0aDoxMzIwcHg7bWFyZ2luOjAgYXV0bzthbGlnbi1pdGVtczpzdGFydH0KQG1lZGlhKG1heC13aWR0aDo5MjBweCl7LmFwcHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyO2dhcDoyMHB4O3BhZGRpbmc6MThweH0uY29udHJvbHN7b3JkZXI6Mn0uc3RhZ2V3cmFwe29yZGVyOjF9fQoKLyogLS0tLS0tLS0tLSBjb250cm9scyAtLS0tLS0tLS0tICovCi5jb250cm9sc3tiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czoyMHB4O3BhZGRpbmc6MjJweDsKICBib3gtc2hhZG93OjAgMThweCA0MHB4IHJnYmEoMjAsNDAsOTAsLjA2KX0KLmNvbnRyb2xzIGgye2ZvbnQ6NzAwIDE0cHggJ1NvcmEnO2xldHRlci1zcGFjaW5nOi4wNGVtO2NvbG9yOnZhcigtLWluayk7bWFyZ2luLWJvdHRvbTo0cHh9Ci5jb250cm9scyAuaGludHtmb250LXNpemU6MTJweDtjb2xvcjp2YXIoLS1tdXRlZCk7bWFyZ2luLWJvdHRvbToxOHB4O2xpbmUtaGVpZ2h0OjEuNX0KLmZsZHttYXJnaW4tYm90dG9tOjE0cHh9Ci5mbGQgbGFiZWx7ZGlzcGxheTpibG9jaztmb250OjYwMCAxMXB4ICdETSBTYW5zJztsZXR0ZXItc3BhY2luZzouMTJlbTt0ZXh0LXRyYW5zZm9ybTp1cHBlcmNhc2U7Y29sb3I6dmFyKC0tbXV0ZWQpO21hcmdpbi1ib3R0b206NnB4fQouZmxkIGlucHV0W3R5cGU9dGV4dF0sLmZsZCBpbnB1dFt0eXBlPW51bWJlcl17d2lkdGg6MTAwJTtwYWRkaW5nOjExcHggMTNweDtib3JkZXItcmFkaXVzOjExcHg7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1maWVsZC1saW5lKTsKICBiYWNrZ3JvdW5kOnZhcigtLWZpZWxkKTtjb2xvcjp2YXIoLS1pbmspO2ZvbnQ6NTAwIDE0cHggJ0RNIFNhbnMnO3RyYW5zaXRpb246LjE2c30KLmZsZCBpbnB1dDpmb2N1c3tvdXRsaW5lOjA7Ym9yZGVyLWNvbG9yOnZhcigtLWJyYW5kKTtib3gtc2hhZG93OjAgMCAwIDNweCB2YXIoLS1icmFuZC1zb2Z0KX0KLnJvdzJ7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDoxMnB4fQoucm93LXNie2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDY0cHggNjRweDtnYXA6MTBweDthbGlnbi1pdGVtczplbmR9Ci5zdWJoZWFke2ZvbnQ6NzAwIDEycHggJ1NvcmEnO2xldHRlci1zcGFjaW5nOi4wNmVtO2NvbG9yOnZhcigtLWJyYW5kLWRlZXApO3RleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTttYXJnaW46MThweCAwIDEwcHg7CiAgZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6OHB4fQouc3ViaGVhZDo6YmVmb3Jle2NvbnRlbnQ6IiI7d2lkdGg6MTRweDtoZWlnaHQ6MnB4O2JvcmRlci1yYWRpdXM6MnB4O2JhY2tncm91bmQ6dmFyKC0tYnJhbmQpfQoudXBsb2Fke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjEwcHh9Ci51cGxvYWQgLmZha2VidG57ZmxleDoxO3BhZGRpbmc6MTBweCAxMnB4O2JvcmRlci1yYWRpdXM6MTFweDtib3JkZXI6MXB4IGRhc2hlZCB2YXIoLS1maWVsZC1saW5lKTtiYWNrZ3JvdW5kOnZhcigtLWZpZWxkKTsKICBjb2xvcjp2YXIoLS1tdXRlZCk7Zm9udDo1MDAgMTNweCAnRE0gU2Fucyc7Y3Vyc29yOnBvaW50ZXI7dGV4dC1hbGlnbjpjZW50ZXI7dHJhbnNpdGlvbjouMTZzfQoudXBsb2FkIC5mYWtlYnRuOmhvdmVye2JvcmRlci1jb2xvcjp2YXIoLS1icmFuZCk7Y29sb3I6dmFyKC0tYnJhbmQpfQoudXBsb2FkIGlucHV0W3R5cGU9ZmlsZV17ZGlzcGxheTpub25lfQoudXBsb2FkIC50aHVtYnt3aWR0aDozOHB4O2hlaWdodDozOHB4O2JvcmRlci1yYWRpdXM6OXB4O29iamVjdC1maXQ6Y292ZXI7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1maWVsZC1saW5lKTtiYWNrZ3JvdW5kOnZhcigtLWZpZWxkKX0KLnVwbG9hZCAuY2xlYXJ7d2lkdGg6MzJweDtoZWlnaHQ6MzJweDtib3JkZXItcmFkaXVzOjlweDtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWZpZWxkLWxpbmUpO2JhY2tncm91bmQ6dmFyKC0tZmllbGQpO2NvbG9yOnZhcigtLW11dGVkKTtjdXJzb3I6cG9pbnRlcn0KLmxlZ2NhcmR7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1maWVsZC1saW5lKTtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzoxM3B4O21hcmdpbi1ib3R0b206MTJweDtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UyKX0KLmxlZ2NhcmQgLmxoZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47bWFyZ2luLWJvdHRvbToxMHB4fQoubGVnY2FyZCAubGhlYWQgYntmb250OjcwMCAxMnB4ICdTb3JhJztjb2xvcjp2YXIoLS1pbmspO2xldHRlci1zcGFjaW5nOi4wNGVtfQoubGVnY2FyZCAucm17Ym9yZGVyOjA7YmFja2dyb3VuZDp0cmFuc3BhcmVudDtjb2xvcjp2YXIoLS1tdXRlZCk7Y3Vyc29yOnBvaW50ZXI7Zm9udC1zaXplOjEzcHg7cGFkZGluZzoycHggNnB4O2JvcmRlci1yYWRpdXM6NnB4fQoubGVnY2FyZCAucm06aG92ZXJ7Y29sb3I6I2UxNTU2YTtiYWNrZ3JvdW5kOmNvbG9yLW1peChpbiBzcmdiLCNlMTU1NmEgMTIlLHRyYW5zcGFyZW50KX0KLmFkZGxlZ3t3aWR0aDoxMDAlO3BhZGRpbmc6MTJweDtib3JkZXItcmFkaXVzOjEycHg7Ym9yZGVyOjFweCBkYXNoZWQgdmFyKC0tYnJhbmQpO2JhY2tncm91bmQ6dmFyKC0tYnJhbmQtc29mdCk7CiAgY29sb3I6dmFyKC0tYnJhbmQtZGVlcCk7Zm9udDo3MDAgMTNweCAnRE0gU2Fucyc7Y3Vyc29yOnBvaW50ZXI7dHJhbnNpdGlvbjouMTZzfQouYWRkbGVnOmhvdmVye2JhY2tncm91bmQ6dmFyKC0tYnJhbmQpO2NvbG9yOiNmZmZ9Ci5hZGRsZWc6ZGlzYWJsZWR7b3BhY2l0eTouNDU7Y3Vyc29yOm5vdC1hbGxvd2VkfQoucm5ne2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjEycHh9Ci5ybmcgaW5wdXRbdHlwZT1yYW5nZV17ZmxleDoxO2FjY2VudC1jb2xvcjp2YXIoLS1icmFuZCl9Ci5ybmcgLnZhbHtmb250OjcwMCAxNHB4ICdTb3JhJztjb2xvcjp2YXIoLS1icmFuZC1kZWVwKTttaW4td2lkdGg6NDJweDt0ZXh0LWFsaWduOnJpZ2h0fQoKLyogLS0tLS0tLS0tLSBwcmV2aWV3IHN0YWdlIC0tLS0tLS0tLS0gKi8KLnN0YWdld3JhcHtwb3NpdGlvbjpyZWxhdGl2ZX0KLnN0YWdlLWxhYmVse2ZvbnQ6NjAwIDExcHggJ0RNIFNhbnMnO2xldHRlci1zcGFjaW5nOi4xOGVtO3RleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTtjb2xvcjp2YXIoLS1tdXRlZCk7bWFyZ2luLWJvdHRvbToxNHB4O3RleHQtYWxpZ246Y2VudGVyfQouc3RhZ2V7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpjZW50ZXI7YWxpZ24taXRlbXM6ZmxleC1zdGFydDttaW4taGVpZ2h0OjIwMHB4fQouc2NhbGVye3RyYW5zZm9ybS1vcmlnaW46dG9wIGNlbnRlcn0KCi8qID09PT09PT09PT09PT09PT09PT09PT0gVEhFIENBUkQgPT09PT09PT09PT09PT09PT09PT09PSAqLwouY2FyZHtwb3NpdGlvbjpyZWxhdGl2ZTt3aWR0aDo1NDBweDtib3JkZXItcmFkaXVzOjI4cHg7b3ZlcmZsb3c6aGlkZGVuO2JveC1zaGFkb3c6dmFyKC0tYy1zaGFkb3cpOwogIGlzb2xhdGlvbjppc29sYXRlO2NvbG9yOnZhcigtLWMtaW5rKX0KLmNhcmQuc2luZ2xle2hlaWdodDo2NzVweH0KLmNhcmQuc2xpcHttaW4taGVpZ2h0OjU2MHB4fQouYXRtb3N7cG9zaXRpb246YWJzb2x1dGU7aW5zZXQ6MDt6LWluZGV4OjA7CiAgYmFja2dyb3VuZDoKICAgIHJhZGlhbC1ncmFkaWVudCgxMjAlIDg2JSBhdCA1MCUgLTEyJSwgdmFyKC0tZ2xvdzIpLCB0cmFuc3BhcmVudCA1OCUpLAogICAgcmFkaWFsLWdyYWRpZW50KDc0JSA1NiUgYXQgMTAlIDI2JSwgdmFyKC0tZ2xvdzEpLCB0cmFuc3BhcmVudCA2MCUpLAogICAgcmFkaWFsLWdyYWRpZW50KDc4JSA1OCUgYXQgOTIlIDc0JSwgdmFyKC0tZ2xvdzEpLCB0cmFuc3BhcmVudCA2MiUpLAogICAgbGluZWFyLWdyYWRpZW50KDE2NmRlZywgdHJhbnNwYXJlbnQgMzglLCByZ2JhKDI1NSwyNTUsMjU1LC4wNSkgNTAlLCB0cmFuc3BhcmVudCA2MiUpLAogICAgcmFkaWFsLWdyYWRpZW50KDEyMCUgMTAwJSBhdCA1MCUgNDYlLCB0cmFuc3BhcmVudCA1NiUsIHZhcigtLXZpZykgMTAwJSksCiAgICBsaW5lYXItZ3JhZGllbnQoMTYyZGVnLCB2YXIoLS1jMSksIHZhcigtLWMyKSl9Ci5ncmFpbntwb3NpdGlvbjphYnNvbHV0ZTtpbnNldDowO3otaW5kZXg6Mztwb2ludGVyLWV2ZW50czpub25lO29wYWNpdHk6LjA1OwogIGJhY2tncm91bmQtaW1hZ2U6dXJsKCJkYXRhOmltYWdlL3BuZztiYXNlNjQsaVZCT1J3MEtHZ29BQUFBTlNVaEVVZ0FBQUVBQUFBQkFDQVlBQUFDcWFYSGVBQUEzTTBsRVFWUjRuRDJiWjBDT2Y5aitqNVoyb1ZMYVE2V2tORWhwSzVKSVNVUTdXU0dFUEQ4clpFWWEya0tVSkJVdFNkS21SYW1rUkV2THJhM2JyZFQ1Zi9IOG4xNWRMNi9yeGZmOFh1ZnhPWTREd2NIQlpHTmpRNzI5dlRRMk5rYXBxYWxrYm01TzgrYk5vd2NQSGxCa1pDUWRPSENBVkZSVUtETXprd29MQzZtdXJvNHlNek9wdDdlWHZMeTg2Ty9mdjhUSHgwZDM3dHdoZDNkM2NuSnlvb2lJQ0txc3JDUUxDd3RTVlZXbEF3Y09VSE56TTZXbHBkSHk1Y3NwSlNXRkRBd015TXJLaXV6dDdVbGZYNStFaElRb05qYVdzckt5cUtXbGhXeHRiVWxQVDQ5VVZWVnBhbXFLcksydHFhV2xoWFIwZENneE1aRTZPanJveUpFajlPelpNMHBNVEtUQndVRlNVVkdoQlFzV2tMT3pNN0d4c2RHM2I5OW8zcng1TkQ0K1R1ZlBuNmZidDIvVHZIbnp5TWpJaUQ1OStrUnNibTV1OVByMWE5eTZkUXVuVDUvR2x5OWZVRjVlRGpzN094UVhGMlBidG0yUWtKREEyclZyOGZEaFE2U21wdUxEaHc5WXVYSWwwdFBUMGREUWdEdDM3a0JWVlJVZlBueEFhbW9xaElTRXdNL1BqNEtDQXF4ZXZScDFkWFZRVUZEQWp4OC9zSEhqUmxoYVdzTER3d05zYkd4SVRVM0Y1czJiTVRZMmh1dlhyMlB2M3IzZzRPQ0FqSXdNMk5uWllXOXZEMTFkWFZSVlZjSFB6dytPam83UTA5T0RyNjh2TGx5NGdQVDBkTFMydHVMSWtTTVFFUkZCY25JeWZ2LytEWEZ4Y1d6ZnZoMENBZ0tZbnA1R2JHd3NqaDgvRGdFQkFiaTd1eU1yS3d0ZnYzNEZ1NkNnSVBqNCtGQlJVUUZQVDAvWTJkbkJ6YzBOZFhWMTBOSFJnWVdGQlpxYW1yQisvWG84Zi80Y3g0OGZoNTJkSFJRVUZMQjE2MWI0K2ZsaDY5YXRzTEN3UUhkM043NTgrWUw0K0hqazVPVEF3Y0VCZzRPRFVGVlZSVnRiR3dEQTA5TVRNVEV4Nk92clExNWVIdjcrL1l0SGp4NWgyYkpsZVB2MkxRNGNPQUFuSnlkMGRIU0F5V1RpOXUzYjZPenNSRlZWRmI1Ky9ZbzdkKzVBVVZFUklpSWl5TS9QaDUyZEhacWFtaUFwS1FsUFQwL0l5OHZEd2NFQnljbkprSktTUWtoSUNCd2RIV0ZrWklScjE2NWhlSGdZNzkrL3g2MWJ0M0R1M0RtdzhmUHpVMnBxS2xSVlZSRVZGWVZObXpZaE5qWVdOVFUxNk8vdng1a3paeEFURXdONWVYbjA5L2VqdnI0ZXRiVzFFQllXUms1T0RrNmZQZzByS3l2SXk4c2pNaklTK3ZyNmtKQ1F3TUtGQ3lFcks0dUZDeGVpdHJZV2lZbUorUExsQzNoNGVKQ1dsZ1liR3h2SXljbmg2dFdyNE9IaHdiWnQyOERKeVFrbWs0a1ZLMVpBV2xvYXQyN2RRa1JFQkV4TVRIRHIxaTBjUFhvVWJtNXUyTGR2SC9idTNZdSt2ajZ3czdPanVMZ1l2YjI5a0pTVVJHWm1KcFNWbFNFbEpRVmJXMXRjdjM0ZEZSVVZzTGUzeDlxMWF6RThQSXltcGlZWUdocWl1N3NidUhUcEVoVVdGcEtWbFJYTnpzN1M0T0FnRFE4UEV5Y25KM0Z3Y0ZCS1NnckZ4OGRUVjFjWFNVaElrS09qSTFsYVdwSzV1VGxwYTJ2VG1UTm5LQ0lpZ2hZc1dFQXNGb3ZjM055SWs1T1QxTlRVNk1XTEYzVC8vbjA2ZCs0Y0ZSY1hVMWRYRnprNU9SR1R5YVNkTzNlU3JxNHVLU29xVW45L1A0V0ZoVkYrZmo0RkJnYlM4UEF3eGNiR2twdWJHNW1hbXBLZ29DQ3RXTEdDenA0OVMxcGFXbFJRVUVDL2YvOG1BTFIvLzM0eU5EUWtjWEZ4dW5mdkhna0tDaEk3T3p1ZE8zZU9tcHFhcUxlM2w0S0RnNm0vdjU5U1UxT0p3V0JRYVdrcDNiaHhnMVJWVllsTlJrYUdWRlJVc0h2M2JuUjFkYUd1cmc1Q1FrS1FsWlZGV1ZrWlVsSlNjUHIwYVNRa0pPRDM3OTk0L2ZvMWlvdUx3V0F3RUJRVUJCTVRFN3g3OXc2VmxaVzRkT2tTbkoyZElTc3JpNjlmdjRLTmpRMmNuSnhZc0dBQi92MzdCMWRYVnhRVUZFQkRRd004UER5WW1wcUNnWUVCWEYxZFVWdGJpOU9uVDhQVDB4Tyt2cjVZdTNZdE9qbzYwTkRRZ0JzM2JxQ3FxZ3ByMTY2Rm01c2JObTdjQ0cxdGJiUzJ0a0pBUUFCZVhsNFFGQlNFdWJrNWhJU0VVRkJRZ0dYTGxpRXRMUTNOemMxSVNrckN2SG56OE9USkUxeS9maDMzNzkrSG9xSWkyTm5ad1phVGswT2NuSnh3Y25MQ3MyZlBVRnBhaXM3T1RpUW1KaUlvS0FoaFlXSGc1ZVhGeDQ4ZmtaV1ZCUWNIQjZpcnE0UEZZdUhqeDQ5Z01wa3dOemZIOWV2WE1YLytmQVFFQkNBbEpRVVpHUm5vNnVwQ1UxTVRZbUppME5QVGc3OS8veUkxTlJVMk5qWUlEZzVHUUVBQTdPenMwTkhSZ2FHaEllemR1eGZuejUvSHBVdVhJQ1FraEI4L2ZvQ1hseGZEdzhPWW1wcENhbW9xYW1wcTRPWGxCUjhmSHl4ZHVoUWZQMzdFOFBBd0JBUUVjT2pRSVV4T1R1TDM3OTlZc1dJRkhqeDRnTGEyTnNqSXlPRDA2ZE00Y2VJRVJFUkVvSzJ0RFNhVGllenNiSENtcEtSQVdGZ1lpWW1KU0VsSmdiMjlQUVFFQkRBN093czdPenNzWDc0Y0Nnb0tlUFBtRGFxcnExRlJVUUViR3h1VWxKVGc0c1dMZVBQbURUdzhQSkNlbm83MTY5Y2pLQ2dJdnI2KzJMNTlPODZmUDQreXNqSllXRmpBdzhNRFUxTlRxS2lvZ0krUEQvcjcrekU3TzR0VnExWkJXRmdZMjdadHc3WnQyOURlM282MnRqWU1EUTBoTHk4UHNyS3lzTEN3Z0p5Y0hLcXFxa0JFR0JnWWdJU0VCTXpNek5EVjFZVy9mLytpcjY4UEV4TVQyTHg1TTJabVp1RG82SWlMRnkvQ3k4c0xibTV1VUZkWHgrdlhyN0ZxMVNxOGZmc1d0Mi9mUm5aMk50RFMwa0oyZG5iMDd0MDd1bjM3Tmlrcks1T1hseGZsNStmVGtTTkhhUDc4K1JRZEhVM0p5Y20wYTljdTJyWnRHODJmUDU4Y0hSM0oyTmlZVkZWVmFXaG9pRHc4UEVoUVVKQjZlbnBJWGw2ZStQajRhTy9ldmJSaHd3WmFzV0lGblQ5L25ob2JHK25VcVZNVUd4dExKU1VsSkNFaFFSY3VYS0M0dURoaU1wbTBkKzllV3J0MkxkMjVjNGZLeXNyb3o1OC85UFRwVTNKM2Q2ZUlpQWlhbUpnZ0RRME51bkRoQXZIeDhaRyt2ajVWVjFmVDZkT25hV3hzak5qWTJNakl5SWpxNnVxSXhXSlJXbG9hM2JoeGczSnljdWp4NDhmRXdjRkJzYkd4NU9qb1NBTURBMlJyYTB1Yy9mMzkrUFhyRjA2ZE9nVXpNelA4K3ZVTENnb0s0T2ZuaDUrZkgwUkVSUEQ5KzNkczJiSUZoWVdGU0VoSVFIaDRPTzdmdnc4bEpTV3dzYkZoZkh3Y29xS2lPSDM2Tk1URXhIRHMyREhJeWNuaHlKRWpPSExrQ0ZKU1VqQTlQUTB1TGk2TWo0OURXMXNiTjIvZVJIRnhNVTZmUG8zNCtIaXdXQ3lJaW9wQ1EwTUR3Y0hCcUtxcXd0VFVGTXJMeXlFcEtZbjM3OStEaDRjSDNOemNpSXFLZ3F1ckt3SURBN0ZzMlRKVVZWWGh6NTgvK1AzN04weE1UT2ErdGErdkQ1T1RrOWkwYVJONmUzdGhhV21KZ0lBQTFOWFZ3ZEhSRVlPRGcrQmN0V29Wb3FPallXbHBDWHQ3ZTl5K2ZSdXBxYWtvTFMzRnIxKy93TW5KQ1hsNWVXellzQUZaV1Zsd2RuYUdnb0lDN08zdFlXbHBDWFYxZFJ3OGVCQ2VucDdZdjM4L3pNM05rWldWQlFVRkJSdzhlQkEzYjk0RVB6OC9wS1dsb2Flbmg5V3JWNk92cncvZDNkMXdjbktDajQ4UFJrZEg4ZS9mUC9UMjltSm1aZ2FIRHgvRy92MzdFUllXQm41K2ZpeGR1aFQ1K2ZsSVNFakFxVk9ud0dLeDBOTFNnamR2M2lBdExRMk9qbzdRMU5URTZ0V3JrWldWaGFpb0tNek96Z0lBeXN2TGNmRGdRZHk3ZHcvdjM3K0hxYWtwcmwrL0Rna0pDU3hac2dUc3QyN2RncmUzTjVLVGsrSHI2NHVCZ1FHSWlvcGlaR1FFcTFldnhwSWxTMkJuWjRkYnQyN2g1TW1UVUZWVnhlam9LTXJMeTdGbHl4Yms1dWFpb3FJQzFkWFZPSFhxRks1ZHU0YXZYNy9DMmRrWlo4NmN3ZnYzNzVHWm1Zbkl5RWo4K3ZVTG82T2pXTEZpQmNURnhaR1ZsWVhJeUVoY3ZYb1ZyMSsvaHJhMk52YnQyNGVFaEFRY1BIZ1FrWkdSeU1qSXdPenNMQW9MQzFGZFhZMkppUW04ZS9jT0V4TVRHQmdZQUlQQndQWHIxMUZUVTRQZTNsN2N1SEVEOWZYMWVQandJVXBLU2xCWldZbWZQMytpdGJVVjN0N2VVRk5UdytIRGg4SE96ZzRQRHcrd256aHhBdG5aMlhCM2Q0ZUlpQWpxNitzeE9qb0tVMU5USkNRa2dKdWJHL2IyOXFpdnI4ZkZpeGR4NHNRSnVMbTVnY1ZpUVZGUkVVbEpTWkNWbFlXaW9pTEV4Y1VoTHkrUG56OS9ncHViRzdPenM4ak56VVZoWVNGRVJFUXdPam9LSlNVbFRFOVBRMFZGQmFhbXBxaXVyb2F6c3pQVTFOVFEydHFLdjMvL3psMkVnWUdCVUZSVXhJNGRPOURhMmdwbloyZDgrdlFKV2xwYTBOSFJnYlMwTkJRVUZNQmdNS0NwcVFrek16TW9LaXJpNXMyYlVGRlJnWlNVRktTa3BCQWRIWTF2Mzc3aDRjT0hPSDc4T0FEZ3paczNxS3lzQkh0M2R6Y1dMbHlJNXVabWVIaDRZTjI2ZFpDVGs4UE16QXhhVzF2UjFkV0Z2cjQrbUppWWdNbGtBZ0NlUEhtQ2xwWVcvUDM3RnprNU9iQzN0NGVrcENUTXpjM1IwZEdCZGV2V1ljZU9IUWdORGNYQmd3Y3hNVEdCYmR1MndkM2RIUXNXTElDWW1CaENRME1SR2hvS0tTa3A2T3ZyNDhXTEYvRDM5OGZYcjEvaDd1NE9IeDhmZUh0N2c0aVFsSlNFczJmUG9yYTJGbEZSVVhqNDhDRUNBd1BSM3Q2T3VybzZzRmdzaElTRW9LNnVEaUlpSXRpL2Z6K3NyYTNoNU9TRXlzcEs3TjY5RzlYVjFjakx5ME45ZlQyVWxaWHg3Tmt6ZUhsNUFYMTlmV1J0YlUzVjFkVkVSQlFRRUVDY25KejA3Tmt6VWxGUm9VK2ZQcEdxcWlvOWVQQ0E5dS9mVDJWbFpSUWVIazYvZi8rbTRlRmh5czdPSms5UFR5b3RMYVhObXpmVHhNUUU3ZG16aDdadTNVcHNiR3gwNnRRcHlzbkpvZi81bi8raGlJZ0lXcjE2TlNrcUtsSlpXUmtWRnhmVHg0OGZ5ZGZYbDR5TWpLaW9xSWh5Y25Jb016T1R6cDA3Ung4L2ZxVHg4WEdTbHBhbXhzWkdTa3BLSWg0ZUhtS3hXTVRPems1S1NrclUwOU5ERVJFUjlPM2JOL3J5NVF2eDhQQ1FsWlVWR1JzYms2K3ZMNlducDFOb2FDaU5qSXpRbHk5ZktDMHRqUjQrZkVnNU9UbFVYMTlQaUlxS0lrVkZSVHAwNkJDeFdDeHFhbXFpZGV2V1VVcEtDbkZ3Y05DL2YvOG9LeXVMM3J4NU0vZVNycTR1NnU3dXBnY1BIbEJjWEJ4NWUzdVRxYWtwblRsemh2VDA5Q2cyTnBZNE9EaW9wcWFHMXExYlI4Ykd4bFJlWGs0VEV4UDA4K2RQMHRYVkpVZEhSN3AwNlJMMTl2YVNuNThmN2Q2OW14WXRXa1NYTGwwaVMwdExhbWhvb0ZldlhwR3ZyeStabTV1VGdJQUFyVm16aHRMVDA2bW5wNGVZVENZWkdCaVFzTEF3MmRqWWtKZVhGMmxyYTVPbXBpWlpXMXZUcDArZlNFZEhoNzU5KzBabFpXVTBNVEZCQXdNRFpHMXRUVDA5UFRRNU9Va3VMaTdFcWFDZ2dPTGlZbXpjdUJGS1Nrb0lEdy9IK3ZYcnNXN2RPcWlycStQcjE2L1lzR0VEeE1URXNHM2JOc2pLeW9MRllrRmFXaG9sSlNYZzVPU0VnNE1EWHI1OGljYkdSckJZTEtTbXBpSTNOeGVxcXFyUTBOREF4TVFFUkVSRThQUG5UMHhOVGVIMTY5ZG9hV2xCWFYwZG5KMmRjZkxrU1h6NjlBbDlmWDN3OC9PRG41OGZqSTJOc1dIREJwU1ZsZUhNbVROZ01wa1lHQmpBNWN1WGNmVG9VWFIwZEVCQVFBQXRMUzBvS1NuQnlwVXJZV2hvQ0hGeGNkeTVjd2Z0N2Uwb0tTbEJkM2Mzcmw2OWlvcUtDcWlwcVNFdkx3KzdkKzlHUUVBQXhNVEV3QlliRzB1dlhyM0MwYU5IY2VUSUVXemF0QW1QSHo5R2MzTXppQWp6NTg5SGMzTXpvcU9qa1pXVkJVNU9Uc2pKeVNFakl3TTNidHdBTHk4dlltTmpvYU9qZzd0Mzc2Szh2QnlIRGgzQyt2WHJJU2twaWNlUEh5TXdNQkFuVHB4QVpXVWxaR1ZsOGZUcFV6UTNOOFBMeXd0YnQyNkZzckl5MXF4WkF3ME5EU1FuSnlNbUpnYkt5c3FJaVlsQlcxc2JkdXpZZ1dmUG51SFBuejhZR3h1RGlvb0twcWVuTVRJeWdwcWFHdXpldlJ2ejU4OEhHeHNiQkFVRndjWEZCUWNIQjZ4Y3VSTDI5dlpJU0VoQWFXa3BtcHViY2VMRUNRUUZCYUczdHhmejU4OEg1L2J0MjhISHh3Y3hNVEZvYUdpZ282TUQ1ODZkdzU0OWU4RER3NFBMbHkvRHlja0o2ZW5wTURNekF3Y0hCMHhOVFdGbVpvYnM3R3lNakl6Z3o1OC8wTkhSZ2JLeU1sYXRXb1VWSzFaQVEwTUQvLzMzSDM3OCtBRXhNVEdjUG4wYXpzN08wTmZYUjB4TURENTgrQUFHZzRHcHFTa2tKU1hCMXRZV1BqNCtXTEZpQlJnTUJoWXVYSWdyVjY2QWs1TVQvLzMzSDVZdVhRb1dpNFhaMlZsd2NYSE5pUzhpZ3JtNU9mcjcrOUhZMklpM2I5OWljSEFRdkx5OGFHeHNoSmlZR01yTHkyRm9hSWlXbGhiOHovLzhEMUpUVTFGZVhvNlRKMDhDTWpJeUZCOGZUN3k4dkpTUmtVRkhqaHdoZlgxOVdyNThPYVducDVPa3BDUjkvUGlSWEZ4Y3lNN09qcVNrcEdoc2JJekN3OFBwNWN1WHhNYkdSbmw1ZVhUeDRrVlNWRlFrRnhjWFlqQVlwS21wU1ltSmlmVDgrWFBLeXNxaW1wb2FZakFZMU43ZVRycTZ1cFNmbjA4c0Zvc0VCQVRvN2R1MzVPVGtSTGEydHBTZG5VMTM3OTRsZFhWMTJyWnRHd1VIQjlQV3JWdnA1TW1UZE8vZVBSSVhGeWR2YjIrYW5Kd2tIeDhmK3ZUcEUvWDA5SkNhbWhxOWZ2MmFObTdjT0lmQWRIVjFxYkN3a0xTMHRNakN3b0tXTFZ0R1BUMDlORG82U2xOVFV5UWdJRURzeGNYRnFLdXJnNHFLQ3FxcnErSGs1SVNSa1JFNE9qcmkrdlhyU0V0THc3eDU4K0RnNElDWm1SazBOalppZUhnWVZWVlYwTlhWeGZIangrSHI2NHVKaVFsa1pHVEF4Y1VGWGw1ZTZPL3Z4NnBWcStEbzZBaE9UazZJaW9yaTJiTm5DQTRPeHNHREJ5RWtKSVFsUzVaQVZsWVdWNjVjUVVOREEvejkvWEh3NEVHa3BLUmc3OTY5c0xLeUFqOC9QejUrL0FndExTM1kyTmlBblowZFJrWkdlUG55SmNURXhORGQzWTM2K25xb3E2dmo2TkdqV0xObURVeE1UTEJ4NDBiNCtQakF6YzBOZ1lHQnVIRGhBalpzMklCTm16YkJ6czRPdGJXMXVIdjNMdGhEUWtKdzlPalJPWlZWVkZTRVJZc1dBY0RjdXRqZTNvNXQyN2FocWFrSlg3NThnYjYrUG9xTGl5RWdJSURpNG1Kb2FXbWhxYWtKWTJOaldMWnNHV1ptWnFDaG9ZRUhEeDVBVFUwTklpSWlVRkpTUW5KeU11enM3QkFURXdNdUxpNEVCUVZoZW5vYVBqNCsyTFZyRjU0K2ZZckt5a293R0F3SUNncUNoNGNIRXhNVHFLcXF3cWxUcDZDbHBRVnVibTZVbEpSZzc5Njl5TTNOeGNtVEo1R2JtNHZtNW1hRWhvWmljbklTeXNyS2lJMk5SVUJBQU1MQ3dqQStQbzZjbkJ3WUdCamcyTEZqWURBWWlJaUlRRWxKQ2RpY25aMUpWRlFVUGo0KzBOVFVSR05qSXg0L2ZneFpXVmxvYUdpQW01c2JiR3hzMkxsejV4eGZjM1oyaG9PREEyUmxaZkhvMFNNSUNRbWhzTEFRVDU0OHdmNzkrMkZsWllWTm16YWhyYTBOYjk2OFFVSkNBb3lNakhEMTZsVXNYcndZd3NMQ21KNmVocUtpSWs2Y09JRmZ2MzdoNGNPSHFLMnRSV2RuSjVxYW1xQ2pvd01KQ1Fsb2FHamcvdjM3bUo2ZUJqczdPL1QxOWVIaTRnSWhJU0djUG4wYStmbjVxSzZ1eHNtVEo4RmlzZERYMTRlaW9pTFkyZGxoeFlvVm1KMmR4ZXZYcnlFcUtvcFZxMWFCd1dEZytQSGp5TTNOUlVoSUNCQVVGRVNDZ29JME96dEw5ZlgxZE9yVUtSSVRFNk85ZS9mU3o1OC9LU29xaXJ5OHZHamh3b1ZrYTJ0THo1NDlvejE3OXREWTJCang4L1BUdG0zYktENCtuc0xEdzJuWnNtVmtaMmRIN096c1ZGSlNRdHpjM0tTaG9VRWNIQnhrWW1KQ2ZuNSt4TWZIUjYydHJhU2hvVUZjWEZ5a3A2ZEhMQmFMYkd4c3lNTENnbkp6YytuWXNXT1VtNXRMWm1abXRHblRKZ29LQ2lJR2cwRTVPVGxVV1ZsSmxwYVc5UERoUXpJMU5hVTllL1lRRHc4UExWMjZsQ1ltSm1oMGRKVDA5ZlVKQUsxWXNZSVdMVnBFSmlZbTlPN2RPMkt4V0RRK1BrNWhZV0drcnE1T1lXRmh4SzZvcUlqQ3drSUVCQVRnOE9IRDJMZHZIMzc4K0lHSWlBakl5Y2xCUmtZRzd1N3VNRFEwQkRzN082U2xwV0ZoWVFFZEhSM2N2SGtUaVltSitQUG5EM2JzMklIUjBWRThmZm9VV1ZsWktDb3FncEtTRXA0OWV3WS9QeitvcXFvaUl5TUQwZEhSc0xhMmhxdXJLNTQ4ZVlLaG9TRklTVW5CeXNvS1RVMU5XTEJnQWQ2K2ZZdm56NStqb3FJQ1RDWVRIaDRlS0M0dVJuRnhNUzVjdURDSHkrYlBudzhwS1Nsb2FtcmkvZnYzdUhmdkhpNWR1Z1FHZ3pFM3BtMXRiY2pMeTBOSlNRbDZlbnF3Zi85K3ZIdjNEaytmUGdXTHhRSmJSa1lHVlZkWFEwcEtDZ3NXTEVCUVVCQjRlWGtSRVJFQmZuNSsxTmJXUWwxZEhicTZ1Z2dJQ0lDVmxSVmV2SGdCWVdGaDZPbnBZV3hzREdGaFlUQXlNa0pEUXdNZVBYb0VPenM3YUd0cnc4TENBdWJtNXREUTBNRERodzhSRmhZR0d4c2I3TisvSC9QbXpjT2FOV3V3ZHUxYVBIcjBDQzB0TFdodWJnYUF1Vi9jdFd2WE1EbzZpcTZ1THN5ZlB4Ky9mdjJDc2JFeDFxMWJCeXNySzNSM2QrUHQyN2RZdG13Wm5qOS9qcGlZR0R4Ky9CaXpzN053ZG5iR25UdDNvS3lzREVWRlJWUlVWT0Rldlh2SXpNeUVzTEF3d3NQRG9hbXBDYzdKeVVsNGVYbmgrZlBuMkxwMUt6ZzRPSkNabVluMjluWVlHaHJDMmRrWlpXVmxrSldWeGZEd01Nek16REE0T0lpb3FDZ3NXTEFBalkyTk1ERXhnYTJ0TFk0ZVBZcWdvQ0JZV2xyQ3lzb0tTVWxKNk9qb0FCSGgyN2R2aUkrUHgvbno1L0hxMVNzVUZ4ZERRVUVCRFEwTitQdjNMelp0Mm9TaG9TR29xNnNqSnljSG56OS9ocm01T1VSRVJIRHUzRGtFQkFSQVRVME5ob2FHS0NvcWdybTVPZlQxOWNITHl3c21rNGxuejU3aHg0OGZXTDU4T2RUVTFEQThQQXdpQWg4Zkg4ek56WEgvL24xOC92d1p4Y1hGVUZGUndmWHIxd0VBYk8zdDdUUXhNVEZuWEJnYkcwTmRYUjBORFEyWW5wN0c2dFdyc1dmUEhuQnpjMk40ZUJoTUpoTXlNaklJQ1FsQllXRWhvcU9qWVc1dURqOC9QNnhac3diTnpjMFFFQkJBWjJjbkZpNWNpS3RYcjJKd2NCRFhybDNENjlldjBkM2RqV1hMbHNIUHp3L0hqeCtIcmEwdG1wcWFZR05qZy9Ed2NPVGw1WUhGWXVIeTVjc1lIaDVHVEV3TWlvdUxzV1hMRmpDWlRGUlhWNk94c1JFK1BqNTQ4ZUlGOHZQem9hU2toSmN2WDZLaG9RRzdkKytHaG9ZR2NuTnprWitmRHdhREFRc0xDNGlLaXVMMjdkc29MeThIaThYQzNyMTdJU3NyQzg0OWUvWkFUVTBORXhNVDZPM3RSVTFORFN3dExTRXRMWTE3OSs3aDVjdVhFQklTZ3F1ckt5SWpJMUZRVURBM0lyYTJ0cmh5NVFwcWEyc3hmLzU4cUtpb0lDVWxCZDNkM1RoNDhDRDI3Tm1EbUpnWVdGdGJnNE9EQXdZR0J2aisvVHUrZmZ1R2dJQUF0TFcxZ1pPVEUwRkJRZkQzOTRlYW1ocUVoSVN3YytkT25EbHpCbU5qWS9EMDlJU1FrQkJHUjBleGF0VXFKQ1Frb0srdkR6bzZPckMxdFlXRGd3T3NyYTNSMk5pSWQrL2VRVnBhR2lvcUt1anY3MGRKU1FsNGVIaHc4K1pOdUxpNElEczdHLy8rL1FNM056ZTJidDJLenM1T3NETVlEQmdhR2tKV1ZoYUNnb0lJREF5RXJhMHRlSGg0c0hidFdzVEh4ME5CUVFFTEZ5N0VzMmZQME52Ymk3UzBORXhPVHFLMnRoYjI5dmJnNStlSHFha3BNak16WVd4c0RCNGVIb2lKaVNFd01CQU9EZzR3TURCQVRrNE9ObTdjaU9YTGwwTkxTd3RDUWtMdzhmSEJtVE5ud0dLeFlHMXRqVHQzN3NEVTFCUnFhbXA0OGVMRm5PV1ZuWjJOZ1lFQlRFMU5ZZEdpUmVEazVJU0ZoUVgyNzkrUDNOeGNiTm15QmNlUEg4ZUhEeDhnSXlPRGYvLytJU1FrQk1iR3hsaStmRGsrZnZ5STFOUlUzTHAxQy9IeDhVaElTTUM4ZWZOZ1lHQUFOZ2FEUWJ0Mzd3WUhCd2NpSWlMZzd1Nk94c1pHY0hKeVl0MjZkY2pNeklTcHFTa1dMVnFFOHZKeUxGeTRFSEZ4Y2VqczdFUndjREFpSWlKUVhWMk4rZlBuUTA5UEQ4M056VkJRVU1DOGVmTVFIUjBOTFMwdDNMaHhBMFNFVDU4K1FWSlNFdno4L0pDVmxjWGV2WHR4OGVKRnJGdTNEb09EZ3pBMU5jV0RCdyt3ZCs5ZVdGaFlZUFhxMVFnUEQ0ZWlvaUlPSGp3SU56YzNhR3RyWS9YcTFUaDgrUEFjRmxOUVVFQjRlRGkrZnYyS1E0Y09ZY09HRGNqSXlFQi9meitFaElRd1BUMk42ZW5wT2I1WVVGQ0FTNWN1UVVSRUJKeXZYcjFDUlVVRkpDUWs1dWdxQUFRRkJXSGx5cFZZdUhBaHpNek1jUGZ1WGJpNnVxSzh2QnhQbmp6QnYzLy9JQzh2RHc0T0RwU1hsMk5pWWdLSmlZbHpGMVI3ZXp0V3JWcUY1Y3VYNDlTcFV4QVNFa0phV2hxMmJObUN6TXhNZlB2MkRkM2QzWGozN2gzYTI5dkJZckZ3NWNvVnhNWEZRVTlQRHp3OFBGQlRVNE9EZ3dPQ2dvSlFVRkNBMHRKU2VIaDR3TWpJQ0FNREF4Z2NITVRzN0N3YUdocHc3dHc1dkg3OUdyVzF0UmdaR1lHSmlRbDZlbnBRWFYyTmxTdFhRbGRYRjQ2T2p1anA2VUZnWUNETXpNemc0dUlDbEphVzBzT0hEOG5JeUloaVltS0l3V0NRaVlrSmRYUjBrS2lvS05YVjFaR0ZoUVZwYTJ2VGpoMDdLQ2twaVFRRkJTazdPNXVLaTR2cDJiTm5wS1dsUmEydHJWUlJVVUZwYVduRVlEREl5TWlJN3R5NVE5Ky9meWNsSlNWeWRIU2t6czVPK3Z6NU14VVdGbEorZmo3WjJ0clNreWRQS0Q4L254WXZYa3pxNnVvVUV4TkQ0dUxpMU5iV1JpSWlJdlRmZi8rUmtaRVJpWXFLa3E2dUxxV2xwVkZqWXlPNXU3dFRXVmtaNmVqb1VIdDdPL242K2xKL2Z6K2RQMytlZHUvZVRTRWhJYVNnb0VCeWNuTGs2ZWxKcDA2ZG9zREFRTHA3OXk0MU5UWFI0c1dMU1ZGUmthQ29xRWh0Ylcxa1pHUkVYNzU4b1lhR0JycDgrVElkUDM2Y0ZCUVV5TkhSa1FJREEybDZlcG9lUFhwRU8zYnNvUGo0ZUtxcnF5TlJVVkZxYUdpZzR1SmkrdnYzTHhVV0Z0SzVjK2ZJMGRHUmVIbDVhV0JnZ0E0ZVBFam56cDBqVzF0YmV2MzZOVlZVVkpDMnRqWU5EUTJScGFVbHRiVzFVV3hzTEYyK2ZKbGFXbHJvMUtsVDlPdlhMenB5NUFodDNyeVpwS1NrcUttcGlaaE1KaFVYRnhNWEZ4ZXhzN09Uam80T05UYzMwNkpGaXlnK1BwN016TXpvekprelZGVlZSWnMyYmFMczdHeTZkKzhlTlRRMDBKSWxTNml2cjQvYTJ0cUlrNU9URWhNVGFXQmdnTFMwdEFqRHc4TzBaczBhNHVMaUlpNHVMcnB4NHdZTkR3K1RuWjBkK2ZuNVVXNXVMdlgwOUpDdnJ5OXBhbXFTckt3c0ZSVVZrYlcxTlhWM2Q5T1BIejlvYkd5TXVydTc2ZHUzYjhSa011bml4WXNrSVNGQkhoNGVsSnViU3hzMmJDQkJRVUd5czdNak56YzMyclp0Ry8zNzk0OXljM09wc2JHUm1wcWFTRUJBZ0k0ZlAwNHRMUzNrN2UxTjc5Ky9Kd2tKQ1hyMDZCSGw1K2ZUM2J0M0tUVTFsYnE2dXVqMzc5K2tyYTFOQlFVRlZGOWZUK3ZYcjZmeDhYRmFzR0RCSERPc3JLeWt5c3BLY25aMkprVkZSVXBOVGFWcjE2NVJhbW9xK2Z2N2s0K1BEelUyTmhLYmxwWVdTVWxKd2NIQkFjYkd4cWlwcVVGTlRRM1UxTlFnS2lvS1MwdExqSXlNNE15Wk01aWFtc0tYTDErd2JOa3loSWFHNHVUSmszajY5Q21Ta3BLUWw1ZUg3ZHUzbzZPakEzRnhjVEF4TWNHREJ3OHdPVG1KblR0M29yeThIQklTRXNqSnlVRmtaQ1JzYkd5UWw1ZUgvdjUreE1mSHc5SFJFZEhSMFFnS0NvS2ZueDg0T1RuUjJ0b0tOalkyaElXRjRjU0pFK0RoNGNINCtEZzRPRGdnSmlZR0p5Y25SRVpHUWtkSEI3VzF0UWdLQ29LbXBpYWVQbjBLWGw1ZVNFdExvNmFtQmsrZVBJRzZ1anErZi84T05qWTI3TisvSDRzV0xZS05qUTNZN096c2FQdjI3ZGl6WncvVTFkVWhMQ3dNYjI5di9QejVFOHVYTDhlZlAzOXc4K1pOS0Nnb3dNek1EUEh4OFFnSUNFQkRRd1BjM2QyeGZ2MTZKQ2NuUTFkWEY4SEJ3ZURqNHdNUklTWW1CcU9qbzJodmIwZDVlVG55OC9QaDYrdUxkZXZXUVVOREF5MHRMZURnNElDeHNUR0NnNE1oS1NtSm1aa1pIRGh3QVAvKy9adjdOVXRLU3FLam93T3hzYkVRRXhNREx5OHZ1cnU3a1ppWWlPVGtaQmdhR2tKYld4dEtTa29ZR1JtQmdvSUNEaHc0Z043ZVhxaXFxcUt4c1JGRFEwT3dzYkZCY25JeUNnc0xZV0ZoZ1hmdjNxR3hzUkhJek15a2h3OGYwb2NQSCtqaHc0ZkV6YzFOaXhjdnBzT0hEOVBvNkNodDNicVZzckt5aU1sa1VuVjFOVDE2OUlqT25qMUw5ZlgxMU5MU1FsTlRVOFRKeVVsWHIxNmxSWXNXVVZWVkZRa0xDOVBZMkJoVlZsYVNxS2dvSFRseWhBNGRPa1J4Y1hHVWxaVkZVMU5USkNjblJ6NCtQaFFlSGs2N2QrK20zNzkvazUyZEhXM2N1SkdpbzZOSlZWV1ZrcEtTaUkrUGorcnE2dWpCZ3dla282TkRYbDVlMU5UVVJQcjYrcVNscFVVakl5UDA5KzlmS2kwdHBZNk9EbnJ4NGdVWkdoclM0Y09IcWJTMGxGUlVWR2gwZEpSc2JHd29PanFhUmtkSGlZT0RnMWdzRnBXVWxCQW5IeDhmcmw2OU9tY2loSWVIUTA5UEQ2MnRyWGovL2owNk96dHg3Tmd4WExseUJUZHUzSUNVbEJUNCtQZ2dKQ1NFaUlnSTdOdTNEOUhSMFJBVUZJU3dzRERXcjE4UE56ZTN1VkU0ZGVvVWhJV0ZFUlFVaEo4L2YyTHQyclZRVUZEQXJsMjc1b3pMbEpRVWNIRnhRVjFkSFNJaUluQjBkTVRZMkJoMjc5Nk4wdEpTaElXRndkemNITXJLeW9pTWpJU1BqdytHaDRlUm5aMk40T0JnTEYyNkZOYlcxZ2dLQ3NMU3BVdHg1Y29WU0VoSUlDc3JDME5EUTdoNDhTTDQrZmx4NmRJbE9EZzRZTisrZlRBeU1vS1dsaGF3ZE9sU3FxcXFJa3RMUzJLeFdNVEp5VWtKQ1FtMGNlTkd1bm56SmgwOWVwU1NrNU5KVTFPVHVMaTQ2Tk9uVDdSLy8zNTY4K1lONWVmbms0dUxDL0h6ODVPb3FDaFZWRlRRbmoxN3lOZlhsMUpTVXNqVDA1TjhmSHlJaDRlSE5EUTBpSWVIaDRpSXZuMzdSa2xKU2RUVTFFVG01dWFVbVpsSmNYRnhaR1ptUnVycTZqUXhNVUdlbnA3RVlERG95NWN2RkI4ZlQxeGNYT1RxNmtwT1RrNVVWbFpHbXBxYTVPM3RUWFYxZGNUUHowOXljbkpVVTFORDN0N2V4R0F3YVAzNjlhU2twRVFlSGg1a2EydExZV0ZoeE0zTlRkemMzTlRZMkVncEtTbFVXbHI2djhiSXJsMjdDQUFORFEzUmYvLzlSNzI5dmFTdHJVMnpzN08wYytkT1dydDJMWTJNakpDOHZEd3BLU25SclZ1M3FMdTdtNVNWbGNuZTNwNWFXMXNwT3p1Ym5KeWNxS1NraEFZSEIrbm8wYU9VbDVkSEowK2VKRE16TTdwOCtUSU5EZzdTNzkrL2FmZnUzV1J0YlUyTmpZMXpJL1A5KzNkeWQzY25WMWRYaW91TG82ZFBueEtEd1NCQlFVRVNFaEtpcEtRa01qYzNwMGVQSHRHdlg3OG9NaktTNU9Ua0tDTWpnMUpTVXNqVzFwYjgvUHlvcUtpSTJ0cmFLREF3a01URnhTa3FLb3FZVENhOWZ2MmEwdFBUYVdCZ2dGUlVWS2kzdDVmKy9mdEgrTDhnbEwrL1AwbEpTVkZGUlFVVkZ4ZFRUMDhQeWNqSTBQSGp4Nm04dkp4eWMzTnArZkxsZFBmdVhiS3dzS0MxYTlmUzh1WExTVTlQajg2ZlAwL0R3OE4wN2RvMXVuMzdOc25JeU5DS0ZTc29JaUtDT0RnNDZObXpaN1J6NTA2YW1KZ2dPVGs1bXBpWUlCTVRFenAyN0JndFdMQ0FHQXdHclYyN2xvYUdocWl6czVPeXM3TnA4ZUxGVkZ4Y1RDOWV2S0QwOUhScWFXbWh0clkya3BhV0poY1hGM3IvL2oydFdMR0NUcHc0UVVsSlNXUm5aMGZIangrbmhRc1gwdWpvS0ltTGk5UFBuejlwYkd5TW5qOS9UaTR1TGxSY1hFeXRyYTBrS2lwS2NYRnhwSyt2VCt3UkVSRndjM09EaUlnSTNyeDVneU5IamlBM054ZHNiR3hJU1VsQmFHZ29YcjkrallxS0NyQllMRGc1T1VGUFR3LzgvUHh6K3Z2bno1OXdjSEJBUVVFQkJnY0hVVnRiQ3dEbzYrdURuWjBkbUV3bS92ejVBejA5UFV4T1RxS21wZ2JpNHVLSWlvckNwMCtmb0tHaGdkKy9mME5MU3dzS0NnclExZFdGa1pFUlFrTkRvYWlvaUlVTEY4NEZLWGZ1M0ltWm1SbW9xS2hBVGs0T0Nnb0tHQjhmaDZhbUptWm5aekU1T1luejU4OURYVjBkSHo1OHdObXpaK0hwNlFrUkVSR2NQMzhleGNYRkdCOGZSM2QzTjR5TmpZR09qZzY2ZCs4ZTNibHpod29LQ3VqTm16ZFVWbFpHMTY1ZEl5RWhJYnA2OVNyVjE5ZFRURXdNalk2T1VraElDSEZ4Y1ZGOWZUM056czdTOSsvZlNWcGFtcXl0cmNuR3hvWU1EUTJwdHJhV0xseTRRRDA5UFhUbnpoMnl0YldsbHk5ZmtwbVpHVVZHUnBLZW5oN2R1WE9IZnY3OFNVVkZSYlJreVJJaUlucnk1QWtORFExUmVIZzR5Y25Ka2FtcEtlM2F0WXZFeGNYcDI3ZHZKQ3dzVEIwZEhiUnYzejc2OGVNSDdkNjltOFRGeFNraElZSHUzTGxEUlVWRkZCSVNRbjUrZnJSMDZWSXlORFFrSHg4ZmV2ZnVIVlZXVnRLbFM1ZG81ODZkOU8zYk4vcjc5eS9KeWNrUjU3eDU4MUJUVXdOWFYxYzhmdndZRWhJU2VQUG1EWFIxZFRFMU5UV1gyTEMydHA3RDRJOGVQUUlmSHg4dVhyeUk2ZWxwZEhkM3ozbUtrcEtTS0NvcUFpY25KNHlNalBEbXpSdTB0N2RqL2ZyMThQZjN4K2pvS0VSRlJSRVNFb0xZMkZnSUN3c2pPam9hOSsvZng4VEVCQTRkT29UMzc5OURUVTBOZCs3Y3diWnQyK0RvNklpb3FDamN2SGtUS1NrcEdCb2Fnb2lJQ01URnhhR2lvb0tZbUpnNWZENHdNSUJIang2aG82TUQzTnpjS0Nnb3dQNzkrK0hwNllucDZXbVltcHJDMDlNVEppWW11SG56NXYrbXhCd2NIUERyMXk4Y1Bud1lEeDQ4UUVCQUFMWnYzdzRqSXlOa1ptWmliR3dNSmlZbVdMZHVIZjc4K1lQczdHd1lHeHRqM3J4NUtDOHZSMkZoSVI0L2ZneDlmWDNjdUhFRHExZXZ4cytmUDZHbXBvYloyVmtJQ2dwaWNuSVNUNTQ4d2VEZ0lPN2R1d2MyTmpaczM3NGREQVlENWVYbElDS3NXclVLcHFhbUtDc3JnNk9qSTE2OWVvWGUzbDRFQndkajhlTEZPSGp3SUd4c2JMQnYzejVzMnJRSlZsWldxSzJ0eGRUVUZOTFQweUVvS0lpQmdRSEl5c3BDVEV3TWNuSnlrSmFXeHBZdFcxQmRYUTFlWGw3OC9mc1hYNzkrUlhwNk91VGs1SUFYTDE2UWc0TURQWC8rbkRRMU5lbnQyN2NVRXhORHNiR3gxTkhSUVh2MjdLRlBuejZSa0pBUWVYbDVrWitmSDYxYnQ0NFlEQVl0WDc2Y2hJV0Z5Y1hGaFlxS2lzak16SXdZREFiSnk4dFRiMjh2S1NvcVVucDZPdm42K2xKZlh4ODVPVGxSZkh3OHljdkwwOVRVRkVsTFN4TTNOemY5K1BHRDFxMWJSOEhCd1RRNk9rcXRyYTFVV1ZsSlUxTlRSRVQwOHVWTGFtNXVKaWFUU1dscGFTUXZMMDhDQWdJME5qWkdFUkVSZE9MRUNmTDI5aVlURXhOYXNtUUpzYkd4a2FDZ0lLMVpzNFlzTFMzcDVzMmJGQmNYUitucDZkVGUzazd0N2UyMGNlTkdZckZZeEdaa1pFVGw1ZVVJQ0FpQWk0c0w1cytmajhlUEgrUHAwNmRnWjJkSFNVa0pabVptRUJvYUNnTURBMVJWVmVISGp4OHdOVFZGYTJzclpHUmtzR0RCQXR5NmRRc3JWNjVFVjFjWE5tL2VqSnljSEN4YnRnelYxZFZnc1Zqdzl2WkdmMzgvTEMwdDhlYk5HMlJrWk9ENTgrZHpYT0QvSE4vazVHUThmZm9VNjlldm4zdkd4c2JDM3Q0ZTE2NWR3NTgvZnlBcEtZbSt2ajVFUlVVaEtTa0pOMi9leEo4L2Y5RFIwWUhLeWtvME56Zmo4ZVBIWUdkbng4R0RCM0hod2dVY1BYb1VuSnljc0xLeWdvR0JBYTVjdVlJRkN4YUFUVmxabVJZdFdvVE5temRqYW1vS2NYRnhDQWdJd05UVUZESXlNbUJ2YjQrZlAzL0MzOThmUkFScGFXbDRlbnJPdVVBdExTMndzcktDazVNVHhzYkdVRnhjREJNVEU2U2twTURMeXd2ZnYzOEhPenM3dm56NWdzREFRS2lycTJONmVocWhvYUg0L2ZzMyt2cjZzSExsU3Z6MzMzL3c5L2RIZkh3ODB0TFNzSFRwVXR5OWV4ZkJ3Y0dRazVPRGc0TURORFEwTURVMWhRc1hMc0ROelExRFEwUDQrdlVyamg4L2pxMWJ0K0xvMGFNNGQrNGN2bjM3aHBtWkdWeTdkZzE1ZVhtNGYvOCtvcUtpa0phV0JuOS9mNVNXbG9LTGkrdC9mY3Y4L0h3RUJ3ZWpyS3dNZS9mdXhaVXJWM0R2M2ozbzZlbmgyTEZqYzBvdk5UVVZyYTJ0YzRocVpHUUVWbFpXcUsrdng4NmRPOUhTMG9MdzhIQWtKU1VoSmlZR3IxNjl3b2NQSDZDbHBRVVhGeGRvYVdtaG9LQUFHemR1UkhkM042U2xwV0ZtWm9haW9pS29xYWtoUHo4Zk5qWTJHQmdZZ0lHQkFRUUZCVEUwTklUaDRXRVVGQlJBVkZRVUxCWUx6czdPME5MU3d2Mzc5NUdYbHdkT1RrNDhlUEFBT2pvNnVIRGhBaFl2WGd4RlJVWDgrdlVMcTFhdFFtZG5KOGJHeHVEazVJVGc0R0F3R0F5SWlZbGg2ZEtsZVB2MkxkaW5wcWF3WnMwYTdOaXhBMVpXVmtoTVRNU05HemZRMmRtSng0OGZ3OERBWUE1NS9WK0dYMDVPRHNlT0hZT3VyaTQrZi82TXJLd3NqSStQdzl6Y0hKS1NraEFRRU1DNWMrZlExdFlHSFIwZEtDb3F3dDdlSGl3V0MrYm01Z2dORFoxTGg1MC9meDdaMmRuSXo4K0hycTR1L3Z2dlA1dzZkUW9WRlJWd2NuSkNSRVFFcksydE1YLytmSGg3ZTBOVVZCU1RrNVBJeXNwQ1hWMGR0TFMwRUJJU0FpYVRpWm1aR1FnSUNNeXAyU2RQbnVESWtTUHc5dllHRnhjWDR1TGk0T3pzakV1WExnRUFWcTVjQ1FRR0JoS0x4YUt1cmk1YXZIZ3hiZG15aFJZc1dFQWFHaHBrYlcxTmFtcHFkUGJzV1VwTVRLVEl5RWpTMXRhbTRPQmdzckN3b0xxNk9nb0xDeU1CQVFGU1ZWV2xreWRQMHFsVHA4amYzNStHaG9Zb096dWJEaDA2Uk1QRHd3U0E2dXJxNXNESSsvZnZLU2twaVlxS2l1akVpUk5VWGw1T0RRME45UFRwVXhJVEU2TWRPM2JReE1RRUZSWVdVbkp5TXZYMjlwSyt2ajdWMTlmVGhnMGI2Ty9mdjNUNDhHSHE3KytuWjgrZTBmZnYzOG5Bd0lEczdPekl5TWlJMXE1ZFN3Y09IS0FsUzVaUVNFZ0lSVVJFRUFCS1QwK24xdFpXMHRUVXBCY3ZYaEM3djc4L2FtcHFjT1RJRVNncEtXRjRlQml5c3JLSWk0dkRyVnUzRUI0ZURqRXhNUncrZkhndVEwUkV1SHo1TXE1ZHV3WU9EZzQ4ZWZJRWZIeDhtSjJkblFNVUZ5NWNBSXZGd3BZdFc3QjU4K1k1bC9qRml4ZHp3ZVd3c0RBY09uUUlPM2JzZ0kyTkRRd05EY0ZrTWlFdkw0OGZQMzRnTGk1dUxvcm43dTQrZDRwVVZGUXdNaklDSnljbjlQWDFnWitmSDY2dXJoZ1lHRUJoWVNGTVRVMXg0c1FKUEg3OEdKOC9mOGFOR3pmUTM5K1B3c0xDdVR0SldWa1p2THk4NER4ejVnd01EUTJockt5TTA2ZFBRMWRYRjE1ZVhsaStmRG5tejUrUDl2WjIyTnZibzZpb0NOKytmVU4rZmo1bVptWncrL1p0UEhqd0FGcGFXaGdaR2NHK2ZmdGdiVzBOTGk0dUxGNjhHRGR2M3NUWnMyY1JIUjBORFEwTi9QcjFDOExDd25CM2QwZEpTUW5hMnRyUTI5dUxmZnYyZ1plWEYxeGNYSFByYzF0Ykd6dzhQTEJzMlRMdzgvUGoxNjlmT0hmdUhONjllNGZkdTNlanFLZ0lBZ0lDS0NrcFFYbDVPYnE2dWhBY0hBd0RBd01zWDc0Y2h3OGZocGFXRnRMUzB2RGl4UXU0dTd0RFVGQVFCZ1lHcUtpb3dPZlBuK0h1N281ZHUzWUJOVFUxZFBmdVhYcjQ4Q0ZOVEV4UVpXVWx5Y3ZMazRpSUNJV0docEs0dURpNXVMalF2My8vYU0rZVBTUXVMazZyVnEyaXpzNU9DZ2tKb1M5ZnZwQ1ltQmg1ZTNzVEx5OHZMVjY4bURnNU9lbisvZnMwUER4TVY2OWVwVGR2M3BDeHNURWxKaWFTazVNVFJVZEhrNUtTRXZIeDhaR0VoQVFORFExUmIyOHZEUTRPMHVUa0pQMzc5NDhZREFiNSsvdlRxbFdyU0VSRWhIeDlmVWxXVnBadTM3NU5zN096ZE92V0xVcE1UQ1FQRHc4YUdocWkrL2Z2MDRJRkN5ZzBOSlNLaTR0cFptYUdidDY4U2I2K3ZtUnZiMDhkSFIxVVdscEtKU1VsdEhyMWF2cjM3eDh0WExpUTJMaTR1T2o0OGVQZzVPVEVwazJic0hqeFlnd05EVUZRVUJBRkJRV29xcXFDbXBvYS9QMzk0ZVRraElhR0JwU1dsdUxmdjMvWXZuMDdIang0QUZkWFYrVGw1VUZjWEJ6MTlmVjQvdnc1YnQyNkJTVWxKYmk0dU9EUG56L1l2SGt6VHB3NGdiTm56MEpHUmdiQ3dzSVlHUmxCUWtJQ3ZuMzdCbVZsWlhCeWNtTHQyclZnWTJQRDkrL2ZjZXZXTFdSbVprSkRRd1BMbGkzRGxTdFg0T2pvQ0VkSFIvVDE5V0hwMHFYZzUrZWZzK2U1dWJsaFptYUcvUHg4V0ZsWklTTWpBOGVPSFlPQ2dnS2lvcUxBeGNVRlVWRlJKQ1Frd04vZkg0OGVQUUo3WTJNajJ0cmFzSFhyVnRqYjIyUHYzcjFJVDAvSHAwK2Z3R1F5c1huelpxeGN1Ukp2Mzc2RnBLUWtNakl5a0pLU2dsZXZYaUVxS2dvbUppYXdzYkhCaFFzWG9LeXNqT0xpWXF4ZXZSb3JWcXlBbFpVVkppY25JU01qZyt2WHJ5TTRPQmdwS1NuSXlzcENaMmNuaW9xS01EWTJocTlmdjhMSXlBaFhybHhCVEV3TWlvcUtRRVM0Zi84K3JLMnQ0ZVBqZzh6TVROeS9meCtWbFpYZzRPREF4bzBiWVdCZ0FIRnhjYXhac3daK2ZuNzQ5T2tUaElXRk1YLytmQmdaR2VISGp4OTQ5KzRkdXJ1NzBkblppVE5uem1ETGxpMDRmUGd3K3ZyNmtKV1ZCZmJ6NTgvajlldlhHQjhmeCt6c0xQNzgrWVBhMmxxWW1wcUNqWTBOWjg2Y2dhdXJLOTYrZll2djM3OGpPam9hOHZMeXFLbXBRWFIwTkthbXBxQ3NySXdEQnc2QWc0TURycTZ1V0xseUpXeHNiTEIrL1hvVUZoWkNWMWNYdmIyOVdMeDRNZjc3Nzc4NUo3aXNyQXg4Zkh3WUhoNUdhV2twdkwyOWNmWHFWVENaVE5UWDErUHg0OGZJenM1R1kyTWpRa0pDa0p1Ymk1R1JFUmdZR09ERGh3OFFGUldGZ0lBQVZGVlZZVzF0RFVORFEweE5UZUhWcTFkSVRFeEVlM3M3RGgwNmhOZXZYNE9mbngvSnljbFFWRlNFbVprWndzTENNREl5QWdRRUJOQ2FOV3RveDQ0ZHRIVHBVaG9lSHFhOWUvZlMwNmRQcWJHeGtlVGw1V2xvYUlpYW01dXB0cmFXQ2dvS3FLeXNqSzVjdVVLZW5wN1UwdEpDd3NMQzVPcnFTaDRlSGhRWkdVbnE2dXBrWm1aR1g3NThvYmEyTnBLVWxLU2lvaUphdEdnUnRiYTIwcVZMbDRpTGk0dXFxcW9vSXlPRC92ejVRMjV1YnNSa01tbmx5cFYwK2ZKbHVuRGhBaDA3ZG13T2FraEtTbEpKU1FtdFg3K2V1TGk0S0RBd2tQTHk4b2lOalkyV0xWdEdWNjllcGQ3ZVhqcDA2QkNWbFpWUlhWMGQ4ZkR3MEtKRmkyamV2SGtVSHg5UC83OGZSVTFOVFZSVlZVWGUzdDdFMmRIUmdiTm56OExNekF3OVBUMVFWVldkS3pEZXUzY1B4Y1hGYUdwcXdxVkxsOERGeFlXc3JDems1dWJpOCtmUFdMMTZOZWJObTRjdFc3YUFuWjBkTWpJeXFLbXB3Y1dMRjZHZ29JQzh2RHhzM3J3WmNuSnlNREV4bVpQSjlmWDFVRlZWUlZkWEY2eXNyREErUG82MWE5ZWlwcVlHU2twS2NITnpRMHhNRE5hc1dRTkZSVVY4Ly80ZDQrUGpPSC8rUEo0K2ZZcDM3OTRoTnpkM3JsWFcyOXVMblR0M29xMnREYjYrdmdnTkRZV0Rnd084dmIyeGFOR2lPVFc0Yjk4K0hEOStISk9UazlEUzBrSk5UYzMvUm1WMzdkb0ZKcE9KUTRjT1llUEdqZmo5K3pjY0hSM2g0ZUdCcTFldllzV0tGZmoxNnhkeWMzTUJBRnUyYkFFZkh4ODJiOTQ4NTlVTENBaWdvYUVCUGo0K2VQTGtDZjcrL1FzMU5UV1VsWldCeVdSaTY5YXRFQmNYQjR2RndydDM3M0RzMkRFWUd4dlA1WWI1K1BqUTM5K1A4UEJ3bEplWFk5KytmWkNVbE1TaFE0ZncvZnQzdkh6NUVrd21jODVBRlJNVEE0UEJnSW1KQ1NZbUpwQ1FrSUQwOUhSTVQwOURSRVFFcXFxcU1EWTJSbFJVRks1ZnY0NTc5KzVCU1VrSlY2OWV4Yk5uejdCbnp4NFVGaGFDL2RpeFk3Q3pzNE9FaEFUOC9QeHc1ODRkOVBiMjR1M2J0M2p5NUFsbVptYVFtWmtKUGo0K0pDY25ZOGVPSGRpMmJSdnUzYnMzbDhWalkyUEQ3ZHUzRVI4Zmo1Q1FFSHovL2gzcjE2L0grUGc0cEtTazRPUGpBd2NIQjVTWGx5TWlJZ0lPRGc0SUN3dURsWlVWZHV6WU1SZGgrZmp4STV5Y25DQXBLUWxWVmRVNTlWZFNVZ0k5UFQzTXpNeWd1TGdZQnc0Y3dJOGZQeUFnSUFCcGFlbTVIWU9kblIyenM3TklTa3FDcTZzcnRtelpncjkvLzJKa1pBVGJ0MjlIZDNjM0xDd3M4UDc5ZXpBWWpQK3R6ckt6czVPWGx4ZWFtcHJBeHNZR1B6OC9MRml3QURJeU11RG01Z1lmSHg5Ky92d0pXMXRieU1qSVlHQmdBRHQzN3NURGh3K2hvNk1EQm9PQndzSkMyTmpZWU1PR0RlRGo0OE9iTjI5Z2JXMDlkelJOVFUyaHI2K1Bpb29LZUh0N1EwQkFBRnBhV3JodzRRTEN3OFBoNU9RRWYzOS9IRDE2RkJZV0ZxaW9xRUJvYUNnK2YvNk0zNzkvUTBwS0NtbHBhVkJVVklTbHBlVmNiK2pMbHkvSXpNd0VQejgvc3JLeXNIcjFhbXpidGcxK2ZuNXo2Ylc2dXJxNUlzZStmZnR3K2ZKbC9QanhBeTlldklDK3ZqN1llM3A2a0pxYUNrMU5UVVJGUllHZm54OExGeTdFMjdkdjBkL2ZQeGVFK0w5ZElDRWhBVjVlWGxCV1ZvYXlzaklLQ2dvUUdCaUk5UFIwV0Z0Ync4dkxDMHdtRTlQVDAranE2a0pzYkN6Ky9QbUQ1T1JrUkVaRzR1UEhqK2pvNklDUWtCQ2NuWjNSM3Q2T3lNaElQSDc4R0haMmRyQ3hzWUdBZ0FDWVRDYmV2MzhQWjJmbk9SRDZmd1hOLzdQd3pwMDdCMkZoWWRUWDEyUHg0c1VRRkJRRU96czdQbi8rREE0T0RuUjFkV0ZtWmdaTGxpeEJRa0lDa3BPVDhlblRKM3orL0JtenM3T3d0YlhGL3dNeVMrSGsrSkMvZEFBQUFBQkpSVTVFcmtKZ2dnPT0iKTtiYWNrZ3JvdW5kLXNpemU6MTUwcHggMTUwcHh9Ci5wYWR7cG9zaXRpb246cmVsYXRpdmU7ei1pbmRleDoyO2hlaWdodDoxMDAlO2Rpc3BsYXk6ZmxleDtmbGV4LWRpcmVjdGlvbjpjb2x1bW59Ci5jYXJkLnNpbmdsZSAucGFke3BhZGRpbmc6MzRweCAzMnB4IDI4cHh9Ci5jYXJkLnNsaXAgLnBhZHtwYWRkaW5nOjMwcHggMjZweCAyNHB4fQoKLmNoZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW59Ci5jaGVhZCBpbWd7aGVpZ2h0OjI0cHg7ZGlzcGxheTpibG9ja30KLmNraWNre3RleHQtYWxpZ246cmlnaHR9Ci5ja2ljayAuY29tcHtmb250OjcwMCAxM3B4ICdTb3JhJztsZXR0ZXItc3BhY2luZzouMjZlbTt0ZXh0LXRyYW5zZm9ybTp1cHBlcmNhc2U7Y29sb3I6dmFyKC0tYnJhbmQtZGVlcCl9Ci5ja2ljayAuc3Vie2Rpc3BsYXk6YmxvY2s7Zm9udDo1MDAgMTFweCAnRE0gU2Fucyc7bGV0dGVyLXNwYWNpbmc6LjJlbTtjb2xvcjp2YXIoLS1jLW11dGVkKTttYXJnaW4tdG9wOjNweDt0ZXh0LXRyYW5zZm9ybTp1cHBlcmNhc2V9CgovKiBtZWRhbGxpb24gKi8KLm1lZGFse3Bvc2l0aW9uOnJlbGF0aXZlO2JvcmRlci1yYWRpdXM6NTAlO292ZXJmbG93OmhpZGRlbjtwYWRkaW5nOjNweDtmbGV4Om5vbmU7CiAgYmFja2dyb3VuZDpsaW5lYXItZ3JhZGllbnQoMTU1ZGVnLHZhcigtLXJpbmctYSksdmFyKC0tcmluZy1iKSk7CiAgYm94LXNoYWRvdzowIDEycHggMjZweCByZ2JhKDIwLDQwLDkwLC4yMCksIGluc2V0IDAgMXB4IDAgcmdiYSgyNTUsMjU1LDI1NSwuNjUpfQoubWVkYWwgLmlubmVye3dpZHRoOjEwMCU7aGVpZ2h0OjEwMCU7Ym9yZGVyLXJhZGl1czo1MCU7b3ZlcmZsb3c6aGlkZGVuO2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OmNlbnRlcjsKICBiYWNrZ3JvdW5kOnZhcigtLWMtc29mdCk7Ym94LXNoYWRvdzppbnNldCAwIDJweCA3cHggcmdiYSgyMCw0MCw5MCwuMTQpfQoubWVkYWwgaW1ne3dpZHRoOjEwMCU7aGVpZ2h0OjEwMCU7b2JqZWN0LWZpdDpjb3ZlcjtkaXNwbGF5OmJsb2NrfQoubWVkYWwgLm1vbm97Zm9udC1mYW1pbHk6J1NvcmEnO2ZvbnQtd2VpZ2h0OjgwMDtjb2xvcjp2YXIoLS1icmFuZCk7bGV0dGVyLXNwYWNpbmc6LjAxZW19Ci5tZWRhbDo6YWZ0ZXJ7Y29udGVudDoiIjtwb3NpdGlvbjphYnNvbHV0ZTtpbnNldDowO2JvcmRlci1yYWRpdXM6NTAlO3BvaW50ZXItZXZlbnRzOm5vbmU7CiAgYmFja2dyb3VuZDpyYWRpYWwtZ3JhZGllbnQoMTIwJSA5MCUgYXQgMzIlIDE4JSwgcmdiYSgyNTUsMjU1LDI1NSwuNiksIHJnYmEoMjU1LDI1NSwyNTUsMCkgNDYlKX0KCi8qIHNpbmdsZSBzdGFnZSAqLwouY3N0YWdle2ZsZXg6MTtkaXNwbGF5OmZsZXg7ZmxleC1kaXJlY3Rpb246Y29sdW1uO2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQoubWljcm97Zm9udDo2MDAgMTFweCAnRE0gU2Fucyc7bGV0dGVyLXNwYWNpbmc6LjQyZW07dGV4dC10cmFuc2Zvcm06dXBwZXJjYXNlO2NvbG9yOnZhcigtLWJyYW5kKTtvcGFjaXR5Oi45O21hcmdpbi1ib3R0b206MTJweH0KLm1hdGNodXB7d2lkdGg6MTAwJTtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVufQoudGVhbXtkaXNwbGF5OmZsZXg7ZmxleC1kaXJlY3Rpb246Y29sdW1uO2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTRweDt3aWR0aDoxMjJweH0KLnNpbmdsZSAubWVkYWx7d2lkdGg6MTA0cHg7aGVpZ2h0OjEwNHB4fQouc2luZ2xlIC5tZWRhbCAubW9ub3tmb250LXNpemU6MzNweH0KLnRuYW1le2ZvbnQ6NjAwIDIwcHggJ0RNIFNhbnMnO2xldHRlci1zcGFjaW5nOi4wN2VtO3RleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTtjb2xvcjp2YXIoLS1jLWluay1zb2Z0KTt0ZXh0LWFsaWduOmNlbnRlcjsKICB3aGl0ZS1zcGFjZTpub3JtYWw7bGluZS1oZWlnaHQ6MS4xMjttYXgtd2lkdGg6MTcwcHg7d29yZC1icmVhazpicmVhay13b3JkfQouc2NvcmV7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjt0cmFuc2Zvcm06dHJhbnNsYXRlWSgtNnB4KX0KLnNjb3JlIC5zbntmb250OjgwMCAxMzBweC8uODIgJ1NvcmEnO2NvbG9yOnZhcigtLWMtaW5rKTtsZXR0ZXItc3BhY2luZzotLjAzZW07Zm9udC12YXJpYW50LW51bWVyaWM6dGFidWxhci1udW1zfQouc2NvcmUgLnNlcHt3aWR0aDo5cHg7aGVpZ2h0OjlweDtib3JkZXItcmFkaXVzOjJweDt0cmFuc2Zvcm06cm90YXRlKDQ1ZGVnKTttYXJnaW46MCAyMnB4O2ZsZXg6bm9uZTsKICBiYWNrZ3JvdW5kOnZhcigtLWJyYW5kKTtib3gtc2hhZG93OjAgMCAwIDVweCB2YXIoLS1jLXNvZnQpfQouY3BpY2t7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpjZW50ZXI7bWFyZ2luLWJvdHRvbTozMHB4fQoucGlsbHtkaXNwbGF5OmlubGluZS1mbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTZweDtwYWRkaW5nOjE1cHggMzBweDtib3JkZXItcmFkaXVzOjk5OXB4OwogIGJhY2tncm91bmQ6dmFyKC0tZ2xhc3MpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tZ2xhc3MtbGluZSk7CiAgYm94LXNoYWRvdzowIDEycHggMjhweCByZ2JhKDIwLDQwLDkwLC4xMyksIGluc2V0IDAgMXB4IDAgcmdiYSgyNTUsMjU1LDI1NSwuOCl9Ci5waWxsIC5wZG90e3dpZHRoOjEwcHg7aGVpZ2h0OjEwcHg7Ym9yZGVyLXJhZGl1czo1MCU7ZmxleDpub25lO2JhY2tncm91bmQ6dmFyKC0tYnJhbmQpO2JveC1zaGFkb3c6MCAwIDAgNXB4IHZhcigtLWMtc29mdCl9Ci5waWxsIC5wbGFiZWx7Zm9udDo3MDAgMTlweCAnRE0gU2Fucyc7bGV0dGVyLXNwYWNpbmc6LjA1ZW07dGV4dC10cmFuc2Zvcm06dXBwZXJjYXNlO2NvbG9yOnZhcigtLWMtaW5rKX0KLnBpbGwgLnBiYXJ7d2lkdGg6MXB4O2hlaWdodDoyMXB4O2JhY2tncm91bmQ6dmFyKC0tYy1saW5lKX0KLnBpbGwgLnBwY3R7Zm9udDo3MDAgMTlweCAnU29yYSc7Y29sb3I6dmFyKC0tYnJhbmQtZGVlcCl9CgovKiBzbGlwICovCi5zbGlwdGl0bGV7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmJhc2VsaW5lO2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO21hcmdpbi10b3A6MTRweH0KLnNsaXB0aXRsZSAubWFpbntmb250OjgwMCAyNnB4ICdTb3JhJztsZXR0ZXItc3BhY2luZzotLjAxZW07Y29sb3I6dmFyKC0tYy1pbmspfQouc2xpcHRpdGxlIC5sZWdze2ZvbnQ6NzAwIDExcHggJ0RNIFNhbnMnO2xldHRlci1zcGFjaW5nOi4xOGVtO3RleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTtjb2xvcjp2YXIoLS1jLW11dGVkKX0KLmxlZ2xpc3R7ZGlzcGxheTpmbGV4O2ZsZXgtZGlyZWN0aW9uOmNvbHVtbjtnYXA6OXB4O21hcmdpbjoxNHB4IDB9Ci5sZWd7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTNweDtwYWRkaW5nOjEycHggMTNweDtib3JkZXItcmFkaXVzOjE0cHg7CiAgYmFja2dyb3VuZDp2YXIoLS1nbGFzcyk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1nbGFzcy1saW5lKTsKICBib3gtc2hhZG93OjAgNnB4IDE2cHggcmdiYSgyMCw0MCw5MCwuMDcpLCBpbnNldCAwIDFweCAwIHJnYmEoMjU1LDI1NSwyNTUsLjcpfQouc2xpcCAubGVnIC5tZWRhbHt3aWR0aDo0MHB4O2hlaWdodDo0MHB4fQouc2xpcCAubGVnIC5tZWRhbCAubW9ub3tmb250LXNpemU6MTNweH0KLmxlZyAubWlke2ZsZXg6MTttaW4td2lkdGg6MH0KLmxlZyAubWF0Y2h7Zm9udDo3MDAgMTVweCAnU29yYSc7Y29sb3I6dmFyKC0tYy1pbmspO3doaXRlLXNwYWNlOm5vd3JhcDtvdmVyZmxvdzpoaWRkZW47dGV4dC1vdmVyZmxvdzplbGxpcHNpc30KLmxlZyAubHBpY2t7Zm9udDo1MDAgMTNweCAnRE0gU2Fucyc7Y29sb3I6dmFyKC0tYnJhbmQtZGVlcCk7bWFyZ2luLXRvcDoycHg7d2hpdGUtc3BhY2U6bm93cmFwO292ZXJmbG93OmhpZGRlbjt0ZXh0LW92ZXJmbG93OmVsbGlwc2lzfQoubGVnIC5yaWdodHtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDoxMXB4O2ZsZXg6bm9uZX0KLmxlZyAubHNje2ZvbnQ6ODAwIDE2cHggJ1NvcmEnO2NvbG9yOnZhcigtLWMtaW5rKTtmb250LXZhcmlhbnQtbnVtZXJpYzp0YWJ1bGFyLW51bXN9Ci5sZWcgLmxwY3R7Zm9udDo3MDAgMTJweCAnRE0gU2Fucyc7Y29sb3I6I2ZmZjtiYWNrZ3JvdW5kOnZhcigtLWJyYW5kKTtwYWRkaW5nOjRweCA5cHg7Ym9yZGVyLXJhZGl1czo4cHg7bWluLXdpZHRoOjQwcHg7dGV4dC1hbGlnbjpjZW50ZXJ9Ci5zbGlwdG90e2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47cGFkZGluZzoxM3B4IDE2cHg7Ym9yZGVyLXJhZGl1czoxM3B4OwogIGJhY2tncm91bmQ6dmFyKC0tYy1zb2Z0KTtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWdsYXNzLWxpbmUpO21hcmdpbi1ib3R0b206NHB4fQouc2xpcHRvdCAubHtmb250OjcwMCAxMXB4ICdETSBTYW5zJztsZXR0ZXItc3BhY2luZzouMTZlbTt0ZXh0LXRyYW5zZm9ybTp1cHBlcmNhc2U7Y29sb3I6dmFyKC0tYnJhbmQtZGVlcCl9Ci5zbGlwdG90IC5ye2ZvbnQ6ODAwIDIxcHggJ1NvcmEnO2NvbG9yOnZhcigtLWJyYW5kLWRlZXApfQoKLyogZm9vdGVyICovCi5jZm9vdHttYXJnaW4tdG9wOmF1dG87cGFkZGluZy10b3A6MTZweH0KLmNhcmQuc2xpcCAuY2Zvb3R7bWFyZ2luLXRvcDoxNHB4fQouaHJ7aGVpZ2h0OjFweDtiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCg5MGRlZyx0cmFuc3BhcmVudCx2YXIoLS1jLWxpbmUpLHRyYW5zcGFyZW50KTttYXJnaW4tYm90dG9tOjE0cHh9Ci5kaXNje2ZvbnQ6NTAwIDEwLjVweCAnRE0gU2Fucyc7bGV0dGVyLXNwYWNpbmc6LjIyZW07dGV4dC10cmFuc2Zvcm06dXBwZXJjYXNlO2NvbG9yOnZhcigtLWMtbXV0ZWQpO3RleHQtYWxpZ246Y2VudGVyfQouY2ItdGFiYmFye3Bvc2l0aW9uOmZpeGVkO2JvdHRvbTowO2xlZnQ6MDtyaWdodDowO3otaW5kZXg6OTk5OTtkaXNwbGF5OmZsZXg7anVzdGlmeS1jb250ZW50OnNwYWNlLWFyb3VuZDtiYWNrZ3JvdW5kOmNvbG9yLW1peChpbiBzcmdiLHZhcigtLXN1cmZhY2UpIDkwJSwgdHJhbnNwYXJlbnQpOy13ZWJraXQtYmFja2Ryb3AtZmlsdGVyOmJsdXIoMjBweCk7YmFja2Ryb3AtZmlsdGVyOmJsdXIoMjBweCk7Ym9yZGVyLXRvcDoxcHggc29saWQgdmFyKC0tbGluZSk7cGFkZGluZzo2cHggNHB4IGNhbGMoNnB4ICsgZW52KHNhZmUtYXJlYS1pbnNldC1ib3R0b20pKTtib3gtc2hhZG93OjAgLTZweCAyNnB4IHJnYmEoMjAsNDAsOTAsMC4xMCk7fS5jYi10YWJiYXIgYXtmbGV4OjE7ZGlzcGxheTpmbGV4O2ZsZXgtZGlyZWN0aW9uOmNvbHVtbjthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjNweDt0ZXh0LWRlY29yYXRpb246bm9uZTtjb2xvcjp2YXIoLS1tdXRlZCk7cGFkZGluZzo2cHggMnB4O2JvcmRlci1yYWRpdXM6MTJweDt9LmNiLXRhYmJhciBhLmFjdGl2ZXtjb2xvcjp2YXIoLS1icmFuZCk7fS5jYi10YWJiYXIgYSAudGx7Zm9udC1zaXplOjAuNjJyZW07Zm9udC13ZWlnaHQ6NzAwO2xldHRlci1zcGFjaW5nOjAuMnB4O30uY2ItdGFiYmFyIGEuYWN0aXZlIC50bHtmb250LXdlaWdodDo5MDA7fS5nYW1lbGlzdHtkaXNwbGF5OmZsZXg7ZmxleC1kaXJlY3Rpb246Y29sdW1uO2dhcDo4cHg7bWF4LWhlaWdodDoyNzBweDtvdmVyZmxvdy15OmF1dG87bWFyZ2luLWJvdHRvbTo2cHg7cGFkZGluZy1yaWdodDoycHg7fS5nYW1lcm93e3Bvc2l0aW9uOnJlbGF0aXZlO3RleHQtYWxpZ246bGVmdDtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWZpZWxkLWxpbmUpO2JhY2tncm91bmQ6dmFyKC0tZmllbGQpO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHggMTNweDtjdXJzb3I6cG9pbnRlcjt0cmFuc2l0aW9uOi4xNXM7fS5nYW1lcm93OmhvdmVye2JvcmRlci1jb2xvcjp2YXIoLS1icmFuZCk7YmFja2dyb3VuZDp2YXIoLS1icmFuZC1zb2Z0KTt9LmdhbWVyb3cuYWRkZWR7Ym9yZGVyLWNvbG9yOnZhcigtLWJyYW5kKTtiYWNrZ3JvdW5kOnZhcigtLWJyYW5kLXNvZnQpO30uZ2FtZXJvdy5hZGRlZCAuZ3ItY29uZntiYWNrZ3JvdW5kOnZhcigtLWJyYW5kKTtjb2xvcjojZmZmO30uZ3ItbWFpbntmb250OjcwMCAxNHB4ICdTb3JhJztjb2xvcjp2YXIoLS1pbmspO30uZ3Itdntjb2xvcjp2YXIoLS1tdXRlZCk7Zm9udC13ZWlnaHQ6NTAwO2ZvbnQtc2l6ZToxMnB4O30uZ3Itc3Vie2ZvbnQ6NTAwIDExcHggJ0RNIFNhbnMnO2NvbG9yOnZhcigtLW11dGVkKTttYXJnaW4tdG9wOjNweDt9LmdyLWNvbmZ7cG9zaXRpb246YWJzb2x1dGU7dG9wOjEwcHg7cmlnaHQ6MTJweDtmb250OjcwMCAxMnB4ICdTb3JhJztjb2xvcjp2YXIoLS1icmFuZC1kZWVwKTtiYWNrZ3JvdW5kOnZhcigtLWJyYW5kLXNvZnQpO3BhZGRpbmc6M3B4IDhweDtib3JkZXItcmFkaXVzOjhweDt9LnBpY2tjaGlwc3tkaXNwbGF5OmZsZXg7ZmxleC13cmFwOndyYXA7Z2FwOjdweDttYXJnaW4tYm90dG9tOjRweDt9LnBjaGlwe2JvcmRlcjoxcHggc29saWQgdmFyKC0tZmllbGQtbGluZSk7YmFja2dyb3VuZDp2YXIoLS1maWVsZCk7Y29sb3I6dmFyKC0taW5rKTtmb250OjYwMCAxMnB4ICdETSBTYW5zJztwYWRkaW5nOjdweCAxMXB4O2JvcmRlci1yYWRpdXM6OTk5cHg7Y3Vyc29yOnBvaW50ZXI7dHJhbnNpdGlvbjouMTVzO30ucGNoaXAgYntjb2xvcjp2YXIoLS1icmFuZC1kZWVwKTt9LnBjaGlwOmhvdmVye2JvcmRlci1jb2xvcjp2YXIoLS1icmFuZCk7fS5wY2hpcC5vbntiYWNrZ3JvdW5kOnZhcigtLWJyYW5kKTtib3JkZXItY29sb3I6dmFyKC0tYnJhbmQpO2NvbG9yOiNmZmY7fS5wY2hpcC5vbiBie2NvbG9yOiNmZmY7fS5zZWxub3Rle2ZvbnQ6NTAwIDEyLjVweCAnRE0gU2Fucyc7Y29sb3I6dmFyKC0taW5rLXNvZnQpO2JhY2tncm91bmQ6dmFyKC0tYnJhbmQtc29mdCk7Ym9yZGVyLXJhZGl1czoxMHB4O3BhZGRpbmc6OXB4IDEycHg7bWFyZ2luOjJweCAwIDRweDt9LnNlbG5vdGUgYntjb2xvcjp2YXIoLS1pbmspO30ubGVnbWluaXtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDo4cHg7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1maWVsZC1saW5lKTtiYWNrZ3JvdW5kOnZhcigtLWZpZWxkKTtib3JkZXItcmFkaXVzOjExcHg7cGFkZGluZzo5cHggMTJweDttYXJnaW4tYm90dG9tOjZweDt9LmxtLW1haW57ZmxleDoxO2Rpc3BsYXk6ZmxleDtmbGV4LWRpcmVjdGlvbjpjb2x1bW47Z2FwOjFweDt9LmxtLW1haW4gYntmb250OjcwMCAxM3B4ICdTb3JhJztjb2xvcjp2YXIoLS1pbmspO30ubG0tbWFpbiBzcGFue2ZvbnQ6NTAwIDExLjVweCAnRE0gU2Fucyc7Y29sb3I6dmFyKC0tbXV0ZWQpO30ubG0tcm17d2lkdGg6MjZweDtoZWlnaHQ6MjZweDtib3JkZXItcmFkaXVzOjhweDtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWZpZWxkLWxpbmUpO2JhY2tncm91bmQ6dmFyKC0tc3VyZmFjZSk7Y29sb3I6dmFyKC0tbXV0ZWQpO2N1cnNvcjpwb2ludGVyO2ZvbnQtc2l6ZToxMnB4O30ubG0tcm06aG92ZXJ7Y29sb3I6dmFyKC0tcmVkKTtib3JkZXItY29sb3I6dmFyKC0tcmVkKTt9LmVkaXRidG57d2lkdGg6MTAwJTt0ZXh0LWFsaWduOmxlZnQ7YmFja2dyb3VuZDpub25lO2JvcmRlcjpub25lO2JvcmRlci10b3A6MXB4IHNvbGlkIHZhcigtLWxpbmUyKTttYXJnaW4tdG9wOjhweDtwYWRkaW5nOjEzcHggMnB4IDRweDtjb2xvcjp2YXIoLS1tdXRlZCk7Zm9udDo3MDAgMTIuNXB4ICdETSBTYW5zJztsZXR0ZXItc3BhY2luZzouMDJlbTtjdXJzb3I6cG9pbnRlcjt9LmVkaXRidG46aG92ZXJ7Y29sb3I6dmFyKC0tYnJhbmQpO30uZWRpdHdyYXB7bWFyZ2luLXRvcDo0cHg7fS5zbGlwZW1wdHl7dGV4dC1hbGlnbjpjZW50ZXI7Y29sb3I6dmFyKC0tbXV0ZWQpO2ZvbnQ6NTAwIDEzcHggJ0RNIFNhbnMnO3BhZGRpbmc6MjZweCAxNHB4O30udnN7Zm9udDo4MDAgNThweCAnU29yYSc7Y29sb3I6dmFyKC0tbXV0ZWQpO29wYWNpdHk6LjU7bGV0dGVyLXNwYWNpbmc6LjAyZW07fS5mbGQgc2VsZWN0e3dpZHRoOjEwMCU7cGFkZGluZzoxMXB4IDM2cHggMTFweCAxM3B4O2JvcmRlci1yYWRpdXM6MTFweDtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWZpZWxkLWxpbmUpO2JhY2tncm91bmQ6dmFyKC0tZmllbGQpO2NvbG9yOnZhcigtLWluayk7Zm9udDo2MDAgMTRweCAnRE0gU2Fucyc7LXdlYmtpdC1hcHBlYXJhbmNlOm5vbmU7YXBwZWFyYW5jZTpub25lO2N1cnNvcjpwb2ludGVyO30uZmxkIHNlbGVjdDpmb2N1c3tvdXRsaW5lOjA7Ym9yZGVyLWNvbG9yOnZhcigtLWJyYW5kKTtib3gtc2hhZG93OjAgMCAwIDNweCB2YXIoLS1icmFuZC1zb2Z0KTt9LmNvbXBzZWwtaGludHtmb250OjUwMCAxMS41cHggJ0RNIFNhbnMnO2NvbG9yOnZhcigtLW11dGVkKTttYXJnaW46LThweCAwIDEycHg7fWJvZHl7cGFkZGluZy1ib3R0b206ODRweDt9Cjwvc3R5bGU+CjwvaGVhZD4KPGJvZHk+CiAgPGRpdiBjbGFzcz0iYmFyIj4KICAgIDxkaXYgY2xhc3M9ImJyYW5kIj48aW1nIGlkPSJiYXJsb2dvIiBhbHQ9ImNtdm5nIj48c3BhbiBjbGFzcz0idGFnIj5TaGFyZSBDYXJkIFN0dWRpbzwvc3Bhbj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9InNlZyIgaWQ9Im1vZGVzZWciPgogICAgICA8YnV0dG9uIGRhdGEtbW9kZT0ic2luZ2xlIiBjbGFzcz0ib24iPlNpbmdsZTwvYnV0dG9uPgogICAgICA8YnV0dG9uIGRhdGEtbW9kZT0ic2xpcCI+U2xpcDwvYnV0dG9uPgogICAgPC9kaXY+CiAgICA8YnV0dG9uIGNsYXNzPSJpY29uLWJ0biIgaWQ9InRoZW1lYnRuIiB0aXRsZT0iVG9nZ2xlIGxpZ2h0IC8gZGFyayI+8J+MmTwvYnV0dG9uPgogICAgPGJ1dHRvbiBjbGFzcz0iZGwiIGlkPSJkbGJ0biI+4qSTIERvd25sb2FkIFBORzwvYnV0dG9uPgogIDwvZGl2PgoKICA8ZGl2IGNsYXNzPSJhcHAiPgogICAgPGRpdiBjbGFzcz0iY29udHJvbHMiIGlkPSJjb250cm9scyI+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJzdGFnZXdyYXAiPgogICAgICA8ZGl2IGNsYXNzPSJzdGFnZS1sYWJlbCIgaWQ9InN0YWdlbGFiZWwiPkxpdmUgcHJldmlldzwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJzdGFnZSI+PGRpdiBjbGFzcz0ic2NhbGVyIiBpZD0ic2NhbGVyIj48ZGl2IGlkPSJjYXJkIj48L2Rpdj48L2Rpdj48L2Rpdj4KICAgIDwvZGl2PgogIDwvZGl2PgoKPHNjcmlwdCBzcmM9Imh0dHBzOi8vY2RuanMuY2xvdWRmbGFyZS5jb20vYWpheC9saWJzL2h0bWwyY2FudmFzLzEuNC4xL2h0bWwyY2FudmFzLm1pbi5qcyI+PC9zY3JpcHQ+CjxzY3JpcHQ+CmNvbnN0IExPR09fTElHSFQ9ImRhdGE6aW1hZ2UvcG5nO2Jhc2U2NCxpVkJPUncwS0dnb0FBQUFOU1VoRVVnQUFBZGtBQUFCOENBWUFBQUFsNGNyckFBQTFTa2xFUVZSNDJ1MTllWmdzVlpIdkwydnY5WGJmaFh1QkJpOENEcmdqS0NveWJ1RGdycThaQm14RWNVZG5jSnp2VThjTmhTZlBjY1oxeGhWSGNIaTArSUNlOFRrZ1B2WDVWTndWSFhkQUZzVytYQzUzYTdwdkw5VzE1UHZqUkpoUnB6T3JzcXBPVmxkVngrLzc2cXM5OCtUSmMrSVhFU2RPaERlOFpTY1VDb1ZDb1ZDNFIwcTdRS0ZRS0JRS0pWbUZRcUZRS0pSa0ZRcUZRcUZRS01rcUZBcUZRcUVrcTFBb0ZBcUZrcXhDb1ZBb0ZBb2xXWVZDb1ZBb2xHUVZDb1ZDb1ZDU1ZTZ1VDb1ZDb1NTclVDZ1VDb1dTckVLaFVDZ1VTcklLaFVLaFVDaVVaQlVLaFVLaFVKSlZLQlFLaFVKSnRndmcwZlhJaDBjUCtadDYvK2RIRmtDbXp1L3Rmc3RFOUtWc0UwTGEweTM5MXV6dk0yMGV4M1BRam1iSDlpQTlqOFQ0YjdiQjhkYjdIbVlTT2xiVzBUR0hyT041QVBLTyt5QWIwbjZ2aWY4MnV0YThPR2EyaWJFais3VWczbStPMmNkZWhBeHB0bDhhWFZlU0dLcnpYU0ZDN2piaUpWc2U1OWVCdjFycXUzUnVjS3pmRlFtL0JSS3BBS2hhTjVpSk53T2diUDJuYXAwbkF5QkhqeFFkeisrQnZpb0FHQWF3MmtBaHFDYllocTBBbGh3ZWJ6T0FaWHBlcEd0cmhLb1FxaGtBcFRyM0x3VWczVUtmakJIeHI3UkEybFVoek95eE5VN0hiUFpZcmM2WE1KUkNqbDBSYlM0NU9BZjNlU3Rqc2RIL1V0WTg1OSt1Tm5rT1QvVG5zbldzVVJwZlM1YnM4ZXZJc0F5QWdRYnlwQnBCZXB2cGY4VU95SkY2OTdjY1F6bXBDSVVoRlhHL0tsYS95SHZYN2hqMnJMNWpHVjVwNldCYXRCMGVUVmcwUVlZRmE4RGtySWxZRlRjOFpRMnNWTUlreGNSeUlFVERMZFAxWmNYMXhtbExpdjZURmhPNUdORlhoU2FGUEpONTFmRTk5YTMyWkFGc0EzQWZFVkdaU0xjYVFwaGhmWk9pWTNsRWpvc1J5a0NyOTFjS2RhL0JPTFRQd1lKZ29jWHoxZnVzRmJEWFlJbTAvMktDYzljWGZiQk1KTFRZNEgvajFMWlZ1dC8xNWowcjFpdDBYWEVVd0RFQWMyTGVsWVExWHhTS20veDlpdHBkSkFMMnFYMnJkZHJHRm1PUnJpRXRGUHl5WTJVVmJZNXBPU1pHTEVJdVcrTXVHMExVUStMellRQjdPbXg4ck1TY214dU9aTU5jeGRVUWNrMWIzMG5OeFE4UmNBTmljSmNqQmxkYWFOdE12S2tPV0lKaExvN1ZCZ01qVDIzbEFUUksvMWxwTUZHek5LR1gydFFlMlgyM2xNQ2tZS3RodnNFNHlZaEpQMVRuM3NZbHJFWWtsS00yVldNU3MxUjBsdXBjOTZhWUFpZ1Zva1M0SEplMm9NeFMreFljSEx0ZFphQ2VvUFNRak5lSmxiUWxxNitITEtVZ2FxeEtXVGJjUkQrTzBIMVlFWE85RTlac3MrTkplczZrblBUcmVGaUdhQjQ5aUdCNXNKVFE5V1JiUGZaR0pGbWZIbUVreXplMllya3BXQXN0a0lDc0NJSE1XcVMwYXNmRStaWnBZcTEwOExxM0F0aFhoMUNMQ1J5L0VLS1JOaHEwdnZYN1FvUkM0RVc0ejZLK3IxcVRRbG9XSTNTL1U5UVBpMDBJalMwMHNlOFg3V3pYQ3M4TGhRMU5XRXRNTnEyNlRDV1paQzNMdmRHYWJ5TVhzeSs4T0ZKUmRVbGdLYXM5MGtxTWN1djVFZU93akZyWFl6MWhQUUxnVUlPMitUU1dmU0VURmlMR3JDK091MHJYd0o2eWxaanpLR1hOYVp1NFU4Smk3NlNDSHpiUGgraDlLNjdyRVpyTGYyeEM3b2NoSFRLT2ZldDFoaFNlQXkzTXk3NGdXYS9CeEtuM0h6L2tzM1RJYjFrNDVHZ0M4M3BJVmhBdFR3cGVzMHVIRVBlcUlOcnlPdlRWSUxWZGFzYzdhY0FlQm1BSFBXK2l5WjJsOXJLQVhBU3dGOEFzakt2MUFJQmRkSnpOMUNlN0VyU0lYRm16T3dIOFBxUnZkZ0E0R3NDeDlIcUErcXNJNEFFQTl3RDRBMTNqWEF5eUdJbGhaZGlLempZRUx0L0RBRHlTZmpNZ1hIL3o5RmdDOEZzQWQxdEVNQ3JjY1hIR21ieFB6YnI0RzhFVzh2SzlLNkxsWXc0MW9TaTFNb1o4NjE2MWEwVkg5YlZ0S1JWb3pPNGdwWFlyM1dNZW03dUpiSGJSYzZsTnBUZEpUMW9VbVI1RjEzZ2tqZnNCb2NUUDAzeTlpK2JobktYWWo5STg4MUc3NU5TeXhSbUM3WFJ1ZnlPU2JGWm9abEhyS2MxTVpEdklaMENRNWhoMTloWmhkWlJwNExCTHRXaHB6eklZaG4relRJOVYxQzZpKytoTVVOUWdnQXNBbkF2Z3FXMGU2NmNBUGdmZ2loYXQ0bFNJMVZTbXlmWVlZU2swR3ozSXg1d0g4Rjhrb0JaSUNNdkovZzRBVHdSd0J0WkdPOWJEVndIY0NPQmoxajJMNDVJUEU3WkhBSGdDZ0FzQlBLdUZmdndTZ0tzQi9IdWJZMGdLL2tlVHd0V01wV0NmZXdlQWJ3SjRISUQvWTFrenJraVcyL3g4Y2hmeWtzVWlnTU90ZHRtQmlTVlNYcFpwVEd3bTVlZ09BTGRiYzJhSnpuVWFqYXRGSW9SNjJFS0s2VUVpREI3UHd3RCtuMFVVUlRFdWp3RndKb0MzME90bXh2L1BBWHdRd1ArMFBoK2w0eHpxSU9IeStuZzFoTFNPQVBCaFVtb25XcEE3L3duZ1BWaTdIcytLMXArSEdFMzF2Qm84TnFyaTh5S0FXeTBsZ2NtN2FVOWdyNUpzd1NKWlNiU3lneXNSTGpJL2hHRFRGZ2xVYUZJY1B6RTE4d1FBanlEQ1pTeUx4NklnVHhtd3NEdzdQVGtQWUwvUXhoWUUrVUw4eHlYUlNoSzdFTUE3QVR3MG9YdnhPd0NYQXBodTBWVXBOY1MzQTdqY1FadHVBM0FpWGZQZDlObVJKSUNlN3VpNnZ3RGd2UUIrM2NTMU1sNEE0REpTS0Z5Z0F1QXFFbDYvYVZFQjQzdHdBWUIvY3pnK25nTGdKM0MvRHNodDNvOWdlMHk3ZUtjWWY5SmlQUVBBMXh5ZDQ1Z1FqOHJaZE84bUhKM2ord0F1cG42M3ZTdEpXdjVST0JuQTZ3Rzh3dUV4N3dMd1JnQTNpYytlQ2VEckRvNDlEZUQ4a0xuUkVucVZaSG5ROElEeFNQdWVJeUk4WkZrMktVR2tLUVRCQjJuaFdwQnJHeHlJTXdSZ1ltSnE1cTlJODlwQ21tRkZFR2xaRUQxYlpUazZ6Z0tDNEtGOXM5T1R2d0R3QzVqQWxFUEN2VkYxNU9KZ0xldmhKQ3pmUkczcEJPWUJ2QS9BcDBtRDMwSHVuVGdSckt3VWZBTEFSUTdhTWt1dUtNQkVrdDRBNEJrSlhmZFZBUDQ2WkNMeWRjcnJmVDJBdndWd2ZFSnRXU1pGNHAxa1NkbnpKSzVIcCtLd1RmOEs0TFZJWmduaFRQSXV1TUtSTUVzaU5rNGlLOG9GVGlRbGtLMnVEd0I0ZkVMajRaOUlFWnl2by9EeC9CdEE4K1FyeHpiTEhubU9SMU1ibnBXZzNMa1d3RFVBdmt6OStDTUh4N3dad0hNYUtNcE5XVHk5aUVQQ3hjZ21QbisyaUNCU05tVzVCM3pVdW1uTFlyRGtoSkFwb1RZQUtrZURjSWcwbXlFUzNsdkU4MWJTcUxjaFdPczhtclRUaHdBNGRtSnE1akVUVXpNbklWZ241YjIzcVpEN2tvcTRYNWtJOXd4b2tMK1ZyS3UzZHBCZzJTMzFQaEx1TDRjSkRpcFQvekMyZGNobHhWNkNTMkRXa0orUjRMa3VKRGZqNlVMWXlPQ2JNb0RIQXJnT3dNY1RKRmpRR0gwTmVVMmVUd1M3bmVaRUJ0RkpVK1FjcWRKOWRJVno2Wmc4M2wyQTU4QWJITGJ6NmdpQ2RZM2YweGk1QXNDM0VpUllBSGd6ekhydENmUmVMZ1BzUUcwVTc2SVl2M0V0Y2lrLzVkYWpFWmdsbFo4blRMQUFjQjVaczIrSGlXWG9PdlFxeWNxb1BRWmJxY01oV2pNVEprY0RaMURyb3VVOWt5dldaeHk1bVJLRGlRTlNob2tzTnlHSWVodEhrR1JnbEVoM25MN2ZTbVI3QW4wM0lJUmVXSFJzdFk0UVRGblc2eUtBdjZEdi8yR2Q3MDJhckxzZkFUaU9yRnBXZVBZaUNOQkpFbHNBZkJIR2pkMEpIQW5nMjJTeDhmbzhSOWkrR01BdEFQNnl3L2ZoUzJRMTd5R1M1eUNZT1AxL2hjTjJETU80Vzh0d0Y1REN3djJGRHR2NWxnN2RsL05oWFBxdjdxRHkrMXZ5b3N5UlFqNUtTbkNWNUJLRU55K3VXLzhlQkh0ZnBUZnJZaGhYN2hzNlBONHZCM0Nsa3F4YlpDd2lTdE9FemhEUmJhWUJkQmhaVU52cGVTdVJYa0dRM3pnTk1Kbit6WFkxODE3U05JS296d0hVYnUwWnBHTU8wUE9RK0UyZUJ2Y1crbjBlUVFCWDNKUjk3TFlZRUFUckViRitwY3Z1eitNQi9CREFpNFJ5a0VYOS9hcXVzTW14QUk2TFR3RjRsMURVM2dzVGxEUzhUdmZnd3dDK0FSTUVCaHFUY3pHdHJUc2N0dU1mRTVBNVQzUFl2Z2RJR2NsR2VJcGN5c25QSUxuNGlIcjRPTXh5ektJMUJ3K0k2MnQybVdBSndSWW8wTmovS01sWmhlVjI2VVY0Q0RMS2pOQXpwd1RNV3dPSDk3SHlXdW9Ta1JzbkJWaEJFRXlWcDBtWFE3QTJ5K2tST2FPSzNQckRBVmd5MjRwSHozWUVzU1JYZGdsWEk2NHRMYzVUUVcxbW1rVTZ4dUV3MFhhUDdOSjd0Qm5BZjhBRStWeUs1RGFLZHhNdWcxbTZ5QU40V3hlMDUra3dnVXd2ZzNFZmo4VWsybGNBK0k2ak5wd0U5NGxZTG5IWVJ5K2g1MzRmbnhmUnZYODdnRk5KQ2E0aVdMdVBteDVVUnFQemZmMG9XYkdLUGlKWkpyaEhUa3pOSEVla3N4VzFpU0dZVkpkbnB5ZDVHODBxYVhLOEozU1ZCdGd5ekZhQUJVRytBOElhSFJUa2JSTTRFMk1HdFJITVBtcmR3WGFpQzE5OEZtYkorb0pZN2NGL0xFd1FWYllIN3RVbE1QdmhYZ2IzZXpLN0VSL3FzdlpjUU1yWWpURUp0Z0RndTQ3YjhEd1lGN1lMak1CZGxQaCtBUDhYOVlQRHZENGFtMitEMlZ2N2NTR3pGaEJrbW9xenoxVEtxeUtBZjRFSi9sUDBDY21tYUVJVUFLUW1wbVpPZ1FrUkIweVFFU2VOWUNKY0FiQTBNVFhEQ1NFNEtJWUgwMEZCeEhjUWNSMmk3OW5seXdUTDVNdVJ4Wkw0WkhvOEQwR2lpaVh4MjBySS84S3lzTmlaa0d3OEI3V2g2NzJBQzJDMkZueEtwOTI2NEhvaXBtL0crTzJxc0U3ZTZPajhyeUdTYlh0TEJNeWVWVmU0VkpDRnZlT2dYL0V4SWxtZlBCdDdTV2JPSTE1Z1lnbEIxcmNQS3NFMkppd1g2S1NtVnhYa04wTEVlaVNNcTNoQVdLaThWcm9KWmwzMmNQcnQwUUFlQmhOS2Z5TE1Yc1duQURoclltcm04ZlFiWGtPVmdVNjVFS3RVQmlqeDN0b1MxcFlQWTJzNGpXQ3pjeG0xZTJzbHdlWWkrblNVQnZkTlBUaldib1pKWUtGWVAzd2VKcW8wemh4THdRUk91Y0p6WVFLd1hPU25QdDlodTc1SXp4em9XTm9nWTRFanFUa1ljUS9KeGpoYlZaaGczd0hnNzNSYUpVdXluaUMwYkJPa20ycnhYSHllT1pvTWh4T0JGbUNDQ2JZVDhYSVpxVEVFS2NsMndPeWRQSktJbDhzL0hRc1QrSlNHeVVhU0Z4T085OVFPa1VVczExL3ROVnQyRlZmSUJWVW1OMHlXamw4UnBIb0lRUlNmYmNuNnFDMnRKak1TTGRPazZEVk1rL1c5RXVFOThYdllzOUpMT055eU91eWxoa3pJL0wzZTRmbFB0WTR2eng4bjg5WTR6ZkVwUisyNUNVRWUzSHJuci9UcFdMaVFYbk1nMUwwaGN6QVRJclAza2J4OWI1L09rNnBMSTlLRkpTdno5ZHBDTTBVVFkxQllsank1Q3ZRZDd5OGR3dHFFL29QMHU1U3dBRXZDK3VQZ0piWnFCeEhrRjg0SXk1WkpseCs4eGlvSldTb0thU0xKek1UVXpHWUVKYzVLd3FXVXR2cEFydE95bGNvQlRCbngrMlU2RGw5SHZRazhSc1RFeC81SkR3N1k2NFhsa2NmNjUxTGQ2SGdGekI1SElMd3VNbWgrOFBZT2x3RkdMN0xrUTBuTU9Wc1dGVUxlSHdSd2pzUDJYQ284UkN0SXByaDhOK005MXZ1UmtERWhhK3FPaTN0MWcwNmx6cENzSDBFVVBISFloVHNpck1udGduQ0hpVWkyME9mYkJlRnlnZ2RPN3JBVndWb3NXNDY4d1YxR3hjWEpXeW5US0hKUWtReEN5b25CZFJpQ3JUSVYxSlpqU21QdEZoeDVURTV1VVJiL2llTXU0MnRaRk84L0JwTkJwZGNzV0NrVWk2aE5UYWxZSHd2bTFSYloyZU9PQzR3L0NKT2Q2RFpINXo0TEpnQk95aDdlL3JZU0lkd2hpQjhBL3NaUlcrNEc4R09oL0FGcml3SDBPNDZHMlYrUG1KNGtscGwvRHhNeHJ1aVFKVnNKSWRwTlJINERSSTdIQVRoNVltcm1kSmpNT0ErRmNkTWVoV0JOZFFjSmdCMUVyRnVJaEpsVTgrSW1aN0IyUzQwc3VOM01kWG1vM2VaVFJsQ0V2WUxhemRxVk9nTXhMZHJBeDVSRjI1blltNm1uV1JZS3lodDZiR3hkRFJQczVDSElTQVhFaTI1VkpJdlhoaWpFdHBJNGlHQjkwbVZneXptV3BjcHBSOE1LUmtnY3BMbmdLbU1XdTV5M3dTekJERzdRc2ZCQk1RNE9Sb3dIbnIvN1NENWZybE1vUHRwZEEyTnk0NjBxVlVFcVdRQ0hVVERSbnhHaERrOU16U3lJQ2NWMUxKZGgxaW5uRVZUR1lJdHhBY0NoMmVuSmd3aEtPN0U3bUlPVDBzSWE5V0phdENuVXBsN2tOZE1TZ2dBbGRsa3YwM01LNFlFQjZUckV5NUhHYkNuRUlSbU9mQjZIMlN6K3YzdHNYSDBmWnJzT1lGeVQ5OURyUnNYVEZaM0JaaUtyM3lFNjBFVjZYTDRGRTJld3hjRzVYd21UbklLUFh3cFJlSG1ibDB4UDZjTnRWcVlmQ0lLVjJBaGJ6Q1M0OEFsN3pYSllHL3pGeGtVWndQdDErblNXWk9Ya2tKTjFsYlRPUHdQd1pIcmVURGZyQVVHTUhMSExXMks0dUhrT1FZVHRDb0RsaWFtWjNRQnVuWjJlOUdqQzgvK2w5WnBHL0EzVlFHMHlDSHVMRFZ1bUJXclhDT29YaEphYjdUbEFpck5JY1NCVGFuWjZNbzRsV3lMdDhRQk1TYmFuZDNoY1ZCRFA3UjZHMjJCSzZiR2ljSS80THU0K3ZDUnhrQjVMQ0FwS3JIY3lqL3RKbVp3bm9mOXc4Z0lsbVRubjJVU3k1VHB6Z3BYS01reWlkeGNwT3g4R0UyQjRYNGpjNEZpSVZJUTEreHBIMS81bUdvZDdoVks3Sk1oOHZmQUFqWU9ma3ZkcUM0SjZ6MG5pQmVSNXlndXlsZU5BSnA2NG9BUDl3UGtLOXBPQk00QWd5TFhuNEdKTlZoSXRoRGEwZFdKcVppZU0zMThtcHVidE1ISk5kanQxNHRFd3J1UUJRY1JEOVA4eCtyeU0ybUNpckxBWUc1RnJwYzduY2tzTkUyUk9UTUFCSVFRZ0NEMGQwWTlWWVNrWFJSODFXK25pbW9USHdDek1Hc3Z4TUc3N0p3STRCU2JIOHFOaGN1NWVTWVRmQ0wrbS81UXNxMmNveGoxSUdqZVJNSGtZekZMRm8yQ1dMaDVGOS9BaDFBK2R3aUlSMTFOSkNYMFk5ZnV6eWZvL0FxWkt5NDBKblYrdWxlZERsR1VXc2lQMC9scUg1MzZyZFY0SUQ1aHRSVXVMeTRXUXJjSlV2cEVFTHVWVHA5ZGs1MGw1NGUyRHg5RzllUWJNOXNMdE1EV0hQNU5nRzg2ajV6RkxodHQ0WTRKdHFBSjROL1hCR0l6bjg3RUFua1RQSEs5eklzeis3ZFdOUXJKUlFwUGRyR01JSW5QVEpIaVBvdzdjZ1NDUGNGNVkxVlg2elFTQ0lDamVaOHBXY2hwQWRtSnFob09mUEVGNG1SaFdXTm9TS3JJbXJReCs0a2RGSEhjbFpuOVdyWE93TzdzWWsyZ1dZZkt6SHB2UXZmOHFFZUpSNUFLNms2eUxIOExrdXIwZHdDOWhvZ2hmQ1JPd1VxL1c2TmVwdlNXeXdPNFhncFRMRVJhUVRNbXplbGlHQ2ZSNUhreldvMzMwdWR3dk9ncXpmZUg5TUlYRzcwcTRUVCtHMmRMMEZwakNBbVZMSWFuU1o3ZkFWTk41VXdKdE9DMUVvTXFpR1ZWQkFwdXBmKzUyZE82TDY4d1hoQWg4d0FSTnVjRE5sb0hBZXo0OWRENlgreDJrVkwwTlp1ZEFPZVRhc3pSZVhnTlQydS91Qk5weHFwZ3JzTHdJc2w5ZW1sQS8vRFBKMThzQTdMYSs0MjJTN0lXNkRXYi85aW5va1dVMEYvdGt3NnhhcmkyNENVRkF3UWlDTlJlMlFEazZXQ2JZSDBXUVczZ01RVUYwVHYvRmVZRVBrZlhCd1VxRGdvQ0Iya0NrbEVXOGJQWG15VUtyVUZ0M0lRaXc4a1hiUitrYzg0aVh4dEFuSzUzM3ZBN1IvdzlRUC9DMkhLNGxLd2xaOXVtN0VycnZaOEJFRmY2UzNoY3NJUjlXR1dnQnBsYnB5OG5kSnZFRElnMG1zTDBobG9GZlIwSHhFblRWL1FWTVRWTjdyTjh2WHMrSjF6OGpLejZwZ2dzL0lNdmsyK0t6SlV1NWdwZ3pBUEFSc25oZFk2ZTR0L2FXR1ZtdDV3Q05peWM2UFBmakl5eFdUNUM3bENldThrQi96SHEvVDR5OVRpcUFONUtpOHoxNnZ6bGlQSllzUmZaWXVJdjJadkQrNDNtTFpObmJXS1Y3bjBSRThlRmtJWWZsQmVEckQxdGUraVZNenVsWG9NdlJEc2w2bG5Dc1dDUVRsa3kvVXNlcXRJbVIxMXg1MzZzL096MVpGRGVEeVZiK1hqN2lnaWRYQ1VFU0NqNTNIb0hMT0JWQ2dtRXVEMm5oVnl3cnVZejYrMFNsSlhFQzNOZEJ2WU9VaVIrSG5GY0srV29Ed3ZzQVRRNG0xQ2NoMkVQY1RYZ3FXWVA1SmdWb0dXYXJpT3NncmJzQnZLb0o1WlhYN3dlSmxNOTMzSjU4SFkvVW9SRFB5bDRBdjNKMDdoZGJNaWhUUjlFNkVXNkNycjZPN3FoVzlRQ0ExeEhCSHlrVUdSL3hFaDg4RFdacHhqWFp3Ym9YSzJJTW5wZEFQN0RIYTFDY044NFd4MUVFc1RKWHdkUXM3anVTdGNuR1RnM29JU2p4NW92ZmxCRzRTNnVDM094dFFMekdtaGF2aTNUVE9UQkpXc0poeEZxUGFMTVdNVll0TWdTQU5MbWpPUUNMYTlDbVFralVGdUFWb1lYeGRaY1FCSGo1RVM0eUNkZjFSMmNCbkUzRU1XLzFReEh4czVwNE1PNzgrNmx2amhZYTZPRmROTFpYaGJVb3lhUVE4LzkzWXUxbS9YWnhYVXpoYUJNTkM1NXB4KzBaRG5FUDJncGoxaUwranpvNjkrc3NpN2xzalVGWk4vbHNSK2U4cWt2RzV0bmtOUU05eXdEVWdSai8zd09UbE44bGpyVEdnaTBQWEpmbnU0aVVETTVsUFlMNGlXcm1MYS9ZdjVNSHF1OHMyU2lod05Zb0o0N3dyRWthWnVYWlpKTkI3VFlkamtCZXNjNGpvNHNsQVRZYkdlc0pBcFJFTCt1OTJtVGM2SGhNMkNWaDZaZHBRTVVodE5NZDMrdVhrb3ZsVVNIM3crNi9SdmY2VGlMYUV0MFR6cGl6QzdYN2l0Y1R2QTl5eExKSTR3UzJNQ2wvMm5HYmJtbEIwNGRGUnYvbXNEM1pPc1J1SzZ0TUJLNENzY1poQWxybTZ2eUdDMzI0Q3JqNU10WS9kV2VWeHNFMjFPNEpIclFVcW5ySXdLeEh1cXo1V3dnNWgvejhLSWZuK2lsTW9aRGpRdVRRU014alpNUTR5Z0Y0WnIrUnJHODk3RW1aZ1hGTGpncWk0dlZIV2RjMUxMMGdrNmNVQkdueGV6NC91M016d3JLT1M2NHlJamlGMm5xemR2azZDRVhBdHlaTEdObFdMRktGK0M5YjRtRmVBVHVsNUtrTzcvTU5DS3F2L0JMQjFveFNpQUNJV3pydlRpSDREeUZZMzl2WEpXUDdCcnBPZG5kdEpVRVdaOTIzU0VyaUV0eXQwKzBsSVI4SGcrSS9QQjVZVWZoYWgrZi9pdkRLZ0R3WTczWjAvaGVKNjh1SENOeEZtSUFuRjBYdkx5RkNYKyswbnRjUWNlMmx0bXdXNUJyWHkxS20rL0F0eDU0ZmlaSzQvenRoSXAxZGdYTW0vNEh1OFU0eFQrTW02MkZGK0NBZDR5RFdMb1AxdkNYclJ3aXNMSXlyZFppRUJaTm1CbXNMTjFlc1oybEI1Z1NSUXBDV3pQU1V0YXhadnFZNEZxMGtUYjZXS21ycndNb0FJRm1pTG16dHRXcVJzMjladmpLalZNcXlIbXlTUFJhMWdTZnQ0bTh0bDQ5ZHpDRVZNcm5pakJ1WlpPUDN3bnV4M3RodDNhZFVDK1RQRnNVZkhiWHByaWJtbkMxd2Q0clhMdk5YdDZwQXpEZzYvOFZpSGtpUGxwd0xybHpGWCtnU21mdEZpOUFXVUxzRzJneCs1YkJkcFFaZUIxZllqNkFPdGxUY3dyeHI5YkNJV2k5cEZzbEU0YThyeVlaQkpzVG5yVGNaOFVnM09HZGFrQUFUbkN5RVhrSVFHY3drS3dPcjBrMElGMmxwZWhZWjhuWGtoWVp0cDI2c3QrZldEbmlTMXZCcUNLbW1ySDV4dWVuNmJnVHJQNXcrcmxpSFpPUDIzeGdKaFR6TW5rNGUrTjJ3ZiswQWtWVEdHbGRMTVMzMUVYSGZsaHkxaWNkTFhLdHNXQ2lZY2x2RDd4ejJrMTlIRm1UcnlJczdIRm5VNHpBbDhDRG00cWpWTGhjRkFiNExreFNsR3dMejdxRDVzMDJRV3g2MWU4a2JnUlh3MngyMksreCs4OUxQZ01QemZGUmNOOWZxWHFINUduZDN3WmcxZm4zNi8zYzNBc25LWTlycnBmTDdiQjJ5a211Z1hKK1ZFMFdrQkZtbkJSbnpjYVJMT1JWQ3FsTGdWaTJMMXJhbTVTTW5DTjBMYVRNSGNMSHJXN2JOcmhWYmpiRGdiV1hGRlY1aWFjMUxJVzF2eFlVMlIyUlVoTmxmdTcxTjY4Z2xobW5pbHEwSkhkZFNseTRyVjJ0NDNKWTRFY3NGc3J5TFdKczhQNm4rVFVjb0JWSUFWMFVmZnNUUmVjK2s2eTJFOU05a2srUVRoWXNRN0J4WWJ5d0xoVmRhWll1SVh3R0krMmpXWWJ0eUlUS2M1OEV4RHM5ekhUMFAwalZ6S2RGY2s3Sm4xSktUQytoU3VDQlp1YWJJS1ExNWorc3lFVjllQ0hmZUZwTkRkRVF3VzMzM2s0QlpGaTZGZ3JBb2kwTFRZbUh2aXpaSVYwakpJanRlSStieWVRL09Uay91RmNKbGpvaUQxMzhyd2txdkNJc3RMZDZ6UzVuMzJnNVN2eXpRNnp2RitjdUM0TXVXVUhNWnBYdEE5THRyQXBjRGUwOFhqV3N2d3IzVTdqSGFRVFBXZ0NUVnBRUVZZOCthSTRnZzgxS0lFdkJsUjZUMVJycmVGUVI3NlJrWE9qaitkOGo2NzVaYXhmV3N0V2F6VGFVVEdndDhYMW5HN1VoZ0hxUXR4YVBaN1hMekNIYWVTQnpxVjB0V3VqOVRJUlpjSmVTOHFZanZwV1Zhb29GWHRLd3RKdkhOQ0hJRGx4QzRZK1d4d3FvRXlVeFA5aGFqS21yVEpjcjEzMHlJMVF1c2paS1cyYWN5bHBYdFcrY01HK1F1SStVV3JBbmpRNkZ3WTVWZjZ1aDRad25sUW1aaWVxNkRZMy9COG1vb21pZHhsMHRBaTVaRkQ3UVd4NUdsZTJyTHM5OTFXeWNtdFVaaGsyNWNUYzdXcEd4WE1FK1V5c1RVekNZaVdiWm8yYTBiRmZVYmRnN2ZPbjRGdGNrdFpENWtkbkdYVytoZkdmZ2thOUpXSTM3dk1ydlBuSktySWlHU3ZjelI4ZjVIaU1YOEpFY0MvU3E5WFMwakNldC96aHBEUUd2NXpNZXNkcWE2VmM2NUpsa1p1SlJ0NDF3YzNldFpWaVlUMUNxTUgzK0VQaXZTNzNNUlZxTGR4cFN3U0QweHdYa1ROdWN1em9qak5WUGhSMFlOLzZtaXlPejBKRnZPWHNTOTROOGU3ZWgrVkpSYyt3N2RkajlkUk8yZVpDbm9nSnVLTzU5RnJidmQwK0hURklvSldMS2xrSHZSU2xHR3hRaUQ2VEhkMW9tdTFtUlRJVVJXejVxTkkwaXFxQzJBTGdNWEtoYVJyRm9rSDZmTmZBNSt6ZXRDN0dKT2t4dUQxMXd6RWRjWjUxeXl5ZzhIR29XdGdkbWxwVndvUFVNZDhGd29OaDY0Q01qRmpvNTN0cGo3MjJBS09yU0xUd3FsdnhuNW9LaFY2UFk3UE9aeGxpSFZLcFlReEpsVUxXT29ieTFadVk3SjY1Y3lRamZLUllvSXNwR1duZHdPWXlldjkxRzc5bW9mQ3lIZnBjU05zUU9YdURqQUFGbktPY3ZxVFVWY2Q5UUE1VFhZR25kM2hFWEM3ZjJPdy91eXJZdXRJRVh2WWdCbVhXMHZUQWFmZHZGNjhmcGtCOGU3RlVFaWZSNzN1aTRiWDRHU2NKbGc1bVdXUFBMYThEVFlYSkx2eHM1MFRiSWNZWnlMSUI1N1QyczZaa2V5RmNqVzN5YWhuZWJFZFpUUU9PMWh4U0kvdG1oWGhHWHJFY0dPSVZpVTl4dVFWQnJoQVYxMlFncDJqL2dScEF3QTF6dThMOXVWWlB2U3VsaHZMSXUydk4zQjhaNE9Vek1XY0pOVTROMUM1cFNocnVKV1NKWmwyYjBPai8zWENMWUUrUll4TmpPMk0xaTdYZlA4YnV6TWxLTmoySlYwbW5YUDJMOHJDOEtUMWkzZmhCRUU3bHZlRHNSa0xKUHYyNW1ZWkVhbXFuVnNqbERtN1QxNU9rOEIwVG1XNDRJelBjbVVpL1dFNXk4YzN1UE5Lak1VanNHRlB6ajk0L2ZnWnV2RTQyaHVQc3ZCc1c2aWVld0x5MXZSbklYSWZYZVA0K00vemdFSGhlVlNudXBYa3BYdVhiWm1WMkN5RnNsTVQwVTB6dmdrSjNFUmdRdDNZSFo2Y2c3QjN0dEZJbzlsSXNaaDFPNUxqWU1SK2orVDM0SzRua01UVXpPYzlXY3ZYY3NxVGRTNCt5MjU5TnNDbmVzQkJMVnJmZEgvU1d2WS80djZMUk9pclhvUldtdytRbk8wWFRzalZuOTJDeW9PanBGeGVDeGJ3V3ZHbXJCZlo3dWdmemxRaFl0ZExBQjRoNFBqUGd2QTN6azR6bXZGSEJ3U2JlMFhUNFRYZ2JFZ00yOWxBUHlIdzJOL0JrRTJLU2xyQm1QK2YwZ1Evd2pKN09lU042UXZTVGJNS3MzVkdWVE5XSU1WOFI4Wk5ad1g1QzJ6UjZXYUZJcHNvWlpRV3lEQUU1WnlPb1pWbnE0ekdWS29yVmRyMTkxTjJ2MlhnM0hEbGJHMjZvb2ZJUVNLWWdKa2hUWE92eCtuMXpJWnhiS1luSXIreGhnOUg0RWdKdUkvSFJ6M0pYQlRuUDM2RUlWQTBab2l4Y3JLZlE2UFBRN2djaG8zQjRWQ3U0VGFRRTBQeHBOb0t4T0xRall0MEhFdTZ0YU9UR29Manl4MDd1SWNYSXRWcGthVU9ZV3JnaHpqa0piY1l1T0xZNWVGaGlnSlBCM1JaN2FiUEcxcG1tSEZBYXFvWDZBYUFINExFN2poQ3A4US9jaDVwZXZ0Z1pOMVprdlc1Nk0wTVFCVGcvSjRRY0tBKzBMbml1N0RnNVlTWENETDRtb0g4cWpkNHV4WGlQR1pRWGVrVXV4MWtnWGNGaU1BekJhdE53dlpjWlFnVUNtblZ5d1p4TjQvbGswZWdHdmhKbkZKVjVLc0YwSkNhUVIxSUNzV29jV3hXbTEzcWgwNHhNUnFsOFB6TGVLb3Q0WXExeHprOXBxd1dySjJkSFNjUGt4YkEwSWVQNDRTVUFid0RZZjMrZEVJaW1TWGFTQ1g2eEJzeWJySFdVSFM4d2hjeXJzUVpGalpybkpwdzRESDhCNWhnUURBKzd1Z2JaK2k1eUVhbzBxeTdjdDRBUGhLQXNmK0IvS0FqQ09vZHJXakFWZklkZlpCbVBTZTUzUnpCN3JPZzVvR2dJbXBtWUxvREJtbTNZeDcxQk9rVUJLV0lKT1l6Q2ZNeDg2aWZxTHpNRXZXM3NNS2kxeFRFUVFkdDErWnlJdUMrT09zeTkzdCtGNS9FaVlaZTFoYjYrV3dsWjk1Q0FLcGlxUTluaTBFN2hBMGluT2pnTmZQTmd2aDk1c0VMSjVtY0EyQW53bUxLQ3grSXErM0xoYnlRbjRWWUVwWmZpQ0I4endQeHROMkVzbU8rOUU0di9NNGdGZlJlRHVyMnpzeWlUVlpqNFJ0R3ExSDVNcklZR25KVmkyU3RiTkNlVTBlLzA5RVBEczl5ZnRZS3dqMitjcm9acitGYS9Bc29sMXRvczkva0lBV2ZpVnFxL0xJQ1NYN2JpUkVJZUhYOC9SOEdvQWJZZGEvM2lNRW0yNFQyaGpnTmZnRFJMZ1Z5NUpjRDN5dWdjTE9RbG9SVDM0eE9ETDdtd21kNjF5WXZkWTNBSGd5Z0lmRExFV04wdGdhcGZjbkU2bCtGaVo0NmlHOTBKR3VTTloycVVxU2FzVVNETE04SzVhYllCREIyaThYQnBEWm4reHpWa09JUXlhNzhMRTI2bGNXQ2FqWFp5blVEMzRDYXJOV3hTR2kvd0x3TDQ3djl3U0FhYkpBanhCOXc0blpPWkhJZ3RWK0dYaFFobG4za2drejNnMFR4UXgwUjlGMlJiS3dsMnRLWW41ZGdkcms3NTNDajhTWUhCTHpVdFlHVmk5TGZFaVAxZ0lwNGo5RFVBMHRDZnczbUpxd3Y0WXA0N2NQd2E2TVdRQS9BWEF6Z0JmM1VrZTJTN0pjcGsydVorWVFCTmFFUmViS3BBMDJPY2xFRVRKYkUxZmk0VVZ3cnUyNlNEZmdJTXhXbXprYUVGdzNkVkg4aHgrckNMYnVsRkM3QjVjdDVBekN3K09yRVlxQ0pPTzBzS3Fadk5uZEhWZlJZQ0Z4UzBMMy9VWVNTRk9valFhdVlxMjdtTnQvSklBUDBYMTRkY2d4ejRHcHlyS2k4cW52NGRjUnlDVzRLeHpRREs0VlZ1cWlHTTlTWVJ6WFc5Y1VSb1JpUFFBVFlmdzNIVHgvRm9GWHRHZmhvc3FDSkJNT2V1SklzVTBJQ251bkVLUXRMRmlDM2M1RlhCREVsQ1hOcGtva21vWForRjRoSXQyRElBUFVmaEx5cTRJc1pTWXF1NGozWFRCcEI0dEUwanRKY3lyRDdJMmRvMmUyUU9jUlhXUmVYc3NLZ2dnNHZuYXVLWHNnUnAreWtKaUJpZG84Sm9GN2Z3ek1HbGFGenZNdDZvOEY2b3NUeWZMZENsTVY2Qmt4am5rSjllSExFQjNwblkwZ2NyVTBlZ3YxM0s1WEFYaGZoOXR6WFl6ZkhORGIxaFFYU0FWbGpwNXZJUG13VGJzc1daS1ZBalFsaUpacnFMSXJsNk9EK1ZsdXR3bXpZUGx6ZTE4czE0TXMwbmV6Wk9WbEVWVGsyUXdUNGNvcDFEajRhc0N5VHFWTG16TkZGWVdMaTRzQjVMQTJSM0lVQ2RpV3VyeWVTb2kyM3doRGdtaWZBN09sSnlta3lRcDFGYUYzQWZYZGVmUitoK1ZpMHZ5eC9ZODlNR3YxZjlsQksvWUI3ZmFPNGRrd3JsdEZnaVFycmNJS2F0Y3ZjNEtVWklLSHRIak4xaDJURzd0c014YWh5WW8xblBKd0JjQmRzOU9UK3dXUmw4aGlQcEsrWjJMTmlVaG5XWGc5RCtCdzBzN0dBTndKNC8vbmtIL3AvdlZRVzdnOURrbXc0bEVTQk5zTXVTeUwrM01iVFBqOFdUMDByczRsNWVmbFJMQUZCRzVrRFl6YUdMaWtneVI3bVNwdkhjV3RNR2tybjZ0ZGtSekoya0ZFdmlDbExHcURvTml5NUJxdVZTRndWN0UyS0Ftb3JjSlRtWjJlbE5tR3lrU01lNGdVaDhrTnRJZUlrdE1ZcGdGa1pxY25tU0JsVHVVRkdEZG9HV2J6KzRNdzY3cWpDTlpscytLUlFmaldtM3JKTnFUMTdzOU9UeTRoS0FyZlRQOE93YXlkN3UreHNTVXQyaFhxUXhXRUd3ZTNBZGhOeW15UytENkNhanVLemlBTHMzWHZkcmlyZmEwa0cwRWc5bnRwTFRJaHlxMDJMR2hYeGZmeUdIYmdVQnExZ1VyOEg2NEFVaVFycVNvc3dLS3d0c05Ja2EzdkF3aGN6UnhFeGIvaG5Nc3kyVVpXdkk4YkljMEJZVlUwVi9nNGk5cTl3WXN3MlZIK3FjZkcxN25VaitmUWRTalJiZ3p3Y3NkN0FIdzY0WE85Vjd1NzQyRFA0b3ZncHN4aFg4UEZGaDYyRXBsSWM2aGRqNjFhRm00cXhGcExXU1NiRWlRbnQ3NHdhUzRKSzNwT0hJK0pzb2dndWxnK09QSTRneUJuOFJ4OUpwTmJ5SlNRc3Z5ZGJIZFV4SnQwajB2cnU0aTExWVRxRFdLcEFBM0NiQVQvV2crT3NiTUJmSldVSVNYWWpRR2VoMWNrZko3ZndXVDh5V2lYZHh3N1lMYjBuSzlka1F6SmhsbHlkaTFaenlMZnJDQmdTYVI1RXNBRENJS1ZKTkh4M2xkSlREbnh2emp3QkdsNnd0cVZGamVmZHdCQjhtbWJjT3Z0aDQyeVpKbDRWOUJjVWd2ZVdqTWlsSXFYdzAxSnNVN2pESmlvUk1YR3c4Y1NQUFlIclhtczZCeFdxZCtuQWJ4Q3V5TjVrcFhrSTZPTks5YjNNcUJJRW03V2V0aVpudGpsS2tteUtpeWpGSUkwWUdIN1czM0xpdVRNUzhQV05WVUFZR0pxaHR1V0VWWnNTaHdMV0x1VnAycFpzN0w5Yk1rMmt3R0x0eHNzQ00zeFBnRFA3OUd4OWx5MU9qWU1wUEtiWkFhb0dhRWtvODc4VjdpRlIvS3BETE1tZTVVU3JYdVNsZitYNjQxTVRCd0Z2Q2cwSGxsTTNhN1NreGJDbDk4ek1lMUhyZXRWRXA2ZHZKNlQ4WHVJcnBmS21pKzdpcVVtUEVTV0k5ZFNIRWFRVW96WFNmbGFtUER0b3ZBVkdQZnVmVEQ3ZHdkZzhuNm1FZFNZYlNYQ2xyZkJmQlBBQ1QwNjNtNUZzRFpySzBHQXU4aGpGNXZYeTQ3YmxPOUNxeXVWMEhGbFFwSTdFeUxhRDhBRU9nNVpKRnRLNEZ5NUxpUTUxL2V2bVdQNVF1N2VTL0x1S3BnOHhQMkFkc3F6SmpMSi9oUkJTNFI2Q0xWMVlNdGtqUzJUOE51RXRibUlaU200UXdpU09DelQrME13THRQVkppODRTa0NXeEdUTWtnQ1VhOGtGQlB0azJmcmxObGF3TnMyam5WOVpGaDFZb1BiTEF2SHQ0bllBVDBoSW9DU0Yxd0o0RjB3NngwcUhoTDNMaWFab1hxNWthUjdQSkhDT1Q5QTg1ZlhmVWV2WkpWem1FTytYaEN1bEVLWHFKcGhvOHIwNkJXb3R1bFlIaXAzcmx6dDdIMHlXb2lJQ3R6RzdQbm05OHlDQ3pFOEYwb1RZbXEyUWhia3NpRW4rdGtqZmx4MEl3cFFnVnc3Z2tobXBwS3VYOTczYUU2NWlFUzByRmxWeERhd2dWQnlSTEFEOG1LenVieFBoZGlzZUJQQW9tRkpXQllRWGYwNUJTNUt0QjVMcWMxOTRGRW93WlJ1WGhWZW9YZHhOTW1aTUNQaVVOUjhWblJzTFhLbG5CY2JqZGhqTUh1bnJPdGorRTJEVy84L29WbzJ6V2FRanRQMWwwbUorUzBMMVFVRXV5L1NlazBpd0MzaUZQdHNONDFhOWs0N3hBTXplMXdlSXVPZUluQi9FMnBKNVhzU2pucElRbGplWkh6S2hobHovTFlyZitDSFdlRlZjSzd1VVYrbC9QbXJMOWJVRHptMWNCSEFxdW5jYncwMHdLVFlmSUlWZ0JSdDN6Y3kxUzN3OTUzK2NhOTB1Q0xBS3R6bHYzMExQY3dnQ0tPZElnVi9za1hIUWpmZXRtZk5uRVpRN1pEaytKbjV6UFV4cTFxUmwwNGNBUEFMR3U5ZVZlZFBiU1VZaFV5dHlVRkVSSm5uRUxRQm1KNlptZHNGa1laSlJ3N3dQTmsyQ2x5T0xtWGpaUGJzc0NFcTZZUDBZNUI5WG02MVlwTWZ1WENiSEpXcmZxaUJKWU8xK1hwdGtWMFA2eUtXYmFCRzFlNVBmUllUMi9TNXlKVjBNNFBNSUFyZEtJVzRtMnh2UWphNjBxdVBqZEpQRm5tUi9MMWp2UHd2Z1h4MGM5MTRZOTdOTTErblZHVnY5alBWYWs2MEt3MFB1ZlorbjU2MWtHTjFHc3Vrek1Ec2pMblhZM2lzQnZCSm1lWUFMR1l6MEU4bjZFWUtTSzdZc0FUZzBPejM1QjdLNmVNMHpUK1MwUk8vSEpxWm14bUN5TG5IdHdEU0M5ZGpGMmVsSlR0Zy9RTjg5R0NLbzVIN1VacTVCV3FwTXJIYTFIZ2czRnhjZWtGWnZ4bm92aVp2WHFWZEUzN2pBSVBVaFIyTlhZT3JQZWdDK0NPQ0Y2emltOWdKNExOYTZoYXM5WnVVbEtSajczVFdlcGZFNVlwSHRsV2cvQ3ZWejlKeXpCRDdQQ1g3dU51dlRwV0tUaE1MV2FnblNFUmdQbzBjeWZKRUlkZ2ZKMW9Pa0dGMUdqMUdoSkIyRHdDdlhDTE13ZVFLK1RzcDdYaEQ3dkpBOWZVV3lNcnBXQmdKVllTbzBWS2pENXhCa1MrTHRPY1AwL2U3WjZjbXFzQXdoM0QrOERqb1A0MHJtcU9ScW13UGNSM1NwTGxrNWg0bGVhbW9WckMwT1g0bm9HeEM1enMxT1Q4Nmp1WXhQamJCRUhvSmQxUFp4R3N5QXljSUNBRzhDOE4rYkdNVHQ0cnNBTG9SSkVNQjl5Vkhpbk9aeXJnSHhkTk42bXVkWXVLYkVuT3VXcEJ4SjlUZXZ4YVl0eGZCeUJ5UjdqUkM2VW1FcGRhR25JQ25DcnJaZ1ZMaHNsOHhGZmxEOFg4NXY5akpJMlRST1J0S1o5SDRVcHZENllXVDlqaE0zY016T0FRQjNFRW5ic29OekhlUnBiQjBrNHU0Ymt1VWI3VmxXSVUvYSs0U2c0c1FPVEp5KzBEeDRiYlNBMm4yeWl3aUtDUEMyR1Y1RGJjWHY3c1VjWkh3TlMzU0QwNlNKODJBK2hDRHFtSU80VXFpTnJ1YWdKNjVseTRXSEQ5RzF1Vm8zMkNXRXpNRVFVdmd3dWVkT0lzSDJzZ1RHei8wQVBnSlRFZWw3OU5rWTNWKzVqM2tocG5YWHp4YWVKd2lvVzVBVXlhN1FQYlVGNDkxa2liUWFuSElsS1hGWlN6SGVTbFpNeXRIOHN1ZVNTMlhMcFV2Ylg2ZXhzQkl5ZjNQVzUyTUk0bWlZREE5YUN1YzhnRisyMGViTkpLZUxYVGkzZ3NFMHZHVm5Kd1JMbUtZcnlacUZMQWREWlZDNzk5Ukg4cTVFSGlnRm1KcW9Jd2pjMlVNUWxYMW9BT1hwSVQvbjZ5a1FDZDQrT3ozNUc1ZzZyYnVKYkRyaEVwV2w4aVNlRCtBbEFFNkhDYlZ2WlUxbmxvVGRkOUIrbWtmYmRUb2l0TmwyWEZ0M3dVMzB1ZDBtM29mZGFES3pKY2Zid3haZ0FnRmR1VEhiM1NmTk1ROXpRdUh0SkJxMWY1ajZhc1VTNEh0aktHeEpZQlJtNjJFaHBpRFBDWVU3STk3dko2SnhaWUdPb1hhWmpZMkVkSXo1STZPeGw4akNiRVUrMlo0Wkxyd3loN1cxb3d0dEdFbmNMajRtRXl4L04wS3l5Y1VXTGs2dWtVZjl1c2xkUWJLOUJKN01hZFNtVmJUTDc2VmgxcEI1alRsbldlczhnQjhrb2JDWEJweE1EVm5zNEhWdEpZRzZHUEhkRVVTNFI4QkVBbThoeFdFUFRMVDNIOGd6OGNjNkFpNkQybnpRclZvTWdKczFOZDBTNU02SzY2YjVXZTNEYy9VeWprQnQ3TVZSSkNkczhOSVdLeXlORkx0c0F6a1NSbjR2Z1VueDZBSi9EK0Q5Y0xTMG95bnVhclg3aXVXR2tRWG1PUjl6aFFpSUJ3TmJLNXdxc2lKSWRVazhHbG4zTGlFSDRUN3JYbk5CKzFYNmJoK0FYelF4WG5MaWVtVHU1M0tMZlc3RGhiVlhkVEF2eXRaN3J3a0ZvaDVSUlhrWnVta2VkQ09xZlhBdXpuQlhkakErS3duY0s2K0pNVEJFQkR1R1lGbmdqd0NlQnJNYzhFNGhpNWhnZDVMaTNnaU41bGxGekZFbTVMTWQ5c1AzUXNaQnk0cVhXcksxQTB3RzYrUUVxUXlnTmxuR2ZnUnViYnNvQWc4Q3p2aFU3TERseWdNaXJvdmRFeGE1blNZVEVkWnBIbXUzUC9HeEJ1QSt1ck1ickNqZXNoVm5vbVd4OGJhVDlLdFZMZHZBSHB1Vk50cWQ1RFd4UENyR0dKL041RkZ2aE8xa2VKd000Q2YwMldOSWVaY1I1aTRVVEpaUkhGRGwwZmwzTyt6SElaSmhrbGhidG1xVlpNT0pWaFkwc0xXOGNzZ2c5aXhMTjZxQWdwK1FCdHFzc0crR2hGMXJ3UFg2WFYxMGlsNGdYb1dCM0t0OEJ0YkdhTHdPcHA3d1ZwakF6eFdzZFRIYlNrSmNqMUVCd1JiTTd3QTR6ZEUxL1FvbVF4MWNrYXk2aTJzaE5WWTdHRXRXOHNrS3dyVHJ6SWE1RmZqL1hvZUZoQjEwTUVqdDJ4ZHpYRlFhYU9TWm1PNmRPUDNlVFlMVGkxQWdXbW1qckV1Y2ltbHBLUHBMYVhjOXhubE1RY2llYXB0dFRLRzVwQjU1UWJDUFFIZ1E1S2NBUEJKQnRxL05STEJSN3U2NEpNYkpMa1poY3FLZjV2Q2VmVFNDVkZzMkFOU1NUVWFUWm5kcUo2MVdWMVptTzR2OUhIRTlyOE9rcTVHMWxNWnViRjhGNnRub1ZraEQ0aFNZUE9yMThIT1lVcGU3eFA5YlZUcWsvSDBQZ0hjN3ZyYUh3T3pMNWZPMEhZaXBKRnZma3JOVFJ6THNuTWR5OElWcFpMSmdmQ2VFUjcxRmVobEczK3BnNzJXWFhkeTJwN3JVeXQ2b2lpMTY2RDRrblhDa1crYmY2VEQ3bnVPV0F2d0NnUE1pcmlkaktWbXJXQnVJeW5nMFRCclppUVN1YWNnaTFURUVnVjB0OVhzNk56aW1VM2d0cWlFUDMvbytGVElBYkxjd0IwNXhFRlVXbmNuNFUyOGdsR1ArYnFORHlWWFJqdnpvWll3TFJUeHZHUkpETkMrT0IvQVZOTGN2OVpGa2ZYS1d1Z09DMEtTc0RhdDJ0aFhBQ3dDOENpYTFaaElsRFY4TGs1NVdvdTNrSm1ySnRxNUp4dEd1T1NDS0g1ekJxcWdDWEtGUTlJQ2NDNU5UandYd013ZkhYNFp4TmQ4T3MvMkhNK054V2RNdEFCNUtsdXRKSGJqZXpRaXlVcm5yUkNYWlJNalhqL2l1WHpSZGhVTFJ2K0NFRVhrWWQra2U4YmtQazFGdFc1OWQ4eWNCdkQ2SkE2ZDBQRGtsMkhxV3JZeFFWaWdVaW03RlBGbDFSU0xZY2ZyOEtQcHVXeDllOCtWSkhWaEoxaDE4Ujc5UktCU0s5VVFCWnIwVUNCTDdud3JnRzMxNnZZK0JpWHhPcEI2dDdwUHRQTkVxRkFwRk4yTVZRZUJtRWNaTi9JOHdKZW42RFZjalNDdWJTUEdKald6SmVvNlBKVGQwcDlDWkhNVUtoVUxoR3JKR2VBckdSZnlUUHJ6T3ZUQWxRTWNSRklIUHVqN0pSdDdDNHprOGpvdzI5aElnY1lWQ29laWtiTndPRStuTDNybXZ3dXlNZUdZZlhlZHhkSTFBa0lYTmVjeU03cE4xQXovaW9WQW9GTDBJV1FDRWpZaGJBRHdGWmx0TnIrTUVtREtldksweU1hTklBNThVQ29WQ0laRkZlSklkQURnVHdKZDYvUHBPZ2RtYm0wR1ExQ0l4bzBoSlZxRlFLQlFTSlFSRkFvYm9PUU5UZkIwQVhnamduM3Z3dW5iUk5kd3FMUFE1b1Zna3dvZEtzZ3FGUXFHd3dmblp1ZjVyR1NZRklydFYzMGhrMnl1NEJpWlQxWDB3U2Y4THFLMDIxSEpSZGlWWmhVS2hVTFJpelRLaEZvZ3JEaUp3cXo0WnhtM3NZVzIrMzI3REZJQ1hDcXQxaVpRRzNnbkNXNVVTNFVRbFdZVkNvVkNFZ2ZNb2pBRVlvTmRId0NUci94NjlId1h3SkFCL0RtQzJ5OXIvR1pqc1ZKOG5RcFhGVWJnTWFSVUpwN3hWa2xVb0ZBcEZQZHlQd0cyOEcwSEI5QXpNSHRvc1RPVHhjUURPQjNEUE9yZjNTekNGNUY5RGJRV0M2bWhabUhYbUFjdHFCeExLK0tRRkFoUUtoVUlSaFN5TXUzaUJpT3ZYekIyb2pjaTEzNTlGaFBzOEFKczYwTTVaQUo4RmNET0FIemJ4UDFtVXZlMEM3VXF5Q29WQ29VZ2FlZFN1Y1o0R1U2ejlyMkFLRDdnazFtdUpYRy92MXM1UWtsVW9GQXBGSi9Gd0dOZnlFd0NjREpQY1ludUV4WHMvZ0hzQi9KNnM2THRnMW9OM3cwRkJkU1ZaaFVLaFVQUWkyTTBNSWtPNVhTYXE1dmJSTU9YMER0Si85aUplMHY2VU9GZkpPcGVTckVLaFVDZzJERElJb253NUFLbU1ZSHNOSXNnNFErOHJDQ0tBRTl2YjZ2cUNGUXFGUXFGd0JhNW93d1FvaVZGdW8xbEVFTFVNbU1BampsaUdaZTJXTGN1MUtvNlpoVW1lc2RxTnBLc2txMUFvRkFxWEtMYjRQeG5sdTJJUlprYVFzRzMxZHAyTFdFbFdvVkFvRkVraFM1Wm5xMG4zbVd3OXNvcDlJdTc1RVA0cVc1OE5XZGF4a3F4Q29WQW8rZ3B4cmNxd0FLaXMrTCtQdFJIRXpGbmxFSUpGdHhHc2txeENvVkFvMWd0K0N3UmQ3cldMMUxTS0NvVkNvVkFveVNvVUNvVkNvU1NyVUNnVUNvVkNTVmFoVUNnVUNpVlpoVUtoVUNpVVpCVUtoVUtoVUNqSktoUUtoVUtoSkt0UUtCUUtoWktzUXFGUUtCUUtKVm1GUXFGUUtKUmtGUXFGUXFGUWtsVW9GQXFGUWlIeC93RnRoQXZGSTRicTJnQUFBQUJKUlU1RXJrSmdnZz09IjsgICAvKiBkYXJrIHdvcmRtYXJrIGZvciBsaWdodCBiZyAqLwpjb25zdCBMT0dPX0RBUksgPSJkYXRhOmltYWdlL3BuZztiYXNlNjQsaVZCT1J3MEtHZ29BQUFBTlNVaEVVZ0FBQWRrQUFBQjhDQVlBQUFBbDRjcnJBQUEwcTBsRVFWUjQydTE5ZVpoc1ZYWHZyK2JxOGZhZHVKY1pCQTB0Q3RLZ3FBU25nRUhScUhtRUFDSUlSaEZONkpqM3FYRkNJUEtNaVZNbk9FRUV3d014Z01ablFQTFU1MU5CUVlVMml0cU1GOFY3Z2N1ZCtuYmZIcXByT1BsanIrVlp0ZnVjcWxOVisxUlhWYS9mOS9WWDNkVlY1K3l6aC9WYmErMjExMHA0bmdlRlFxRlFLQlR1a2RRdVVDZ1VDb1ZDU1ZhaFVDZ1VDaVZaaFVLaFVDZ1VTcklLaFVLaFVDakpLaFFLaFVLaEpLdFFLQlFLaFVKSlZxRlFLQlFLSlZtRlFxRlFLSlJrRlFxRlFxRlFLTWtxRkFxRlFxRWtxMUFvRkFxRmtxeENvVkFvRkFvbFdZVkNvVkFvbEdRVkNvVkNvVkNTN1FBazZIbmtUNEorNUdkcWZaOS9NZ0RTTlQ1djkxczZwQzlsbXhEUW5rN3B0MFkvbjI3eE9na0g3V2gwYnZmVDYxQ0U3MmJxWEcrbHh6QWQwN1V5anE0NVlGMHZBU0RudUE4eUFlMVBOUERkZXMrYUU5Zk1OREIzWkwvbXhkL3JJdlp4SWtTR05Ob3Y5WjRyVGd6VStGOCtSTzdXNHlWYkh1ZFdnTCthNnJ0RWo5V1REVnBrWGhQWDhBSUdPQ1grTGtSWVlFeThGUUJMOU5ycHlOUFBqTlYvN1p3a0d3RHNkRFFYUEJKdXV3RnNBckM5d1dzTUFTZ0RtSzlENWtrQXBRYXZQVUxmMjlQa1BHVmh0bUROcmJYaW1zMG9KbkhQMHdFQWM0NUl0aGlqOFpFRnNPaWdqYVdRc1IybWUreXNJM3RzdVpJTEdQTW9mVDRJWUsrRFoycUhBbGtTL2VkRldGdHA2ZytQK3JEaVNIWnczNVVpeVB4VlE3TE5kaWdUYURtaXNHTnRqQWMvS3daV0VtcVFBRzZISUdOaUNWdndHZkc4bFloQ0p5UDZxVUtUemd2cG04VUcrei9odUUva0l1SDJaQUJzQlBBRUVWR0poSDNGZXM1VVNOOGs2Vm9Kc29yblFzaTMyZkdWd3FXZXNMWHZNVUR2elRaNXYxcnZOUVAyR3N3VEtSUmlYTHVlcFhEMFJTRHh0ZFMySlJydld1dWVGZVpGZXE3NWlBclV0S1VNSkVSZnBDMEZnUld1T2ZyL01MVm5pWDY4T2haamdaNGhSZGZPMGpqT3I3QnNsZk5KemducEVTalNaMHAxRktnQjhmNWdFd3B6cThiSFlzUzF1ZXBJTnNoVlhBa2cxNVQxdnlSTjJxREZsNlNGekpPN0ZESzVVdlFqTGRta2RaOTJ1VGlXNmt5TUhMV1ZKOUF3ZldleHprTE4wSUtlYjlIYVpmZmRmQXlMZ3EyR21UcnpKQzBXL1VDTnNZMUtXUFZJS0V0dHFrUWtacW5vek5kNDdqVVJCVkF5UUlsd09TOXRRWm1oOXMwNnVIYXJ5a0F0UVpsQVBKNGJWdExtcmI2MkxmdXd1U3BsMldBRC9UaEU0N0FvMW5xaERYS24wZmtrM2NaU1RubzFQRDBEdEk3Mnd0OGVqTXU3MGJUblpEV1NyQ2ZjQ2piSjhzQ1d4U1NwQ0MwMFR3S3lMQVF5YTVIU3FoMFI5MXVnaGRWT04wMHRsNnVMUlJaMC9YeUFSbHB2MHRxdW9IeUlRbUJ2QTlUN2Y4VmFGTkt5R0tMeFRsSS96RFVnTk5iVHduNUt0TE5WS3p3bkZEWTBZQzFKTjFtbHliWGhpYkdRbG51OVBkOUtuZkh3aEJkSEtxb3VDU3hwdFVkYWllV1E3M2doODFDNmROTjFoUFVRZ0gxMTJ1YlJYUGFFVEpnTm1iT2V1TzRTUFFON3loWWpycU9rdGFadDRrNEtpNzNkMjFiMk9oK2d2OHROeUtFaFdzdS9hMER1QnlFVk1JODk2L2MwS1R5N20xaVhQVUd5aVRvTHA5WjN2SUQzVWdHZlplR1FwUVdjSmdzMkk0aVdGMFdSUHBzS0lPNGxRYlNsRmVpcmZtcTcxSTRQb3dtN0g0RE45THFHRm5lRzJzc0NjZzdBRGdCYllWeXR1d0Zzbyt1c296N1pGcU5GNU1xYVBRekFid0w2WmpPQVF3QWNRYi8zVVg4VkFEd040REVBdjZWbm5JNUFGa01SckF4YjBka0kzK1c3SDREbjBHZjZoT3R2aG43bUFVd0IyR0lSd2JCd3gwV1paM0tjR25YeDE0TXQ1T1hmcm9pV3IrbHFqemRzRG5uV1dMVnFSWWYxdFcwcDVXbk9iaWFsZGdPTk1jL05KNGxzdHRGcnNVV2xOMDVQV2hpWkhrelBlQ0ROK3o2aHhNL1FlbjJVMXVHMHBkZ1AwenJ6VUwzbDVIS3ZmaFBkMjF1TkpKc1JtbG5ZZmtvakQyWkh1UFVKMGh5aHpsNHZyQTdlQ0dlWGFzSFNubGx6cklqUExORFBrbWkzdEt6YlFiYm5BVGdMd0V0YnZOWWtnQzhCdUxwSnF6Z1pZRFdWYUxFZEt5eUZScU1IK1pvekFQNkxCTlFzQ1dHNTJEOEE0SVVBVHNIeWFNZGErQmFBMndCY1pZMVpGSmQ4a0xBOUFNQUxBRndBNEpWTjlPTTNBRndQNEdzdHppRXArSThoaGFzUlM4Rys5MllBM3dNd0J1RC9XdGFNSzVMbE5yK1czSVc4WlRFSFlIK3JYUlhybmtWU1hoWm9UcXdqNWVnaEFBOWFhMmFlN25VU3phczVJb1JhV0UrSzZSNGlESjdQZ3dEK3YwVVVCVEV2RHdkd0tvRDMwTytOelArZkEvZ0VnUDl0dlQ5TTE5blhSc0lOQ3NoajBqb0F3S2RJcVQyb0NibnpId0F1dy9MOWVGYTBYaEpnTk5YeWF2RGNxSWozQ3dEdXM1UUVKdStHUFlIZFNySjVpMlFsMGNvT0xvZTR5THdBZ2sxWkpGQ21SZkhNMGZISkZ3QTRtZ2lYc1NCKzVnUjV5b0NGaGFtSnNSa0F1NFEyTml2SUYrSTdMZ2RDa3RnRkFENEk0Qmt4amNYREFDNEhjR09UcmtxcEliNGZ3SlVPMnZRQWdGRjY1aTMwM29Fa2dGN3U2TG0vQXVBakFIN1Z3TE15L2dUQUZhUlF1RUFad0hVa3ZIN2RwQUxHWTNBZWdIOTFPRC8rRU1DOWNMOFB5RzNlQmY5NFRLdjRvSmgvMG1JOUJjQzNIZDNqOEFDUHloazBkZ2M1dXNmZEFDNmhmcmU5SzNGYS9tRTRIc0E3QUZ6bzhKcVBBaGdIY0x0NDc0OEFmTWZCdFc4RWNHN0EybWdLM1VxeVBHbDR3aVJJKzU0bUl0eG5XVFpKUWFSSitNRUhLZUZha0hzYkhJZ3pBT0NnMGZISlB5Zk5hejFwaG1WQnBDVkI5R3lWWmVrNnMvQ0RoM1pPVFl6OUFzQXZZQUpUOWduM1JzV1JpNE8xckdlVHNId1h0YVVkbUFId1VRQmZJQTErTTdsM29rU3dzbEx3V1FBWE8yakxWbkpGQVNhUzlGWUFyNGpwdWE4RDhKY0JDNUdmVXo3dk93RDhOWUJueHRTV0JWSWtQa2lXbEwxT29ucDB5ZzdiOUM4QUxrSThXd2lua25mQkZRNkUyUkt4Y1J4WlVTNHdTa29nVzEwZkIvRDhtT2JEUDVJaU9GTkQ0ZVAxMTRmR3lWZk9iWlk5OGg3SFVCdGVHYVBjdVFuQURRQytTZjM0RXdmWHZBUEFxK3NveWcxWlBOMklmY0xGeUNZK3Z6Y0hQMUkyYWJrSFBGUzdhVXRpc21TRmtDbWlPZ0FxUzVOd2dEU2JBUkxlNjhYckJ0S29OOExmNnp5RXROTkRBUnd4T2o1NTdPajQ1SEh3OTBrNTZVVXlZRnlTSWVPVkRuSFBnQ2I1ZThtNmVtOGJDWmJkVWg4bDRmNW1tT0NnRXZVUFkyT2JYRmJzSmJnVVpnLzVGVEhlNndKeU01NHNoSTBNdmlrQmVCNkFtd0Y4SmthQ0JjM1J0NUhYNUxWRXNKdG9UYVFSbmpSRnJwRUtqYU1ybkVYWDVQbnVBcndHM3Vtd25kZUhFS3hyL0libXlOVUF2aDhqd1FMQXUySDJhNCtpditVMndHWlVSL0hPaWZrYjFTS1g4bE1lUFJxQzJWTDVlY3dFQ3dCbmt6WDdmcGhZaG81RHQ1S3NqTnBqc0pVNkdLQTFNMkZ5TkhBYTFTNWFQak81YUwzSGtadEpNWms0SUdXUXlISU4vS2kzdGZUYVR4TnVIYjAzUkNSOEtFMzRZZmdCTm14bDIrMnQxQkNDU2N0Nm5RUHd4L1QvdjEvaHNVbVJkZmNUQUVlU1Zjc0t6dzc0QVRweFlqMkFyOE80c2R1QkF3SDhnQ3cyM3AvbkNOczNBTGdUd0orMWVSeStRVmJ6ZGlKNURvS0owdjlYTzJ6SElJeTd0UVIzQVNrczNGL25zSjN2YWRPNG5BdmowbjlyRzVYZktmS2lUSk5DUGt4S2NJWGtFb1EzTDZwYi96SDRaMStsTitzU0dGZnVPOXM4MzY4RWNLMlNyRnVrTFNKSzBZSk9FOUd0b3dtMEgxbFFtK2gxQTVGZVhwRGZXcHBnTXYyYjdXcm1zNlFwK0ZHZmZhZysydE5QMSt5ajF3SHhtUnhON3ZYMCtSejhBSzZvS2Z2WWJkRW5DRFpCeFBxZkhUWSt6d2Z3WXdDdkY4cEJCclhQcTdyQ0dzY0NPQ28rRCtCRFFsSDdDRXhRMHVBS2pjR25BSHdYSmdnTU5DZW5JMXBiRHpsc3h6L0VJSE5lNXJCOVQ1TXlrZ254RkxtVWs5Y2d2dmlJV3ZnTXpIYk1uTFVHZDR2bmEzU2JZQjcrRVNqUTNKOGdPYXV3M0M3ZGlBVDhqREpEOURvSTR5ck1XUk9IejdIeVh1bzhrUnNuQlZpRUgweVZvMFdYaGI4M200Vy9oeXR6YURMcDJ0bFdFdlJxUnhCTGNtV1hjQ1hrMlZMaVBtVlVaNmFabzJ2c0R4TnQ5NXdPSGFOMUFQNGRKc2puY3NSM1VMeVRjQVhNMWtVT3dQczZvRDB2aHdsa09oL0dmVHdTa1dndkJIQ1hvelljQi9lSldDNTEyRWZuMEd1dno4K0xhZXpmRCtCRVVvSXI4UGZ1VXhISFIwYWo4N2hPa0JXcjZDR1NaWUo3enVqNDVKRkVPaHRRblJpQ1NYVmhhbUtNajlFc2tTYkhaMEtYYUlJdHdCd0ZtQlhrMnllczBYNUIzamFCTXpHbVVSM0I3S0hhSFd3bnV2REVlMEdXckNlSTFaNzhSOEFFVVdXNllLd3VoVGtQZHo3Y244bnNSSHl5dzlwekhpbGp0MFVrMkR5QUh6cHV3MnRnWE5ndU1BUjNVZUs3QVB3LzFBNE9TL1RRM0h3ZnpObmF6d2laTlFzLzAxU1VjNlpTWGhVQS9ETk04SitpUjBnMlNRc2lEeUE1T2o1NUFreUlPR0NDakRocEJCUGhJb0Q1MGZGSlRnakJRVEU4bWZZSUluNklpR3NmL1o5ZHZreXdUTDRjV1N5SlQ2YkhTOEJQVkRFdlBsc08rRjVRRnBaNlNiRmZqZXJROVc3QWVUQkhDejZ2eTI1RmNBc1IwL2NpZkhaSldDZmpqdTcvTmlMWmxvOUV3SnhaZFlYTEJWbllKdzU2RlZjUnlYcmsyZGhCTW5NRzBRSVRpL0N6dm4xQ0NiWStZYmxBT3pXOWlpQy9JU0xXQTJGY3hYM0NRdVc5MGpVdys3TDcwMmNQQWZBc21GRDZVWml6aW44STRMVFI4Y25uMDJkNEQxVUdPbVVEckZJWm9NUm5hNHRZWGo2TXJlRVUvTVBPSlZTZnJaVUVtdzNwMDJHYTNMZDM0Vnk3QXlhQmhXTGw4R1dZcU5Jb2F5d0pFempsQ3FmREJHQzV5RTk5cnNOMmZaMWVPZEN4dUVybUFrZFNjekRpZHBLTlVZNnFNTUYrQU1EZjZMS0tsMlFUZ3RBeURaQnVzc2w3OFgybWFUSHNUd1NhaHdrbTJFVEVPd3cvNHBkVGttMkdPVHQ1SUJIdkFNeWU0UkV3Z1U4cG1Hd2tPYkhnK0V6dEFGbkVjdi9WM3JObFYzR1pYRkFsY3NOazZQcGxRYXI3NEVmeDJaYXNCejlOSTFDZGtXaUJGa1czNFVheXZoZER2Q2RlRjN0V3VnbjdXMWFIdmRXUURsaS90emk4LzRuVzllWDlvMlRlV2t0ci9JMk8ybk03L0R5NHRlNWY3dEc1Y0FIOXpvRlFqd2Vzd1hTQXpONUo4dllqUGJwT0tpNk5TQmVXck16WGF3dk5KQzJNZm1GWjh1TEswLy80Zk9rQWxpZjA3NmZQSllVRldCVFdId2N2c1ZYYkR6Ky9jRnBZdGt5Ni9NTjdySktRcGFLUUlwSk1qNDVQcm9OZjRxd29YRW9wcXcva1BpMWJxUnpBSk92Ukx0QjErRGxxTGVBUklpYSs5cjFkT0dGdkVaWkhEaXVmUzNXMTQwS1lNNDRJR0FzV0xuM3dqM2U0RERCNnZTVWZpbUxOMmJJb0gvRDNIZ0JuT216UDVjSkR0SWg0aXN0M01pNnovaDRLbUJNbE1UZldpckc2VlpkU2UwaldDeUVLWGpqc3doMFMxdVFtUWJpRFJDVHI2ZjFOZ25BNXdRTW5kOWdBZnkrV0xVYys0QzZqNHFMa3JaUnBGRG1vU0FZaFpjWGsyZy8rVVpreXFzc3hwYkQ4Q0k2OEppZTNLSW52UkhHWDhiUE1pYit2Z3NtZzBtMFdyQlNLQlZTbnBsU3NqQVh6Vm92czdIbTNRSzk3WWJJVFBlRG8zcWZCQk1CSjJjUEgzeFpEaERzRThRUEFYemxxeXhZQVB4WEtIN0M4R0VDdjR4Q1k4L1dJNkVsaW1mbTNNQkhqaWpaWnN1VUFvbDFENU5kSDVIZ2tnT05IeHlkUGhzbU04d3dZTiszQjhQZFVONU1BMkV6RXVwNUltRWsxSndZNWplVkhhbVRCN1VhZUs0SHFZejRsK0VYWXk2ZytyRjJ1TVJGVG9nMThUVm0wblltOWtYcWFKYUdndkxQTDV0YjFNTUZPQ2ZnWnFZQm8wYTJLZUhGUmdFSnNLNG45OFBjblhRYTJuR2xacXB4Mk5LaGdoTVFlV2d1dU1tYXh5M2tqekJaTS95cWRDNThRODJCUHlIemc5YnVUNVBPVnVvU2lvOVU5TUNZM1BxcFNFYVNTQWJBZkJSUDlBUkhxNE9qNDVLeFlVRnpIY2dGbW4zSUdmbVVNdGhobkFleWJtaGpiQTcrMEU3dURPVGdwSmF6UlJFU0xOb25xMUl1OFoxcUVINkRFTHVzRmVrMGlPREFnVllONE9kS1lMWVVvSk1PUnoydGhEb3Yvbnk2YlYzZkRITmNCakd2eU1mcTlYdkYwUlh1d2pzanFZWVFIdWtpUHkvZGg0Z3pXTzdqM1cyQ1NVL0QxaXdFS0x4L3prdWtwUGJqTnluU1BJRmlKMVhERVRJSUxuN0RYTEl2bHdWOXNYSlFBZkV5WFQzdEpWaTRPdVZpWFNPdjhBd0F2cHRkMU5GaFBDMkxraUYwK0VzUEZ6YlB3STJ3WEFTeU1qazgrQ2VDK3FZbXhCQzE0L3I2MFhsT0lmcUFhcUU0R1lSK3hZY3MwVCswYVF1MkMwUEt3UFFkSWNSWXBEbVJLVGsyTVJiRmtpNlE5N29ZcHlmYnlOcytMTXFLNTNZUHdBRXdwUFZZVUhoUC9pM29PTDA3c29aOTUrQVVsVmpxWngxT2tUTTZRMEg4MmVZSGl6Snp6S2lMWlVvMDF3VXBsQ1NiUnU0dVVuYytDQ1RCOElrQnVjQ3hFTXNTYWZadWpaMzgzemNNZFFxbWRGMlMrVW5pYTVzRWtlYS9XdzYvM0hDZitoRHhQT1VHMmNoN0l4QlBudGFFZk9GL0JMakp3K3VBSHVYWWRYT3pKU3FLRjBJWTJqSTVQSGdiajk1ZUpxZms0ak55VDNVU2RlQWlNSzdsUEVQRUFmWCtFM2kraE9wZ29JeXpHZXVSYXJ2RytQRkxEQkprVkM3QlBDQUVJUWsrRjlHTkZXTW9GMFVlTlZycTRJZVk1c0JWbWorV1pNRzc3RndJNEFTYkg4akV3T1hldkpjS3ZoMS9SZDRxVzFUTVFZUXppeHUwa1RKNEZzMVh4WEppdGkrZlNHQjVLL2RBdXpCRnh2WlNVMEdkUnY3K0tyUDhEWUtxMDNCYlQvZVZlZVM1QVdXWWhPMFIvMytUdzN1KzE3Z3ZoQWJPdGFHbHh1UkN5RlpqS041TEFwWHhxOTU3c0RDa3ZmSHp3U0JxYlY4QWNMOXdFVTNQNG1oamJjRGE5amxneTNNWjRqRzJvQVBndzljRUlqT2Z6ZVFCZVJLOGNyek1LYzM1N2FiV1FiSmpRWkRmckNQekkzQlFKM2lPcEF6ZkR6eU9jRTFaMWhUNXpFUHdnS0Q1bnlsWnlDa0JtZEh5U2c1OFNndkRTRWF5d2xDVlVaRTFhR2Z6RVAyVngzY1dJL1ZteDdzSHU3RUpFb3BtRHljOTZSRXhqL3kwaXhJUEpCZlFJV1JjL2hzbDEreUNBKzJHaUNOOENFN0JTcTlib2Q2aTlSYkxBbmhLQ2xNc1I1aEZQeWJOYVdJQUo5SGtOVE5ham5mUytQQzg2REhOODRXTXdoY1lmamJsTlA0VTUwdlFlbU1JQ0pVc2hxZEI3ZDhKVTAzbFhERzA0S1VDZ3lxSVpGVUVDNjZoL3RqaTY5eVUxMWdzQ0JENWdncVpjNEE3TFFPQXpud20wUDVmN1E2UlV2US9tNUVBcDROa3pORi9lQmxQYWIwc003VGhSckJWWVhnVFpMMitLcVIvK2llVHJGUUNldFA3SHh5VFpDL1VBelBudEU5QWwyMmd1enNrR1diVmNXM0FOL0lDQ0lmaDdMbXlCY25Td1RMQS9ERCszOEFqOGd1aWMvb3Z6QXU4ajY0T0RsZm9GQVFQVmdVaEppM2paNnMyUmhWYW10bTZESDJEbGliWVAwejFtRUMyTm9VZFdPcDk1SGFEdjc2Wis0R001WEV0V0VyTHMwdy9GTk82bndFUVYzazkvNXkwaEgxUVphQmFtVnVtYnlkMG1jUStSQmhQWWpnREx3S3Vob0NSaWROWDlNVXhOVTN1dVB5VitueGEvLzR5cytMZ0tMdHhEbHNrUHhIdnpsbklGc1dZQTROTms4YnJHWVdKczdTTXpzbHJQYnBvWEwzUjQ3K2VIV0t3SlFlNVNucmpLQTMyVjlmZE9NZmZhcVFEZVJvck9qK2p2ZFNIenNXZ3Bza2ZBWGJRM2c4OGZ6MWdreTk3R0NvMTlIQkhGKzVPRkhKUVhnSjgvYUh2cGZwaWMweGVpdzlFS3lTWXM0VmkyU0NZb21YNjVobFZwRXlQdnVmSzVWMjlxWXF3Z0JvUEpWbjVlL2tRRkw2NGkvQ1FVZk84Y2ZKZHhNb0FFZzF3ZTBzSXZXMVp5Q2JYUGlVcEw0aWk0cjRQNkVDa1RQdzI0cnhUeWxUcUU5M0ZhSEV5b0w0Si9ocmlUOEZLeUJuTU5DdEFTekZFUjEwRmFXd0Q4UlFQS0srL2Y5eE1wbit1NFBia2FIcWw5QVo2VkhRQis2ZWplYjdCa1VMcUdvalVLTjBGWDMwRm5WS3Q2R3NEYmllQVBGSXFNaDJpSkQxNEdzelhqbXV4Z2pjV2ltSU5ueDlBUDdQSHFGL2VOY3NSeEdINnN6SFV3Tll0N2ptUnRzckZUQXliZ2wzanp4R2RLOE4ybEZVRnU5akVnM21OTmlkOExOT2djbUNRdDRTQmlyVVcwR1lzWUt4WVpBa0NLM05FY2dNVTFhSk1CSkdvTDhMTFF3dmk1aS9BRHZMd1FGNW1FNi9xald3R2NRY1F4WS9WREFkR3ptaVJnM1BsUFVkOGNJalRRL1R0b2JpOEphMUdTU1Q3aTl4L0I4c1A2cmVMbWlNTFJKaG9XUERjNmJzOWdnSHZRVmhnekZ2RlBPTHIzMnkyTHVXVE5RVmszK1F4SDk3eXVRK2JtR2VRMUE3M0tBTlMrQ04vZkRwT1UzeVVPdE9hQ0xROWNsK2U3bUpRTXptVTloT2lKYW1Zc3I5alh5QVBWYzVac21GQmdhNVFUUnlTc1JScGs1ZGxrazBiMU1SMk9RRjYwN2lPaml5VUJOaG9abXhBRUtJbGUxbnUxeWJqZTlaaXdpOExTTDlHRWlrSm9KenNlNnplUmkrVzVBZU5oOTErOXNYNkVpTFpJWThJWmM3YWgrbHp4U29MUFFRNVpGbW1Vd0JZbTVTODRidE9kVFdqNnNNam9YeDIySjFPRDJHMWxsWW5BVlNEV1dwaUFsdWthbitGQ0g2NENicjZKbFUvZFdhRjVzQkhWWjRMN0xZV3FGdEl3KzVFdWEvN21BKzRoM3ovWTRiMG1ZUXFGSEJrZ2g0WWlYaU10NWxFV3dCLzFHc2w2MW8rOUtOTXdic2xoUVZTOC95anJ1Z2FsRjJUeWxJSWdKVDdQOTJkM2JscFkxbEhKVlVZRUoxRmRiOVl1WHdlaENIaldZZ2tpMjdKRnFoRGZaVXM4eUN0Z3A1UTgwZUU0M3dxLytzcjk4STltRkFNRVFOVFNlWThJd2I4UC92N2V6ZzZaMjdmU2M3Szdhd01Kc2lqN3ZnVlNFdWZoYnA5dUJ3bjVLT2dYMytINXdJckN0OXU4L2hlRlZ3Ymt3Zml3by91L1hqeGZMa0RnenNFRVBMa29lbjhwRWZwS3AvVzhnWWhyQjdWbG5TRFhxRjZXRW8zRDl4MTdmaVNLWXZ3UGc0bDBkZ1hPbWZ4Ykd1UER4RHFObXF5SEZlRTlkSTA5V0w0TjF2V1dyQmNpc0RJd3J0WkJFaFpNbW1rc0w5eGN0bDZsQlprVlJBcEJXakxUVThheVp2bVpvbGkwa2pUNVdTcW9yZ01yQTRCa2licWd2ZGVLUmM2ZVpmbktqRkpKeTNxd1NmWUlWQWVldElxL3RsdytkakdIWk1EaWlqSnZaSktOM3dqdnhVcmpTV3Vja2syUVAxc1V2M1BVcGtjYldITzJ3RDFNL080eWYzV3pDc1JYSGQzL0VyRU9wRWRMcmdWWHJ1S3ZkSWpNL2JwRmFMT28zZ050Qkw5MDJLNWlIYStESyt5Q1h3ZGJLbTVCM3JWYW1FTzFselNEZUtMd1Y1UmtneUFUNHZQUm03VDRTZFc1WjBxUUFCT2NMSVJlaEI4WnpDUXJBNnRTRFFnWGFXa21MRExrNThnSkRkdE8zVmpyekswZDhDU3Q0YVVBVWsxYS9lTHkwUFVXK1BzL25ENnVVSU5rby9iZkNBbUZITXlaVHA3NG5YQitiVGVSVk5xYVYvTVJMZlVoTVc3emp0ckU4eVdxVlRZb0ZFeDVyT0ZoaC8zazFaQUZtUnJ5NGlGSEZ2VmFtQko0RUd0eDJHcVhpNElBUDRSSml0SUpnWGtQMGZyWktNZ3RoK3F6NVBYQUN2aUREdHNWTk42ODlkUG44RDRUNHJtNVZ2Y2lyZGVvcHd0R3JQbnIwZmQvdUJwSVZsN1QzaStWLzgvVUlDdTVCOHIxV1RsUlJGS1FkVXFRTVY5SHVwU1RBYVFxQlc3RnNtaHRhMXIrWkFXaEp3TGF6QUZjN1BxV2JiTnJ4VlpDTEhoYldYR0ZjeXl0ZVQ2ZzdjMjQwS2FKakFvdzUyczN0V2dkdWNRZ0xkeVN0YUNqV3VyU1plVnFENC9iRWlWaU9VK1dkd0hMaytmSDFiK3BFS1ZBQ3VDSzZNTlBPN3J2cWZTOCtZRCsrUjhOa2s4WUxvWi9jbUNsc1NBVVhtbVZ6U0Y2QlNEdW82ME8yNVVOa09HOERnNTNlSitiNmJXZm5wbExpV1libEQzRGxweWNSWWZDQmNuS1BVVk9hY2huWEJlSStISkN1UE94bUN6Q0k0TFo2bnVLQk15Q2NDbmtoVVZaRUpvV0MzdFB0RUc2UW9vVzJmRWVNWmZQMnpzMU1iWkRDSmRwSWc3ZS95MExLNzBzTExhVStKdGR5bnpXdHAvNlpaWitmMFRjdnlRSXZtUUpOWmRSdXJ0RnY3c21jRG14dDNmUXZFNkV1SmRhdlVZcmFNUWFrS1E2SDZOaW5MRFdDRUxJdkJpZ0JIelRFV21OMC9NdXdqOUx6N2pBd2ZYdkl1dS9VMm9WMTdMV0dzMDJsWXBwTHZDNHNvemJITU02U0ZtS1I2UEg1V2Jnbnp5UjJOZXJscXgwZnlZRExMaHl3SDJUSWYrWGxtbVJKbDdCc3JhWXhOZkJ6dzFjaE8rT2xkY0txaElrTXozWlI0d3FxRTZYS1BkLzB3RldMN0E4U2xwbW4wcGJWclpuM1ROb2tydU1sSnUxRm93SGhjS05WWDY1byt1ZEpwUUxtWW5wZEFmWC9vcmwxVkEwVHVJdXQ0RG1MSXNlYUM2T0kwTmphc3V6aHp1dEUrUGFvN0JKTjZvbVoydFN0aXVZRjBwNWRIeHlEWkVzVzdUczFnMkwrZzI2aDJkZHY0enE1Qll5SHpLN3VFdE45SzhNZkpJMWFTc2huM2VaM1dkYXlWVVJFOGxlNGVoNi95dkFZbjZSSTRGK25RNVgwNGpEK3ArMjVoRFFYRDd6RWF1ZHlVNlZjNjVKVmdZdVpWcTRGMGYzSml3cmt3bHFDY2FQUDBUdkZlanoyUkFyMFc1alVsaWtDYkhBK1JBMjV5NU9pK3MxVXVGSFJnMy92cUxJMU1RWVc4NkprTEhneng3aWFEektTcTQ5aDA0YlR4ZFJ1OGRaQ2pyZ3B1TE9GMUh0YmsvbzlHa0loUmdzMldMQVdEUlRsR0V1eEdBNnR0TTYwZFdlYkRLQXlHcFpzMUVFU1FYVkJkQmw0RUxaSXBJbGkrU2p0Sm52d2IvenZoQzdtRlBreHVBOTEzVEljMGE1bDZ6eXc0RkdRWHRnZG1rcEYwclBRQnM4RjRyVkJ5NENjb21qNjUwaDF2NUdtSUlPcmVKelF1bHZSRDRvcWhXNlhRNnZlYVJsU0RXTGVmaHhKaFhMR09wWlMxYnVZL0wrcFl6UURYT1JJb1JzcEdVbmo4UFl5ZXM5Vk8rOTJ0ZEN3UCtTWW1Ec3dDVXVEdEJIbG5MV3NucVRJYzhkTmtGNUQ3YkszUjFpa1hCNzczSTRMaHM3MkFwU2RDLzZZUGJWZHNCazhHa1Y3eEMvSCsvZ2V2ZkJUNlRQODE3M1phTXJVQkl1RTh5Y2I4bWpSQXVlQnB0TGNwM1ltYTVKbGlPTXN5SEVZNTlwVFVYc1NMWUMyZnBiSTdUVHJIaU9JdXFuUFN4YjVNY1c3YUt3YkJORXNDUHdOK1c5T2lTVlFuQkFsNTJRZ3QwalhnZ3BBOEF0RHNkbGs1SnNUMW9YSzQwRjBaYjNPN2pleTJGcXhnSnVrZ3A4V01pY0V0UlYzQXpKc2l4NzNPRzEveEwra1NEUElzWkc1bllheTQ5cm50dUpuWmwwZEEyN2trNmo3aG43Y3lWQmVOSzY1VUVZZ3UrKzVlTkFUTVl5K2I2ZGlVbG1aS3BZMStZSVpUN2VrNlA3NUJHZVl6a3FPTk9UVExsWVMzait3dUVZcjFPWm9YQU1MdnpCNlI5L0JEZEhKOFpvYmI3U3diVnVwM1hzQ2N0YjBaaUZ5SDMzbU9Qcmp6bmdvS0JjeW0vc1ZaS1Y3bDIyWmhkaHNoYkpURThGMU0vNEpCZHhBYjRMdDI5cVltd2EvdG5iT1NLUEJTTEdRVlNmUzQyQ0lmbytrOStzZUo1OW8rT1RuUFZuQnozTEVpM1VxT2N0dWZUYkxOM3JhZmkxYXozUi8zRnIyUDlHL1pZTzBGWVRJVnBzTGtSenRGMDdRMVovZGdyS0RxNlJkbmd0VzhGcnhKcXdmODkwUVA5eW9Bb1h1NWdGOEFFSDEzMGxnTDl4Y0oyTHhCb2NFRzN0RlU5RW9nMXpRV2JlU2dQNGQ0Zlh2Z1orTmlrcGEvb2pmbjlBRVA4UXllelR5UnZTa3lRYlpKVm1hMHlxUnF6QnN2aU9qQnJPQ2ZLVzJhT1NEUXBGdGxDTHFDNFFrQkNXY2lxQ1ZaNnFzUmlTcUs1WGE5ZmRqZHY5bDRWeHc1V3d2T3FLRnlJRUNtSUJaSVExenA5ZlM3L0xaQlFMWW5FcWVoc2o5SG9BL0ppSS8zQnczWFBncGpqN0xRRUtnYUk1UllxVmxTY2NYbnN0Z0N0cDN1d1JDdTA4cWdNMUV6Q2VSRnVabUJPeWFaYXVjM0duZG1SY1IzaGtvWE1YOStCYXJESTFvc3dwWEJIa0dJVzA1QkViVDF5N0pEUkVTZUNwa0Q2ejNlUXBTOU1NS2c1UVFlMEMxUUF3QlJPNDRRcWZGZjNJZWFWcm5ZR1RkV2FMMXZ2RHREQUFVNFB5bVlLRUFmZUZ6aFdkaDcyV0Vwd255K0o2Qi9LbzFlTHNWNHY1bVVabnBGTHNkcElGM0JZakFNd1JyWGNMMlhHd0lGQXBweGN0R2NUZVA1Wk5DUUEzd1UzaWtvNGsyVVFBQ2FYZzE0RXNXNFFXeFdxMTNhbDI0QkFUcTEwT3o3T0lvOVllcXR4emtNZHJnbXJKMnRIUlVmb3daVTBJZWYwb1NrQUp3SGNkanZNeDhJdGtsMmdpbDJvUWJORWE0NHdnNlJuNEx1VnQ4RE9zYkZLNXRHckFjM2k3c0VBQTRHTWQwTGJQMCtzQXpWRWwyZFpsUEFEOFp3elgvbnZ5Z0t5RlgrMXFjeDJ1a1B2cy9URHBQYy9zNUE1MG5RYzFCUUNqNDVONTBSa3lUTHNSOTJoQ2tFSlJXSUpNWWpLZk1GODdnOXFKem9Nc1dmc01LeXh5VFlZUWROUitaU0l2Q09LUHNpKzN4ZkZZZnc0bUdYdFFXMnZsc0pYdkplQUhVaFZJZXp4RENOd0JhQlRuYWdIdm42MFR3dS9YTVZnOGplQUdBRDhURmxGUS9FUk9oeTRTY2tKKzVXRktXWDQ4aHZ1OEJzYlRkaHpKanFkUVA3L3pXZ0IvUWZQdHRFN3Z5RGoyWkJNa2JGTm9QaUpYUmdaTFM3WmlrYXlkRlNyUjRQVi9UOFJURTJOOGpyVU0vNXl2akc3Mm1uaUdoRVcwU3czMCtUMHhhT0hYb3JvcWoxeFFzdStHQWhRUy9uMkdYazhDY0J2TS90ZGxRckRwTWFIVkFkNkQzMDJFVzdZc3laWEFsK29vN0N5a0ZkSGtGNE1qczc4WDA3M09namxyZlN1QUZ3TjROc3hXMURETnJXSDYrM2dpMVMvQ0JFOGQyZzBkNllwa2JaZXFKS2xtTE1FZ3k3TnN1UW42NGUvOWNtRUFtZjNKdm1jbGdEaGtzZ3NQeTZOK1paR0FXbjJXUk8zZ0o2QTZhMVVVSXZvdkFQL3NlTHdQQW5BaldhQUhpTDdoeE95Y1NHVFdhcjhNUENqQjdIdkpoQmtmaG9saUJqcWphTHNpWHRqYk5VV3h2cTVHZGZMM2R1RW5ZazRPaUhVcGF3T3JseVU2cEVkcmxoVHhuOEd2aGhZSC9oU21KdXl2WU1yNDdZUi9LbU1yZ0hzQjNBSGdEZDNVa2EyU0xKZHBrL3VaV2ZpQk5VR1J1VEpwZzAxT01sR0V6TmJFbFhoNEU1eHJ1ODdSQU95Qk9Xb3pUUk9DNjZiT2llL3d6eEw4b3p0RlZKL0JaUXM1amVEdytFcUlvaURKT0NXc2FpWnZkbmRIVlRSWVNOd1owN2pmUmdMcGphaU9CcTVndWJ1WTIzOGdnRS9TT0x3MTRKcG53bFJsV1ZUNTFQUHdhZ2prSXR3VkRtZ0VOd2tyZFU3TVo2a3dydFdoYXdoRFFySHVnNGt3L3FzMjNqOEQzeXZhdFhCUlpVR1NDUWM5Y2FUWUd2aUZ2WlB3MHhibUxjRnU1eUxPQzJMS2tHWlRJUkxOd0J4OEx4T1Jib2VmQVdvWENma2xRWll5RTVWZHhQdFJtTFNEQlNMcHcwaHpLc0djaloybVY3WkFaeEJlWkY0K3l5TDhDRGgrZHE0cHV6dENuN0tRK0NwTTFPYmhNWXo5NFRCN1dHVzZ6L2VwUDJhcEwwYko4dDBBVXhYb0ZSR3VlU24xNGZrSWovVE9oQkM1V2hyZGhWcHUxK3NBZkxUTjdiazV3bWQyNjdBMXhBVlNRWm1tMTF0SlBtelVMb3VYWktVQVRRcWk1UnFxN01ybDZHQitsY2R0Z2l4WWZ0OCtGOHYxSUF2MHY2MWs1V1hnVitSWkJ4UGh5aW5VT1BpcXo3Sk9wVXViTTBVVmhJdUxpd0Zrc1R4SGNoZ0oySmE2Zko1eWdMWmZEd09DYUY4TmM2UW5McVRJQ25VVm9YY2U5ZDNaOVBkbXk4V2srV043SDl0aDl1ci9ySTFXN05QYTdXM0RxMkJjdDRvWVNWWmFoV1ZVNzE5bUJTbkpCQThwOFR0YmQweHU3TEpOVzRRbUs5Wnd5c05GQUk5T1RZenRFa1JlSkl2NVFQby9FMnRXUkRyTHd1czVBUHVUZGpZQzRCRVkveitIL0V2M2J3TFZoZHVqa0FRckhrVkJzSTJReTRJWW53ZGd3dWRQNjZKNWRSWXBQMjhtZ3MzRGR5TnJZTlRxd0tWdEpOa3JWSGxySys2RFNWdDV1blpGZkNSckJ4RjVncFF5cUE2Q1lzdVNhN2hXaE1CZHd2S2dKS0M2Q2s5NWFtSk1aaHNxRVRGdUoxSWNKRGZRZGlKS1RtT1lBcENlbWhoamdwUTVsV2RoM0tBbG1NUHZlMkgyZFlmaDc4dG14RThhd1VkdmFpWGJrTmE3TnpVeE5nKy9LSHdqL1RzQXMzZTZxOHZtbHJSb0Y2a1BWUkN1SGp3QTRFbFNadVBFM2ZDcjdTamFnd3pNMGIwSDRhNzJ0WkpzQ0lIWWYwdHJrUWxSSHJWaFFic2svaSt2WVFjT3BWQWRxTVRmNFFvZ0JiS1NLc0lDTEFock80Z1UyZnJlRGQvVnpFRlUvQm5PdVN5VGJXVEUzMUVqcERrZ3JJTEdDaDluVUgwMmVBNG1POG8vZHRuOE9vdjY4VXg2RGlYYTFRSGU3cmdNd0JkaXZ0ZEh0THZiRHZZc3ZoNXV5aHoyTkZ3YzRXRXJrWWswaStyOTJJcGw0U1lEckxXa1JiSkpRWEx5NkF1VDVyeXdvcWZGOVpnb0MvQ2ppK1VQUng2bjRlY3NucWIzWkhJTG1SSlNscitUN1E2TGVKUHVjV2w5RjdDOG1sQ3RTU3dWb0g2WWcrRGY3c0k1ZGdhQWI1RXlwQVM3T3NEcjhPcVk3L013VE1hZnRIWjUyN0VaNWtqUHVkb1Y4WkJza0NWbjE1Sk5XT1NiRVFRc2lUUkhBcmdQZnJDU0pEbysreXFKS1N1K0Z3VUpRWm9KWWUxS2k1dnYyd2MvK2JSTnVMWE93NFpac2t5OGkyZ3NxUVVmclJrU1NzV2I0YWFrV0x0eENreFVvbUwxNGFvWXIvMEpheDByMm9jbDZ2Y2JBVnlvM1JFL3lVcnlrZEhHWmV2L01xQklFbTdHK3JFelBiSExWWkprUlZoR1NmaHB3SUxPdDNxV0ZjbVpsd2F0WnlvRHdPajRKTGN0TGF6WXBMZ1dzUHdvVDhXeVptWDcyWkp0SkFNV0h6ZVlGWnJqRXdCZTI2Vno3WFMxT2xZTnBQSWJad2FvcndvbEdUWFd2OEl0RWlTZlNqQjdzdGNwMGJvbldmbDl1ZC9JeE1SUndITkM0NUhGMU8wcVBTa2hmUGx2SnFaZHFIYTlTc0t6azlkek12NEV3dXVsc3ViTHJtS3BDUStRNWNpMUZBZmhweFRqZlZKK0ZpWjh1eWg4R2NhOSt3VE0rZDArbUx5ZktmZzFacHVKc09Wak1OOERjRlNYenJmNzRPL04ya29RNEM3eTJNWGg5WkxqTnVVNjBPcEt4blJkbVpEa2taaUk5dU13Z1k0REZza1dZN2hYdGdOSnp2WDROWEl0VDhqZHgwbmVYUWVUaDdnWDBFcDUxbGdXMmU4amFJbFE5Nkc2RG15SnJMRUZFbjVyc0R3WHNTd0Z0dzkrRW9jRituc2ZqTXQwcWNFSERoT1FSYkVZTXlRQTVWNXlIdjQ1V2JaK3VZMWxMRS96YU9kWGxrVUhacW45c2tCOHEzZ1F3QXRpRWloeDRTSUFINEpKNTFodWs3QjN1ZEFVamN1VkRLM2pyOFp3ajgvU091WDkzMkhyMVNWYzVoRHZsWVFyeFFDbDZuYVlhUElkdWdTcUxicG1KNHFkNjVjN2V5ZE1scUlDZkxjeHV6NTV2M01QL014UGVkS0UySm90azRXNUlJaEpmclpBL3k4NUVJUkpRYTRjd0NVelVrbFhMNTk3dFJkYzJTSmFWaXdxNGhsWVFTZzdJbGtBK0NsWjNUOGd3dTFVN0FYd1hKaFNWbmtFRjM5T1FrdVNyUVRpNm5OUGVCU0tNR1ViRjRSWHFGVnNJUmt6SWdSODBscVBpdmJOQmE3VXN3ampjZHNQNW96MHpXMXMvMUV3Ky8rbmRLckcyU2hTSWRyK0Fta3hVeVJVOXdweVdhQy9PWWtFdTRBWDZiMG5ZZHlxajlBMW5vWTUrL28wRWZjMGtmTmVMQytabHdqNXFhVWtCT1ZONWgrWlVFUHUveGJFWjd3QWE3d2lucFZkeWt2MFBRL1Y1ZnBhQWVjMkxnQTRFWjE3ak9GMm1CU2JUNU5Dc0lqVnUyZm0yaVcra3VzL3lyTnVFZ1JZZ2R1Y3QrK2gxMm40QVpUVHBNRFBkY2s4Nk1SeGErVCtHZmpsRGxtT2o0alAzQUtUbWpWdTJmUkpBRWZEZVBjNk1tOTZLOGtvWkdwRkRpb3F3Q1NQdUJQQTF0SHh5VzB3V1poazFEQ2ZnMDJSNE9YSVlpWmVkczh1Q0lLU0xsZ3ZBdmxIMVdiTEZ1bXhPNWZKY1o3YXR5UklFbGgrbnRjbTJhV0FQbkxwSnBwRDlkbmtEeEdoM2QxQnJxUkxBSHdaZnVCV01jRE5aSHNET3RHVlZuRjhuVTZ5Mk9QczcxbnI3eThDK0JjSDEzMGN4djBzMDNVbWFzeXRYc1pLN2NsV2hPRWh6NzdQME9zR01vd2VJTmwwRGN6SmlNc2R0dmRhQUcrQjJSN2dRZ1pEdlVTeVhvaWc1SW90OHdEMlRVMk0vWmFzTHQ3enpCRTV6ZFBmSTZQamt5TXdXWmU0ZG1BSy9uN3MzTlRFR0Nmczc2UC83UTBRVlBJOGFpUFBJQzFWSmxhN1dnK0VtNHNMRDBpck4yMzlMWW1iOTZrWFJkKzRRRC8xSVVkamwySHF6eVlBZkIzQTYxWndUdTBBOER3c2R3dFh1c3pLaTFNdzlycHJQRVB6YzhnaTIydlJlaFRxbCtnMWF3bDhYaFA4Mm1uV3AwdkZKZzZGcmRrU3BFTXdIc1lFeWZBNUl0ak5KRnYza0dKMEJmME1DeVhwY1BoZXVYcllDcE1uNER1a3ZPY0VzYzhJMmROVEpDdWphMlVnVUFXbVFrT1pPbndhZnJZa1BwNHpTUDkvY21waXJDSXNRd2ozRCsrRHpzQzRramtxdWRMaUJQY1FYcXBMVnM1aG9wZWFXaG5MaThPWFEvb0dSSzdUVXhOak0yZ3M0MU05ekpPSFlCdTFmUzFOWnNCa1lRR0Fkd0g0dXdZbWNhdjRJWUFMWUJJRWNGOXlsRGludVp5dVF6eWR0SitXY0N4Y2syTE5kVXBTanJqNm0vZGlVNVppZUtVRGtyMUJDRjJwc0JRNzBGTVFGMkZYbWpBcVhMWkw1aUxmSTc0djF6ZDdHYVJzV2t0RzBxbjA5ekJNNGZYOXlQcGRTOXpBTVR1N0FUeEVKRzNMRHM1MWtLTzV0WWVJdTJkSWxnYzZZVm1GdkdpZkVJS0tFenN3Y1hwQzgrQzkwVHlxejhuT3dTOGl3TWRtZUErMUdiOTdJdUlrNDJlWXB3Rk9rU2JPazNrZi9LaGpEdUpLb2pxNm1vT2V1Sll0Rng3ZVI4L21hdDlnbXhBeWV3Skk0VlBrbmp1T0JOdjVNY3lmcHdCOEdxWWkwby9vdlJFYVgzbU9lVGFpZGRmTEZsNUNFRkNuSUM2U1hhUXh0UVhqRnJKRW1nMU91WmFVdUl5bEdHOGdLeWJwYUgzWmE4bWxzdVhTcGUydDBGeFlERmkvV2V2OUVmaHhORXlHZXl5RmN3YkEvUzIwZVIzSjZVSUhyaTEvTW5tZTF3N0JFcVRwU3JKbUljdkJVR2xVbnozMUVMOHJrU2RLSHFZbTZoQjhkL1lBUkdVZm1rQTUrcEh2OC9Qa2lRUWZuSm9ZK3pWTW5kWW5pV3phNFJLVnBmSWtYZ3ZnSEFBbnc0VGFON09uczVXRTNWMW9QYzJqN1RvZEV0cHNLNjZ0UitFbSt0eHVFNS9EcnJlWTJaTGo0Mkd6TUlHQXJ0eVlyWjZUNXBpSGFhSHd0aFAxMmo5SWZiVm9DZkFkRVJTMk9EQU1jL1F3SDFHUVo0WENuUlovN3lLaWNXV0JqcUI2bTQyTmhGU0U5U09qc2VmSndteEdQdG1lR1M2OE1vM2x0YVB6TFJoSjNDNitKaE1zLzIrSVpKT0xJMXljWENPSDJuV1RPNEprdXdtOG1GT29UcXRvbDk5THdld2g4eDV6MXJMV2VRTHZKYUd3Z3lhY1RBMVphT056YlNDQk9oZnl2d09JY0ErQWlRUmVUNHJEZHBobzc5K1NaK0ozTlFSY0d0WDVvSnUxR0FBM2UycDZKTWlkRmRkSjY3UFNnL2ZxWmh5QTZ0aUxnMGxPMk9DdExWWlk2aWwybVRweUpJajh6b0ZKOGVnQ2Z3dmdZM0MwdGFNa1d5MWMrS3dzVzZleUNvOU1GeW0xS3JaV09GVmtXWkRxdlBocHB4QUxtb1JwY1grT21HNlU3RmtybjdldTJXdUovKzNGeGVrMWl3M01KYTlCTDROaWRTanlTUWZySlczSklaY3lFQkd2eS9ONEJOWGJBaStEMlE3NFlJQXNPb3dVZHhmcmsrVU9FL0xYQUx6QlVUKzhCR1lMVENwYlRTdGVTckxWRTB3RzYyUUZxZlNoT2xuR0x2aHViYnNvQXJ0Z1NvTElDaXV3bUtPNjJCUENJcmZUWkNMRU9zMWgrZkVudmxZZjNFZDNkb0p3NUNOYlVSWmFQVTFjMFQxV3RXd0RlMndXVzJoM25NL0U4cWdRWVg0MmtrZTlIamJCZUw2T0IzQXZ2WGNzZ0YrZ09zTGNoWUxKTW9vRHFoSjAveWNkOXVNQXlUQkpyRTFidFVxeXdVUXJDeHJZV2w0cFlCSW5CTWtHVFY2K25oZVRCdHFvc0crRWhGMXJ3TFg2WFYxMGltNGdYb1dCUEt0OENwYkhhTHdkcHA3d0JwakF6MFVzZHpIYlNrSlVqMUVlL2hITXV3Q2M1T2laZmdtVG9RNnVTRllyb2xSRGFxeDJNSmFzNUpNUmhHblhtUTF5Sy9EM0UyMFdFbmJRUVQrMWIyZkVlVkd1bzVHbkErN1RiTDkza3VCTWhDZ1F6YlJSMWlWT1JyUTBGTDJsdEx1ZTR6eW5JR1JQcGNVMkp0RllVbytjSU5pakVSd0UrWGtBejRHZjdXc2RFV3lZdXpzcWlYR3lpMkdZbk9nbk9SeXppUkJTYmRvQVVFczJIazJhM2FudHRGcGRXWm10YlBaenhQV01UcE9PUnNaU0dqdXhmV1dvWjZOVElRMkpFMkR5cU5mQ3oyRktYVzRUMzI5VzZaRHk5eklBSDNiOGJJZkNuTXZsKzdRY2lLa2tXOXVTczFOSE11eWN4M0x5Qldsa3NtQjhPNFJIclUxNkdVYmY3R1R2WnBkZDFMWW5POVRLWHEyS0xicG9IT0pPT05JcDYrOWttSFBQVVVzQmZnWEEyU0hQazdhVXJDVWhXKzFuUFFZbWpleEJNVHpUZ0VXcUkvQUR1NXJxZHlYWjVoYTdKT0phaDdoVFdKN05xa0tEcUIydlVDZzZFVEpMazMxU1lZRCtQZ0ltQW5kakU5Zi9BTXplNTkySW5ncHhBOHkrN3dzQmpNZjAzQmNCdU5vNWFTakpOazIyVWJUcmhFVzBuTUdxb0NTclVDaTZRTTRGeWFubkFmaVpnK3N2d0xpYUg0UTVYOHVaOGJpczZYb0F6eURMOWJnMlBPODZvVndveVhZNCtYcDFyR0RkYTFJb0ZKMEtUaGlSZzNHWGJoZnZlekFaMVRiMjJETi9Ec0E3WWlFRkpWbW5CQnNGMnVFS2hhTFR3V2tMQWQ5OWZEU01tN2NYY1JEOHdDeW5TT3BjY2diUDBXY1VDb1ZpSlpFWEJNdUovVThFOE4wZWZkNWppV0JqcVVlcjUyVGJUN1FLaFVMUnlWaUNIL0ZiZ0hFVC93Tk1TYnBldy9Vd21hbUFtSXBQckdaTE51SDRXdkpBZDlMeDlSVUtoYUpka0RYQ2t6RDdzL2YyNEhQdWdDa0J1aForRWZpTWtteG5rcldNTms1WTd5c1VDa1czeWJSTnFFNVc4ajloanQ3MEVvNmgxd1g0eDVTYzV4elh3Q2VGUXFGUTJKQkphK1J4eFcvRG5GZnRkaHdGYzNTSWN4ZkVsdUJETFZtRlFxRlFTR1Fzd3BGWnowNEY4STB1Zjc0VGlHRFQ4SXU4eDJadEtza3FGQXFGUXFJSTMyMDZRSzlwbU9MckFQQTZBUC9VaGMrMWpaN2hQbUdoVHd2RkloWStWSkpWS0JRS2hRMU9CY3YxWDBzdzZXRFpkVHhPWk5zdHVBRW1VOVVUTUVuLzg2amVmMjI2S0x1U3JFS2hVQ2lhc1dhWlVQUEVGWHZndTFWZkRPTTJUZ0M0cDhPZjVZMEEzaVNzMW5sU0d2Z2tDQjlWaW9VVGxXUVZDb1ZDRVFUT296QUNvSTkrUHdBbVdmK1A2TzloQUM4QzhCSUFXenVzL2RmQXBILzhNaEdxckl6RVpVZ3JpRG5sclVZWEt4UUtoU0lJR1N3LzBzSlJ1TVBDSXVUUDVRQ2NBZUR2QUJ5K2d1MytCb0QzQWZoMVFOdlRNS1g1UEN5dkV6dUVHQkpTS01rcUZBcUZvaGJSNW9sOGpnYndLNHRzRWZMM2FRRE9CZkFhQUd2YTBNNnRBTDRJNEE0QVAyN2dlN0lvZThzRjJwVmtGUXFGUWhFM1pBM2FKSUNUWUlxMS96bE00UUdYeEhvVGtldURuZG9aU3JJS2hVS2hhQ2VlRGVCSUFDOEFjRHhNemRoTklSYnZVd0FlQi9BYnNxSWZoZGtQZmhKK3NveU9ocEtzUXFGUUtGeUQzY3dnTXBSN3UySFpsUTRCY0RCTUZQTWlURzdoS0h1a1NYR3ZJbUpJamFna3ExQW9GSXB1UUJwK2xPOEFUTlJ5Q2Y3eEdvU1FjWnIrTHNPUEFJN3RiS3VTckVLaFVDZzZGVnpSaGdtd0hKRU0rNGxNWnlKWXJoWExhazdCbE9qck9OSlZrbFVvRkFwRko2RWZ4bDFjc1N4Z0p1SHBickJnbFdRVkNvVkNFUWN5TUM3Z1Zza2xRVmF4Qno5YUdSYnhscXozQnVDbmdsU1NWU2dVQ3NXcVJWQUFWRkFDREp0WUVVQ3Vhc2txRkFxRlFySGFvTG1MRlFxRlFxRlFrbFVvRkFxRlFrbFdvVkFvRkFxRmtxeENvVkFvRkVxeUNvVkNvVkFveVNvVUNvVkNvVkNTVlNnVUNvVkNTVmFoVUNnVUNpVlpoVUtoVUNnVVNySUtoVUtoVUNqSktoUUtoVUtoSkt0UUtCUUtoVUxpdndIa0VSWkZ1dGw4aHdBQUFBQkpSVTVFcmtKZ2dnPT0iOyAgLyogd2hpdGUgd29yZG1hcmsgZm9yIGRhcmsgYmcgKi8KCmNvbnN0IHN0YXRlPXsKICBtb2RlOiJzaW5nbGUiLCB0aGVtZToibGlnaHQiLCBzaG93RWRpdDpmYWxzZSwgZ2FtZXM6W10sCiAgY29tcDoiVG9kYXkncyBQaWNrcyIsIHN1YjoiTWF0Y2hkYXkgMSIsCiAgc2luZ2xlOnsgdGVhbUE6e25hbWU6IkZyYW5jZSIsY29kZToiRlJBIixpbWc6dGVhbUxvZ29VUkwoIkZyYW5jZSIpfSwgdGVhbUI6e25hbWU6IkJyYXppbCIsY29kZToiQlJBIixpbWc6dGVhbUxvZ29VUkwoIkJyYXppbCIpfSwKICAgICAgICAgICBzYToiIiwgc2I6IiIsIHBpY2s6IkZyYW5jZSBvciBEcmF3IiwgcGN0OjczLCBfb3B0czpbXSB9LAogIHNsaXA6eyB0aXRsZToiQWNjdW11bGF0b3IiLCB0b3RhbDoiIiwgbGVnczpbXSB9Cn07CmNvbnN0IGVzYz1zPT5TdHJpbmcocz09bnVsbD8iIjpzKS5yZXBsYWNlKC9bJjw+Il0vZyxjPT4oeyImIjoiJmFtcDsiLCI8IjoiJmx0OyIsIj4iOiImZ3Q7IiwnIic6IiZxdW90OyJ9W2NdKSk7CmNvbnN0IGNvZGUzPXM9PihzfHwiIikudHJpbSgpLnNwbGl0KC9ccysvKVswXS5zbGljZSgwLDMpLnRvVXBwZXJDYXNlKCl8fCLigJQiOwpjb25zdCAkPWlkPT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZChpZCk7CgpmdW5jdGlvbiBtZWRhbChjb2RlLGltZyl7CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJtZWRhbCI+PGRpdiBjbGFzcz0iaW5uZXIiPiR7aW1nP2A8aW1nIHNyYz0iJHtpbWd9IiBkYXRhLWNvZGU9IiR7ZXNjKGNvZGUpfSIgb25lcnJvcj0ibG9nb0Vycih0aGlzLCcke2VzYyhjb2RlKX0nKSI+YDpgPHNwYW4gY2xhc3M9Im1vbm8iPiR7ZXNjKGNvZGUpfTwvc3Bhbj5gfTwvZGl2PjwvZGl2PmA7Cn0KCmZ1bmN0aW9uIHJlbmRlckNhcmQoKXsKICBjb25zdCBjPSQoImNhcmQiKTsgYy5jbGFzc05hbWU9ImNhcmQgIitzdGF0ZS5tb2RlOwogIGNvbnN0IGxvZ28gPSBzdGF0ZS50aGVtZT09PSJsaWdodCI/TE9HT19MSUdIVDpMT0dPX0RBUks7CiAgY29uc3QgaGVhZCA9IGA8ZGl2IGNsYXNzPSJjaGVhZCI+PGltZyBzcmM9IiR7bG9nb30iIGFsdD0iY212bmciPgogICAgICA8ZGl2IGNsYXNzPSJja2ljayI+PHNwYW4gY2xhc3M9ImNvbXAiPiR7ZXNjKHN0YXRlLmNvbXApfTwvc3Bhbj48c3BhbiBjbGFzcz0ic3ViIj4ke2VzYyhzdGF0ZS5zdWIpfTwvc3Bhbj48L2Rpdj48L2Rpdj5gOwogIGNvbnN0IGZvb3QgPSBgPGRpdiBjbGFzcz0iY2Zvb3QiPjxkaXYgY2xhc3M9ImhyIj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0iZGlzYyI+Rm9yIGVudGVydGFpbm1lbnQmbmJzcDvCtyZuYnNwO05vdCBmaW5hbmNpYWwgYWR2aWNlJm5ic3A7wrcmbmJzcDsxOCs8L2Rpdj48L2Rpdj5gOwoKICBpZihzdGF0ZS5tb2RlPT09InNpbmdsZSIpewogICAgY29uc3Qgcz1zdGF0ZS5zaW5nbGU7CiAgICBjLmlubmVySFRNTD1gPGRpdiBjbGFzcz0iYXRtb3MiPjwvZGl2PjxkaXYgY2xhc3M9InBhZCI+JHtoZWFkfQogICAgICA8ZGl2IGNsYXNzPSJjc3RhZ2UiPgogICAgICAgIDxkaXYgY2xhc3M9Im1pY3JvIj5QcmVkaWN0ZWQgRnVsbC1UaW1lPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ibWF0Y2h1cCI+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJ0ZWFtIj4ke21lZGFsKHMudGVhbUEuY29kZXx8Y29kZTMocy50ZWFtQS5uYW1lKSxzLnRlYW1BLmltZyl9PGRpdiBjbGFzcz0idG5hbWUiPiR7ZXNjKHMudGVhbUEubmFtZSl9PC9kaXY+PC9kaXY+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJzY29yZSI+PHNwYW4gY2xhc3M9InNuIj4ke2VzYyhzLnNhKX08L3NwYW4+PHNwYW4gY2xhc3M9InNlcCI+PC9zcGFuPjxzcGFuIGNsYXNzPSJzbiI+JHtlc2Mocy5zYil9PC9zcGFuPjwvZGl2PgogICAgICAgICAgPGRpdiBjbGFzcz0idGVhbSI+JHttZWRhbChzLnRlYW1CLmNvZGV8fGNvZGUzKHMudGVhbUIubmFtZSkscy50ZWFtQi5pbWcpfTxkaXYgY2xhc3M9InRuYW1lIj4ke2VzYyhzLnRlYW1CLm5hbWUpfTwvZGl2PjwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICA8L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0iY3BpY2siPjxkaXYgY2xhc3M9InBpbGwiPjxzcGFuIGNsYXNzPSJwZG90Ij48L3NwYW4+PHNwYW4gY2xhc3M9InBsYWJlbCI+JHtlc2Mocy5waWNrKX08L3NwYW4+PHNwYW4gY2xhc3M9InBiYXIiPjwvc3Bhbj48c3BhbiBjbGFzcz0icHBjdCI+JHtlc2Mocy5wY3QpfSU8L3NwYW4+PC9kaXY+PC9kaXY+CiAgICAgICR7Zm9vdH08L2Rpdj48ZGl2IGNsYXNzPSJncmFpbiI+PC9kaXY+YDsKICB9IGVsc2UgewogICAgY29uc3Qgc2w9c3RhdGUuc2xpcDsKICAgIGNvbnN0IHJvd3M9KHNsLmxlZ3MubGVuZ3RoP3NsLmxlZ3MubWFwKGw9PmA8ZGl2IGNsYXNzPSJsZWciPiR7bWVkYWwobC5jb2RlfHxjb2RlMyhsLm1hdGNoKSxsLmltZyl9CiAgICAgICAgPGRpdiBjbGFzcz0ibWlkIj48ZGl2IGNsYXNzPSJtYXRjaCI+JHtlc2MobC5tYXRjaCl9PC9kaXY+PGRpdiBjbGFzcz0ibHBpY2siPiR7ZXNjKGwucGljayl9PC9kaXY+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0icmlnaHQiPiR7bC5zY29yZT9gPHNwYW4gY2xhc3M9ImxzYyI+JHtlc2MobC5zY29yZSl9PC9zcGFuPmA6IiJ9PHNwYW4gY2xhc3M9ImxwY3QiPiR7ZXNjKGwucGN0KX0lPC9zcGFuPjwvZGl2PjwvZGl2PmApLmpvaW4oIiIpOmA8ZGl2IGNsYXNzPSJzbGlwZW1wdHkiPlBpY2sgZ2FtZXMgZnJvbSB5b3VyIGxpc3QgdG8gYnVpbGQgdGhpcyBzbGlwLjwvZGl2PmApOwogICAgY29uc3QgdG90PXNsLnRvdGFsP2A8ZGl2IGNsYXNzPSJzbGlwdG90Ij48c3BhbiBjbGFzcz0ibCI+VG90YWwgb2Rkczwvc3Bhbj48c3BhbiBjbGFzcz0iciI+JHtlc2Moc2wudG90YWwpfTwvc3Bhbj48L2Rpdj5gOiIiOwogICAgYy5pbm5lckhUTUw9YDxkaXYgY2xhc3M9ImF0bW9zIj48L2Rpdj48ZGl2IGNsYXNzPSJwYWQiPiR7aGVhZH0KICAgICAgPGRpdiBjbGFzcz0ic2xpcHRpdGxlIj48c3BhbiBjbGFzcz0ibWFpbiI+JHtlc2Moc2wudGl0bGUpfTwvc3Bhbj48c3BhbiBjbGFzcz0ibGVncyI+JHtzbC5sZWdzLmxlbmd0aH0gbGVnczwvc3Bhbj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ibGVnbGlzdCI+JHtyb3dzfTwvZGl2PiR7dG90fSR7Zm9vdH08L2Rpdj48ZGl2IGNsYXNzPSJncmFpbiI+PC9kaXY+YDsKICB9CiAgZml0KCk7Cn0KCmZ1bmN0aW9uIGZpdCgpewogIGNvbnN0IGNhcmQ9JCgiY2FyZCIpLCBzY2FsZXI9JCgic2NhbGVyIiksIHN0YWdlPWNhcmQucGFyZW50RWxlbWVudC5wYXJlbnRFbGVtZW50OwogIHNjYWxlci5zdHlsZS50cmFuc2Zvcm09InNjYWxlKDEpIjsKICBjb25zdCBhdmFpbD1zdGFnZS5jbGllbnRXaWR0aC00OwogIGNvbnN0IHM9TWF0aC5taW4oMSwgYXZhaWwvNTQwKTsKICBzY2FsZXIuc3R5bGUudHJhbnNmb3JtPWBzY2FsZSgke3N9KWA7CiAgc2NhbGVyLnN0eWxlLmhlaWdodD0oY2FyZC5vZmZzZXRIZWlnaHQqcykrInB4IjsKfQp3aW5kb3cuYWRkRXZlbnRMaXN0ZW5lcigicmVzaXplIixmaXQpOwoKLyogLS0tLS0tLS0tLS0tLS0tLSBjb250cm9scyAtLS0tLS0tLS0tLS0tLS0tICovCmZ1bmN0aW9uIGJpbmQoZWxtLGZuKXtlbG0uYWRkRXZlbnRMaXN0ZW5lcigiaW5wdXQiLGU9PntmbihlLnRhcmdldC52YWx1ZSk7cmVuZGVyQ2FyZCgpO30pO30KZnVuY3Rpb24gaW1nRmllbGQobGFiZWwsZ2V0SW1nLHNldEltZyl7CiAgY29uc3Qgd3JhcD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTt3cmFwLmNsYXNzTmFtZT0iZmxkIjsKICB3cmFwLmlubmVySFRNTD1gPGxhYmVsPiR7bGFiZWx9PC9sYWJlbD48ZGl2IGNsYXNzPSJ1cGxvYWQiPgogICAgJHtnZXRJbWcoKT9gPGltZyBjbGFzcz0idGh1bWIiIHNyYz0iJHtnZXRJbWcoKX0iIG9uZXJyb3I9InRoaXMuc3R5bGUuZGlzcGxheT0nbm9uZSciPmA6IiJ9CiAgICA8bGFiZWwgY2xhc3M9ImZha2VidG4iPiR7Z2V0SW1nKCk/IlJlcGxhY2UgaW1hZ2UiOiJVcGxvYWQgZmxhZyAvIGxvZ28ifTxpbnB1dCB0eXBlPSJmaWxlIiBhY2NlcHQ9ImltYWdlLyoiPjwvbGFiZWw+CiAgICAke2dldEltZygpP2A8YnV0dG9uIGNsYXNzPSJjbGVhciIgdGl0bGU9IlJlbW92ZSI+4pyVPC9idXR0b24+YDoiIn08L2Rpdj5gOwogIHdyYXAucXVlcnlTZWxlY3RvcigiaW5wdXRbdHlwZT1maWxlXSIpLmFkZEV2ZW50TGlzdGVuZXIoImNoYW5nZSIsZT0+ewogICAgY29uc3QgZj1lLnRhcmdldC5maWxlc1swXTsgaWYoIWYpcmV0dXJuOyBjb25zdCByPW5ldyBGaWxlUmVhZGVyKCk7CiAgICByLm9ubG9hZD0oKT0+e3NldEltZyhyLnJlc3VsdCk7cmVuZGVyQ29udHJvbHMoKTtyZW5kZXJDYXJkKCk7fTsgci5yZWFkQXNEYXRhVVJMKGYpOwogIH0pOwogIGNvbnN0IGNscj13cmFwLnF1ZXJ5U2VsZWN0b3IoIi5jbGVhciIpOyBpZihjbHIpY2xyLmFkZEV2ZW50TGlzdGVuZXIoImNsaWNrIiwoKT0+e3NldEltZygiIik7cmVuZGVyQ29udHJvbHMoKTtyZW5kZXJDYXJkKCk7fSk7CiAgcmV0dXJuIHdyYXA7Cn0KZnVuY3Rpb24gZmllbGQobGFiZWwsdmFsLGZuLHR5cGU9InRleHQiKXsKICBjb25zdCBmPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoImRpdiIpO2YuY2xhc3NOYW1lPSJmbGQiOwogIGYuaW5uZXJIVE1MPWA8bGFiZWw+JHtsYWJlbH08L2xhYmVsPjxpbnB1dCB0eXBlPSIke3R5cGV9IiB2YWx1ZT0iJHtlc2ModmFsKX0iPmA7CiAgYmluZChmLnF1ZXJ5U2VsZWN0b3IoImlucHV0IiksZm4pOyByZXR1cm4gZjsKfQoKZnVuY3Rpb24gbG9nb0VycihlbCxjb2RlKXt0cnl7ZWwucGFyZW50Tm9kZS5pbm5lckhUTUw9JzxzcGFuIGNsYXNzPSJtb25vIj4nKyhjb2RlfHwiIikrJzwvc3Bhbj4nO31jYXRjaChlKXt9fQpmdW5jdGlvbiB0ZWFtTG9nb1VSTChuYW1lKXtyZXR1cm4gIi9hcHAvdGVhbS1sb2dvP25hbWU9IitlbmNvZGVVUklDb21wb25lbnQobmFtZXx8IiIpO30KZnVuY3Rpb24gcmVuZGVyQ29tcFNlbGVjdChyb290KXsKICB2YXIgb3B0cz1bIlRvZGF5J3MgUGlja3MiLCJQcmVtaWVyIExlYWd1ZSIsIkNoYW1waW9ucyBMZWFndWUiLCJMYSBMaWdhIiwiU2VyaWUgQSIsIkJ1bmRlc2xpZ2EiLCJMaWd1ZSAxIiwiSW50ZXJuYXRpb25hbCBGcmllbmRsaWVzIiwiRkEgQ3VwIiwiV29ybGQgQ3VwIDIwMjYiXTsKICB2YXIgbGlzdD1vcHRzLnNsaWNlKCk7CiAgaWYoc3RhdGUuY29tcCAmJiBsaXN0LmluZGV4T2Yoc3RhdGUuY29tcCk8MCkgbGlzdD1bc3RhdGUuY29tcF0uY29uY2F0KGxpc3QpOwogIHZhciB3PWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoImRpdiIpO3cuY2xhc3NOYW1lPSJmbGQiOwogIHcuaW5uZXJIVE1MPSc8bGFiZWw+Q29tcGV0aXRpb248L2xhYmVsPjxzZWxlY3Q+JytsaXN0Lm1hcChmdW5jdGlvbihvKXtyZXR1cm4gJzxvcHRpb24nKyhvPT09c3RhdGUuY29tcD8nIHNlbGVjdGVkJzonJykrJz4nK2VzYyhvKSsnPC9vcHRpb24+Jzt9KS5qb2luKCcnKSsnPC9zZWxlY3Q+JzsKICB3LnF1ZXJ5U2VsZWN0b3IoInNlbGVjdCIpLmFkZEV2ZW50TGlzdGVuZXIoImNoYW5nZSIsZnVuY3Rpb24oZSl7c3RhdGUuY29tcD1lLnRhcmdldC52YWx1ZTtyZW5kZXJDYXJkKCk7fSk7CiAgcm9vdC5hcHBlbmRDaGlsZCh3KTsKfQpmdW5jdGlvbiByZW5kZXJHYW1lUGlja2VyKHJvb3QpewogIGNvbnN0IHdyYXA9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7CiAgd3JhcC5pbm5lckhUTUw9YDxkaXYgY2xhc3M9InN1YmhlYWQiPiR7c3RhdGUubW9kZT09PSJzaW5nbGUiPyJQaWNrIGEgZ2FtZSI6IkFkZCBnYW1lcyB0byBzbGlwIn08L2Rpdj5gOwogIGlmKCFzdGF0ZS5nYW1lcy5sZW5ndGgpewogICAgY29uc3Qgbj1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTtuLmNsYXNzTmFtZT0iaGludCI7bi5zdHlsZS5tYXJnaW5Ub3A9Ii00cHgiOwogICAgbi50ZXh0Q29udGVudD0iTm8gbGl2ZSBwaWNrcyBsb2FkZWQgeWV0IOKAlCB0aGUgZW5naW5lIHJ1bnMgZXZlcnkgZmV3IGhvdXJzLiBZb3UgY2FuIHN0aWxsIGJ1aWxkIG9uZSBpbiBDdXN0b21pemUgYmVsb3cuIjsKICAgIHdyYXAuYXBwZW5kQ2hpbGQobik7IHJvb3QuYXBwZW5kQ2hpbGQod3JhcCk7IHJldHVybjsKICB9CiAgY29uc3QgbGlzdD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTtsaXN0LmNsYXNzTmFtZT0iZ2FtZWxpc3QiOwogIHN0YXRlLmdhbWVzLmZvckVhY2goZz0+ewogICAgY29uc3QgbmFtZT1nLmhvbWUrIiB2ICIrZy5hd2F5OwogICAgY29uc3QgYWRkZWQ9c3RhdGUubW9kZT09PSJzbGlwIiYmc3RhdGUuc2xpcC5sZWdzLnNvbWUobD0+bC5tYXRjaD09PW5hbWUpOwogICAgY29uc3Qgcm93PWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoImJ1dHRvbiIpO3Jvdy50eXBlPSJidXR0b24iO3Jvdy5jbGFzc05hbWU9ImdhbWVyb3ciKyhhZGRlZD8iIGFkZGVkIjoiIik7CiAgICBjb25zdCBjb25mPShnLnBpY2tzJiZnLnBpY2tzWzBdKT9nLnBpY2tzWzBdLmNvbmZpZGVuY2U6IiI7CiAgICByb3cuaW5uZXJIVE1MPWA8ZGl2IGNsYXNzPSJnci1tYWluIj4ke2VzYyhnLmhvbWUpfSA8c3BhbiBjbGFzcz0iZ3ItdiI+djwvc3Bhbj4gJHtlc2MoZy5hd2F5KX08L2Rpdj5gKwogICAgICBgPGRpdiBjbGFzcz0iZ3Itc3ViIj4ke2VzYyhnLmxlYWd1ZXx8IiIpfSR7Zy5raWNrb2ZmPygiIMK3ICIrZXNjKGcua2lja29mZikpOiIifSDCtyAkeyhnLnBpY2tzfHxbXSkubGVuZ3RofSBwaWNrczwvZGl2PmA7CiAgICBjb25zdCBiPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoInNwYW4iKTtiLmNsYXNzTmFtZT0iZ3ItY29uZiI7CiAgICBiLnRleHRDb250ZW50PWFkZGVkPyLinJMgQWRkZWQiOihjb25mIT09IiI/Y29uZisiJSI6IiIpOwogICAgaWYoYi50ZXh0Q29udGVudCkgcm93LmFwcGVuZENoaWxkKGIpOwogICAgcm93LmFkZEV2ZW50TGlzdGVuZXIoImNsaWNrIiwoKT0+IHN0YXRlLm1vZGU9PT0ic2luZ2xlIj9waWNrR2FtZShnKTp0b2dnbGVMZWcoZykpOwogICAgbGlzdC5hcHBlbmRDaGlsZChyb3cpOwogIH0pOwogIHdyYXAuYXBwZW5kQ2hpbGQobGlzdCk7IHJvb3QuYXBwZW5kQ2hpbGQod3JhcCk7Cn0KZnVuY3Rpb24gdG9nZ2xlTGVnKGcpewogIGNvbnN0IG5hbWU9Zy5ob21lKyIgdiAiK2cuYXdheTsKICBjb25zdCBpZHg9c3RhdGUuc2xpcC5sZWdzLmZpbmRJbmRleChsPT5sLm1hdGNoPT09bmFtZSk7CiAgaWYoaWR4Pj0wKXsgc3RhdGUuc2xpcC5sZWdzLnNwbGljZShpZHgsMSk7IHJlbmRlckNvbnRyb2xzKCk7IHJlbmRlckNhcmQoKTsgfQogIGVsc2UgeyBhZGRMZWdGcm9tR2FtZShnKTsgfQp9CmZ1bmN0aW9uIHBpY2tHYW1lKGcpewogIGNvbnN0IHM9c3RhdGUuc2luZ2xlOwogIHMudGVhbUEubmFtZT1nLmhvbWU7IHMudGVhbUEuY29kZT1jb2RlMyhnLmhvbWUpOyBzLnRlYW1BLmltZz10ZWFtTG9nb1VSTChnLmhvbWUpOwogIHMudGVhbUIubmFtZT1nLmF3YXk7IHMudGVhbUIuY29kZT1jb2RlMyhnLmF3YXkpOyBzLnRlYW1CLmltZz10ZWFtTG9nb1VSTChnLmF3YXkpOwogIHMuc2E9IiI7IHMuc2I9IiI7CiAgaWYoZy5waWNrcyYmZy5waWNrcy5sZW5ndGgpeyBzLnBpY2s9Zy5waWNrc1swXS5waWNrOyBzLnBjdD1nLnBpY2tzWzBdLmNvbmZpZGVuY2U7IH0KICBzLl9vcHRzPWcucGlja3N8fFtdOwogIGlmKGcubGVhZ3VlKSBzdGF0ZS5jb21wPWcubGVhZ3VlOwogIGlmKGcud2hlbnx8Zy5raWNrb2ZmKSBzdGF0ZS5zdWI9Zy53aGVufHxnLmtpY2tvZmY7CiAgcmVuZGVyQ29udHJvbHMoKTsgcmVuZGVyQ2FyZCgpOwp9CmZ1bmN0aW9uIHJlbmRlckJlc3RQaWNrcyhyb290KXsKICBjb25zdCBzPXN0YXRlLnNpbmdsZTsgaWYoIXMuX29wdHN8fCFzLl9vcHRzLmxlbmd0aCkgcmV0dXJuOwogIGNvbnN0IHdyYXA9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7CiAgd3JhcC5pbm5lckhUTUw9YDxkaXYgY2xhc3M9InN1YmhlYWQiPkJlc3QgcGlja3MgZm9yIHRoaXMgZ2FtZTwvZGl2PmA7CiAgY29uc3QgY2hpcHM9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7Y2hpcHMuY2xhc3NOYW1lPSJwaWNrY2hpcHMiOwogIHMuX29wdHMuZm9yRWFjaChwPT57CiAgICBjb25zdCBjPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoImJ1dHRvbiIpO2MudHlwZT0iYnV0dG9uIjsKICAgIGMuY2xhc3NOYW1lPSJwY2hpcCIrKHAucGljaz09PXMucGljaz8iIG9uIjoiIik7CiAgICBjLmlubmVySFRNTD1gJHtlc2MocC5waWNrKX0gPGI+JHtwLmNvbmZpZGVuY2V9JTwvYj5gOwogICAgYy5hZGRFdmVudExpc3RlbmVyKCJjbGljayIsKCk9PntzLnBpY2s9cC5waWNrO3MucGN0PXAuY29uZmlkZW5jZTtyZW5kZXJDb250cm9scygpO3JlbmRlckNhcmQoKTt9KTsKICAgIGNoaXBzLmFwcGVuZENoaWxkKGMpOwogIH0pOwogIHdyYXAuYXBwZW5kQ2hpbGQoY2hpcHMpOyByb290LmFwcGVuZENoaWxkKHdyYXApOwp9CmZ1bmN0aW9uIGFkZExlZ0Zyb21HYW1lKGcpewogIGlmKHN0YXRlLnNsaXAubGVncy5sZW5ndGg+PTgpIHJldHVybjsKICBjb25zdCBuYW1lPWcuaG9tZSsiIHYgIitnLmF3YXk7CiAgaWYoc3RhdGUuc2xpcC5sZWdzLnNvbWUobD0+bC5tYXRjaD09PW5hbWUpKSByZXR1cm47CiAgY29uc3QgdG9wPShnLnBpY2tzJiZnLnBpY2tzWzBdKXx8e3BpY2s6IlBpY2siLGNvbmZpZGVuY2U6NjV9OwogIHN0YXRlLnNsaXAubGVncy5wdXNoKHttYXRjaDpuYW1lLGNvZGU6Y29kZTMoZy5ob21lKSxwaWNrOnRvcC5waWNrLHNjb3JlOiIiLHBjdDp0b3AuY29uZmlkZW5jZSxpbWc6dGVhbUxvZ29VUkwoZy5ob21lKSxfb3B0czpnLnBpY2tzfHxbXX0pOwogIHJlbmRlckNvbnRyb2xzKCk7IHJlbmRlckNhcmQoKTsKfQpmdW5jdGlvbiBsZWdQaWNrQ2hpcHMobCl7CiAgY29uc3Qgd3JhcD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTt3cmFwLnN0eWxlLm1hcmdpbj0iMnB4IDAgNnB4IjsKICBjb25zdCBjaGlwcz1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTtjaGlwcy5jbGFzc05hbWU9InBpY2tjaGlwcyI7CiAgbC5fb3B0cy5mb3JFYWNoKHA9PnsKICAgIGNvbnN0IGM9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiYnV0dG9uIik7Yy50eXBlPSJidXR0b24iOwogICAgYy5jbGFzc05hbWU9InBjaGlwIisocC5waWNrPT09bC5waWNrPyIgb24iOiIiKTsKICAgIGMuaW5uZXJIVE1MPWAke2VzYyhwLnBpY2spfSA8Yj4ke3AuY29uZmlkZW5jZX0lPC9iPmA7CiAgICBjLmFkZEV2ZW50TGlzdGVuZXIoImNsaWNrIiwoKT0+e2wucGljaz1wLnBpY2s7bC5wY3Q9cC5jb25maWRlbmNlO3JlbmRlckNvbnRyb2xzKCk7cmVuZGVyQ2FyZCgpO30pOwogICAgY2hpcHMuYXBwZW5kQ2hpbGQoYyk7CiAgfSk7CiAgd3JhcC5hcHBlbmRDaGlsZChjaGlwcyk7IHJldHVybiB3cmFwOwp9CmFzeW5jIGZ1bmN0aW9uIGxvYWRHYW1lcygpewogIHRyeXsKICAgIGNvbnN0IHI9YXdhaXQgZmV0Y2goIi9hcHAvY2FyZHMtZGF0YSIse2NhY2hlOiJuby1zdG9yZSJ9KTsKICAgIGNvbnN0IGQ9YXdhaXQgci5qc29uKCk7CiAgICBzdGF0ZS5nYW1lcz0oZCYmQXJyYXkuaXNBcnJheShkLmdhbWVzKSk/ZC5nYW1lczpbXTsKICAgIGlmKGQmJmQuZGF0ZSkgc3RhdGUuc3ViPWQuZGF0ZTsKICB9Y2F0Y2goZSl7IHN0YXRlLmdhbWVzPXdpbmRvdy5fX1NBTVBMRV9HQU1FU3x8W107IH0KICBpZighc3RhdGUuZ2FtZXMubGVuZ3RoJiZ3aW5kb3cuX19TQU1QTEVfR0FNRVMpIHN0YXRlLmdhbWVzPXdpbmRvdy5fX1NBTVBMRV9HQU1FUzsKICByZW5kZXJDb250cm9scygpOyByZW5kZXJDYXJkKCk7Cn0KZnVuY3Rpb24gZWRpdFRvZ2dsZSgpewogIGNvbnN0IGI9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiYnV0dG9uIik7Yi5jbGFzc05hbWU9ImVkaXRidG4iOwogIGIudGV4dENvbnRlbnQ9c3RhdGUuc2hvd0VkaXQ/IuKWviAgSGlkZSBkZXRhaWxzIjoi4pa4ICBDdXN0b21pemUgY2FyZCI7CiAgYi5hZGRFdmVudExpc3RlbmVyKCJjbGljayIsKCk9PntzdGF0ZS5zaG93RWRpdD0hc3RhdGUuc2hvd0VkaXQ7cmVuZGVyQ29udHJvbHMoKTt9KTsKICByZXR1cm4gYjsKfQpmdW5jdGlvbiByZW5kZXJDb250cm9scygpewogIGNvbnN0IHJvb3Q9JCgiY29udHJvbHMiKTsgcm9vdC5pbm5lckhUTUw9IiI7CiAgY29uc3QgaD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTsKICBoLmlubmVySFRNTD1gPGgyPiR7c3RhdGUubW9kZT09PSJzaW5nbGUiPyJNYWtlIGEgc2hhcmUgY2FyZCI6IkJ1aWxkIGFuIGFjY3VtdWxhdG9yIGNhcmQifTwvaDI+CiAgICA8ZGl2IGNsYXNzPSJoaW50Ij4ke3N0YXRlLmdhbWVzLmxlbmd0aD8iVGFwIGEgZ2FtZSBmcm9tIHlvdXIgbGl2ZSBwaWNrcyDigJQgdGhlIGNhcmQgZmlsbHMgaW4gd2l0aCBjcmVzdCwgcGljayBhbmQgY29uZmlkZW5jZSBhdXRvbWF0aWNhbGx5LiI6Ik5vIHBpY2tzIGxvYWRlZCB5ZXQuIEJ1aWxkIG9uZSBtYW51YWxseSBpbiBDdXN0b21pemUgYmVsb3cuIn08L2Rpdj5gOwogIHJvb3QuYXBwZW5kQ2hpbGQoaCk7CiAgcmVuZGVyQ29tcFNlbGVjdChyb290KTsKICByZW5kZXJHYW1lUGlja2VyKHJvb3QpOwoKICBpZihzdGF0ZS5tb2RlPT09InNpbmdsZSIpewogICAgY29uc3Qgcz1zdGF0ZS5zaW5nbGU7CiAgICByZW5kZXJCZXN0UGlja3Mocm9vdCk7CiAgICBpZihzLl9vcHRzJiZzLl9vcHRzLmxlbmd0aCl7CiAgICAgIGNvbnN0IHNtPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoImRpdiIpO3NtLmNsYXNzTmFtZT0ic2Vsbm90ZSI7CiAgICAgIHNtLmlubmVySFRNTD1gT24gdGhlIGNhcmQ6IDxiPiR7ZXNjKHMudGVhbUEubmFtZSl9IHYgJHtlc2Mocy50ZWFtQi5uYW1lKX08L2I+IOKAlCAke2VzYyhzLnBpY2spfSDCtyAke2VzYyhzLnBjdCl9JWA7CiAgICAgIHJvb3QuYXBwZW5kQ2hpbGQoc20pOwogICAgfQogICAgcm9vdC5hcHBlbmRDaGlsZChlZGl0VG9nZ2xlKCkpOwogICAgaWYoc3RhdGUuc2hvd0VkaXQpewogICAgICBjb25zdCB3PWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoImRpdiIpO3cuY2xhc3NOYW1lPSJlZGl0d3JhcCI7CiAgICAgIGNvbnN0IHI9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7ci5jbGFzc05hbWU9InJvdzIiOwogICAgICByLmFwcGVuZENoaWxkKGZpZWxkKCJDb21wZXRpdGlvbiIsc3RhdGUuY29tcCx2PT5zdGF0ZS5jb21wPXYpKTsKICAgICAgci5hcHBlbmRDaGlsZChmaWVsZCgiU3VibGluZSIsc3RhdGUuc3ViLHY9PnN0YXRlLnN1Yj12KSk7CiAgICAgIHcuYXBwZW5kQ2hpbGQocik7CiAgICAgIHcuYXBwZW5kQ2hpbGQoc3ViKCJIb21lIikpOwogICAgICBjb25zdCByYT1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTtyYS5jbGFzc05hbWU9InJvdzIiOwogICAgICByYS5hcHBlbmRDaGlsZChmaWVsZCgiVGVhbSBuYW1lIixzLnRlYW1BLm5hbWUsdj0+e3MudGVhbUEubmFtZT12O30pKTsKICAgICAgcmEuYXBwZW5kQ2hpbGQoZmllbGQoIk1vbm9ncmFtIixzLnRlYW1BLmNvZGUsdj0+cy50ZWFtQS5jb2RlPXYudG9VcHBlckNhc2UoKSkpOwogICAgICB3LmFwcGVuZENoaWxkKHJhKTsKICAgICAgdy5hcHBlbmRDaGlsZChpbWdGaWVsZCgiQ3Jlc3QgLyBmbGFnIChvdmVycmlkZSkiLCgpPT5zLnRlYW1BLmltZyx2PT5zLnRlYW1BLmltZz12KSk7CiAgICAgIHcuYXBwZW5kQ2hpbGQoc3ViKCJBd2F5IikpOwogICAgICBjb25zdCByYj1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTtyYi5jbGFzc05hbWU9InJvdzIiOwogICAgICByYi5hcHBlbmRDaGlsZChmaWVsZCgiVGVhbSBuYW1lIixzLnRlYW1CLm5hbWUsdj0+e3MudGVhbUIubmFtZT12O30pKTsKICAgICAgcmIuYXBwZW5kQ2hpbGQoZmllbGQoIk1vbm9ncmFtIixzLnRlYW1CLmNvZGUsdj0+cy50ZWFtQi5jb2RlPXYudG9VcHBlckNhc2UoKSkpOwogICAgICB3LmFwcGVuZENoaWxkKHJiKTsKICAgICAgdy5hcHBlbmRDaGlsZChpbWdGaWVsZCgiQ3Jlc3QgLyBmbGFnIChvdmVycmlkZSkiLCgpPT5zLnRlYW1CLmltZyx2PT5zLnRlYW1CLmltZz12KSk7CiAgICAgIHcuYXBwZW5kQ2hpbGQoc3ViKCJQcmVkaWN0aW9uIikpOwogICAgICBjb25zdCBycz1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTtycy5jbGFzc05hbWU9InJvdy1zYiI7CiAgICAgIHJzLmFwcGVuZENoaWxkKGZpZWxkKCJQaWNrIixzLnBpY2ssdj0+cy5waWNrPXYpKTsKICAgICAgcnMuYXBwZW5kQ2hpbGQoZmllbGQoIlNjb3JlIixzLnNhLHY9PnMuc2E9dikpOwogICAgICBycy5hcHBlbmRDaGlsZChmaWVsZCgiU2NvcmUiLHMuc2Isdj0+cy5zYj12KSk7CiAgICAgIHcuYXBwZW5kQ2hpbGQocnMpOwogICAgICB3LmFwcGVuZENoaWxkKHJhbmdlRmllbGQoIkNvbmZpZGVuY2UiLHMucGN0LHY9PntzLnBjdD0rdjt9KSk7CiAgICAgIHJvb3QuYXBwZW5kQ2hpbGQodyk7CiAgICB9CiAgfSBlbHNlIHsKICAgIGNvbnN0IHNsPXN0YXRlLnNsaXA7CiAgICBpZihzbC5sZWdzLmxlbmd0aCl7CiAgICAgIHJvb3QuYXBwZW5kQ2hpbGQoc3ViKHNsLmxlZ3MubGVuZ3RoKyIgcGlja3Mgb24gdGhpcyBzbGlwIikpOwogICAgICBzbC5sZWdzLmZvckVhY2goKGwsaSk9PnsKICAgICAgICBjb25zdCByb3c9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7cm93LmNsYXNzTmFtZT0ibGVnbWluaSI7CiAgICAgICAgcm93LmlubmVySFRNTD1gPGRpdiBjbGFzcz0ibG0tbWFpbiI+PGI+JHtlc2MobC5tYXRjaCl9PC9iPjxzcGFuPiR7ZXNjKGwucGljayl9IMK3ICR7ZXNjKGwucGN0KX0lPC9zcGFuPjwvZGl2PmA7CiAgICAgICAgY29uc3Qgcm09ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiYnV0dG9uIik7cm0uY2xhc3NOYW1lPSJsbS1ybSI7cm0udGl0bGU9IlJlbW92ZSI7cm0udGV4dENvbnRlbnQ9IuKclSI7CiAgICAgICAgcm0uYWRkRXZlbnRMaXN0ZW5lcigiY2xpY2siLCgpPT57c2wubGVncy5zcGxpY2UoaSwxKTtyZW5kZXJDb250cm9scygpO3JlbmRlckNhcmQoKTt9KTsKICAgICAgICByb3cuYXBwZW5kQ2hpbGQocm0pOyByb290LmFwcGVuZENoaWxkKHJvdyk7CiAgICAgICAgaWYobC5fb3B0cyYmbC5fb3B0cy5sZW5ndGgpIHJvb3QuYXBwZW5kQ2hpbGQobGVnUGlja0NoaXBzKGwpKTsKICAgICAgfSk7CiAgICB9CiAgICByb290LmFwcGVuZENoaWxkKGVkaXRUb2dnbGUoKSk7CiAgICBpZihzdGF0ZS5zaG93RWRpdCl7CiAgICAgIGNvbnN0IHc9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7dy5jbGFzc05hbWU9ImVkaXR3cmFwIjsKICAgICAgY29uc3QgcjI9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7cjIuY2xhc3NOYW1lPSJyb3cyIjsKICAgICAgcjIuYXBwZW5kQ2hpbGQoZmllbGQoIlNsaXAgdGl0bGUiLHNsLnRpdGxlLHY9PnNsLnRpdGxlPXYpKTsKICAgICAgcjIuYXBwZW5kQ2hpbGQoZmllbGQoIlRvdGFsIG9kZHMgKG9wdGlvbmFsKSIsc2wudG90YWwsdj0+c2wudG90YWw9dikpOwogICAgICB3LmFwcGVuZENoaWxkKHIyKTsKICAgICAgc2wubGVncy5mb3JFYWNoKChsLGkpPT57CiAgICAgICAgY29uc3QgY2FyZD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTtjYXJkLmNsYXNzTmFtZT0ibGVnY2FyZCI7CiAgICAgICAgY2FyZC5pbm5lckhUTUw9YDxkaXYgY2xhc3M9ImxoZWFkIj48Yj5MZWcgJHtpKzF9PC9iPjwvZGl2PmA7CiAgICAgICAgY2FyZC5hcHBlbmRDaGlsZChmaWVsZCgiTWF0Y2giLGwubWF0Y2gsdj0+bC5tYXRjaD12KSk7CiAgICAgICAgY29uc3QgcnI9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7cnIuY2xhc3NOYW1lPSJyb3ctc2IiOwogICAgICAgIHJyLmFwcGVuZENoaWxkKGZpZWxkKCJQaWNrIixsLnBpY2ssdj0+bC5waWNrPXYpKTsKICAgICAgICByci5hcHBlbmRDaGlsZChmaWVsZCgiU2NvcmUiLGwuc2NvcmUsdj0+bC5zY29yZT12KSk7CiAgICAgICAgcnIuYXBwZW5kQ2hpbGQoZmllbGQoIk1vbm8iLGwuY29kZSx2PT5sLmNvZGU9di50b1VwcGVyQ2FzZSgpKSk7CiAgICAgICAgY2FyZC5hcHBlbmRDaGlsZChycik7CiAgICAgICAgY2FyZC5hcHBlbmRDaGlsZChyYW5nZUZpZWxkKCJDb25maWRlbmNlIixsLnBjdCx2PT57bC5wY3Q9K3Y7fSkpOwogICAgICAgIGNhcmQuYXBwZW5kQ2hpbGQoaW1nRmllbGQoIkNyZXN0IC8gZmxhZyAob3ZlcnJpZGUpIiwoKT0+bC5pbWcsdj0+bC5pbWc9dikpOwogICAgICAgIHcuYXBwZW5kQ2hpbGQoY2FyZCk7CiAgICAgIH0pOwogICAgICByb290LmFwcGVuZENoaWxkKHcpOwogICAgfQogICAgaWYoIXN0YXRlLmdhbWVzLmxlbmd0aCl7CiAgICAgIGNvbnN0IGFkZD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJidXR0b24iKTthZGQuY2xhc3NOYW1lPSJhZGRsZWciO2FkZC50ZXh0Q29udGVudD0iKyBBZGQgbGVnIG1hbnVhbGx5IjsKICAgICAgYWRkLmRpc2FibGVkPXNsLmxlZ3MubGVuZ3RoPj04OwogICAgICBhZGQuYWRkRXZlbnRMaXN0ZW5lcigiY2xpY2siLCgpPT57IGlmKHNsLmxlZ3MubGVuZ3RoPj04KXJldHVybjsKICAgICAgICBzbC5sZWdzLnB1c2goe21hdGNoOiJUZWFtIEEgdiBUZWFtIEIiLGNvZGU6IiIscGljazoiUGljayIsc2NvcmU6IiIscGN0OjY1LGltZzoiIixfb3B0czpbXX0pOwogICAgICAgIHJlbmRlckNvbnRyb2xzKCk7cmVuZGVyQ2FyZCgpO30pOwogICAgICByb290LmFwcGVuZENoaWxkKGFkZCk7CiAgICB9CiAgfQp9CgpmdW5jdGlvbiBzdWIodCl7Y29uc3QgZD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTtkLmNsYXNzTmFtZT0ic3ViaGVhZCI7ZC50ZXh0Q29udGVudD10O3JldHVybiBkO30KZnVuY3Rpb24gcmFuZ2VGaWVsZChsYWJlbCx2YWwsZm4pewogIGNvbnN0IGY9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7Zi5jbGFzc05hbWU9ImZsZCI7CiAgZi5pbm5lckhUTUw9YDxsYWJlbD4ke2xhYmVsfTwvbGFiZWw+PGRpdiBjbGFzcz0icm5nIj48aW5wdXQgdHlwZT0icmFuZ2UiIG1pbj0iNDAiIG1heD0iOTkiIHZhbHVlPSIke3ZhbH0iPjxzcGFuIGNsYXNzPSJ2YWwiPiR7dmFsfSU8L3NwYW4+PC9kaXY+YDsKICBjb25zdCBpbnA9Zi5xdWVyeVNlbGVjdG9yKCJpbnB1dCIpLGxhYj1mLnF1ZXJ5U2VsZWN0b3IoIi52YWwiKTsKICBpbnAuYWRkRXZlbnRMaXN0ZW5lcigiaW5wdXQiLGU9PntsYWIudGV4dENvbnRlbnQ9ZS50YXJnZXQudmFsdWUrIiUiO2ZuKGUudGFyZ2V0LnZhbHVlKTtyZW5kZXJDYXJkKCk7fSk7CiAgcmV0dXJuIGY7Cn0KCi8qIC0tLS0tLS0tLS0tLS0tLS0gYmFyIGFjdGlvbnMgLS0tLS0tLS0tLS0tLS0tLSAqLwokKCJtb2Rlc2VnIikucXVlcnlTZWxlY3RvckFsbCgiYnV0dG9uIikuZm9yRWFjaChiPT5iLmFkZEV2ZW50TGlzdGVuZXIoImNsaWNrIiwoKT0+ewogIHN0YXRlLm1vZGU9Yi5kYXRhc2V0Lm1vZGU7CiAgJCgibW9kZXNlZyIpLnF1ZXJ5U2VsZWN0b3JBbGwoImJ1dHRvbiIpLmZvckVhY2goeD0+eC5jbGFzc0xpc3QudG9nZ2xlKCJvbiIseD09PWIpKTsKICAkKCJzdGFnZWxhYmVsIikudGV4dENvbnRlbnQ9c3RhdGUubW9kZT09PSJzaW5nbGUiPyJMaXZlIHByZXZpZXcg4oCUIHNpbmdsZSBtYXRjaCI6IkxpdmUgcHJldmlldyDigJQgYWNjdW11bGF0b3IiOwogIHJlbmRlckNvbnRyb2xzKCk7cmVuZGVyQ2FyZCgpOwp9KSk7CiQoInRoZW1lYnRuIikuYWRkRXZlbnRMaXN0ZW5lcigiY2xpY2siLCgpPT57CiAgc3RhdGUudGhlbWU9c3RhdGUudGhlbWU9PT0ibGlnaHQiPyJkYXJrIjoibGlnaHQiOwogIGRvY3VtZW50LmRvY3VtZW50RWxlbWVudC5zZXRBdHRyaWJ1dGUoImRhdGEtdGhlbWUiLHN0YXRlLnRoZW1lKTtsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgiY212bmctdGhlbWUiLHN0YXRlLnRoZW1lKTsKICAkKCJ0aGVtZWJ0biIpLnRleHRDb250ZW50PXN0YXRlLnRoZW1lPT09ImxpZ2h0Ij8i8J+MmSI6IuKYgO+4jyI7CiAgc2V0TG9nb3MoKTtyZW5kZXJDYXJkKCk7Cn0pOwpmdW5jdGlvbiBzZXRMb2dvcygpeyAkKCJiYXJsb2dvIikuc3JjID0gc3RhdGUudGhlbWU9PT0ibGlnaHQiP0xPR09fTElHSFQ6TE9HT19EQVJLOyB9CgokKCJkbGJ0biIpLmFkZEV2ZW50TGlzdGVuZXIoImNsaWNrIixhc3luYygpPT57CiAgY29uc3QgYnRuPSQoImRsYnRuIiksY2FyZD0kKCJjYXJkIiksc2NhbGVyPSQoInNjYWxlciIpOwogIGJ0bi50ZXh0Q29udGVudD0iUmVuZGVyaW5n4oCmIjsgYnRuLmRpc2FibGVkPXRydWU7CiAgY29uc3QgcHJldj1zY2FsZXIuc3R5bGUudHJhbnNmb3JtOyBzY2FsZXIuc3R5bGUudHJhbnNmb3JtPSJzY2FsZSgxKSI7CiAgZnVuY3Rpb24gY292ZXJGaXgoZG9jKXt0cnl7dmFyIHQ9ZG9jdW1lbnQuZG9jdW1lbnRFbGVtZW50LmdldEF0dHJpYnV0ZSgnZGF0YS10aGVtZScpfHwnbGlnaHQnO2RvYy5kb2N1bWVudEVsZW1lbnQuc2V0QXR0cmlidXRlKCdkYXRhLXRoZW1lJyx0KTt2YXIgcz13aW5kb3cuZ2V0Q29tcHV0ZWRTdHlsZShkb2N1bWVudC5kb2N1bWVudEVsZW1lbnQpO3ZhciBpbmw9Jyc7Zm9yKHZhciBrPTA7azxzLmxlbmd0aDtrKyspe3ZhciBuPXNba107aWYobi5jaGFyQ29kZUF0KDApPT09NDUmJm4uY2hhckNvZGVBdCgxKT09PTQ1KXt2YXIgdj1zLmdldFByb3BlcnR5VmFsdWUobik7aWYodilpbmwrPW4rJzonK3YudHJpbSgpKyc7Jzt9fWRvYy5kb2N1bWVudEVsZW1lbnQuc3R5bGUuY3NzVGV4dCs9aW5sO3RyeXt2YXIgYmI9d2luZG93LmdldENvbXB1dGVkU3R5bGUoZG9jdW1lbnQuYm9keSk7ZG9jLmJvZHkuc3R5bGUuYmFja2dyb3VuZD1iYi5iYWNrZ3JvdW5kfHxiYi5iYWNrZ3JvdW5kQ29sb3I7fWNhdGNoKF8pe310cnl7dmFyIGxjPWRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoJyNjYXJkJyl8fGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoJy5jYXJkJyk7dmFyIGNjPWRvYy5xdWVyeVNlbGVjdG9yKCcjY2FyZCcpfHxkb2MucXVlcnlTZWxlY3RvcignLmNhcmQnKTtpZihsYyYmY2Mpe3ZhciBjcz13aW5kb3cuZ2V0Q29tcHV0ZWRTdHlsZShsYyk7aWYoY3MuYmFja2dyb3VuZCljYy5zdHlsZS5iYWNrZ3JvdW5kPWNzLmJhY2tncm91bmQ7ZWxzZSBpZihjcy5iYWNrZ3JvdW5kQ29sb3IpY2Muc3R5bGUuYmFja2dyb3VuZENvbG9yPWNzLmJhY2tncm91bmRDb2xvcjtpZihjcy5iYWNrZ3JvdW5kSW1hZ2UmJmNzLmJhY2tncm91bmRJbWFnZSE9PSdub25lJyljYy5zdHlsZS5iYWNrZ3JvdW5kSW1hZ2U9Y3MuYmFja2dyb3VuZEltYWdlO319Y2F0Y2goXyl7fX1jYXRjaChlKXt9dmFyIGc9ZG9jLnF1ZXJ5U2VsZWN0b3IoJy5ncmFpbicpO2lmKGcpZy5zdHlsZS5kaXNwbGF5PSdub25lJzt2YXIgTD1kb2N1bWVudC5xdWVyeVNlbGVjdG9yQWxsKCcjY2FyZCAubWVkYWwgaW1nJyksQz1kb2MucXVlcnlTZWxlY3RvckFsbCgnI2NhcmQgLm1lZGFsIGltZycpO2Zvcih2YXIgaT0wO2k8Qy5sZW5ndGg7aSsrKXt2YXIgbHY9TFtpXTtpZighbHYpY29udGludWU7dmFyIG53PWx2Lm5hdHVyYWxXaWR0aCxuaD1sdi5uYXR1cmFsSGVpZ2h0LHI9bHYuZ2V0Qm91bmRpbmdDbGllbnRSZWN0KCksY3c9ci53aWR0aCxjaD1yLmhlaWdodDtpZihudz4wJiZuaD4wJiZjdz4wJiZjaD4wKXt2YXIgc2M9TWF0aC5tYXgoY3cvbncsY2gvbmgpO0NbaV0uc3R5bGUub2JqZWN0Rml0PSdmaWxsJztDW2ldLnN0eWxlLmZsZXhTaHJpbms9JzAnO0NbaV0uc3R5bGUud2lkdGg9TWF0aC5yb3VuZChudypzYykrJ3B4JztDW2ldLnN0eWxlLmhlaWdodD1NYXRoLnJvdW5kKG5oKnNjKSsncHgnO0NbaV0uc3R5bGUubWF4V2lkdGg9J25vbmUnO0NbaV0uc3R5bGUubWF4SGVpZ2h0PSdub25lJzt9fX0KICBmdW5jdGlvbiBzaG9vdChzYWZlKXtyZXR1cm4gaHRtbDJjYW52YXMoY2FyZCx7c2NhbGU6NCxiYWNrZ3JvdW5kQ29sb3I6bnVsbCx1c2VDT1JTOnRydWUsbG9nZ2luZzpmYWxzZSxpbWFnZVRpbWVvdXQ6NjAwMCxyZW1vdmVDb250YWluZXI6dHJ1ZSxvbmNsb25lOmZ1bmN0aW9uKGRvYyl7Y292ZXJGaXgoZG9jKTtpZihzYWZlKXt2YXIgc3Q9ZG9jLmNyZWF0ZUVsZW1lbnQoInN0eWxlIik7c3QudGV4dENvbnRlbnQ9Ii5hdG1vc3tiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCgxNjJkZWcsdmFyKC0tYzEpLHZhcigtLWMyKSkhaW1wb3J0YW50fS5tZWRhbDo6YWZ0ZXJ7ZGlzcGxheTpub25lIWltcG9ydGFudH0uaHJ7YmFja2dyb3VuZDp2YXIoLS1jLWxpbmUpIWltcG9ydGFudH0iO2RvYy5oZWFkLmFwcGVuZENoaWxkKHN0KTt9fX0pO30KICB0cnl7CiAgICBhd2FpdCBkb2N1bWVudC5mb250cy5yZWFkeTsKICAgIGF3YWl0IFByb21pc2UuYWxsKFtdLnNsaWNlLmNhbGwoY2FyZC5xdWVyeVNlbGVjdG9yQWxsKCJpbWciKSkubWFwKGZ1bmN0aW9uKGltKXtpZihpbS5jb21wbGV0ZSYmaW0ubmF0dXJhbFdpZHRoPjApcmV0dXJuIFByb21pc2UucmVzb2x2ZSgpO3JldHVybiBuZXcgUHJvbWlzZShmdW5jdGlvbihyZXMpe2ltLmFkZEV2ZW50TGlzdGVuZXIoImxvYWQiLHJlcyx7b25jZTp0cnVlfSk7aW0uYWRkRXZlbnRMaXN0ZW5lcigiZXJyb3IiLGZ1bmN0aW9uKCl7dHJ5e2ltLnBhcmVudE5vZGUuaW5uZXJIVE1MPSc8c3BhbiBjbGFzcz0ibW9ubyI+JysoaW0uZ2V0QXR0cmlidXRlKCJkYXRhLWNvZGUiKXx8IiIpKyc8L3NwYW4+Jzt9Y2F0Y2goZSl7fXJlcygpO30se29uY2U6dHJ1ZX0pO3NldFRpbWVvdXQocmVzLDMwMDApO30pO30pKTsKICAgIHZhciBjYW52YXM7IHRyeXsgY2FudmFzPWF3YWl0IHNob290KGZhbHNlKTsgfWNhdGNoKGUxKXsgY29uc29sZS53YXJuKCJmdWxsIHJlbmRlciBmYWlsZWQsIHJldHJ5aW5nIHNhZmU6IixlMSYmZTEubWVzc2FnZSk7IGNhbnZhcz1hd2FpdCBzaG9vdCh0cnVlKTsgfQogICAgY29uc3QgYT1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJhIik7CiAgICBhLmRvd25sb2FkPWBjbXZuZ18ke3N0YXRlLm1vZGV9X2NhcmQucG5nYDsgYS5ocmVmPWNhbnZhcy50b0RhdGFVUkwoImltYWdlL3BuZyIpOyBhLmNsaWNrKCk7CiAgfWNhdGNoKGUpeyBhbGVydCgiRXhwb3J0IGZhaWxlZDogIitlLm1lc3NhZ2UpOyB9CiAgc2NhbGVyLnN0eWxlLnRyYW5zZm9ybT1wcmV2OyBidG4udGV4dENvbnRlbnQ9IuKkkyBEb3dubG9hZCBQTkciOyBidG4uZGlzYWJsZWQ9ZmFsc2U7Cn0pOwoKLyogaW5pdCAqLwooZnVuY3Rpb24oKXt2YXIgX3Q9bG9jYWxTdG9yYWdlLmdldEl0ZW0oImNtdm5nLXRoZW1lIik7aWYoX3Qpe3N0YXRlLnRoZW1lPV90O2RvY3VtZW50LmRvY3VtZW50RWxlbWVudC5zZXRBdHRyaWJ1dGUoImRhdGEtdGhlbWUiLF90KTt9dmFyIF9iPWRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCJ0aGVtZWJ0biIpO2lmKF9iKV9iLnRleHRDb250ZW50PXN0YXRlLnRoZW1lPT09ImRhcmsiPyLimIDvuI8iOiLwn4yZIjt9KSgpO3NldExvZ29zKCk7cmVuZGVyQ29udHJvbHMoKTtsb2FkR2FtZXMoKTsKZG9jdW1lbnQuZm9udHMucmVhZHkudGhlbihyZW5kZXJDYXJkKTsgcmVuZGVyQ2FyZCgpOwo8L3NjcmlwdD4KPGRpdiBjbGFzcz0iY2ItdGFiYmFyIj48YSBocmVmPSIvIj48c3BhbiBjbGFzcz0iaWMiPjxzdmcgd2lkdGg9IjIyIiBoZWlnaHQ9IjIyIiB2aWV3Qm94PSIwIDAgMjQgMjQiIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2Utd2lkdGg9IjIiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIgc3R5bGU9ImRpc3BsYXk6YmxvY2siPjxwYXRoIGQ9Ik0zIDEwLjQgMTIgM2w5IDcuNCIvPjxwYXRoIGQ9Ik01LjUgOS4yVjIwYTEgMSAwIDAgMCAxIDFoMTFhMSAxIDAgMCAwIDEtMVY5LjIiLz48cGF0aCBkPSJNOS41IDIxdi01LjVhMSAxIDAgMCAxIDEtMWgzYTEgMSAwIDAgMSAxIDFWMjEiLz48L3N2Zz48L3NwYW4+PHNwYW4gY2xhc3M9InRsIj5Ib21lPC9zcGFuPjwvYT48YSBocmVmPSIvYXBwL3BpY2tzIj48c3BhbiBjbGFzcz0iaWMiPjxzdmcgd2lkdGg9IjIyIiBoZWlnaHQ9IjIyIiB2aWV3Qm94PSIwIDAgMjQgMjQiIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2Utd2lkdGg9IjIiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIgc3R5bGU9ImRpc3BsYXk6YmxvY2siPjxjaXJjbGUgY3g9IjEyIiBjeT0iMTIiIHI9IjkiLz48cGF0aCBkPSJtMTIgNy4xIDMuMyAyLjQtMS4yNiAzLjlIOS45Nkw4LjcgOS41eiIvPjxwYXRoIGQ9Ik0xMiAzdjQuMU01LjEgOWwyLjg1IDIuMjVNMTguOSA5bC0yLjg1IDIuMjVNOC40NSAyMC4xIDkuOTYgMTMuNE0xNS41NSAyMC4xIDE0LjA0IDEzLjQiLz48L3N2Zz48L3NwYW4+PHNwYW4gY2xhc3M9InRsIj5QaWNrczwvc3Bhbj48L2E+PGEgaHJlZj0iL2FwcC9jb2RlcyI+PHNwYW4gY2xhc3M9ImljIj48c3ZnIHdpZHRoPSIyMiIgaGVpZ2h0PSIyMiIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9ImN1cnJlbnRDb2xvciIgc3Ryb2tlLXdpZHRoPSIyIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0eWxlPSJkaXNwbGF5OmJsb2NrIj48cGF0aCBkPSJNMy41IDguNkExLjYgMS42IDAgMCAwIDUgN1Y2LjJBMS4yIDEuMiAwIDAgMSA2LjIgNWgxMS42QTEuMiAxLjIgMCAwIDEgMTkgNi4yVjdhMS42IDEuNiAwIDAgMCAxLjUgMS42djJBMS42IDEuNiAwIDAgMCAxOSAxMi4ydjUuNmExLjIgMS4yIDAgMCAxLTEuMiAxLjJINi4yQTEuMiAxLjIgMCAwIDEgNSAxNy44di01LjZBMS42IDEuNiAwIDAgMCAzLjUgMTAuNnoiLz48cGF0aCBkPSJNMTIgNy41djEuNk0xMiAxMS4ydjEuNk0xMiAxNC45djEuNiIvPjwvc3ZnPjwvc3Bhbj48c3BhbiBjbGFzcz0idGwiPkNvZGVzPC9zcGFuPjwvYT48YSBocmVmPSIvYXBwL2NhcmRzIiBjbGFzcz0iYWN0aXZlIj48c3BhbiBjbGFzcz0iaWMiPjxzdmcgd2lkdGg9IjIyIiBoZWlnaHQ9IjIyIiB2aWV3Qm94PSIwIDAgMjQgMjQiIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2Utd2lkdGg9IjIiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIgc3R5bGU9ImRpc3BsYXk6YmxvY2siPjxyZWN0IHg9IjMiIHk9IjQuNSIgd2lkdGg9IjE4IiBoZWlnaHQ9IjE1IiByeD0iMi41Ii8+PGNpcmNsZSBjeD0iOC41IiBjeT0iOS41IiByPSIxLjYiLz48cGF0aCBkPSJtMy44IDE3LjUgNC40LTQuM2EyIDIgMCAwIDEgMi44IDBsNS4yIDUuMSIvPjxwYXRoIGQ9Im0xMy41IDE0IDItMmEyIDIgMCAwIDEgMi44IDBsMiAyIi8+PC9zdmc+PC9zcGFuPjxzcGFuIGNsYXNzPSJ0bCI+Q2FyZHM8L3NwYW4+PC9hPjxhIGhyZWY9Ii9hcHAvcGFwZXItcG9seSI+PHNwYW4gY2xhc3M9ImljIj48c3ZnIHdpZHRoPSIyMiIgaGVpZ2h0"
    "PSIyMiIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9ImN1cnJlbnRDb2xvciIgc3Ryb2tlLXdpZHRoPSIyIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0eWxlPSJkaXNwbGF5OmJsb2NrIj48Y2lyY2xlIGN4PSIxMiIgY3k9IjEyIiByPSI5Ii8+PHBhdGggZD0iTTEyIDYuNHYxMS4yIi8+PHBhdGggZD0iTTE0LjkgOWMtLjUtLjktMS42LTEuNDUtMi45LTEuNDUtMS43NSAwLTMuMDUuOTUtMy4wNSAyLjM1IDAgMS4zIDEgMS45NSAzLjA1IDIuMzVzMy4wNS45NSAzLjA1IDIuMzVjMCAxLjQtMS4zIDIuMzUtMy4wNSAyLjM1LTEuMzUgMC0yLjQ1LS41NS0yLjk1LTEuNDUiLz48L3N2Zz48L3NwYW4+PHNwYW4gY2xhc3M9InRsIj5DcnlwdG88L3NwYW4+PC9hPjxhIGhyZWY9Ii9hcHAvcmVzdWx0cyI+PHNwYW4gY2xhc3M9ImljIj48c3ZnIHdpZHRoPSIyMiIgaGVpZ2h0PSIyMiIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9ImN1cnJlbnRDb2xvciIgc3Ryb2tlLXdpZHRoPSIyIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0eWxlPSJkaXNwbGF5OmJsb2NrIj48cGF0aCBkPSJNMyAzdjE2LjVBMS41IDEuNSAwIDAgMCA0LjUgMjFIMjEiLz48cGF0aCBkPSJtNyAxNC41IDMuNC0zLjQgMyAzIDQuNi01LjEiLz48cGF0aCBkPSJNMTggNWgydjIiLz48L3N2Zz48L3NwYW4+PHNwYW4gY2xhc3M9InRsIj5SZXN1bHRzPC9zcGFuPjwvYT48L2Rpdj48L2JvZHk+CjwvaHRtbD4K"
).decode("utf-8")


@app.route("/app/cards")
def share_cards():
    return SHARE_STUDIO_HTML


@app.route("/app/cards-data")
def cards_data():
    """Read-only feed of today's games + best picks for the Cards builder."""
    mp = _FB_CACHE.get("match_picks", {}) or {}
    games = []
    for match, picks in mp.items():
        if not picks:
            continue
        first = picks[0]
        games.append({
            "match": match,
            "home": first.get("home", ""),
            "away": first.get("away", ""),
            "league": first.get("league", ""),
            "kickoff": _fb_fmt_kickoff(first.get("kickoff_ts")),
            "when": _fb_fmt_when(first.get("kickoff_ts")),
            "picks": [{"pick": p.get("pick", ""),
                       "confidence": int(round(p.get("confidence", 0) or 0))}
                      for p in picks if p.get("pick")],
        })
    return jsonify({"date": _FB_CACHE.get("date") or _fb_today_human(), "games": games})


_COUNTRY_ISO = {
 "qatar":"qa","ecuador":"ec","senegal":"sn","netherlands":"nl","holland":"nl","england":"gb-eng",
 "iran":"ir","usa":"us","united states":"us","wales":"gb-wls","argentina":"ar","saudi arabia":"sa",
 "mexico":"mx","poland":"pl","france":"fr","australia":"au","denmark":"dk","tunisia":"tn","spain":"es",
 "costa rica":"cr","germany":"de","japan":"jp","belgium":"be","canada":"ca","morocco":"ma","croatia":"hr",
 "brazil":"br","serbia":"rs","switzerland":"ch","cameroon":"cm","portugal":"pt","ghana":"gh","uruguay":"uy",
 "south korea":"kr","korea republic":"kr","korea":"kr","north korea":"kp","ivory coast":"ci","cote d'ivoire":"ci",
 "italy":"it","scotland":"gb-sct","northern ireland":"gb-nir","ireland":"ie","republic of ireland":"ie",
 "sweden":"se","norway":"no","finland":"fi","iceland":"is","austria":"at","czech republic":"cz","czechia":"cz",
 "slovakia":"sk","slovenia":"si","hungary":"hu","romania":"ro","bulgaria":"bg","greece":"gr","turkey":"tr",
 "ukraine":"ua","russia":"ru","belarus":"by","georgia":"ge","armenia":"am","azerbaijan":"az","israel":"il",
 "egypt":"eg","algeria":"dz","nigeria":"ng","south africa":"za","kenya":"ke","mali":"ml","burkina faso":"bf",
 "dr congo":"cd","congo":"cg","angola":"ao","zambia":"zm","zimbabwe":"zw","tanzania":"tz","uganda":"ug",
 "sudan":"sd","guinea":"gn","gabon":"ga","cape verde":"cv","mauritania":"mr","benin":"bj","togo":"tg",
 "niger":"ne","madagascar":"mg","mozambique":"mz","namibia":"na","botswana":"bw","libya":"ly","ethiopia":"et",
 "chile":"cl","colombia":"co","peru":"pe","paraguay":"py","bolivia":"bo","venezuela":"ve","panama":"pa",
 "honduras":"hn","el salvador":"sv","guatemala":"gt","jamaica":"jm","trinidad and tobago":"tt","haiti":"ht",
 "curacao":"cw","china":"cn","india":"in","indonesia":"id","thailand":"th","vietnam":"vn","malaysia":"my",
 "singapore":"sg","philippines":"ph","uae":"ae","united arab emirates":"ae","iraq":"iq","jordan":"jo",
 "syria":"sy","lebanon":"lb","oman":"om","kuwait":"kw","bahrain":"bh","yemen":"ye","palestine":"ps",
 "uzbekistan":"uz","kazakhstan":"kz","new zealand":"nz","albania":"al","north macedonia":"mk","macedonia":"mk",
 "bosnia":"ba","bosnia and herzegovina":"ba","montenegro":"me","kosovo":"xk","moldova":"md","luxembourg":"lu",
 "cyprus":"cy","malta":"mt","estonia":"ee","latvia":"lv","lithuania":"lt","gibraltar":"gi","faroe islands":"fo",
}


def _flag_url(name):
    iso = _COUNTRY_ISO.get((name or "").lower().strip())
    return ("https://flagcdn.com/w320/%s.png" % iso) if iso else None


_TEAM_LOGO_URL = {}
_TEAM_LOGO_IMG = {}


@app.route("/app/team-logo")
def team_logo():
    """Same-origin badge proxy. National team -> free flag (flagcdn, no key);
    club -> API-Football crest. Served as image bytes so cards render AND export
    without CORS. 404 -> the card falls back to a monogram. URL + bytes cached so
    each team is fetched at most once."""
    name = (request.args.get("name", "") or "").strip()
    if not name:
        return ("", 404)
    key = name.lower()
    url = _TEAM_LOGO_URL.get(key, "__miss__")
    if url == "__miss__":
        url = _flag_url(name)
        if not url:
            try:
                data = _apifootball_get("/teams", {"search": name})
                resp = (data or {}).get("response") or []
                if resp:
                    url = (resp[0].get("team") or {}).get("logo")
            except Exception:
                url = None
        _TEAM_LOGO_URL[key] = url
    if not url:
        return ("", 404)
    cached = _TEAM_LOGO_IMG.get(url)
    if cached is None:
        try:
            r = _sports_req.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and r.content:
                cached = (r.content, r.headers.get("Content-Type", "image/png"))
                if len(_TEAM_LOGO_IMG) < 600:
                    _TEAM_LOGO_IMG[url] = cached
        except Exception:
            cached = None
    if not cached:
        return ("", 404)
    from flask import Response
    out = Response(cached[0], mimetype=cached[1])
    out.headers["Cache-Control"] = "public, max-age=604800"
    return out


@app.route("/app/picks")
def fb_picks_page():
    return render_picks_page(_FB_CACHE.get("match_picks", {}),
                             _FB_CACHE.get("date") or _fb_today_human())


@app.route("/app/codes")
def fb_codes_page():
    """Render the codes page with FRESH data from the database — the in-memory
    _FB_CACHE only refreshes at scan time, but the settle thread updates
    accumulator results on its own hourly cadence.  Without this DB reload,
    a slip that's been graded WON in the database would still render as
    'pending' until the next 12h scan rebuilt the cache."""
    accumulators = []
    date_str = None
    try:
        conn = get_db()
        # Pull only the latest run_id (matches fb_load_latest semantics) but
        # include result + pending_reason + settle_last_attempt — the columns
        # the previous SELECT was missing.  The previous bug: the loader
        # didn't SELECT `result`, so a.get("result") was always None and
        # every slip rendered as PENDING regardless of the settled state.
        rows = conn.run(
            "SELECT tier, label, target_odds, total_odds, num_selections, "
            "selections_json, sportybet_code, match_date, created_at, "
            "result, pending_reason, settle_last_attempt "
            "FROM sportybet_accumulators "
            "WHERE run_id = (SELECT run_id FROM sportybet_accumulators "
            "                ORDER BY created_at DESC LIMIT 1) "
            "ORDER BY total_odds ASC")
        conn.close()
        for r in (rows or []):
            (tier, label, tgt, tot, ns, sj, code,
             mdate, created, result, pending_reason, settle_ts) = r
            try:
                sels = json.loads(sj or "[]")
            except Exception:
                sels = []
            if not sels:
                continue
            accumulators.append({
                "tier": tier, "label": label,
                "emoji": TIER_CONFIG.get(tier, {}).get("emoji", "🟢"),
                "target_odds": tgt, "total_odds": tot,
                "num_selections": ns or len(sels),
                "selections": sels, "code": code or None,
                "sportybet_code": code or None,
                # NEW: surface result + pending_reason to the renderer.
                "result": (result or "pending"),
                "pending_reason": pending_reason or None,
                "settle_last_attempt": settle_ts,
                "match_date": mdate,
            })
            if date_str is None and mdate is not None:
                try:
                    date_str = mdate.strftime("%A, %B %d, %Y")
                except Exception:
                    date_str = str(mdate)
    except Exception as e:
        print("[FB] codes page DB read failed, falling back to cache: {}".format(e))
        accumulators = _FB_CACHE.get("accumulators", [])
        date_str = _FB_CACHE.get("date") or _fb_today_human()
    if not accumulators:
        # No fresh DB rows yet (e.g. very first deploy) — fall back to cache.
        accumulators = _FB_CACHE.get("accumulators", [])
        date_str = date_str or _FB_CACHE.get("date") or _fb_today_human()
    return render_codes_page(accumulators, date_str or _fb_today_human())


@app.route("/app/fb-rescan")
def fb_rescan_now():
    """Manual 'Rescan now' trigger: re-runs the football engine immediately in the
    background and re-anchors the 12h schedule (the scheduler keys off
    last_run_ts, which the run updates). Refresh picks/codes WITHOUT a deploy."""
    back = ("<p style='font:15px system-ui;margin-top:14px'>"
            "<a href='/app/codes'>← Back to Codes</a></p>")
    if _FB_CACHE.get("running"):
        return ("<h3 style='font:600 18px system-ui'>Rescan already in progress…</h3>"
                "<p style='font:15px system-ui'>The engine is running now. Give it a "
                "minute, then refresh Codes.</p>" + back), 200
    age = time.time() - (_FB_CACHE.get("last_run_ts") or 0)
    if age < 120:
        return ("<h3 style='font:600 18px system-ui'>Just scanned {}s ago.</h3>"
                "<p style='font:15px system-ui'>To avoid hammering the sources, wait a "
                "moment before forcing another rescan.</p>".format(int(age)) + back), 200
    def _bg():
        try:
            run_football_engine(get_db, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, send_telegram)
        except Exception as e:
            print("[FB] manual rescan error: {}".format(e))
    threading.Thread(target=_bg, daemon=True).start()
    return ("<h3 style='font:600 18px system-ui'>Rescan started ✅</h3>"
            "<p style='font:15px system-ui'>Fresh picks + codes are building now and the "
            "12-hour clock has been reset. Refresh Codes in ~1–2 minutes.</p>" + back), 200


@app.route("/app/sports-scan-now")
def sports_scan_now():
    """Manual 'Sports scan now' trigger. The sports scanner normally fires
    once a day at 08:00 Lagos (07:00 UTC), and on boot any already-passed
    slot for the day is marked complete without firing (per the explicit
    'no catch-up' rule). This route lets you force a scan TODAY without
    waiting for tomorrow's slot.

    Runs the full pipeline: scrape FP.com + Forebet predictions, fetch
    Polymarket + Limitless sports markets, match predictions to markets,
    send Telegram alerts gated by the sports_alerts_log dedup (so picks
    that were already sent in a previous run won't fire again).

    Runs in a background thread so the response returns immediately."""
    back = ("<p style='font:15px system-ui;margin-top:14px'>"
            "<a href='/app/codes'>← Back to Codes</a></p>")
    def _bg():
        try:
            _sports_scan_and_alert()
        except Exception as e:
            print("[SPORTS] manual scan error: {}".format(e))
    threading.Thread(target=_bg, daemon=True).start()
    return ("<h3 style='font:600 18px system-ui'>Sports scan started ✅</h3>"
            "<p style='font:15px system-ui'>The scanner is running now. "
            "Telegram alerts will arrive for any matches that the model has "
            "an edge on AND that haven't been alerted before (dedup is "
            "active — same pick won't fire twice).</p>"
            "<p style='font:13px system-ui;color:#666'>Watch the deploy logs "
            "for <code>[SPORTS] ALERT:</code> lines, or <code>[SPORTS] DEDUP: skip ...</code>"
            " if a pick was already sent previously.</p>" + back), 200


@app.route("/app/settle-now")
def fb_settle_now():
    """Manual 'Settle pending slips now' trigger.  The background settle thread
    runs hourly; this route lets the user force an immediate pass after a
    normalizer or score-feed improvement, instead of waiting up to 60 minutes.

    Runs synchronously (~3-15s typically) so the user sees the result tally
    on the response page. Safe to call repeatedly — settled slips are
    skipped via the 'result IS NULL' filter inside _fb_settle_accumulators."""
    back = ("<p style='font:15px system-ui;margin-top:14px'>"
            "<a href='/app/codes'>← Back to Codes</a></p>")
    started = time.time()
    try:
        _fb_settle_accumulators(get_db)
    except Exception as e:
        return ("<h3 style='font:600 18px system-ui'>Settle pass failed ❌</h3>"
                "<pre style='font:13px ui-monospace,monospace;color:#b91c1c;"
                "background:#fef2f2;padding:14px;border-radius:10px;white-space:pre-wrap'>"
                "{}</pre>".format(str(e)[:600]) + back), 200
    # Quick summary: pending vs graded counts after the run.
    try:
        conn = get_db()
        rows = list(conn.run(
            "SELECT result, COUNT(*) FROM sportybet_accumulators "
            "WHERE created_at > NOW() - INTERVAL '14 days' GROUP BY result"))
        conn.close()
        tallies = {(r[0] or "pending"): r[1] for r in rows}
    except Exception:
        tallies = {}
    elapsed = time.time() - started
    tally_html = " · ".join(
        '<b>{}</b> {}'.format(v, k) for k, v in sorted(tallies.items()))
    return ("<h3 style='font:600 18px system-ui'>Settle pass complete ✅</h3>"
            "<p style='font:15px system-ui'>Ran in {:.1f}s. Last 14 days: "
            "{}.</p>"
            "<p style='font:13px system-ui;color:#666'>Pending slips display "
            "their reason on the Codes page (look for the 'Why pending?' note).</p>".format(
                elapsed, tally_html or "no slips") + back), 200


@app.route("/app/builder")
def fb_builder_page():
    return render_builder_page(_FB_CACHE.get("bet_builders", []),
                               _FB_CACHE.get("date") or _fb_today_human())


@app.route("/app/model-test")
def model_test():
    """Live validation for the new ClubElo + football-data + Dixon-Coles model.
    Usage: /app/model-test?home=Burgos&away=Eibar&code=SP2&season=2526
    Confirms the proven sources reach Railway and shows the extra-options output."""
    home = request.args.get("home", "")
    away = request.args.get("away", "")
    code = request.args.get("code", "SP2")
    season = request.args.get("season", "2526")
    out = {"home": home, "away": away, "code": code, "season": season}
    try:
        rows = _fd_league(code, season)
        out["football_data_rows"] = len(rows)
        if not rows:
            out["verdict"] = ("football-data.co.uk returned no rows — check the league "
                              "code/season, or it's unreachable from here.")
            return jsonify(out)
        if not home or not away:
            out["verdict"] = ("football-data reachable ({} matches loaded). Add "
                              "?home=&away= to model a fixture.".format(len(rows)))
            return jsonify(out)
        model = model_club_match(home, away, code, season)
        if not model:
            out["verdict"] = ("Loaded the league but couldn't match one of the team "
                              "names — check spelling against football-data.co.uk.")
            return jsonify(out)
        out["model"] = model
        ce = _ce_day_ratings()
        out["clubelo"] = {"reachable": bool(ce), "teams_loaded": len(ce),
                          "home_elo": _ce_elo(home, ce), "away_elo": _ce_elo(away, ce)}
        out["verdict"] = "WORKING — proven sources reached Railway and the model produced output."
    except Exception as e:
        out["verdict"] = "error: {}: {}".format(type(e).__name__, str(e)[:200])
    return jsonify(out)


@app.route("/app/market-search")
def market_search():
    """Diagnostic: search LIVE Poly + Limitless markets for keywords (corner,
    card, booking) to verify what market types actually exist — both what the
    bot currently fetches AND a raw unfiltered Limitless category scan.
    Usage: /app/market-search?q=corner,card,booking"""
    q = request.args.get("q", "corner,card,booking")
    kws = [k.strip().lower() for k in q.split(",") if k.strip()]
    out = {"keywords": kws}

    def _scan(markets):
        hits = []
        for m in markets:
            text = ((m.get("question", "") or m.get("title", "") or "") + " " +
                    (m.get("sports_market_type", "") or "")).lower()
            if any(k in text for k in kws):
                hits.append({"q": (m.get("question") or m.get("title") or "")[:120],
                             "smt": m.get("sports_market_type", ""),
                             "url": m.get("url", "")})
        return hits

    # 1) What the bot currently fetches
    try:
        poly = _sports_fetch_polymarket_sports()
        ph = _scan(poly)
        out["polymarket_botfetch"] = {"total": len(poly), "hits": len(ph),
                                      "samples": ph[:15]}
    except Exception as e:
        out["polymarket_botfetch"] = {"error": "{}: {}".format(
            type(e).__name__, str(e)[:150])}
    try:
        lim = _sports_fetch_limitless_sports()
        lh = _scan(lim)
        out["limitless_botfetch"] = {"total": len(lim), "hits": len(lh),
                                     "samples": lh[:15]}
    except Exception as e:
        out["limitless_botfetch"] = {"error": "{}: {}".format(
            type(e).__name__, str(e)[:150])}

    # 2) Raw unfiltered Limitless scan (bypass the bot's soccer keyword filter)
    raw_hits, raw_total = [], 0
    try:
        r = _sports_req.get("{}/markets/categories/count".format(LIMITLESS_API),
                            timeout=10)
        j = r.json() if r.status_code == 200 else {}
        cats = (j.get("category", {}) if isinstance(j, dict) else {}) or {}
        for cat_id, count in list(cats.items())[:30]:
            try:
                cr = _sports_req.get("{}/markets/active/{}".format(
                    LIMITLESS_API, cat_id), params={"page": 1, "limit": 30},
                    timeout=10)
                if cr.status_code != 200:
                    continue
                cj = cr.json()
                items = cj.get("data", []) if isinstance(cj, dict) else []
                for m in items:
                    raw_total += 1
                    title = (m.get("title", "") or "").lower()
                    if any(k in title for k in kws):
                        slug = m.get("slug", "") or m.get("address", "")
                        raw_hits.append({"q": (m.get("title") or "")[:120],
                                         "cat": cat_id,
                                         "url": "https://limitless.exchange/markets/{}".format(slug) if slug else ""})
                time.sleep(0.15)
            except Exception:
                continue
        out["limitless_raw"] = {"scanned": raw_total, "hits": len(raw_hits),
                                "samples": raw_hits[:20]}
    except Exception as e:
        out["limitless_raw"] = {"error": "{}: {}".format(
            type(e).__name__, str(e)[:150])}

    total_hits = ((out.get("polymarket_botfetch", {}).get("hits", 0) or 0) +
                  (out.get("limitless_botfetch", {}).get("hits", 0) or 0) +
                  (out.get("limitless_raw", {}).get("hits", 0) or 0))
    out["verdict"] = (
        "FOUND {} corner/card markets — they exist; can be linked/picked.".format(total_hits)
        if total_hits else "No corner/card markets found in this scan.")
    return jsonify(out)


@app.route("/app/sofa-test")
def sofa_test():
    """Instant proxy + Sofascore diagnostic. Reports WHY a request failed
    (exception detail), checks the proxy against a neutral IP-echo first to
    isolate 'proxy broken' from 'Sofascore blocking', and shows the proxy in
    masked form so you can verify the URL/credentials are set correctly."""
    out = {"impersonate": _CF_IMPERSONATE, "cloudscraper": bool(_cloudscraper)}

    # masked view of the proxy so you can confirm it's set right (no secrets leaked)
    if _SCRAPE_PROXY:
        try:
            from urllib.parse import urlparse
            p = urlparse(_SCRAPE_PROXY)
            host = p.hostname or "?"
            port = p.port or "?"
            scheme = p.scheme or "?"
            has_auth = bool(p.username)
            out["proxy"] = "{}://{}***@{}:{}".format(
                scheme, (p.username[:2] + "…") if p.username else "", host, port) \
                if has_auth else "{}://{}:{}".format(scheme, host, port)
            out["proxy_scheme"] = scheme
            out["proxy_has_auth"] = has_auth
        except Exception:
            out["proxy"] = "set (unparseable)"
    else:
        out["proxy"] = "none"

    proxies = {"http": _SCRAPE_PROXY, "https": _SCRAPE_PROXY} if _SCRAPE_PROXY else None

    # Test 1 — does the PROXY itself work? Hit a neutral IP-echo and report the IP.
    def _probe(url):
        if _cf is not None:
            try:
                kw = {"timeout": 15, "impersonate": _CF_IMPERSONATE, "headers": _HEADERS}
                if proxies:
                    kw["proxies"] = proxies
                r = _cf.get(url, **kw)
                return {"status": r.status_code, "body": (r.text or "")[:140]}
            except Exception as e:
                return {"error": "{}: {}".format(type(e).__name__, str(e)[:160])}
        return {"error": "curl_cffi unavailable"}

    out["proxy_check"] = _probe("https://api.ipify.org?format=json")
    out["sofascore"] = _probe("{}/search/all?q=arsenal".format(SOFA))

    # plain-English verdict
    pc, sc = out["proxy_check"], out["sofascore"]
    if pc.get("error"):
        out["verdict"] = ("PROXY IS FAILING — the request can't get out through it. "
                          "Check the SCRAPE_PROXY URL format (http://user:pass@host:port), "
                          "credentials, and that this server's IP is whitelisted if your "
                          "provider uses IP auth. Error: " + pc["error"])
    elif sc.get("status") == 200:
        out["verdict"] = "WORKING — proxy is fine AND Sofascore is reachable. We can use Sofascore."
    elif sc.get("status") in (403, 503):
        out["verdict"] = ("Proxy works (got an IP) but Sofascore still challenges it — "
                          "this IP isn't trusted enough; try a cleaner residential IP.")
    else:
        out["verdict"] = "Proxy works; Sofascore returned status {}".format(sc.get("status"))
    return jsonify(out)


@app.route("/app/results")
def fb_results_page():
    date_q = request.args.get("date")
    ym_q = request.args.get("ym")

    # Day-detail view
    if date_q:
        sets = []          # one entry per engine run, newest first
        try:
            conn = get_db()
            rows = conn.run(
                "SELECT label, total_odds, selections_json, sportybet_code, result, "
                "tier, run_id, created_at "
                "FROM sportybet_accumulators WHERE match_date = :d ORDER BY id DESC",
                d=date_q)
            conn.close()
            order = {"2_odds": 0, "3_odds": 1, "5_odds": 2, "10_odds": 3, "1000_odds": 4}
            gmap = {}
            for r in rows:
                # group key: run_id when present, else fall back to created_at
                # (legacy rows saved before run_id existed)
                key = r[6] or (str(r[7])[:16] if r[7] else "legacy")
                if key not in gmap:
                    grp = {"key": key, "run_id": r[6], "created_at": r[7],
                           "accas": [], "_tiers": set()}
                    gmap[key] = grp
                    sets.append(grp)        # preserves newest-first order
                grp = gmap[key]
                tier = r[5]
                if tier in grp["_tiers"]:    # dedupe accidental repeats within a run
                    continue
                grp["_tiers"].add(tier)
                try:
                    sels = json.loads(r[2]) if r[2] else []
                except Exception:
                    sels = []
                grp["accas"].append({"label": r[0], "total_odds": r[1] or 0,
                                     "selections": sels, "sportybet_code": r[3],
                                     "result": r[4], "tier": tier})
            for grp in sets:
                grp["accas"].sort(key=lambda a: order.get(a.get("tier"), 9))
        except Exception as e:
            print("[FB] day results error: {}".format(e))
        try:
            human = _dt.date.fromisoformat(date_q).strftime("%A, %B %d, %Y")
        except Exception:
            human = date_q
        return render_results_day(date_q, human, sets)

    # Calendar view
    today = _dt.date.today()
    if ym_q:
        try:
            yy, mm = ym_q.split("-")
            year, month = int(yy), int(mm)
        except Exception:
            year, month = today.year, today.month
    else:
        year, month = today.year, today.month

    day_data = {}
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT EXTRACT(DAY FROM match_date)::int, result FROM sportybet_accumulators "
            "WHERE EXTRACT(YEAR FROM match_date) = :y AND EXTRACT(MONTH FROM match_date) = :m",
            y=year, m=month)
        conn.close()
        for r in rows:
            day = int(r[0]); result = (r[1] or "pending").lower()
            d = day_data.setdefault(day, {"slips": 0, "won": 0, "lost": 0, "pending": 0})
            d["slips"] += 1
            if result == "won":
                d["won"] += 1
            elif result == "lost":
                d["lost"] += 1
            else:
                d["pending"] += 1
    except Exception as e:
        print("[FB] calendar query error: {}".format(e))

    return render_results_calendar(year, month, day_data, _fb_get_results(), today.isoformat())


@app.route("/api/telegram-webhook", methods=["POST"])
def telegram_webhook():
    try:
        update = request.get_json(force=True, silent=True) or {}
        fb_handle_telegram_update(
            update, TELEGRAM_TOKEN,
            get_crypto_signals=_fb_get_crypto_signals,
            get_sports_markets=_fb_get_sports_markets,
            get_live_bets=_fb_get_live_bets,
            get_results=_fb_get_results)
    except Exception as e:
        print("[TG] webhook error: {}".format(e))
    return jsonify({"ok": True})


@app.route("/app/run-football")
def fb_manual_run():
    """Manual trigger to run the football engine now (for testing)."""
    threading.Thread(
        target=run_football_engine,
        args=(get_db, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, send_telegram),
        daemon=True).start()
    return jsonify({"ok": True, "msg": "Football engine started — check logs and /app/codes in a minute"})


@app.route("/app/debug-scrape")
def fb_debug_scrape():
    """One-click diagnostic: runs the football scrapers right now and shows, per
    match, whether it has a score, was recovered from probabilities, or dropped —
    so you can SEE what the engine receives in your browser, no logs needed.
    (Runs the scrapers synchronously, so it can take ~15-30s.)"""
    try:
        all_predictions, src_counts = [], {}
        for scraper, name in [
            (_sports_scrape_footballpredictions_com, "fp.com"),
            (_sports_scrape_forebet, "forebet"),
            (_sports_scrape_footballpredictions_net, "fp.net"),
        ]:
            try:
                got = scraper() or []
            except Exception:
                got = []
            all_predictions.extend(got)
            src_counts[name] = len(got)
        matches = {}
        for p in all_predictions:
            h = _sports_normalize_team(p.get("home", ""))
            a = _sports_normalize_team(p.get("away", ""))
            if not h or not a:
                continue
            key, rev = (h, a), (a, h)
            if key in matches:
                matches[key]["preds"].append(p)
            elif rev in matches:
                matches[rev]["preds"].append(p)
            else:
                matches[key] = {"home": p.get("home"), "away": p.get("away"), "preds": [p]}
        rows = []
        kept = recov = dropped = 0
        for md in matches.values():
            score = next((p.get("score") for p in md["preds"]
                          if _fb_parse_score(p.get("score"))), None)
            if score:
                status, cls = "score", "ok"; kept += 1
            elif _fb_estimate_goals_from_preds(md["preds"]):
                status, cls = "recovered", "warn"; recov += 1
            else:
                status, cls = "DROPPED", "bad"; dropped += 1
            srcs = ",".join(sorted(set(p.get("source", "?") for p in md["preds"])))
            rows.append((md["home"], md["away"], score or "—", status, cls, srcs))
        order = {"bad": 0, "warn": 1, "ok": 2}
        rows.sort(key=lambda r: order.get(r[4], 3))
        summary = ("raw={} &nbsp; sources={} &nbsp; unique={} &nbsp;|&nbsp; "
                   "with&nbsp;score={} &nbsp; recovered={} &nbsp; dropped={}").format(
                       len(all_predictions), src_counts, len(matches), kept, recov, dropped)
        html = ["<html><head><meta name=viewport content='width=device-width,initial-scale=1'>",
                "<style>body{font-family:system-ui;margin:12px;background:#0f1115;color:#e6e6e6}",
                "table{border-collapse:collapse;width:100%;font-size:13px}",
                "td,th{border:1px solid #333;padding:4px 6px;text-align:left}",
                ".ok{color:#7CFC8A}.warn{color:#FFD479}.bad{color:#FF6B6B;font-weight:700}",
                "h3{margin:8px 0}</style></head><body>",
                "<h3>Scrape funnel — {}</h3>".format(_fb_today_human()),
                "<div>{}</div><br>".format(summary),
                "<table><tr><th>Home</th><th>Away</th><th>Score</th>"
                "<th>Status</th><th>Sources</th></tr>"]
        for h, a, sc, st, cls, srcs in rows:
            html.append("<tr><td>{}</td><td>{}</td><td>{}</td>"
                        "<td class={}>{}</td><td>{}</td></tr>".format(h, a, sc, cls, st, srcs))
        html.append("</table></body></html>")
        return "".join(html)
    except Exception as e:
        return "<pre>debug-scrape error: {}</pre>".format(e)


# ═══════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════

print("=" * 60)
print("CMVNG BOT v2 — CONFIRMATION TRADING ENGINE")
print("=" * 60)

try:
    init_db()
    # reset_db()  # Uncomment to reset — already ran once on first deploy
    _v2_load_balances()
    print("[V2] Balances: {}".format(
        ", ".join("{}=${:.2f}".format(k, v["balance"]) for k, v in _v2_balances.items())))
except Exception as e:
    print("[V2] DB init error: {}".format(e))

# Start RTDS thread
threading.Thread(target=_rtds_loop, daemon=True, name="v2-rtds").start()
print("[V2] RTDS thread launched")

# Start watcher threads
threading.Thread(target=_v2_hourly_watcher, daemon=True, name="v2-hourly").start()
threading.Thread(target=_v2_fifteen_min_watcher, daemon=True, name="v2-15m").start()
threading.Thread(target=_v2_daily_watcher, daemon=True, name="v2-daily").start()
# Hedge monitor DISABLED — if entries are correct, hedging is unnecessary
# threading.Thread(target=_v2_monitor_thread, daemon=True, name="v2-monitor").start()
threading.Thread(target=_v2_resolve_loop, daemon=True, name="v2-resolve").start()
threading.Thread(target=_v2_fill_checker, daemon=True, name="v2-fills").start()
threading.Thread(target=_v2_cleanup_loop, daemon=True, name="v2-cleanup").start()

# Sports prediction scanner
threading.Thread(target=_sports_scanner_thread, daemon=True, name="sports-scanner").start()
print("[SPORTS] Scanner thread launched")

# ── Football v3 engine ──
try:
    fb_init_db(get_db)
except Exception as e:
    print("[FB] DB init error: {}".format(e))

# Telegram commands + webhook
try:
    tg_set_commands(TELEGRAM_TOKEN)
    _webhook_base = os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if _webhook_base:
        if not _webhook_base.startswith("http"):
            _webhook_base = "https://" + _webhook_base
        _wh = tg_set_webhook(TELEGRAM_TOKEN, _webhook_base.rstrip("/") + "/api/telegram-webhook")
        print("[TG] Webhook set to {}/api/telegram-webhook -> {}".format(_webhook_base.rstrip("/"), _wh))
    else:
        print("[TG] No WEBHOOK_BASE_URL / RAILWAY_PUBLIC_DOMAIN set — set one so /commands work")
except Exception as e:
    print("[TG] Webhook setup error: {}".format(e))

# Football scanner thread (scrape -> analyze -> build -> codes -> telegram, every 12h)
try:
    fb_scanner_thread(get_db, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, send_telegram, interval_hours=12)
except Exception as e:
    print("[FB] scanner thread error: {}".format(e))

# Football settlement thread (marks accumulators won/lost from final scores, hourly)
try:
    fb_settle_thread(get_db)
except Exception as e:
    print("[FB] settle thread error: {}".format(e))

# Football live-status thread (league + LIVE/FT + scores for the picks page)
try:
    _fb_live_refresh_thread()
except Exception as e:
    print("[FB] live-status thread error: {}".format(e))

print("[V2] All threads launched — engine running")
print("=" * 60)

send_telegram("🚀 <b>CMVNG BOT v3 STARTED</b>\n\n"
              "💰 <b>Crypto:</b> Polymarket + Limitless (1H/15M/Daily)\n"
              "⚽ <b>Football:</b> analysis engine + SportyBet codes\n"
              "📊 <b>Sports markets:</b> live scanner\n\n"
              "Commands: /picks /codes /sports /crypto /live /results\n"
              "Tap /start for the menu.")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
