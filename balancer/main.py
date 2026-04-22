from __future__ import annotations

import json
import os
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from eth_account import Account
from web3 import Web3

sys.path.insert(0, "/app")
try:
    from notify import notify as _notify
except ImportError:
    def _notify(text: str, **_: object) -> None:  # type: ignore[misc]
        pass


_USDT_BSC = "0x55d398326f99059fF775485246999027B3197955"
_USDCE_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

_DEFAULT_BSC_RPCS = [
    "https://bsc-dataseed.binance.org",
    "https://rpc.ankr.com/bsc",
    "https://bsc-rpc.publicnode.com",
]

_DEFAULT_POLYGON_RPCS = [
    "https://polygon-rpc.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon.llamarpc.com",
    "https://polygon-bor-rpc.publicnode.com",
]

# Gnosis Safe — minimal ABI for execTransaction / getTransactionHash / nonce / getOwners
_SAFE_ABI = [
    {
        "inputs": [],
        "name": "nonce",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getOwners",
        "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "to", "type": "address"},
            {"internalType": "uint256", "name": "value", "type": "uint256"},
            {"internalType": "bytes", "name": "data", "type": "bytes"},
            {"internalType": "uint8", "name": "operation", "type": "uint8"},
            {"internalType": "uint256", "name": "safeTxGas", "type": "uint256"},
            {"internalType": "uint256", "name": "baseGas", "type": "uint256"},
            {"internalType": "uint256", "name": "gasPrice", "type": "uint256"},
            {"internalType": "address", "name": "gasToken", "type": "address"},
            {"internalType": "address", "name": "refundReceiver", "type": "address"},
            {"internalType": "uint256", "name": "_nonce", "type": "uint256"},
        ],
        "name": "getTransactionHash",
        "outputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "to", "type": "address"},
            {"internalType": "uint256", "name": "value", "type": "uint256"},
            {"internalType": "bytes", "name": "data", "type": "bytes"},
            {"internalType": "uint8", "name": "operation", "type": "uint8"},
            {"internalType": "uint256", "name": "safeTxGas", "type": "uint256"},
            {"internalType": "uint256", "name": "baseGas", "type": "uint256"},
            {"internalType": "uint256", "name": "gasPrice", "type": "uint256"},
            {"internalType": "address", "name": "gasToken", "type": "address"},
            {"internalType": "address", "name": "refundReceiver", "type": "address"},
            {"internalType": "bytes", "name": "signatures", "type": "bytes"},
        ],
        "name": "execTransaction",
        "outputs": [{"internalType": "bool", "name": "success", "type": "bool"}],
        "stateMutability": "payable",
        "type": "function",
    },
]

# Kernel smart-wallet (ERC-4337) — minimal ABI for execute()
_KERNEL_ABI = [
    {
        "inputs": [
            {"internalType": "bytes32", "name": "execMode", "type": "bytes32"},
            {"internalType": "bytes", "name": "executionCalldata", "type": "bytes"},
        ],
        "name": "execute",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    }
]

_ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_to", "type": "address"},
            {"name": "_value", "type": "uint256"},
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function",
    },
]


@dataclass(frozen=True)
class _ChainCfg:
    name: str
    chain_id: int
    rpc_url: str
    token_address: str
    token_symbol: str
    token_decimals_hint: int
    wallet_address: str
    private_key_env: str


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    if v is None or not v.strip():
        return default
    try:
        return float(v)
    except ValueError:
        return default


_SETTINGS_FILE = Path("/data/settings.json")


def _settings_float(name: str, default: float) -> float:
    """Read float from /data/settings.json, fall back to env, then default."""
    try:
        data = json.loads(_SETTINGS_FILE.read_text())
        if name in data:
            return float(data[name])
    except Exception:
        pass
    return _env_float(name, default)


def _settings_bool(name: str, default: bool) -> bool:
    """Read bool from /data/settings.json, fall back to env, then default."""
    try:
        data = json.loads(_SETTINGS_FILE.read_text())
        if name in data:
            v = data[name]
            if isinstance(v, bool):
                return v
            return str(v).lower() in ("true", "1", "yes")
    except Exception:
        pass
    return _env_bool(name, default)


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or not v.strip():
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _normalize_hex_key(k: str) -> str:
    k = (k or "").strip()
    if not k:
        return ""
    return k if k.startswith("0x") else "0x" + k


def _parse_rpc_list(raw: str) -> list[str]:
    urls: list[str] = []
    for part in (raw or "").split(","):
        u = part.strip()
        if u:
            urls.append(u)
    return urls


def _dedupe_keep_order(urls: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _get_web3(rpc_urls: list[str]) -> tuple[Web3, str]:
    last_err: Exception | None = None
    proxy_url = os.environ.get("PROXY_URL", "").strip() or None
    request_kwargs: dict[str, Any] = {"timeout": 20}
    if proxy_url:
        request_kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
    tried = 0
    for rpc_url in rpc_urls:
        tried += 1
        try:
            w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs=request_kwargs))
            # Inject PoA middleware for BSC / other PoA chains (handles 280-byte extraData)
            try:
                from web3.middleware import ExtraDataToPOAMiddleware  # web3 >= 6.x
                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            except ImportError:
                try:
                    from web3.middleware import geth_poa_middleware  # web3 < 6.x
                    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                except ImportError:
                    pass
            # Some public RPC providers disable the method used by Web3.is_connected
            # (often web3_clientVersion). Treat the RPC as connected if eth_chainId works.
            try:
                _ = int(w3.eth.chain_id)
                return w3, rpc_url
            except Exception:
                if w3.is_connected():
                    return w3, rpc_url
                last_err = RuntimeError(f"rpc_not_connected:{rpc_url}")
        except Exception as e:
            last_err = e
            continue
    if last_err is not None:
        raise RuntimeError(f"rpc_not_connected tried={tried} last={rpc_urls[-1] if rpc_urls else ''} err={last_err}")
    raise RuntimeError("rpc_not_connected")


def _erc20(w3: Web3, token_address: str):
    return w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=_ERC20_ABI)


def _token_decimals(w3: Web3, token_address: str, hint: int) -> int:
    try:
        d = _erc20(w3, token_address).functions.decimals().call()
        return int(d)
    except Exception:
        return hint


def _balance_base_unit(w3: Web3, token_address: str, wallet_address: str) -> int:
    c = _erc20(w3, token_address)
    bal = c.functions.balanceOf(Web3.to_checksum_address(wallet_address)).call()
    return int(bal)


def _to_base_unit(amount: float, decimals: int) -> int:
    return int(round(amount * (10**decimals)))


def _from_base_unit(amount: int, decimals: int) -> float:
    return float(amount) / float(10**decimals)


def _withdraw_from_kernel_wallet(
    *,
    w3: Web3,
    chain_id: int,
    kernel_address: str,         # PREDICT_ACCOUNT — Kernel smart wallet
    usdt_address: str,
    private_key: str,            # EOA owner of the Kernel wallet
    to_address: str,             # destination (funder EOA)
    amount_base_unit: int,
) -> str:
    """Transfer USDT out of a Kernel smart wallet to `to_address`.

    The EOA calls kernel.execute(execMode=bytes32(0), calldata) where
    calldata = usdt_addr(20) + value(32) + transfer_calldata.
    This matches the predict_sdk encoding used in set_ctf_exchange_allowance.
    """
    pk = _normalize_hex_key(private_key)
    if not pk:
        raise RuntimeError("missing_private_key")

    acct = Account.from_key(pk)

    # Encode ERC-20 transfer(to, amount) calldata
    usdt_contract = _erc20(w3, usdt_address)
    transfer_calldata_raw = usdt_contract.encode_abi("transfer", [Web3.to_checksum_address(to_address), int(amount_base_unit)])
    # encode_abi may return hex string or bytes depending on web3 version
    if isinstance(transfer_calldata_raw, str):
        transfer_calldata_bytes = bytes.fromhex(transfer_calldata_raw[2:] if transfer_calldata_raw.startswith("0x") else transfer_calldata_raw)
    else:
        transfer_calldata_bytes = bytes(transfer_calldata_raw)

    # Encode Kernel execution payload: target(20) + value(32) + calldata
    usdt_addr_bytes = bytes.fromhex(usdt_address[2:] if usdt_address.startswith("0x") else usdt_address)
    value_bytes = (0).to_bytes(32, "big")
    execution_calldata = usdt_addr_bytes + value_bytes + transfer_calldata_bytes

    exec_mode = bytes(32)  # ZERO_HASH — single call mode

    kernel = w3.eth.contract(
        address=Web3.to_checksum_address(kernel_address),
        abi=_KERNEL_ABI,
    )
    nonce = w3.eth.get_transaction_count(acct.address)
    tx = kernel.functions.execute(exec_mode, execution_calldata).build_transaction(
        {
            "from": acct.address,
            "nonce": nonce,
            "chainId": int(chain_id),
            "value": 0,
        }
    )
    try:
        tx.setdefault("gas", int(w3.eth.estimate_gas(tx) * 12 // 10))
    except Exception:
        tx.setdefault("gas", 250000)

    # Legacy transaction (type 0) — совместимо с BSC и Polygon
    tx.pop("maxFeePerGas", None)
    tx.pop("maxPriorityFeePerGas", None)
    tx.setdefault("gasPrice", int(w3.eth.gas_price))

    signed = w3.eth.account.sign_transaction(tx, private_key=pk)
    raw_tx = signed.raw_transaction if hasattr(signed, "raw_transaction") else signed.rawTransaction
    tx_hash = w3.eth.send_raw_transaction(raw_tx)
    return tx_hash.hex()


def _gnosis_safe_transfer(
    *,
    w3: Web3,
    chain_id: int,
    safe_address: str,        # Gnosis Safe contract (POLY_FUNDER / BALANCER_POLY_WALLET)
    token_address: str,       # ERC20 token to transfer out of the Safe
    owner_private_key: str,   # EOA owner of the Safe (derived from POLY_PRIVATE_KEY); pays gas
    to_address: str,          # transfer destination
    amount_base_unit: int,
) -> str:
    """Transfer ERC20 out of a Polymarket Gnosis Safe using the owner EOA's signature.

    For POLY_SIGNATURE_TYPE=2 (POLY_GNOSIS_SAFE), POLY_FUNDER is a Gnosis Safe contract
    and POLY_PRIVATE_KEY is the EOA that owns it (addresses differ — this is expected).
    The owner signs a SafeTx EIP-712 hash and submits execTransaction() from their EOA.
    The owner EOA needs MATIC on Polygon to pay gas.
    """
    pk = _normalize_hex_key(owner_private_key)
    if not pk:
        raise RuntimeError("missing_private_key")

    acct = Account.from_key(pk)

    # Build ERC-20 transfer(to, amount) calldata
    token_contract = _erc20(w3, token_address)
    transfer_calldata_raw = token_contract.encode_abi("transfer", [Web3.to_checksum_address(to_address), int(amount_base_unit)])
    if isinstance(transfer_calldata_raw, str):
        transfer_calldata = bytes.fromhex(transfer_calldata_raw[2:] if transfer_calldata_raw.startswith("0x") else transfer_calldata_raw)
    else:
        transfer_calldata = bytes(transfer_calldata_raw)

    safe = w3.eth.contract(address=Web3.to_checksum_address(safe_address), abi=_SAFE_ABI)
    safe_nonce = safe.functions.nonce().call()
    zero_addr = "0x0000000000000000000000000000000000000000"

    # Get the canonical EIP-712 SafeTx hash from the Safe itself
    tx_hash_bytes = safe.functions.getTransactionHash(
        Web3.to_checksum_address(token_address),  # to
        0,                                          # value (ETH amount = 0)
        transfer_calldata,                          # data
        0,                                          # operation: 0=CALL
        0,                                          # safeTxGas
        0,                                          # baseGas
        0,                                          # gasPrice
        zero_addr,                                  # gasToken
        zero_addr,                                  # refundReceiver
        safe_nonce,                                 # _nonce
    ).call()

    # Sign the raw SafeTx hash directly (no eth_sign prefix) → v=27/28
    # Gnosis Safe verifies: ecrecover(dataHash, v, r, s) for v ∈ {27, 28}
    # unsafe_sign_hash is the public API (eth-account>=0.8); fall back to _sign_hash for older versions
    _sign_fn = getattr(Account, "unsafe_sign_hash", None) or Account._sign_hash
    signed_hash = _sign_fn(tx_hash_bytes, private_key=pk)
    signature = bytes(signed_hash.signature)  # 65 bytes: r(32) + s(32) + v(1)

    # Submit execTransaction from the owner EOA (owner pays MATIC gas)
    eoa_nonce = w3.eth.get_transaction_count(acct.address)
    tx = safe.functions.execTransaction(
        Web3.to_checksum_address(token_address),
        0,
        transfer_calldata,
        0,          # CALL
        0,          # safeTxGas
        0,          # baseGas
        0,          # gasPrice
        zero_addr,  # gasToken
        zero_addr,  # refundReceiver
        signature,
    ).build_transaction({
        "from": acct.address,
        "nonce": eoa_nonce,
        "chainId": int(chain_id),
        "value": 0,
    })

    try:
        tx.setdefault("gas", int(w3.eth.estimate_gas(tx) * 12 // 10))
    except Exception:
        tx.setdefault("gas", 300000)

    # Legacy transaction (type 0) — совместимо с BSC и Polygon
    tx.pop("maxFeePerGas", None)
    tx.pop("maxPriorityFeePerGas", None)
    tx.setdefault("gasPrice", int(w3.eth.gas_price))

    signed_tx = w3.eth.account.sign_transaction(tx, private_key=pk)
    raw_tx = signed_tx.raw_transaction if hasattr(signed_tx, "raw_transaction") else signed_tx.rawTransaction
    tx_hash = w3.eth.send_raw_transaction(raw_tx)
    return tx_hash.hex()


def _send_erc20(
    *,
    w3: Web3,
    chain_id: int,
    token_address: str,
    private_key: str,
    to_address: str,
    amount_base_unit: int,
) -> str:
    pk = _normalize_hex_key(private_key)
    if not pk:
        raise RuntimeError("missing_private_key")

    acct = Account.from_key(pk)
    from_addr = acct.address

    c = _erc20(w3, token_address)
    nonce = w3.eth.get_transaction_count(from_addr)

    tx = c.functions.transfer(Web3.to_checksum_address(to_address), int(amount_base_unit)).build_transaction(
        {
            "from": from_addr,
            "nonce": nonce,
            "chainId": int(chain_id),
        }
    )

    try:
        tx.setdefault("gas", int(w3.eth.estimate_gas(tx) * 12 // 10))
    except Exception:
        tx.setdefault("gas", 250000)

    # Legacy transaction (type 0) — совместимо с BSC и Polygon
    tx.pop("maxFeePerGas", None)
    tx.pop("maxPriorityFeePerGas", None)
    tx.setdefault("gasPrice", int(w3.eth.gas_price))

    signed = w3.eth.account.sign_transaction(tx, private_key=pk)
    raw_tx = signed.raw_transaction if hasattr(signed, "raw_transaction") else signed.rawTransaction
    tx_hash = w3.eth.send_raw_transaction(raw_tx)
    return tx_hash.hex()


def _addr_from_pk(pk: str) -> str | None:
    pk = _normalize_hex_key(pk)
    if not pk:
        return None
    return Account.from_key(pk).address


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _http() -> requests.Session:
    s = requests.Session()
    proxy_url = os.environ.get("PROXY_URL", "").strip()
    if proxy_url:
        s.proxies.update({"http": proxy_url, "https": proxy_url})
    return s


def _bridge_get_supported_assets() -> dict[str, Any]:
    s = _http()
    r = s.get("https://bridge.polymarket.com/supported-assets", timeout=20)
    r.raise_for_status()
    return r.json()


def _bridge_deposit_address(poly_wallet: str) -> dict[str, Any]:
    s = _http()
    r = s.post(
        "https://bridge.polymarket.com/deposit",
        json={"address": poly_wallet},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def _bridge_withdraw_address(*, poly_wallet: str, to_chain_id: int, to_token: str, recipient: str) -> dict[str, Any]:
    s = _http()
    r = s.post(
        "https://bridge.polymarket.com/withdraw",
        json={
            "address": poly_wallet,
            "toChainId": str(int(to_chain_id)),
            "toTokenAddress": to_token,
            "recipientAddr": recipient,
        },
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def _bridge_status(deposit_addr: str) -> dict[str, Any]:
    s = _http()
    r = s.get(f"https://bridge.polymarket.com/status/{deposit_addr}", timeout=20)
    r.raise_for_status()
    return r.json()


def _extract_deposit_evm_addr(resp: dict[str, Any]) -> str:
    addr = (resp or {}).get("address") or {}
    evm = (addr.get("evm") or "").strip()
    if not evm:
        raise RuntimeError("bridge_missing_evm_address")
    return evm


def _wait_bridge_status(*, deposit_addr: str, timeout_sec: float, poll_sec: float) -> str:
    """Polls Polymarket bridge status endpoint for a deposit address.

    Returns last seen status:
      - COMPLETED
      - FAILED
      - or last intermediate value / "UNKNOWN" if timed out with no transactions.
    """

    deadline = time.time() + max(1.0, timeout_sec)
    last_status = "UNKNOWN"
    while time.time() < deadline:
        st = _bridge_status(deposit_addr)
        txs = (st or {}).get("transactions") or []
        if not txs:
            _sleep(poll_sec)
            continue
        last_status = str(txs[0].get("status") or "UNKNOWN")
        if last_status in {"COMPLETED", "FAILED"}:
            return last_status
        _sleep(poll_sec)
    return last_status


def _sleep(sec: float) -> None:
    if sec <= 0:
        return
    time.sleep(sec)


def _fetch_poly_portfolio_usd(safe_address: str, proxy: str | None = None) -> float:
    """Returns current USD value of all open Polymarket positions (shares * curPrice)."""
    try:
        s = requests.Session()
        if proxy:
            s.proxies.update({"http": proxy, "https": proxy})
        r = s.get(
            f"https://data-api.polymarket.com/positions?user={safe_address}&sizeThreshold=0.01&limit=500",
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            data = data.get("positions", data.get("data", [])) or []
        return sum(float(p.get("size", 0)) * float(p.get("curPrice", 0)) for p in (data or []))
    except Exception as e:
        print(f"[BALANCER][WARN] poly_portfolio_fetch_failed err={e}")
        return 0.0


def _fetch_predict_portfolio_usd(predict_account: str, pred_pk: str, proxy: str | None = None) -> float:
    """Returns current USD value of all open Predict.fun positions."""
    try:
        from eth_account import Account
        from eth_account.messages import encode_defunct
        from eth_abi import encode as abi_encode
        from web3 import Web3

        s = requests.Session()
        if proxy:
            s.proxies.update({"http": proxy, "https": proxy})
        _api_key = os.environ.get("PREDICT_API_KEY", "").strip()
        if _api_key:
            s.headers.update({"x-api-key": _api_key})

        # ── JWT auth (same as claimer) ──
        ECDSA_VALIDATOR = "0x845ADb2C711129d4f3966735eD98a9F09fC4cE57"
        CHAIN_ID = 56

        _msg_resp = s.get("https://api.predict.fun/v1/auth/message", timeout=8)
        _msg_json = _msg_resp.json()
        if "data" not in _msg_json:
            print(f"[BALANCER][WARN] predict_auth_message_unexpected status={_msg_resp.status_code} body={str(_msg_json)[:300]}")
            return 0.0
        message = _msg_json["data"]["message"]

        eip191_prefix = b"\x19Ethereum Signed Message:\n" + str(len(message)).encode() + message.encode()
        message_hash_bytes: bytes = bytes(Web3.keccak(eip191_prefix))
        kernel_type_hash: bytes = bytes(Web3.keccak(text="Kernel(bytes32 hash)"))
        encoded_km = abi_encode(["bytes32", "bytes32"], [kernel_type_hash, message_hash_bytes])
        kernel_hash: bytes = bytes(Web3.keccak(encoded_km))
        domain_type_hash: bytes = bytes(Web3.keccak(
            text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
        ))
        predict_account_cs = Web3.to_checksum_address(predict_account)
        domain_sep = abi_encode(
            ["bytes32", "bytes32", "bytes32", "uint256", "address"],
            [domain_type_hash, bytes(Web3.keccak(text="Kernel")), bytes(Web3.keccak(text="0.3.1")), CHAIN_ID, predict_account_cs],
        )
        domain_separator: bytes = bytes(Web3.keccak(domain_sep))
        digest: bytes = bytes(Web3.keccak(b"\x19\x01" + domain_separator + kernel_hash))
        acct = Account.from_key(pred_pk)
        signable = encode_defunct(primitive=digest)
        signed = acct.sign_message(signable)
        sig_hex = signed.signature.hex()
        if not sig_hex.startswith("0x"):
            sig_hex = "0x" + sig_hex
        signature = "0x01" + ECDSA_VALIDATOR[2:] + sig_hex[2:]

        resp = s.post(
            "https://api.predict.fun/v1/auth",
            json={"signer": predict_account, "message": message, "signature": signature},
            timeout=8,
        )
        resp.raise_for_status()
        _auth_json = resp.json()
        if "data" not in _auth_json:
            print(f"[BALANCER][WARN] predict_auth_unexpected status={resp.status_code} body={str(_auth_json)[:300]}")
            return 0.0
        token = _auth_json["data"]["token"]
        s.headers.update({"Authorization": f"Bearer {token}"})

        # ── Fetch positions ──
        positions = s.get("https://api.predict.fun/v1/positions?limit=500", timeout=12).json().get("data") or []
        total = 0.0
        for pos in positions:
            # Prefer explicit USD valuation fields if present
            try:
                val = pos.get("valueUsd") or pos.get("currentValue") or pos.get("value")
                if val is not None:
                    total += float(val)
                    continue
            except Exception:
                pass

            market = pos.get("market") or {}
            outcome = pos.get("outcome") or {}
            try:
                amount_wei = int(pos.get("amount", 0))
            except Exception:
                amount_wei = 0
            shares = amount_wei / 1e18
            if shares <= 0:
                continue
            # For resolved WON positions: worth full $1/share until claimed
            if market.get("status") == "RESOLVED":
                if (outcome.get("status") or "").upper() == "WON":
                    total += shares
                # LOST positions are worth $0 — skip
                continue
            try:
                cur_yes = float(market.get("curYesPrice", 0) or 0)
            except Exception:
                cur_yes = 0.0
            side = (outcome.get("side") or "YES").upper()
            price = cur_yes if side == "YES" else (1.0 - cur_yes)
            total += shares * price
        return total
    except Exception as e:
        print(f"[BALANCER][WARN] predict_portfolio_fetch_failed err={e}")
        return 0.0


def _fetch_hourly_balance_snapshot(
    *,
    bsc_rpcs: list[str],
    polygon_rpcs: list[str],
    bsc_token: str,
    poly_token: str,
    pred_wallet: str,
    poly_wallet: str,
    poly_funder: str | None,
    predict_account: str | None,
    pred_pk: str,
    proxy: str | None,
) -> tuple[float, float, float, float]:
    """Fresh on-chain USDT (BSC) + USDC.e (Polygon) and venue API position values (USD).

    Mirrors the main loop’s poly_display, pred_trigger_bal, portfolio fetches.
    """
    w3_bsc, _ = _get_web3(bsc_rpcs)
    w3_poly, _ = _get_web3(polygon_rpcs)
    bsc_dec = _token_decimals(w3_bsc, bsc_token, 18)
    poly_dec = _token_decimals(w3_poly, poly_token, 6)
    bsc_bal_bu = _balance_base_unit(w3_bsc, bsc_token, pred_wallet)
    poly_bal_bu = _balance_base_unit(w3_poly, poly_token, poly_wallet)
    poly_funder_bal_bu: int | None = None
    if poly_funder:
        try:
            poly_funder_bal_bu = _balance_base_unit(w3_poly, poly_token, poly_funder)
        except Exception as e:
            print(f"[BALANCER][WARN] hourly_poly_funder_balance err={e}")
    bsc_bal = _from_base_unit(bsc_bal_bu, bsc_dec)
    poly_bal = _from_base_unit(poly_bal_bu, poly_dec)
    if (
        poly_funder_bal_bu is not None
        and poly_funder
        and poly_funder.lower() != (poly_wallet or "").lower()
    ):
        poly_display = _from_base_unit(poly_funder_bal_bu, poly_dec)
    else:
        poly_display = poly_bal
    predict_acct_bal = 0.0
    if predict_account:
        try:
            pab = _balance_base_unit(w3_bsc, bsc_token, predict_account)
            predict_acct_bal = _from_base_unit(pab, bsc_dec)
        except Exception as e:
            print(f"[BALANCER][WARN] hourly_predict_acct_balance err={e}")
    pred_trigger_bal = predict_acct_bal + bsc_bal
    _addr_f = (poly_funder or poly_wallet).strip()
    poly_port = _fetch_poly_portfolio_usd(_addr_f, proxy=proxy) if _addr_f else 0.0
    pred_port = 0.0
    if predict_account and pred_pk.strip():
        try:
            pred_port = _fetch_predict_portfolio_usd(predict_account, pred_pk, proxy=proxy)
        except Exception as e:
            print(f"[BALANCER][WARN] hourly_pred_portfolio err={e}")
    return poly_display, pred_trigger_bal, poly_port, pred_port


def _sleep_until_local_minute(
    target_minute: int, *, poll_max_sec: float = 30.0
) -> None:
    """Block until the local clock is in the **entire** target minute (e.g. :00:00–:00:59 for min=0).

    Previously a 10s window with an accidental cap of 30s: if the thread woke a few
    seconds *after* the window (RPC/GIL load), the whole hour was skipped until next
    hour — hourly TG never arrived.
    """
    tm = max(0, min(59, int(target_minute)))
    while True:
        now = datetime.now()
        if now.minute == tm:
            return
        # next occurrence of local HH:tm:00
        cand = now.replace(minute=tm, second=0, microsecond=0)
        if cand <= now:
            cand = cand + timedelta(hours=1)
        wait = (cand - now).total_seconds()
        if wait > 1.5:
            time.sleep(min(float(poll_max_sec), max(0.2, wait * 0.4)))
        else:
            time.sleep(min(1.0, max(0.05, wait)))


def _json_ts_to_unix(raw: object) -> float | None:
    """Event time as Unix sec. Strips Z and applies UTC; numeric ts allowed."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if not s:
        return None
    utc_flag = s.endswith("Z") or s.endswith("z")
    if utc_flag:
        s = s[:-1]
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is None and utc_flag:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return float(dt.timestamp())
    except Exception:
        return None


def _local_prev_full_hour_bounds(now: float) -> tuple[float, float, str]:
    """Previous full local calendar hour [start, end), end exclusive, as Unix times + short label.

    e.g. if now is 13:10 local → [12:00, 13:00). Trades at 13:04 go into the *next* hour's report.
    """
    dt = datetime.fromtimestamp(now)
    end_dt = dt.replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(hours=1)
    if start_dt.date() == end_dt.date():
        label = f"{start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}"
    else:
        label = f"{start_dt.strftime('%d.%m %H:%M')}–{end_dt.strftime('%d.%m %H:%M')}"
    return start_dt.timestamp(), end_dt.timestamp(), label


def _hourly_pnl(
    since_ts: float, until_ts: float | None = None
) -> tuple[float, int, int, int]:
    """Return (net_pnl, total_count, plus_count, minus_count) for successful trades in the window.

    If until_ts is set, only since_ts <= ts < until_ts. Otherwise ts >= since_ts (rolling window).
    """
    success_file = os.environ.get("TRADER_SUCCESS_TRADES_FILE", "/data/trades_success.jsonl")
    p = Path(success_file)
    if not p.exists():
        return 0.0, 0, 0, 0
    total, count, plus_n, minus_n = 0.0, 0, 0, 0
    pred_fee_bps = float(os.environ.get("PREDICT_FEE_BPS", "0") or "0")
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            ts = _json_ts_to_unix(row.get("ts"))
            if ts is None:
                continue
            if ts < since_ts or not row.get("ok"):
                continue
            if until_ts is not None and ts >= until_ts:
                continue
            # Use stored net_pnl if available (most accurate, avoids wrong fee defaults)
            if "net_pnl" in row:
                trade_pnl = float(row["net_pnl"])
            else:
                lr = row.get("live_hedge_recheck") or {}
                hq = float(lr.get("hedge_qty") or 0)
                pb = float(lr.get("pred_bid") or 0)
                vwap = float(lr.get("live_poly_vwap") or 0)
                lf = float(lr.get("live_poly_fee") or 0)
                gross = hq * (1.0 - pb - vwap)
                trade_pnl = gross - lf * hq - pred_fee_bps / 10_000 * pb * hq
            total += trade_pnl
            count += 1
            if trade_pnl >= 0:
                plus_n += 1
            else:
                minus_n += 1
        except Exception:
            pass
    return total, count, plus_n, minus_n


def _hourly_trade_details(
    since_ts: float, until_ts: float | None = None
) -> tuple[int, dict[str, int], dict[str, int]]:
    """Return (ok_count, incidents_by_code, skips_by_code) for trades in the time window.

    If until_ts is set, only since_ts <= ts < until_ts.
    """
    p = Path(os.environ.get("TRADER_TRADES_FILE", "/data/trades.jsonl"))
    if not p.exists():
        return 0, {}, {}
    ok_n = 0
    incidents: dict[str, int] = {}
    skips: dict[str, int] = {}
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            ts = _json_ts_to_unix(row.get("ts"))
            if ts is None:
                continue
            if ts < since_ts:
                continue
            if until_ts is not None and ts >= until_ts:
                continue
            if row.get("ok"):
                ok_n += 1
            elif row.get("skipped"):
                sr = row.get("skip_reason") or {}
                code = sr.get("code") or (row.get("summary") or {}).get("reason_code") or "unknown"
                skips[code] = skips.get(code, 0) + 1
            else:
                code = (row.get("summary") or {}).get("reason_code") or "incident"
                incidents[code] = incidents.get(code, 0) + 1
        except Exception:
            pass
    return ok_n, incidents, skips


def main() -> None:
    poly_wallet = os.environ.get("BALANCER_POLY_WALLET", "").strip() or "0x187042aEF3a09C534E76612440ED086e58c9ACaD"
    pred_wallet = os.environ.get("BALANCER_PREDICT_WALLET", "").strip() or "0x1b7FD55c2D2c243CE917eb998f12CDEB9E686Fc9"

    # Re-read dynamically inside the loop; initial values just for logging before loop starts
    threshold_usd = _settings_float("BALANCER_THRESHOLD_USD", 10.0)
    target_usd = _settings_float("BALANCER_TARGET_USD", 25.0)
    min_bridge_usd = _env_float("BALANCER_MIN_BRIDGE_USD", 3.0)
    enable_transfers = _env_bool("BALANCER_ENABLE_TRANSFERS", False)
    enable_bsc_to_poly = _env_bool("BALANCER_ENABLE_BSC_TO_POLY", enable_transfers)
    enable_poly_to_bsc = _env_bool("BALANCER_ENABLE_POLY_TO_BSC", enable_transfers)
    interval_sec = _env_float("BALANCER_INTERVAL_SEC", 30.0)
    cooldown_sec = _env_float("BALANCER_COOLDOWN_SEC", 300.0)
    status_timeout_sec = _env_float("BALANCER_STATUS_TIMEOUT_SEC", 1200.0)
    status_poll_sec = _env_float("BALANCER_STATUS_POLL_SEC", 10.0)

    bsc_rpcs = (
        _dedupe_keep_order(
            _parse_rpc_list(os.environ.get("BSC_RPC_URLS", ""))
            + _parse_rpc_list(os.environ.get("BSC_RPC_URL", ""))
            + list(_DEFAULT_BSC_RPCS)
        )
    )
    polygon_rpcs = (
        _dedupe_keep_order(
            _parse_rpc_list(os.environ.get("POLYGON_RPC_URLS", ""))
            + _parse_rpc_list(os.environ.get("POLYGON_RPC_URL", ""))
            + list(_DEFAULT_POLYGON_RPCS)
        )
    )

    bsc_rpc = bsc_rpcs[0]
    polygon_rpc = polygon_rpcs[0]

    proxy_url = os.environ.get("PROXY_URL", "").strip()
    print(
        "[BALANCER] rpc_config "
        f"proxy_set={bool(proxy_url)} bsc_rpcs={len(bsc_rpcs)} polygon_rpcs={len(polygon_rpcs)}"
    )
    if bsc_rpcs:
        print("[BALANCER] bsc_rpc_candidates " + " ".join(bsc_rpcs))
    if polygon_rpcs:
        print("[BALANCER] polygon_rpc_candidates " + " ".join(polygon_rpcs))

    bsc_usdt = os.environ.get("BSC_USDT_ADDRESS", "").strip() or _USDT_BSC
    polygon_usdce = os.environ.get("POLYGON_USDCE_ADDRESS", "").strip() or _USDCE_POLYGON

    pred_pk = os.environ.get("PREDICT_PRIVATE_KEY", "")
    # BALANCER_POLY_PRIVATE_KEY — dedicated key for BALANCER_POLY_WALLET on Polygon.
    # Falls back to POLY_PRIVATE_KEY, but POLY_PRIVATE_KEY is often a Polymarket signer key
    # (SIGNATURE_TYPE=2) that does NOT control the on-chain wallet address directly.
    poly_pk = (
        os.environ.get("BALANCER_POLY_PRIVATE_KEY", "").strip()
        or os.environ.get("POLY_PRIVATE_KEY", "")
    )
    pred_pk_addr = _addr_from_pk(pred_pk)
    poly_pk_addr = _addr_from_pk(poly_pk)
    pred_pk_mismatch = bool(pred_pk_addr and pred_pk_addr.lower() != pred_wallet.lower())
    if pred_pk_mismatch:
        print(f"[BALANCER][WARN] predict_pk_addr_mismatch pk_addr={pred_pk_addr} wallet={pred_wallet}")
    # POLY_SIGNATURE_TYPE=2 means POLY_GNOSIS_SAFE: POLY_PRIVATE_KEY is the EOA *owner* of the
    # Safe contract at BALANCER_POLY_WALLET — the two addresses deliberately differ.
    # poly→bsc uses safe.execTransaction() signed by the owner EOA (needs MATIC for gas).
    if poly_pk_addr:
        print(f"[BALANCER] poly_safe_owner={poly_pk_addr} gnosis_safe={poly_wallet}")
    else:
        print("[BALANCER][WARN] no poly private key configured; poly->bsc direction disabled")
    if enable_bsc_to_poly and pred_pk_mismatch:
        print("[BALANCER][ERROR] bsc_to_poly_enabled_but_predict_pk_mismatch -> disabling_bsc_to_poly")
        enable_bsc_to_poly = False
    if enable_poly_to_bsc and not poly_pk:
        print("[BALANCER][ERROR] poly_to_bsc_enabled_but_no_poly_pk -> disabling_poly_to_bsc")
        enable_poly_to_bsc = False

    poly_funder = os.environ.get("POLY_FUNDER", "").strip() or None
    if poly_funder:
        print(f"[BALANCER] poly_funder_detected address={poly_funder}")

    predict_account_addr = os.environ.get("PREDICT_ACCOUNT", "").strip() or None
    if predict_account_addr:
        print(f"[BALANCER] predict_account_detected address={predict_account_addr}")

    bsc = _ChainCfg(
        name="BSC",
        chain_id=56,
        rpc_url=bsc_rpc,
        token_address=bsc_usdt,
        token_symbol="USDT",
        token_decimals_hint=18,
        wallet_address=pred_wallet,
        private_key_env="PREDICT_PRIVATE_KEY",
    )
    polygon = _ChainCfg(
        name="POLYGON",
        chain_id=137,
        rpc_url=polygon_rpc,
        token_address=polygon_usdce,
        token_symbol="USDC.e",
        token_decimals_hint=6,
        wallet_address=poly_wallet,
        private_key_env="POLY_PRIVATE_KEY",
    )

    print(
        "[BALANCER] started "
        f"threshold_usd={threshold_usd:.2f}$ target_usd={target_usd:.2f}$ interval_sec={interval_sec:.1f} cooldown_sec={cooldown_sec:.1f} "
        f"poly_wallet={poly_wallet} pred_wallet={pred_wallet} "
        + (f"predict_account={predict_account_addr} " if predict_account_addr else "")
        + f"enable_transfers={enable_transfers} "
        f"enable_bsc_to_poly={enable_bsc_to_poly} enable_poly_to_bsc={enable_poly_to_bsc}"
    )

    last_action_ts: float = 0.0
    _last_pnl_checkpoint_ts: float = time.time() - 3600  # tracks start of current PnL window
    # Shared status published via simple HTTP server for bots/monitoring
    BALANCER_STATUS: dict = {}
    BALANCER_STATUS_LOCK = threading.Lock()

    class _StatusHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/status":
                self.send_response(404)
                self.end_headers()
                return
            try:
                with BALANCER_STATUS_LOCK:
                    body = json.dumps(BALANCER_STATUS).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_response(500)
                self.end_headers()

    def _start_status_server(port: int) -> None:
        try:
            server = HTTPServer(("0.0.0.0", port), _StatusHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            print(f"[BALANCER] status_server_started port={port}")
        except Exception as e:
            print(f"[BALANCER][WARN] failed_start_status_server err={e}")

    try:
        status_port = int(os.environ.get("BALANCER_STATUS_PORT", "8081"))
    except Exception:
        status_port = 8081
    _start_status_server(status_port)

    # ── Hourly stats thread: wake at BALANCER_HOURLY_STATS_MIN (default 0 = top of hour),
    #    fresh RPC + Polymarket data-api + Predict API balances; PnL/trades = previous full local hour
    def _hourly_notify_worker() -> None:
        _last_fired: tuple[int, int, int, int] | None = None
        while True:
            try:
                try:
                    _stats_min = int(
                        float(os.environ.get("BALANCER_HOURLY_STATS_MIN", "0") or "0")
                    )
                except Exception:
                    _stats_min = 0
                _stats_min = max(0, min(59, _stats_min))
                _sleep_until_local_minute(_stats_min, poll_max_sec=30.0)
                _tm = time.localtime()
                _fkey = (_tm.tm_year, _tm.tm_yday, _tm.tm_hour, _stats_min)
                if _fkey == _last_fired:
                    time.sleep(12.0)
                    continue
                _proxy = proxy_url or None
                _poly_cash, _pred_cash, _poly_port, _pred_port = _fetch_hourly_balance_snapshot(
                    bsc_rpcs=bsc_rpcs,
                    polygon_rpcs=polygon_rpcs,
                    bsc_token=bsc_usdt,
                    poly_token=polygon_usdce,
                    pred_wallet=pred_wallet,
                    poly_wallet=poly_wallet,
                    poly_funder=poly_funder,
                    predict_account=predict_account_addr,
                    pred_pk=pred_pk,
                    proxy=_proxy,
                )
                _poly_total = _poly_cash + _poly_port
                _pred_total = _pred_cash + _pred_port
                _total_cash = _poly_cash + _pred_cash
                _total_with_pos = _poly_total + _pred_total
                _now = time.time()
                _since_ts, _until_ts, _prev_hour_label = _local_prev_full_hour_bounds(_now)
                _h1_pnl, _, _, _ = _hourly_pnl(
                    since_ts=_since_ts, until_ts=_until_ts
                )
                _ok_n, _incidents, _skips = _hourly_trade_details(
                    since_ts=_since_ts, until_ts=_until_ts
                )
                _inc_n = sum(_incidents.values())
                _skip_n = sum(_skips.values())
                _pnl_emoji = "📈" if _h1_pnl >= 0 else "📉"
                _snap_lbl = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                def _bal_line(name: str, cash: float, total: float) -> str:
                    pos = total - cash
                    if pos > 0.01:
                        return f"{name}: <b>${total:.2f}</b>  <i>(cash ${cash:.2f} + pos ${pos:.2f})</i>\n"
                    return f"{name}: <b>${cash:.2f}</b>\n"

                _poly_line = _bal_line("Polymarket", _poly_cash, _poly_total)
                _pred_line = _bal_line("Predict", _pred_cash, _pred_total)
                _total_line = f"<b>TOTAL: ${_total_with_pos:.2f}</b>"
                if _total_with_pos > _total_cash + 0.01:
                    _total_line += f"  <i>(liquid ${_total_cash:.2f})</i>"
                _total_line += "\n"
                _tlines = ["<b>TRADES</b>", f"🟢 Successful: <b>{_ok_n}</b>"]
                if _incidents:
                    _tlines.append(f"🔴 Incidents: <b>{_inc_n}</b>")
                    for _code, _cnt in sorted(_incidents.items(), key=lambda x: -x[1]):
                        _tlines.append(f"  • {_code}: {_cnt}")
                else:
                    _tlines.append("🔴 Incidents: <b>0</b>")
                if _skips:
                    _tlines.append(f"⏭ Skipped: <b>{_skip_n}</b>")
                    for _code, _cnt in sorted(_skips.items(), key=lambda x: -x[1]):
                        _tlines.append(f"  - {_code}: {_cnt}")
                else:
                    _tlines.append("⏭ Skipped: <b>0</b>")
                _halt_line = "🛑 <b>Trader halted (low balance)</b>\n" if Path("/data/halt").exists() else ""
                _tz_hint = (time.tzname[0] or "local")
                _notify(
                    f"📊 <b>HOURLY STATS</b>\n"
                    f"<i>Trades &amp; PnL — previous full local hour: <b>{_prev_hour_label}</b> "
                    f"({_tz_hint}, server time)</i>\n"
                    f"<i>Balances — on-chain + venue APIs, snapshot: <b>{_snap_lbl}</b> ({_tz_hint})</i>\n"
                    f"\n"
                    f"<b>BALANCE</b>\n"
                    + _poly_line
                    + _pred_line
                    + _total_line
                    + f"\n"
                    + f"{_pnl_emoji} PnL (that hour): <b>{_h1_pnl:+.2f}$</b>\n"
                    + f"\n"
                    + "\n".join(_tlines) + "\n"
                    + _halt_line
                )
                print(
                    f"[BALANCER] hourly_notify_sent local_hour={_tm.tm_hour} "
                    f"prev_window={_prev_hour_label} total_with_pos={_total_with_pos:.2f} "
                    f"snap={_snap_lbl}"
                )
                _last_fired = _fkey
            except Exception as _e:
                print(f"[BALANCER][WARN] hourly_notify_failed err={_e}")
            time.sleep(12.0)

    _hn_thread = threading.Thread(target=_hourly_notify_worker, daemon=True)
    _hn_thread.start()
    try:
        _hsm = int(float(os.environ.get("BALANCER_HOURLY_STATS_MIN", "0") or "0"))
    except Exception:
        _hsm = 0
    print(
        f"[BALANCER] hourly_notify_thread_started "
        f"at_minute={_hsm} window=prev_full_local_hour fresh_snapshot=rpc+api"
    )

    while True:
        now = time.time()
        # Re-read configurable thresholds each cycle so bot /settings changes take effect
        threshold_usd = _settings_float("BALANCER_THRESHOLD_USD", 10.0)
        target_usd = _settings_float("BALANCER_TARGET_USD", 25.0)
        _transfers = _settings_bool("BALANCER_ENABLE_TRANSFERS", enable_transfers)
        enable_bsc_to_poly = _settings_bool("BALANCER_ENABLE_BSC_TO_POLY", _transfers)
        enable_poly_to_bsc = _settings_bool("BALANCER_ENABLE_POLY_TO_BSC", _transfers)
        try:
            w3_bsc, bsc_rpc_used = _get_web3(bsc_rpcs)
            w3_poly, poly_rpc_used = _get_web3(polygon_rpcs)
            if bsc_rpc_used != bsc.rpc_url:
                print(f"[BALANCER] bsc_rpc_selected url={bsc_rpc_used}")
                bsc = _ChainCfg(
                    name=bsc.name,
                    chain_id=bsc.chain_id,
                    rpc_url=bsc_rpc_used,
                    token_address=bsc.token_address,
                    token_symbol=bsc.token_symbol,
                    token_decimals_hint=bsc.token_decimals_hint,
                    wallet_address=bsc.wallet_address,
                    private_key_env=bsc.private_key_env,
                )
            if poly_rpc_used != polygon.rpc_url:
                print(f"[BALANCER] polygon_rpc_selected url={poly_rpc_used}")
                polygon = _ChainCfg(
                    name=polygon.name,
                    chain_id=polygon.chain_id,
                    rpc_url=poly_rpc_used,
                    token_address=polygon.token_address,
                    token_symbol=polygon.token_symbol,
                    token_decimals_hint=polygon.token_decimals_hint,
                    wallet_address=polygon.wallet_address,
                    private_key_env=polygon.private_key_env,
                )

            bsc_dec = _token_decimals(w3_bsc, bsc.token_address, bsc.token_decimals_hint)
            poly_dec = _token_decimals(w3_poly, polygon.token_address, polygon.token_decimals_hint)

            bsc_bal_bu = _balance_base_unit(w3_bsc, bsc.token_address, bsc.wallet_address)
            poly_bal_bu = _balance_base_unit(w3_poly, polygon.token_address, polygon.wallet_address)

            poly_funder_bal_bu: int | None = None
            if poly_funder:
                try:
                    poly_funder_bal_bu = _balance_base_unit(w3_poly, polygon.token_address, poly_funder)
                except Exception as e:
                    print(f"[BALANCER][WARN] poly_funder_balance_error err_type={type(e).__name__} err={e}")

            bsc_bal = _from_base_unit(bsc_bal_bu, bsc_dec)
            poly_bal = _from_base_unit(poly_bal_bu, poly_dec)

            # Polymarket display balance: prefer POLY_FUNDER if set, otherwise use poly_wallet
            if poly_funder_bal_bu is not None and poly_funder and poly_funder.lower() != poly_wallet.lower():
                poly_display = _from_base_unit(poly_funder_bal_bu, poly_dec)
            else:
                poly_display = poly_bal

            # Predict.fun trading account balance (separate from the EOA funder wallet)
            predict_acct_bal: float | None = None
            if predict_account_addr:
                try:
                    predict_acct_bal_bu = _balance_base_unit(w3_bsc, bsc.token_address, predict_account_addr)
                    predict_acct_bal = _from_base_unit(predict_acct_bal_bu, bsc_dec)
                except Exception as _e:
                    print(f"[BALANCER][WARN] predict_account_balance_error err_type={type(_e).__name__} err={_e}")

            # --- balance log ---
            print(
                f"[BALANCER] POLYMARKET {polygon.token_symbol}={poly_display:.2f}$ "
                f"wallet={poly_funder or poly_wallet}"
            )
            # Funder (EOA) is a pass-through wallet — not monitored, shown for info only
            print(
                f"[BALANCER] PREDICT.FUN funder {bsc.token_symbol}={bsc_bal:.2f}$ "
                f"[transit] wallet={pred_wallet}"
            )
            if predict_acct_bal is not None:
                print(
                    f"[BALANCER] PREDICT.FUN trading {bsc.token_symbol}={predict_acct_bal:.2f}$ "
                    f"account={predict_account_addr}"
                )

            # --- авто-форвард: если фандер держит USDT (после моста) — сразу шлём на PREDICT_ACCOUNT ---
            if bsc_bal >= 0.5 and predict_account_addr and pred_pk:
                try:
                    fwd_bu = _balance_base_unit(w3_bsc, bsc.token_address, bsc.wallet_address)
                    if fwd_bu > 0:
                        fwd_txh = _send_erc20(
                            w3=w3_bsc,
                            chain_id=bsc.chain_id,
                            token_address=bsc.token_address,
                            private_key=pred_pk,
                            to_address=predict_account_addr,
                            amount_base_unit=fwd_bu,
                        )
                        fwd_usd = _from_base_unit(fwd_bu, bsc_dec)
                        print(f"[BALANCER] auto_forward_funder_to_predict amount={fwd_usd:.2f}$ tx={fwd_txh}")
                        _sleep(4.0)
                        bsc_bal_bu = _balance_base_unit(w3_bsc, bsc.token_address, bsc.wallet_address)
                        bsc_bal = _from_base_unit(bsc_bal_bu, bsc_dec)
                        if predict_acct_bal is not None:
                            predict_acct_bal += fwd_usd
                except Exception as _af_e:
                    print(f"[BALANCER][ERROR] auto_forward_failed err={_af_e}")

            # --- equalization summary ---
            # pred_trigger_bal = kernel account + funder transit (оба на BSC принадлежат predict.fun)
            pred_trigger_bal = (predict_acct_bal if predict_acct_bal is not None else 0.0) + bsc_bal
            total_bal = poly_display + pred_trigger_bal
            equal_each = total_bal / 2.0
            imbalance = poly_display - pred_trigger_bal  # positive = poly has more
            print(
                f"[BALANCER] TOTAL={total_bal:.2f}$ "
                f"imbalance={imbalance:+.2f}$ threshold=±{threshold_usd:.2f}$ "
                + (f"→ will equalize to {equal_each:.2f}$ each" if abs(imbalance) > threshold_usd else "→ balanced")
            )

            # Publish status for external callers (bot, monitoring)
            try:
                _poly_portfolio = 0.0
                _pred_portfolio = 0.0
                try:
                    _poly_portfolio = _fetch_poly_portfolio_usd(poly_funder or poly_wallet, proxy=proxy_url)
                except Exception:
                    _poly_portfolio = 0.0
                try:
                    if predict_account_addr and pred_pk:
                        _pred_portfolio = _fetch_predict_portfolio_usd(predict_account_addr, pred_pk, proxy=proxy_url)
                except Exception:
                    _pred_portfolio = 0.0
                with BALANCER_STATUS_LOCK:
                    BALANCER_STATUS.update({
                        "poly_cash": round(poly_display, 6),
                        "poly_portfolio": round(_poly_portfolio, 6),
                        "poly_total": round(poly_display + _poly_portfolio, 6),
                        "poly_wallet": polygon.wallet_address,
                        "poly_funder": poly_funder or "",
                        "bsc_cash": round(bsc_bal, 6),
                        "predict_account_cash": round(predict_acct_bal or 0.0, 6),
                        "predict_portfolio": round(_pred_portfolio, 6),
                        "pred_trigger_bal": round(pred_trigger_bal, 6),
                        "total_cash": round(total_bal, 6),
                        "total_with_pos": round(poly_display + _poly_portfolio + pred_trigger_bal + _pred_portfolio, 6),
                        "imbalance": round(imbalance, 6),
                        "predict_account": predict_account_addr or "",
                        "last_update_ts": time.time(),
                    })
            except Exception:
                pass

            # ── Low balance halt ────────────────────────────────────────────
            _stop_threshold = _settings_float("BOT_STOP_TOTAL_USD", 25.0)
            _halt_file = Path("/data/halt")
            if total_bal < _stop_threshold:
                if not _halt_file.exists():
                    _halt_file.write_text(f"total={total_bal:.2f} < {_stop_threshold:.2f}")
                    _notify(
                        f"🛑🛑🛑 <b>BOT STOPPED</b>\n"
                        f"\n"
                        f"Balance dropped below ${_stop_threshold:.0f}\n"
                        f"TOTAL: <b>${total_bal:.2f}</b>\n"
                        f"\n"
                    )
            else:
                if _halt_file.exists():
                    _halt_file.unlink(missing_ok=True)

            if (now - last_action_ts) < cooldown_sec:
                _sleep(interval_sec)
                continue

            # Equalization strategy:
            #   trigger: если у любого из кошельков баланс < threshold_usd → выравниваем
            #   send half the imbalance → both sides end up at total/2
            need_bsc = pred_trigger_bal < threshold_usd and imbalance > 0   # predict мало, poly даёт
            need_poly = poly_display < threshold_usd and imbalance < 0      # poly мало, predict даёт

            if need_bsc and not need_poly:
                amt = imbalance / 2.0  # half the excess from poly side
                if amt <= 0:
                    _sleep(interval_sec)
                    continue

                if amt < min_bridge_usd:
                    print(
                        "[BALANCER] skip_small_bridge "
                        f"direction=poly_to_bsc amount_usd={amt:.2f}$ min_bridge_usd={min_bridge_usd:.2f}$"
                    )
                    _sleep(interval_sec)
                    continue

                print(
                    f"[BALANCER] action=equalize_predict amount_usd={amt:.2f}$ "
                    f"(poly={poly_display:.2f}$ predict={pred_trigger_bal:.2f}$ → each≈{equal_each:.2f}$)"
                )
                if not enable_poly_to_bsc:
                    print("[BALANCER] poly_to_bsc_disabled skip_send")
                    _sleep(interval_sec)
                    continue
                wd = _bridge_withdraw_address(
                    poly_wallet=polygon.wallet_address,
                    to_chain_id=bsc.chain_id,
                    to_token=bsc.token_address,
                    recipient=bsc.wallet_address,
                )
                deposit_addr = _extract_deposit_evm_addr(wd)

                amt_bu = _to_base_unit(amt, poly_dec)
                # BALANCER_POLY_WALLET is a Gnosis Safe (POLY_SIGNATURE_TYPE=2).
                # POLY_PRIVATE_KEY is the Safe owner EOA — use execTransaction() to transfer.
                txh = _gnosis_safe_transfer(
                    w3=w3_poly,
                    chain_id=polygon.chain_id,
                    safe_address=polygon.wallet_address,
                    token_address=polygon.token_address,
                    owner_private_key=poly_pk,
                    to_address=deposit_addr,
                    amount_base_unit=amt_bu,
                )
                print(
                    "[BALANCER] sent_poly_usdce_via_safe "
                    f"to={deposit_addr} amount_base_unit={amt_bu} tx_hash={txh}"
                )

                st = _wait_bridge_status(
                    deposit_addr=deposit_addr,
                    timeout_sec=status_timeout_sec,
                    poll_sec=status_poll_sec,
                )
                print(f"[BALANCER] bridge_status deposit_addr={deposit_addr} status={st}")
                if st == "FAILED":
                    _notify(
                        f"🔴🔴🔴 <b>TRANSFER FAILED</b>\n"
                        f"\n"
                        f"poly → predict  ${amt:.2f}\n"
                        f"Bridge status: FAILED\n"                    )
                    raise RuntimeError("bridge_failed")
                try:
                    _proxy = proxy_url or None
                    _poly_portfolio_now = _fetch_poly_portfolio_usd(poly_funder or poly_wallet, proxy=_proxy)
                except Exception:
                    _poly_portfolio_now = 0.0
                try:
                    _pred_portfolio_now = _fetch_predict_portfolio_usd(predict_account_addr, pred_pk, proxy=_proxy) if predict_account_addr and pred_pk else 0.0
                except Exception:
                    _pred_portfolio_now = 0.0

                _poly_sub = poly_display + (_poly_portfolio_now or 0.0)
                _pred_sub = pred_trigger_bal + (_pred_portfolio_now or 0.0)
                _total_with_pos_now = _poly_sub + _pred_sub

                _notify(
                    f"🔄 <b>TRANSFER: POLY → PREDICT</b>\n"
                    f"${amt:.2f} bridged - status: {st}\n"
                )

                # Forward USDT from EOA funder to PREDICT_ACCOUNT so it is available for trading
                # Ждём дольше — мост может задержать зачисление на BSC на несколько секунд
                if predict_account_addr and pred_pk:
                    _sleep(15.0)
                    try:
                        eoa_bal_bu = _balance_base_unit(w3_bsc, bsc.token_address, bsc.wallet_address)
                        eoa_bal = _from_base_unit(eoa_bal_bu, bsc_dec)
                        if eoa_bal >= 0.01:
                            fwd_txh = _send_erc20(
                                w3=w3_bsc,
                                chain_id=bsc.chain_id,
                                token_address=bsc.token_address,
                                private_key=pred_pk,
                                to_address=predict_account_addr,
                                amount_base_unit=eoa_bal_bu,
                            )
                            print(
                                f"[BALANCER] forwarded_to_predict_account "
                                f"amount={eoa_bal:.2f}$ to={predict_account_addr} tx={fwd_txh}"
                            )
                        else:
                            print(
                                f"[BALANCER][WARN] funder_empty_after_bridge "
                                f"eoa_bal={eoa_bal:.2f}$ (авто-форвард исправит на следующем цикле)"
                            )
                    except Exception as _fwd_e:
                        print(f"[BALANCER][ERROR] forward_to_predict_account_failed err={_fwd_e}")

                last_action_ts = time.time()

            elif need_poly and not need_bsc:
                amt = (-imbalance) / 2.0  # half the excess from predict side
                if amt <= 0:
                    _sleep(interval_sec)
                    continue

                if amt < min_bridge_usd:
                    print(
                        "[BALANCER] skip_small_bridge "
                        f"direction=bsc_to_poly amount_usd={amt:.2f}$ min_bridge_usd={min_bridge_usd:.2f}$"
                    )
                    _sleep(interval_sec)
                    continue

                print(
                    f"[BALANCER] action=equalize_polymarket amount_usd={amt:.2f}$ "
                    f"(poly={poly_display:.2f}$ predict={pred_trigger_bal:.2f}$ → each≈{equal_each:.2f}$)"
                )
                if not enable_bsc_to_poly:
                    print("[BALANCER] bsc_to_poly_disabled skip_send")
                    _sleep(interval_sec)
                    continue

                # Check funder EOA balance; pull from Kernel wallet if short
                funder_bal_bu = _balance_base_unit(w3_bsc, bsc.token_address, bsc.wallet_address)
                funder_bal = _from_base_unit(funder_bal_bu, bsc_dec)
                amt_bu = _to_base_unit(amt, bsc_dec)
                if funder_bal_bu < amt_bu and predict_account_addr and pred_pk:
                    shortfall = amt - funder_bal
                    print(
                        f"[BALANCER] funder_insufficient funder={funder_bal:.2f}$ needed={amt:.2f}$ "
                        f"pulling {shortfall:.2f}$ from predict_account={predict_account_addr}"
                    )
                    pull_bu = _to_base_unit(shortfall, bsc_dec)
                    try:
                        pull_txh = _withdraw_from_kernel_wallet(
                            w3=w3_bsc,
                            chain_id=bsc.chain_id,
                            kernel_address=predict_account_addr,
                            usdt_address=bsc.token_address,
                            private_key=pred_pk,
                            to_address=bsc.wallet_address,
                            amount_base_unit=pull_bu,
                        )
                        print(f"[BALANCER] pulled_from_predict_account amount={shortfall:.2f}$ tx={pull_txh}")
                        _sleep(4.0)  # wait for receipt
                    except Exception as _pull_e:
                        print(f"[BALANCER][ERROR] pull_from_predict_account_failed err={_pull_e}")
                        _sleep(interval_sec)
                        continue

                dep = _bridge_deposit_address(polygon.wallet_address)
                deposit_addr = _extract_deposit_evm_addr(dep)

                txh = _send_erc20(
                    w3=w3_bsc,
                    chain_id=bsc.chain_id,
                    token_address=bsc.token_address,
                    private_key=os.environ.get(bsc.private_key_env, ""),
                    to_address=deposit_addr,
                    amount_base_unit=amt_bu,
                )
                print(
                    "[BALANCER] sent_bsc_usdt "
                    f"to={deposit_addr} amount_base_unit={amt_bu} tx_hash={txh}"
                )

                st = _wait_bridge_status(
                    deposit_addr=deposit_addr,
                    timeout_sec=status_timeout_sec,
                    poll_sec=status_poll_sec,
                )
                print(f"[BALANCER] bridge_status deposit_addr={deposit_addr} status={st}")
                if st == "FAILED":
                    _notify(
                        f"🔴🔴🔴 <b>TRANSFER FAILED</b>\n"
                        f"\n"
                        f"predict → poly  ${amt:.2f}\n"
                        f"Bridge status: FAILED\n"                    )
                    raise RuntimeError("bridge_failed")
                try:
                    _proxy = proxy_url or None
                    _poly_portfolio_now = _fetch_poly_portfolio_usd(poly_funder or poly_wallet, proxy=_proxy)
                except Exception:
                    _poly_portfolio_now = 0.0
                try:
                    _pred_portfolio_now = _fetch_predict_portfolio_usd(predict_account_addr, pred_pk, proxy=_proxy) if predict_account_addr and pred_pk else 0.0
                except Exception:
                    _pred_portfolio_now = 0.0

                _poly_sub = poly_display + (_poly_portfolio_now or 0.0)
                _pred_sub = pred_trigger_bal + (_pred_portfolio_now or 0.0)
                _total_with_pos_now = _poly_sub + _pred_sub

                _notify(
                    f"🔄 <b>TRANSFER: PREDICT → POLY</b>\n"
                    f"${amt:.2f} bridged - status: {st}\n"
                )

                last_action_ts = time.time()

            elif need_bsc and need_poly:
                # Shouldn't happen with equalization logic, but guard just in case
                print(
                    "[BALANCER][WARN] conflicting_signals "
                    f"poly={poly_display:.2f}$ predict={pred_trigger_bal:.2f}$ imbalance={imbalance:+.2f}$ "
                    "no_action"
                )

            _sleep(interval_sec)

        except Exception as e:
            print(f"[BALANCER][ERROR] err_type={type(e).__name__} err={e}")
            _sleep(max(5.0, interval_sec))


if __name__ == "__main__":
    main()
