from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import requests
from eth_account import Account
from web3 import Web3


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


def main() -> None:
    poly_wallet = os.environ.get("BALANCER_POLY_WALLET", "").strip() or "0x187042aEF3a09C534E76612440ED086e58c9ACaD"
    pred_wallet = os.environ.get("BALANCER_PREDICT_WALLET", "").strip() or "0x1b7FD55c2D2c243CE917eb998f12CDEB9E686Fc9"

    threshold_usd = _env_float("BALANCER_THRESHOLD_USD", 10.0)
    target_usd = _env_float("BALANCER_TARGET_USD", 25.0)
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

    while True:
        now = time.time()
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
                    raise RuntimeError("bridge_failed")

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
                    raise RuntimeError("bridge_failed")

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
