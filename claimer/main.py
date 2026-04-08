"""Claimer service — auto-redeems resolved Polymarket and predict.fun positions.

Планировщик: запускается в :01 каждого часа (маркеты разрешаются на :00).

Polymarket (Polygon, Gnosis Safe):
  • GET data-api.polymarket.com/positions → позиции с redeemable=True + curPrice>0.95
  • Вызывает ConditionalTokens.redeemPositions через Gnosis Safe execTransaction

Predict.fun (BSC, Kernel wallet):
  • GET /v1/positions (JWT, Smart Wallet подпись) — все текущие позиции
  • Фильтр: market.status=RESOLVED + outcome.status=WON
  • Вызывает builder.redeem_positions() из predict_sdk
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from eth_account import Account

# web3 v6 compatibility shim: predict_sdk imports ExtraDataToPOAMiddleware (web3 v7 name)
# but we run web3 6.x where it's called geth_poa_middleware
import web3.middleware as _w3mw
if not hasattr(_w3mw, "ExtraDataToPOAMiddleware"):
    _w3mw.ExtraDataToPOAMiddleware = getattr(_w3mw, "geth_poa_middleware", None)

sys.path.insert(0, "/app")
try:
    from notify import notify as _notify
except ImportError:
    def _notify(text: str, **_: object) -> None:  # type: ignore[misc]
        pass
from eth_account.messages import encode_defunct
from web3 import Web3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CLAIMER] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Contract addresses ──────────────────────────────────────────────────────
POLY_CTF_ADDRESS  = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # ConditionalTokens Polygon
POLY_USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e Polygon
BSC_CTF_ADDRESS   = "0x22DA1810B194ca018378464a58f6Ac2B10C9d244"  # ConditionalTokens BSC

POLY_POSITIONS_URL  = "https://data-api.polymarket.com/positions"
PREDICT_MARKETS_URL = "https://api.predict.fun/v1/markets"

# ── ABIs ────────────────────────────────────────────────────────────────────
_SAFE_ABI = [
    {
        "inputs": [],
        "name": "nonce",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
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

_CTF_ABI = [
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
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
]


# ── Helpers ─────────────────────────────────────────────────────────────────

def _normalize_hex_key(k: str) -> str:
    k = (k or "").strip()
    return k if (not k or k.startswith("0x")) else "0x" + k


def _parse_rpc_list(raw: str) -> list[str]:
    return [u.strip() for u in (raw or "").split(",") if u.strip()]


def _get_web3(rpc_urls: list[str]) -> Web3:
    proxy_url = os.environ.get("PROXY_URL", "").strip() or None
    req_kwargs: dict[str, Any] = {"timeout": 20}
    if proxy_url:
        req_kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
    for url in rpc_urls:
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs=req_kwargs))
            try:
                from web3.middleware import ExtraDataToPOAMiddleware
                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            except ImportError:
                try:
                    from web3.middleware import geth_poa_middleware
                    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                except ImportError:
                    pass
            try:
                _ = int(w3.eth.chain_id)
                return w3
            except Exception:
                if w3.is_connected():
                    return w3
        except Exception:
            continue
    raise RuntimeError(f"rpc_not_connected tried={len(rpc_urls)}")


def _make_session() -> requests.Session:
    s = requests.Session()
    proxy = os.environ.get("PROXY_URL", "").strip()
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    return s


def _encode_abi_bytes(raw: str | bytes) -> bytes:
    """Normalize web3.encode_abi output (may be str or bytes) to bytes."""
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    raw = raw.strip()
    return bytes.fromhex(raw[2:] if raw.startswith("0x") else raw)


_CLAIMS_FILE = Path(os.environ.get("CLAIMS_FILE", "/data/claims.jsonl"))
_PENDING_BAL_FILE = Path(os.environ.get("PENDING_BAL_FILE", "/data/pending_bal.json"))


def _append_claims(record: dict) -> None:
    try:
        _CLAIMS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _CLAIMS_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning(f"claims_write_failed err={e}")


def _write_pending_bal(poly_usd: float, pred_usd: float) -> None:
    try:
        _PENDING_BAL_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _PENDING_BAL_FILE.open("w", encoding="utf-8") as f:
            json.dump({"ts": datetime.utcnow().isoformat() + "Z", "poly_usd": round(poly_usd, 2), "pred_usd": round(pred_usd, 2)}, f)
    except Exception as e:
        log.warning(f"pending_bal_write_failed err={e}")


# ── Gnosis Safe execution ───────────────────────────────────────────────────

def _gnosis_safe_execute(
    *,
    w3: Web3,
    chain_id: int,
    safe_address: str,
    to_address: str,
    calldata: bytes,
    owner_private_key: str,
) -> str:
    """Execute arbitrary calldata via Gnosis Safe using single EOA owner signature.

    Uses the same EIP-712 SafeTx hash flow as balancer._gnosis_safe_transfer():
    getTransactionHash() → sign raw bytes (no eth_sign prefix) → execTransaction().
    Owner EOA (POLY_PRIVATE_KEY) needs MATIC for gas on Polygon.
    """
    pk = _normalize_hex_key(owner_private_key)
    if not pk:
        raise RuntimeError("missing_private_key")

    acct = Account.from_key(pk)
    safe = w3.eth.contract(address=Web3.to_checksum_address(safe_address), abi=_SAFE_ABI)
    safe_nonce = safe.functions.nonce().call()
    zero_addr = "0x0000000000000000000000000000000000000000"

    tx_hash_bytes = safe.functions.getTransactionHash(
        Web3.to_checksum_address(to_address),
        0,           # value (ETH)
        calldata,
        0,           # operation: CALL
        0,           # safeTxGas
        0,           # baseGas
        0,           # gasPrice
        zero_addr,   # gasToken
        zero_addr,   # refundReceiver
        safe_nonce,
    ).call()

    # Sign raw SafeTx hash (no eth_sign prefix) → v=27/28, expected by Gnosis Safe
    _sign_fn = getattr(Account, "unsafe_sign_hash", None) or Account._sign_hash
    signed = _sign_fn(tx_hash_bytes, private_key=pk)
    signature = bytes(signed.signature)

    eoa_nonce = w3.eth.get_transaction_count(acct.address)
    tx = safe.functions.execTransaction(
        Web3.to_checksum_address(to_address),
        0,
        calldata,
        0, 0, 0, 0,
        zero_addr,
        zero_addr,
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
        tx.setdefault("gas", 400000)

    # Legacy type-0 tx — compatible with Polygon (and consistent with balancer)
    tx.pop("maxFeePerGas", None)
    tx.pop("maxPriorityFeePerGas", None)
    # Use 130% of current gas price to handle replacement (underpriced) errors
    tx["gasPrice"] = int(w3.eth.gas_price * 1.3)

    signed_tx = w3.eth.account.sign_transaction(tx, private_key=pk)
    raw = signed_tx.raw_transaction if hasattr(signed_tx, "raw_transaction") else signed_tx.rawTransaction
    return w3.eth.send_raw_transaction(raw).hex()


# ── Polymarket claiming ─────────────────────────────────────────────────────

def _fetch_poly_positions(session: requests.Session, safe_address: str) -> list[dict]:
    """Return all redeemable Polymarket positions for the given address."""
    r = session.get(
        f"{POLY_POSITIONS_URL}?user={safe_address}&sizeThreshold=0.01&limit=100",
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        data = data.get("positions", data.get("data", []))
    return [p for p in (data or []) if p.get("redeemable")]


def _claim_polymarket(
    *,
    w3: Web3,
    chain_id: int,
    safe_address: str,
    owner_pk: str,
    session: requests.Session,
    claimed: set[str],
    dry_run: bool,
) -> int:
    """Redeem winning Polymarket positions via Gnosis Safe. Returns count redeemed."""
    positions = _fetch_poly_positions(session, safe_address)
    winning = [p for p in positions if float(p.get("curPrice", 0)) > 0.95]
    log.info(f"poly_positions total={len(positions)} winning={len(winning)}")

    ctf = w3.eth.contract(address=Web3.to_checksum_address(POLY_CTF_ADDRESS), abi=_CTF_ABI)
    total_usd = sum(float(p.get("size", 0)) for p in winning)
    redeemed = 0

    for pos in winning:
        condition_id: str = pos["conditionId"]
        if condition_id in claimed:
            log.info(f"poly_skip already_claimed condition={condition_id[:14]}...")
            continue

        outcome_index = int(pos.get("outcomeIndex", 0))
        index_set = 1 << outcome_index  # outcomeIndex=0 → 1, outcomeIndex=1 → 2
        size = float(pos.get("size", 0))

        # Verify on-chain balance before submitting tx (asset = ERC1155 token ID)
        asset_id = pos.get("asset")
        if asset_id:
            try:
                bal = ctf.functions.balanceOf(
                    Web3.to_checksum_address(safe_address),
                    int(asset_id),
                ).call()
                if bal == 0:
                    log.info(f"poly_skip zero_balance condition={condition_id[:14]}...")
                    continue
                log.info(f"poly_balance condition={condition_id[:14]}... bal={bal} (~{size:.2f}sh)")
            except Exception as e:
                log.warning(f"poly_balance_check_failed condition={condition_id[:14]}... err={e}")

        cid_bytes = bytes.fromhex(
            condition_id[2:] if condition_id.startswith("0x") else condition_id
        )
        calldata = _encode_abi_bytes(
            ctf.encode_abi(
                "redeemPositions",
                [
                    Web3.to_checksum_address(POLY_USDC_ADDRESS),
                    bytes(32),   # parentCollectionId = bytes32(0)
                    cid_bytes,
                    [index_set],
                ],
            )
        )

        log.info(
            f"poly_redeem condition={condition_id[:14]}... "
            f"index_set={index_set} size={size:.2f} dry_run={dry_run}"
        )

        if dry_run:
            claimed.add(condition_id)
            redeemed += 1
            continue

        try:
            txh = _gnosis_safe_execute(
                w3=w3,
                chain_id=chain_id,
                safe_address=safe_address,
                to_address=POLY_CTF_ADDRESS,
                calldata=calldata,
                owner_private_key=owner_pk,
            )
            log.info(f"poly_redeem_tx condition={condition_id[:14]}... tx={txh}")
            claimed.add(condition_id)
            redeemed += 1
            _append_claims({
                "ts": datetime.utcnow().isoformat() + "Z",
                "source": "polymarket",
                "condition_id": condition_id,
                "index_set": index_set,
                "size": size,
                "tx_hash": txh,
            })
            _notify(                f"💰 <b>КЛЕЙМ POLYMARKET</b>\n"
                f"+{size:.2f}$\n"
                f"\n"
                f"<tg-spoiler>tx={txh}</tg-spoiler>\n"            )
            time.sleep(3)  # brief pause between consecutive txs
        except Exception as e:
            log.error(f"poly_redeem_failed condition={condition_id[:14]}... err={e}")
            traceback.print_exc()

    return redeemed, total_usd


# ── Predict.fun JWT auth + positions ────────────────────────────────────────

def _predict_sign_account_message(pk: str, predict_account: str, message: str) -> str:
    """
    Вручную реализует sign_predict_account_message из predict_sdk,
    обходя баг двойного '0x' в hexbytes v0.3+ при использовании web3 v6.

    Алгоритм (Kernel Smart Wallet EIP-712):
    1. EIP-191 hash сообщения
    2. Kernel-обёртка через EIP-712
    3. Подпись EOA приватным ключом
    4. Формат: 0x01 + ECDSA_VALIDATOR + signature
    """
    from eth_account import Account
    from eth_account.messages import encode_defunct
    from eth_abi import encode as abi_encode
    from web3 import Web3

    ECDSA_VALIDATOR = "0x845ADb2C711129d4f3966735eD98a9F09fC4cE57"
    CHAIN_ID = 56  # BNB Mainnet

    # Step 1: EIP-191 hash (кrackt без web3.HexBytes)
    eip191_prefix = b"\x19Ethereum Signed Message:\n" + str(len(message)).encode() + message.encode()
    message_hash_bytes: bytes = bytes(Web3.keccak(eip191_prefix))  # bytes(), не HexBytes

    # Step 2: Kernel type hash = keccak("Kernel(bytes32 hash)")
    kernel_type_hash: bytes = bytes(Web3.keccak(text="Kernel(bytes32 hash)"))

    # Step 3: hash_kernel_message
    encoded_km = abi_encode(["bytes32", "bytes32"], [kernel_type_hash, message_hash_bytes])
    kernel_hash: bytes = bytes(Web3.keccak(encoded_km))

    # Step 4: EIP-712 domain separator для Kernel
    domain_type_hash: bytes = bytes(Web3.keccak(
        text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
    ))
    predict_account_cs = Web3.to_checksum_address(predict_account)
    domain_sep = abi_encode(
        ["bytes32", "bytes32", "bytes32", "uint256", "address"],
        [
            domain_type_hash,
            bytes(Web3.keccak(text="Kernel")),
            bytes(Web3.keccak(text="0.3.1")),
            CHAIN_ID,
            predict_account_cs,
        ],
    )
    domain_separator: bytes = bytes(Web3.keccak(domain_sep))

    # Step 5: EIP-712 final digest = keccak("\x19\x01" + domain_sep + kernel_hash)
    digest: bytes = bytes(Web3.keccak(b"\x19\x01" + domain_separator + kernel_hash))

    # Step 6: Sign digest with EOA
    acct = Account.from_key(pk)
    signable = encode_defunct(primitive=digest)
    signed = acct.sign_message(signable)
    sig_hex: str = signed.signature.hex()
    if not sig_hex.startswith("0x"):
        sig_hex = "0x" + sig_hex

    # Step 7: Kernel format: 0x01 + ECDSA_VALIDATOR (без 0x) + signature (без 0x)
    return "0x01" + ECDSA_VALIDATOR[2:] + sig_hex[2:]


def _predict_get_jwt(
    session: requests.Session,
    pk: str,
    predict_account: str,
) -> str:
    """Получает JWT токен predict.fun через EIP-1271 подпись Smart Wallet."""
    msg = session.get("https://api.predict.fun/v1/auth/message", timeout=8).json()["data"]["message"]
    sig = _predict_sign_account_message(pk, predict_account, msg)
    r = session.post(
        "https://api.predict.fun/v1/auth",
        json={"signer": predict_account, "message": msg, "signature": sig},
        timeout=8,
    )
    r.raise_for_status()
    return r.json()["data"]["token"]


def _fetch_predict_positions(session: requests.Session) -> list[dict]:
    """GET /v1/positions — все позиции авторизованного аккаунта."""
    r = session.get("https://api.predict.fun/v1/positions?limit=100", timeout=12)
    r.raise_for_status()
    return r.json().get("data") or []


def _claim_predict(
    *,
    session: requests.Session,
    predict_account: str,
    predict_pk: str,
    bsc_rpc_urls: list[str],
    claimed: set[str],
    dry_run: bool,
) -> int:
    """Redeem winning predict.fun positions via /v1/positions + Kernel wallet."""
    from predict_sdk import ChainId, OrderBuilder, OrderBuilderOptions

    builder = OrderBuilder.make(
        ChainId.BNB_MAINNET,
        predict_pk,
        OrderBuilderOptions(predict_account=predict_account),
    )
    # Override SDK's internal web3 with our own (proxy-aware + multi-RPC fallback)
    _our_w3 = _get_web3(bsc_rpc_urls)
    builder._web3 = _our_w3
    ctf = _our_w3.eth.contract(
        address=Web3.to_checksum_address(BSC_CTF_ADDRESS),
        abi=_CTF_ABI,
    )

    positions = _fetch_predict_positions(session)
    log.info(f"predict_positions total={len(positions)}")

    redeemed = 0
    total_usd: float = 0.0
    for pos in positions:
        market = pos.get("market") or {}
        outcome = pos.get("outcome") or {}

        market_status = market.get("status")
        outcome_status = outcome.get("status")

        # Пропускаем всё что не RESOLVED (REGISTERED = открытая позиция — не трогаем!)
        if market_status != "RESOLVED":
            log.info(
                f"predict_skip market={market.get('id')} status={market_status!r} "
                f"(not resolved — open position, skip)"
            )
            continue
        # Пропускаем проигранные (LOST) — там $0, ничего не клеймим
        if outcome_status != "WON":
            log.info(
                f"predict_skip market={market.get('id')} outcome.status={outcome_status!r} "
                f"(not WON — skip)"
            )
            continue

        condition_id: str = market.get("conditionId", "")
        index_set: int = int(outcome.get("indexSet", 0))
        is_neg_risk: bool = bool(market.get("isNegRisk", False))
        is_yield_bearing: bool = bool(market.get("isYieldBearing", False))
        amount_raw: int = int(pos.get("amount", 0))  # 18-decimal wei shares
        on_chain_id: int = int(outcome.get("onChainId", 0))
        market_id = market.get("id", "?")

        if not condition_id or index_set == 0 or on_chain_id == 0:
            continue

        claim_key = f"{condition_id}:{index_set}"
        if claim_key in claimed:
            continue

        # On-chain balance check
        try:
            bal = ctf.functions.balanceOf(
                Web3.to_checksum_address(predict_account),
                on_chain_id,
            ).call()
        except Exception as e:
            log.warning(f"predict_balance_check_failed market={market_id} err={e}")
            continue

        if bal == 0:
            log.info(f"predict_skip zero_balance market={market_id} condition={condition_id[:14]}...")
            continue

        shares = bal / 1e18
        total_usd += shares
        log.info(
            f"predict_redeem market={market_id} title={market.get('title','')!r} "
            f"condition={condition_id[:14]}... index_set={index_set} "
            f"shares={shares:.4f} dry_run={dry_run}"
        )

        if dry_run:
            claimed.add(claim_key)
            redeemed += 1
            continue

        try:
            amount_arg = amount_raw if is_neg_risk else None
            result = builder.redeem_positions(
                condition_id,
                index_set,
                amount_arg,
                is_neg_risk=is_neg_risk,
                is_yield_bearing=is_yield_bearing,
            )
            if result.success:
                receipt = result.receipt
                txh = receipt.transactionHash.hex() if receipt and hasattr(receipt, "transactionHash") else "unknown"
                log.info(f"predict_redeem_ok market={market_id} tx={txh} shares={shares:.4f}")
                claimed.add(claim_key)
                redeemed += 1
                _append_claims({
                    "ts": datetime.utcnow().isoformat() + "Z",
                    "source": "predict",
                    "title": market.get("title", ""),
                    "condition_id": condition_id,
                    "index_set": index_set,
                    "amount_usd": round(shares, 4),
                    "tx_hash": txh,
                    "market_id": market_id,
                })
                _notify(                    f"💰 <b>КЛЕЙМ PREDICT</b>\n"
                    f"+{shares:.2f}$\n"
                    f"\n"
                    f"<tg-spoiler>tx={txh}</tg-spoiler>\n"                )
            else:
                log.error(
                    f"predict_redeem_failed market={market_id} "
                    f"cause={getattr(result, 'cause', None)}"
                )
        except Exception as e:
            log.error(f"predict_redeem_error market={market_id} err={e}")
            traceback.print_exc()

    return redeemed, total_usd


# ── Main loop ───────────────────────────────────────────────────────────────

_JWT_TTL = 3 * 3600  # обновляем JWT раз в 3 часа


def _next_run_at() -> datetime:
    """Следующий запуск в :01 текущего или следующего часа."""
    now = datetime.now()
    candidate = now.replace(minute=1, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(hours=1)
    return candidate


def main() -> None:
    dry_run = os.environ.get("CLAIMER_DRY_RUN", "false").lower() in ("1", "true", "yes")

    # Polymarket (Polygon, Gnosis Safe)
    safe_address = (
        os.environ.get("POLY_FUNDER", "").strip()
        or os.environ.get("BALANCER_POLY_WALLET", "").strip()
        or "0x187042aEF3a09C534E76612440ED086e58c9ACaD"
    )
    owner_pk = (
        os.environ.get("BALANCER_POLY_PRIVATE_KEY", "").strip()
        or os.environ.get("POLY_PRIVATE_KEY", "").strip()
    )
    _poly_rpc_fallbacks = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://polygon-rpc.com",
        "https://1rpc.io/matic",
    ]
    _poly_user = (
        os.environ.get("POLYGON_RPC_URLS", "").strip()
        or os.environ.get("POLYGON_RPC_URL", "").strip()
    )
    poly_rpc_urls = _parse_rpc_list(_poly_user) + [
        u for u in _poly_rpc_fallbacks if u not in _parse_rpc_list(_poly_user)
    ] or _poly_rpc_fallbacks

    # Predict.fun (BSC, Kernel wallet)
    predict_account = os.environ.get("PREDICT_ACCOUNT", "").strip()
    predict_pk = _normalize_hex_key(
        os.environ.get("PREDICT_PRIVATE_KEY", "").strip()
    )
    predict_api_key = os.environ.get("PREDICT_API_KEY", "").strip()
    _bsc_rpc_fallbacks = [
        "https://bsc-dataseed1.binance.org",
        "https://bsc-dataseed2.binance.org",
        "https://bsc-dataseed3.binance.org",
        "https://bsc-dataseed4.binance.org",
        "https://bsc.publicnode.com",
        "https://bsc-dataseed1.defibit.io",
        "https://bsc-dataseed1.ninicoin.io",
        "https://bsc-dataseed.bnbchain.org",
    ]
    _bsc_user = (
        os.environ.get("BSC_RPC_URLS", "").strip()
        or os.environ.get("BSC_RPC_URL", "").strip()
    )
    bsc_rpc_urls = _parse_rpc_list(_bsc_user) + [
        u for u in _bsc_rpc_fallbacks if u not in _parse_rpc_list(_bsc_user)
    ] or _bsc_rpc_fallbacks

    log.info(
        f"claimer_start dry_run={dry_run} safe={safe_address} "
        f"predict_account={predict_account} schedule=:01_each_hour"
    )

    # In-memory claimed set — idempotent (on-chain balance check предотвращает двойной клейм)
    claimed: set[str] = set()
    session = _make_session()
    session.headers.update({"x-api-key": predict_api_key})

    jwt_ts: float = 0.0

    while True:
        # ── Ждём :01 следующего часа ────────────────────────────────────────
        target = _next_run_at()
        wait = (target - datetime.now()).total_seconds()
        if wait > 0:
            log.info(f"claimer_sleep until={target.strftime('%H:%M:%S')} ({wait:.0f}s)")
            time.sleep(wait)

        try:
            log.info(f"=== claimer_cycle_start ts={datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
            _poly_pending_usd: float = 0.0
            _pred_pending_usd: float = 0.0

            # ── Polymarket ──────────────────────────────────────────────────
            if owner_pk and safe_address:
                try:
                    w3_poly = _get_web3(poly_rpc_urls)
                    chain_id = int(w3_poly.eth.chain_id)
                    n, _poly_pending_usd = _claim_polymarket(
                        w3=w3_poly,
                        chain_id=chain_id,
                        safe_address=safe_address,
                        owner_pk=owner_pk,
                        session=session,
                        claimed=claimed,
                        dry_run=dry_run,
                    )
                    log.info(f"poly_claimed count={n}")
                except Exception as e:
                    log.error(f"poly_claim_cycle_error err={e}")
                    traceback.print_exc()
            else:
                log.info("poly_skip no_owner_pk_or_safe_address")

            # ── Predict.fun ─────────────────────────────────────────────────
            if predict_account and predict_pk and predict_api_key:
                try:
                    # Обновляем JWT если истёк
                    if time.time() - jwt_ts > _JWT_TTL:
                        jwt = _predict_get_jwt(session, predict_pk, predict_account)
                        session.headers.update({"Authorization": f"Bearer {jwt}"})
                        jwt_ts = time.time()
                        log.info("predict_jwt_refreshed")

                    n, _pred_pending_usd = _claim_predict(
                        session=session,
                        predict_account=predict_account,
                        predict_pk=predict_pk,
                        bsc_rpc_urls=bsc_rpc_urls,
                        claimed=claimed,
                        dry_run=dry_run,
                    )
                    log.info(f"predict_claimed count={n}")
                except Exception as e:
                    log.error(f"predict_claim_cycle_error err={e}")
                    traceback.print_exc()
            else:
                log.info("predict_skip no_account_or_pk_or_api_key")

            _write_pending_bal(_poly_pending_usd, _pred_pending_usd)
            log.info("=== claimer_cycle_done ===")
        except Exception as e:
            log.error(f"claimer_main_loop_error err={e}")
            traceback.print_exc()


if __name__ == "__main__":
    main()
