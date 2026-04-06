"""One-shot $1 test for both transfer directions.

Usage (from project root, with .env loaded):
    docker compose run --rm balancer python balancer/test_transfer.py

    # Or locally:
    set -a && source .env && set +a
    .venv/bin/python balancer/test_transfer.py

Directions tested:
  A) predict_account → funder EOA  (kernel.execute / withdraw $1)
  B) funder EOA → predict_account  (ERC-20 transfer $1 back)

Bridge is NOT involved – this is an on-chain BSC round-trip only.
"""
from __future__ import annotations

import os
import sys
import time

# Make balancer package importable when run directly
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from balancer.main import (
    _DEFAULT_BSC_RPCS,
    _USDT_BSC,
    _balance_base_unit,
    _dedupe_keep_order,
    _from_base_unit,
    _get_web3,
    _normalize_hex_key,
    _parse_rpc_list,
    _send_erc20,
    _to_base_unit,
    _token_decimals,
    _withdraw_from_kernel_wallet,
)

_AMOUNT_USD = 1.0

# ERC-20 Transfer event topic
_ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _sep(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def _print_balances(w3, usdt_addr: str, dec: int, funder: str, acct: str) -> None:
    time.sleep(2)  # give RPC time to reflect confirmed state
    funder_bal = _from_base_unit(_balance_base_unit(w3, usdt_addr, funder), dec)
    acct_bal = _from_base_unit(_balance_base_unit(w3, usdt_addr, acct), dec)
    print(f"  funder   ({funder}): {funder_bal:.4f} USDT")
    print(f"  predict_account ({acct}): {acct_bal:.4f} USDT")


def main() -> None:
    # --- config ---
    pred_pk_raw = os.environ.get("PREDICT_PRIVATE_KEY", "").strip()
    pred_pk = _normalize_hex_key(pred_pk_raw) if pred_pk_raw else ""
    funder_addr = os.environ.get("BALANCER_PREDICT_WALLET", "").strip()
    predict_account = os.environ.get("PREDICT_ACCOUNT", "").strip()
    usdt_addr = os.environ.get("BSC_USDT_ADDRESS", "").strip() or _USDT_BSC
    proxy = os.environ.get("PROXY_URL", "").strip()

    if not pred_pk:
        sys.exit("[ERROR] PREDICT_PRIVATE_KEY not set")
    if not funder_addr:
        sys.exit("[ERROR] BALANCER_PREDICT_WALLET not set")
    if not predict_account:
        sys.exit("[ERROR] PREDICT_ACCOUNT not set")

    bsc_rpcs = _dedupe_keep_order(
        _parse_rpc_list(os.environ.get("BSC_RPC_URLS", ""))
        + list(_DEFAULT_BSC_RPCS)
    )

    print(f"[TEST] proxy_set={bool(proxy)}")
    print(f"[TEST] funder = {funder_addr}")
    print(f"[TEST] predict_account = {predict_account}")
    print(f"[TEST] amount = ${_AMOUNT_USD}")

    print("\n[TEST] Connecting to BSC...")
    w3, rpc_used = _get_web3(bsc_rpcs)
    print(f"[TEST] Connected via {rpc_used}")

    dec = _token_decimals(w3, usdt_addr, 18)
    amt_bu = _to_base_unit(_AMOUNT_USD, dec)

    # ------------------------------------------------------------------ A
    _sep("STEP A: predict_account → funder  (kernel withdraw $1)")
    print("[TEST] Balances before:")
    _print_balances(w3, usdt_addr, dec, funder_addr, predict_account)

    acct_bal_bu = _balance_base_unit(w3, usdt_addr, predict_account)
    if acct_bal_bu < amt_bu:
        print(f"[ERROR] predict_account has only {_from_base_unit(acct_bal_bu, dec):.4f} USDT – not enough for test")
        sys.exit(1)

    print(f"\n[TEST] Calling kernel.execute() to withdraw {_AMOUNT_USD}$ from predict_account...")
    txh_a = _withdraw_from_kernel_wallet(
        w3=w3,
        chain_id=56,
        kernel_address=predict_account,
        usdt_address=usdt_addr,
        private_key=pred_pk,
        to_address=funder_addr,
        amount_base_unit=amt_bu,
    )
    print(f"[TEST] tx sent: {txh_a}")
    print(f"[TEST] BSCScan: https://bscscan.com/tx/{txh_a}")
    print("[TEST] Waiting for receipt...")
    receipt_a = w3.eth.wait_for_transaction_receipt(txh_a, timeout=90)
    status_a = "SUCCESS" if receipt_a.status == 1 else "FAILED"
    print(f"[TEST] STEP A: {status_a}  gas_used={receipt_a.gasUsed}")

    # Check Transfer event in logs
    transfer_logs_a = [
        lg for lg in receipt_a.logs
        if lg.topics and lg.topics[0].hex() == _ERC20_TRANSFER_TOPIC
    ]
    if transfer_logs_a:
        for lg in transfer_logs_a:
            amt_transferred = int(lg.data.hex(), 16) if isinstance(lg.data, bytes) else int(lg.data, 16)
            print(f"[TEST] Transfer event: amount={_from_base_unit(amt_transferred, dec):.4f} USDT")
    else:
        print("[TEST][WARN] No Transfer event found in logs – token may not have moved")

    print("\n[TEST] Balances after step A:")
    _print_balances(w3, usdt_addr, dec, funder_addr, predict_account)

    if receipt_a.status != 1:
        sys.exit("[TEST] Step A failed – stopping")

    # ------------------------------------------------------------------ B
    _sep("STEP B: funder → predict_account  (ERC-20 transfer $1 back)")

    time.sleep(2)
    funder_bal_bu = _balance_base_unit(w3, usdt_addr, funder_addr)
    if funder_bal_bu < amt_bu:
        print(f"[ERROR] funder has only {_from_base_unit(funder_bal_bu, dec):.4f} USDT – not enough for step B")
        sys.exit(1)

    print(f"\n[TEST] Sending {_AMOUNT_USD}$ from funder back to predict_account...")
    txh_b = _send_erc20(
        w3=w3,
        chain_id=56,
        token_address=usdt_addr,
        private_key=pred_pk,
        to_address=predict_account,
        amount_base_unit=amt_bu,
    )
    print(f"[TEST] tx sent: {txh_b}")
    print(f"[TEST] BSCScan: https://bscscan.com/tx/{txh_b}")
    print("[TEST] Waiting for receipt...")
    receipt_b = w3.eth.wait_for_transaction_receipt(txh_b, timeout=90)
    status_b = "SUCCESS" if receipt_b.status == 1 else "FAILED"
    print(f"[TEST] STEP B: {status_b}  gas_used={receipt_b.gasUsed}")

    transfer_logs_b = [
        lg for lg in receipt_b.logs
        if lg.topics and lg.topics[0].hex() == _ERC20_TRANSFER_TOPIC
    ]
    if transfer_logs_b:
        for lg in transfer_logs_b:
            amt_transferred = int(lg.data.hex(), 16) if isinstance(lg.data, bytes) else int(lg.data, 16)
            print(f"[TEST] Transfer event: amount={_from_base_unit(amt_transferred, dec):.4f} USDT")
    else:
        print("[TEST][WARN] No Transfer event found in logs")

    print("\n[TEST] Balances after step B (should match start):")
    _print_balances(w3, usdt_addr, dec, funder_addr, predict_account)

    # ------------------------------------------------------------------ summary
    _sep("SUMMARY")
    print(f"  A (kernel withdraw $1): {status_a}  tx={txh_a}")
    print(f"  B (EOA forward $1):     {status_b}  tx={txh_b}")
    if status_a == "SUCCESS" and status_b == "SUCCESS":
        print("\n[TEST] Both directions OK — balancer logic is functional")
    else:
        print("\n[TEST] One or more steps FAILED — check logs above")
        sys.exit(1)


if __name__ == "__main__":
    main()
