#!/usr/bin/env python3
import argparse
import csv
import re
from typing import Dict, List, Tuple

# Tags we care about
TAG_MSGTYPE = "35"
TAG_CLORDID = "11"
TAG_ORIGCLORDID = "41"
TAG_TRANSACTTIME = "60"
TAG_SYMBOL = "55"
TAG_SIDE = "54"
TAG_ORDERQTY = "38"
TAG_ORDTYPE = "40"
TAG_PRICE = "44"         # Limit price on the order
TAG_EXECTYPE = "150"
TAG_ORDSTATUS = "39"
TAG_AVGPX = "6"
TAG_LASTMKT = "30"

# Values
MSG_NEW_ORDER_SINGLE = "D"
MSG_EXEC_REPORT = "8"
EXECTYPE_FILL = "2"      # F (full fill) per assignment
ORDSTATUS_FILLED = "2"
ORDTYPE_LIMIT = "2"

HEADER = [
    "OrderID",
    "OrderTransactTime",
    "ExecutionTransactTime",
    "Symbol",
    "Side",
    "OrderQty",
    "LimitPrice",
    "AvgPx",
    "LastMkt",
]

def parse_fix_line(line: str) -> Dict[str, str]:
    """
    Parse a single FIX message line into a dict of tag->value.
    Accepts delimiters: SOH (\x01), '|' or spaces (robust for classroom logs).
    """
    line = line.strip()
    if not line:
        return {}

    # Prefer SOH; if not present fall back to '|' or spaces between tag=val
    if "\x01" in line:
        fields = line.split("\x01")
    elif "|" in line:
        fields = line.split("|")
    else:
        # Split on spaces but keep tag=value groups
        fields = re.split(r"\s+", line)

    msg = {}
    for f in fields:
        if not f:
            continue
        if "=" not in f:
            continue
        k, v = f.split("=", 1)
        msg[k] = v
    return msg


def load_orders_and_fills(lines: List[str]) -> Tuple[Dict[str, Dict[str, str]], List[Dict[str, str]]]:
    """
    Returns:
      orders_by_id: map[ClOrdID] -> NewOrderSingle fields we need
      fills: list of ExecutionReport (full fill) messages
    """
    orders_by_id: Dict[str, Dict[str, str]] = {}
    fills: List[Dict[str, str]] = []

    for line in lines:
        msg = parse_fix_line(line)
        if not msg:
            continue

        msgtype = msg.get(TAG_MSGTYPE)

        # New Order Single
        if msgtype == MSG_NEW_ORDER_SINGLE:
            clid = msg.get(TAG_CLORDID)
            if not clid:
                continue
            # Only store LIMIT orders
            ordtype = msg.get(TAG_ORDTYPE)
            if ordtype != ORDTYPE_LIMIT:
                continue
            # Store the useful fields
            orders_by_id[clid] = {
                TAG_CLORDID: clid,
                TAG_TRANSACTTIME: msg.get(TAG_TRANSACTTIME, ""),
                TAG_SYMBOL: msg.get(TAG_SYMBOL, ""),
                TAG_SIDE: msg.get(TAG_SIDE, ""),
                TAG_ORDERQTY: msg.get(TAG_ORDERQTY, ""),
                TAG_PRICE: msg.get(TAG_PRICE, ""),
                TAG_ORDTYPE: ordtype,
            }

        # Execution Report (only full fills; ignore partials/rejects)
        elif msgtype == MSG_EXEC_REPORT:
            if msg.get(TAG_EXECTYPE) == EXECTYPE_FILL and msg.get(TAG_ORDSTATUS) == ORDSTATUS_FILLED:
                fills.append(msg)

    return orders_by_id, fills


def build_rows(orders_by_id: Dict[str, Dict[str, str]], fills: List[Dict[str, str]]) -> List[List[str]]:
    rows: List[List[str]] = []

    for ex in fills:
        # Prefer OrigClOrdID if provided; many venues echo ClOrdID on fills.
        clid = ex.get(TAG_ORIGCLORDID) or ex.get(TAG_CLORDID)
        if not clid:
            continue

        order = orders_by_id.get(clid)
        if not order:
            # No matching NOS; skip
            continue

        # Ensure LIMIT either on exec (some streams repeat tag 40) or on stored order
        ordtype_exec = ex.get(TAG_ORDTYPE)
        if ordtype_exec is not None and ordtype_exec != ORDTYPE_LIMIT:
            continue

        # Compose CSV row
        row = [
            order.get(TAG_CLORDID, ""),                    # OrderID
            order.get(TAG_TRANSACTTIME, ""),               # OrderTransactTime (from order)
            ex.get(TAG_TRANSACTTIME, ""),                  # ExecutionTransactTime (from exec)
            order.get(TAG_SYMBOL, "") or ex.get(TAG_SYMBOL, ""),   # Symbol
            order.get(TAG_SIDE, "") or ex.get(TAG_SIDE, ""),       # Side
            order.get(TAG_ORDERQTY, "") or ex.get(TAG_ORDERQTY, ""),  # OrderQty
            order.get(TAG_PRICE, ""),                      # LimitPrice
            ex.get(TAG_AVGPX, ""),                         # AvgPx
            ex.get(TAG_LASTMKT, ""),                       # LastMkt (exchange/broker)
        ]
        rows.append(row)

    return rows


def main():
    ap = argparse.ArgumentParser(description="Convert FIX execution fills to CSV.")
    ap.add_argument("--input_fix_file", required=True, help="Path to input FIX log file")
    ap.add_argument("--output_csv_file", required=True, help="Path to output CSV")
    args = ap.parse_args()

    with open(args.input_fix_file, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    orders_by_id, fills = load_orders_and_fills(lines)
    rows = build_rows(orders_by_id, fills)

    with open(args.output_csv_file, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(HEADER)
        writer.writerows(rows)


if __name__ == "__main__":
    main()
