import json
from kafka import KafkaConsumer
from collections import defaultdict
import itertools
import numpy as np
import pandas as pd

# Constants
KAFKA_TOPIC = 'mock_l1_stream'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
ORDER_SIZE = 5000
VENUE_COUNT = 3  # Adjust if needed
STEP = 100
FEE = 0.0
REBATE = 0.0

# Parameter search ranges
LAMBDA_OVER_RANGE = [0.1, 0.4, 0.7]
LAMBDA_UNDER_RANGE = [0.2, 0.6, 1.0]
THETA_QUEUE_RANGE = [0.1, 0.3, 0.5]


def compute_cost(split, venues, order_size, lambda_over, lambda_under, theta_queue):
    executed = 0
    cash_spent = 0
    for i in range(len(venues)):
        exe = min(split[i], venues[i]['ask_size'])
        executed += exe
        cash_spent += exe * (venues[i]['ask'] + venues[i]['fee'])
        maker_rebate = max(split[i] - exe, 0) * venues[i]['rebate']
        cash_spent -= maker_rebate
    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    risk_pen = theta_queue * (underfill + overfill)
    cost_pen = lambda_under * underfill + lambda_over * overfill
    return cash_spent + risk_pen + cost_pen


def allocate(order_size, venues, lambda_over, lambda_under, theta_queue):
    splits = [[]]
    for v in range(len(venues)):
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, venues[v]['ask_size'])
            for q in range(0, max_v + 1, STEP):
                new_splits.append(alloc + [q])
        splits = new_splits
    best_cost = float('inf')
    best_split = []
    for alloc in splits:
        if sum(alloc) != order_size:
            continue
        cost = compute_cost(alloc, venues, order_size, lambda_over, lambda_under, theta_queue)
        if cost < best_cost:
            best_cost = cost
            best_split = alloc
    return best_split, best_cost


def run_backtest(snapshots, order_size, lambda_over, lambda_under, theta_queue, strategy='allocator'):
    """
    snapshots: list of (ts, [venue dicts])
    strategy: 'allocator', 'best_ask', 'twap', 'vwap'
    """
    unfilled = order_size
    total_cash = 0
    fills = []
    n_timestamps = len(snapshots)
    if strategy == 'twap':
        # Split order into equal portions over 60s intervals
        # Find the timestamps that are at least 60s apart
        ts_list = [ts for ts, _ in snapshots]
        ts_pd = pd.to_datetime(ts_list)
        intervals = [ts_pd[0]]
        for t in ts_pd[1:]:
            if (t - intervals[-1]).total_seconds() >= 60:
                intervals.append(t)
        n_intervals = len(intervals)
        portion = order_size // n_intervals if n_intervals > 0 else order_size
        interval_idx = 0
        for i, (ts, venues) in enumerate(snapshots):
            if unfilled <= 0:
                break
            t = ts_pd[i]
            if interval_idx < n_intervals and t >= intervals[interval_idx]:
                to_fill = min(portion, unfilled)
                # Fill greedily at best price
                venues_sorted = sorted(venues, key=lambda v: v['ask'])
                executed = 0
                cash = 0
                for v in venues_sorted:
                    fill = min(to_fill, v['ask_size'])
                    executed += fill
                    cash += fill * (v['ask'] + v['fee'])
                    to_fill -= fill
                    if to_fill <= 0:
                        break
                fills.append({'ts': ts, 'executed': executed, 'cash': cash})
                unfilled -= executed
                total_cash += cash
                interval_idx += 1
    elif strategy == 'vwap':
        # Allocate fills in proportion to ask_sz_00 at each timestamp
        for ts, venues in snapshots:
            if unfilled <= 0:
                break
            total_sz = sum(v['ask_size'] for v in venues)
            if total_sz == 0:
                continue
            executed = 0
            cash = 0
            for v in venues:
                alloc = int(order_size * (v['ask_size'] / total_sz))
                fill = min(alloc, v['ask_size'], unfilled - executed)
                executed += fill
                cash += fill * (v['ask'] + v['fee'])
                if executed >= unfilled:
                    break
            fills.append({'ts': ts, 'executed': executed, 'cash': cash})
            unfilled -= executed
            total_cash += cash
    else:
        for ts, venues in snapshots:
            if unfilled <= 0:
                break
            if strategy == 'allocator':
                split, _ = allocate(unfilled, venues, lambda_over, lambda_under, theta_queue)
                executed = 0
                cash = 0
                for i, alloc in enumerate(split):
                    fill = min(alloc, venues[i]['ask_size'])
                    executed += fill
                    cash += fill * (venues[i]['ask'] + venues[i]['fee'])
                fills.append({'ts': ts, 'executed': executed, 'cash': cash})
                unfilled -= executed
                total_cash += cash
            elif strategy == 'best_ask':
                venues_sorted = sorted(venues, key=lambda v: v['ask'])
                executed = 0
                cash = 0
                to_fill = unfilled
                for v in venues_sorted:
                    fill = min(to_fill, v['ask_size'])
                    executed += fill
                    cash += fill * (v['ask'] + v['fee'])
                    to_fill -= fill
                    if to_fill <= 0:
                        break
                fills.append({'ts': ts, 'executed': executed, 'cash': cash})
                unfilled -= executed
                total_cash += cash
    total_filled = order_size - unfilled
    avg_fill_px = total_cash / total_filled if total_filled > 0 else 0
    return {
        'total_cash': total_cash,
        'avg_fill_px': avg_fill_px,
        'fills': fills
    }


def get_snapshots_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    # Aggregate all messages by timestamp, then group into list of (ts, [venues])
    snapshots_by_ts = defaultdict(list)
    for message in consumer:
        snapshot = message.value
        ts = snapshot['ts_event']
        venue = {
            'ask': float(snapshot['ask_px_00']),
            'ask_size': int(snapshot['ask_sz_00']),
            'fee': FEE,
            'rebate': REBATE,
            'publisher_id': snapshot['publisher_id']
        }
        snapshots_by_ts[ts].append(venue)
        # Stop after enough data (optional: break after N timestamps)
        if len(snapshots_by_ts) > 1000:
            break
    # Convert to sorted list
    snapshots = sorted(snapshots_by_ts.items())
    return snapshots


def main():
    # 1. Get all snapshots from Kafka (or from a file/cache for speed)
    snapshots = get_snapshots_from_kafka()

    # 2. Parameter search (grid search)
    best_result = None
    best_params = None
    for lo, lu, tq in itertools.product(LAMBDA_OVER_RANGE, LAMBDA_UNDER_RANGE, THETA_QUEUE_RANGE):
        result = run_backtest(snapshots, ORDER_SIZE, lo, lu, tq, strategy='allocator')
        if best_result is None or result['total_cash'] < best_result['total_cash']:
            best_result = result
            best_params = {'lambda_over': lo, 'lambda_under': lu, 'theta_queue': tq}

    # 3. Baseline: Best Ask
    best_ask_result = run_backtest(snapshots, ORDER_SIZE, 0, 0, 0, strategy='best_ask')

    # 4. Baseline: TWAP
    twap_result = run_backtest(snapshots, ORDER_SIZE, 0, 0, 0, strategy='twap')

    # 5. Baseline: VWAP
    vwap_result = run_backtest(snapshots, ORDER_SIZE, 0, 0, 0, strategy='vwap')

    # 6. Compute savings in bps
    def bps(base, opt):
        return 10000 * (base - opt) / base if base else 0
    savings_vs_baselines_bps = {
        'best_ask': bps(best_ask_result['total_cash'], best_result['total_cash']),
        'twap': bps(twap_result['total_cash'], best_result['total_cash']),
        'vwap': bps(vwap_result['total_cash'], best_result['total_cash'])
    }

    # 7. Print JSON summary
    summary = {
        'best_parameters': best_params,
        'optimized': {
            'total_cash': best_result['total_cash'],
            'avg_fill_px': best_result['avg_fill_px']
        },
        'baselines': {
            'best_ask': {
                'total_cash': best_ask_result['total_cash'],
                'avg_fill_px': best_ask_result['avg_fill_px']
            },
            'twap': {
                'total_cash': twap_result['total_cash'],
                'avg_fill_px': twap_result['avg_fill_px']
            },
            'vwap': {
                'total_cash': vwap_result['total_cash'],
                'avg_fill_px': vwap_result['avg_fill_px']
            }
        },
        'savings_vs_baselines_bps': savings_vs_baselines_bps
    }
    print(json.dumps(summary, indent=2))

if __name__ == '__main__':
    main() 
