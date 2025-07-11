import csv
import time
import json
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from functools import lru_cache

# Configuration
ETHERSCAN_API_KEY = 'N7DKBT416X6PBC33UAHE3G6B3479CBGFQQ'
ETHERSCAN_API_URL = 'https://api.etherscan.io/api'
CSV_FILE = 'collected_addresses.csv'
CACHE_DIR = Path('etherscan_cache')
CACHE_DIR.mkdir(exist_ok=True)
REQUEST_DELAY = 0.55
MAX_WORKERS = 1  
MAX_RESULTS = 1000  

def load_exchange_addresses(csv_file):
    """Load exchange addresses and their labels from CSV with robust error handling"""
    exchange_addresses = set()
    exchange_labels = dict()
    try:
        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("CSV file is empty or malformed")

            address_column = 'Address' if 'Address' in reader.fieldnames else 'address'
            label_column = 'Label' if 'Label' in reader.fieldnames else None
            name_column = 'Exchange Name' if 'Exchange Name' in reader.fieldnames else None
            for row in reader:
                if address := row.get(address_column, '').strip():
                    address = address.lower()
                    exchange_addresses.add(address)
                    label = row.get(label_column, '').strip() if label_column else ''
                    name = row.get(name_column, '').strip() if name_column else ''
                    # Prefer label, then name, then address
                    if label:
                        exchange_labels[address] = label
                    elif name:
                        exchange_labels[address] = name
                    else:
                        exchange_labels[address] = address
        print(f"✓ Loaded {len(exchange_addresses)} exchange addresses")
        return exchange_addresses, exchange_labels
    except Exception as e:
        print(f"✗ Failed to load CSV: {str(e)}")
        return set(), dict()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_etherscan_data(params):
    """Robust API fetcher with timeout and validation"""
    try:
        response = requests.get(ETHERSCAN_API_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if not isinstance(data, dict):
            raise ValueError("Invalid API response format")
            
        if data.get('status') != '1' and data.get('message') != 'No transactions found':
            # Print more details for debugging
            print(f"[Etherscan API Error] message: {data.get('message')}, result: {data.get('result')}, params: {params}")
            raise ValueError(data.get('message', 'Unknown API error'))
            
        return data
    except Exception as e:
        print(f"⚠️ API request failed for {params.get('action')}: {str(e)}")
        raise

def get_all_transactions(address, action):
    """Get complete transaction history with pagination, respecting Etherscan's 10,000 result window limit"""
    all_txs = []
    page = 1
    params = {
        'module': 'account',
        'action': action,
        'address': address,
        'sort': 'asc',
        'apikey': ETHERSCAN_API_KEY,
        'offset': MAX_RESULTS,
        'page': page
    }

    while True:
        # Etherscan: PageNo x Offset size must be <= 10000
        if page * MAX_RESULTS > 10000:
            print(f"⚠️ Etherscan pagination limit reached for {address} ({action}), only partial data fetched.")
            break
        try:
            data = fetch_etherscan_data(params)
            time.sleep(REQUEST_DELAY)
            
            if not (txs := data.get('result', [])):
                break
                
            all_txs.extend(txs)
            
            if len(txs) < MAX_RESULTS:
                break
                
            page += 1
            params['page'] = page
            
        except Exception as e:
            print(f"⚠️ Stopping transaction fetch due to: {str(e)}")
            break

    return all_txs

# Best-practice: cache contract checks to avoid redundant API calls
@lru_cache(maxsize=2048)
def is_contract(address):
    """Check if an address is a smart contract using Etherscan's getsourcecode API."""
    try:
        params = {
            'module': 'contract',
            'action': 'getsourcecode',
            'address': address,
            'apikey': ETHERSCAN_API_KEY
        }
        response = requests.get(ETHERSCAN_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('status') == '1' and data.get('result'):
            contract_info = data['result'][0]
            # If ContractName is not empty, it's a contract
            if contract_info.get('ContractName'):
                return True
        return False
    except Exception as e:
        print(f"⚠️ Failed to check contract status for {address}: {str(e)}")
        # If in doubt, treat as contract to avoid false positives
        return True

def analyze_deposit(deposit, exchange_set, sender_threshold=1000):
    """Analyze deposit address for clustering: multiple senders to deposit, deposit forwards to exchange"""
    # Best-practice: skip smart contracts unless they are in the exchange list
    if deposit not in exchange_set and is_contract(deposit):
        print(f"⏩ Skipping contract address {deposit}")
        return None
    try:
        if not deposit:
            return None
        print(f"⌛ Analyzing deposit: {deposit[:8]}...", end='\r')

        # Fetch all transactions for the deposit address
        normal_txs = get_all_transactions(deposit, 'txlist')
        internal_txs = get_all_transactions(deposit, 'txlistinternal')
        all_txs = normal_txs + internal_txs

        # Best-practice: skip addresses with >=10,000 transactions (pagination limit reached, likely a service)
        if len(all_txs) >= 10000:
            print(f"⏩ Skipping high-activity address {deposit} (>=10,000 transactions, likely a service)")
            return None

        # Find all unique senders to this deposit address (exclude self, exclude exchanges)
        incoming_senders = set()
        for tx in all_txs:
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            if tx_to == deposit and tx_from not in exchange_set and tx_from != deposit:
                incoming_senders.add(tx_from)

        # Behavioral heuristic: skip addresses with too many unique senders (likely a service)
        if len(incoming_senders) > sender_threshold:
            print(f"⏩ Skipping high-activity address {deposit[:8]} ({len(incoming_senders)} unique senders)")
            return None

        # Check if deposit forwards funds to a known exchange wallet
        forwarded_to_exchange = None
        for tx in all_txs:
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            if tx_from == deposit and tx_to in exchange_set:
                forwarded_to_exchange = tx_to
                break

        if forwarded_to_exchange and len(incoming_senders) > 1:
            print(f"✓ Found cluster at {deposit[:8]}".ljust(40))
            return {
                'deposit': deposit,
                'exchange': forwarded_to_exchange,
                'related_users': list(incoming_senders),
                'cluster_size': len(incoming_senders)
            }
    except Exception as e:
        print(f"⚠️ Failed to analyze {deposit[:8]}: {str(e)[:50]}".ljust(40))
    return None

def cluster_addresses(user_address, exchange_addresses):
    """Parallel clustering engine with progress tracking (new heuristic)"""
    user_address = user_address.lower()
    print(f"\n🔍 Analyzing {user_address}")

    # Get all transactions
    print("📥 Fetching transactions...")
    normal_txs = get_all_transactions(user_address, 'txlist')
    internal_txs = get_all_transactions(user_address, 'txlistinternal')
    all_txs = normal_txs + internal_txs

    if not all_txs:
        print("❌ No transactions found for this address")
        return []

    print(f"✅ Found {len(all_txs)} total transactions")

    # Extract deposit addresses (addresses the user sent ETH to)
    deposit_addresses = {
        tx['to'].lower()
        for tx in all_txs
        if tx.get('from', '').lower() == user_address and tx.get('to')
    }

    if not deposit_addresses:
        print("❌ No deposit addresses found")
        return []

    print(f"🔄 Found {len(deposit_addresses)} deposit addresses to analyze")

    clusters = []
    exchange_set = set(exchange_addresses)

    def analyze_with_progress(deposit, exchange_set):
        # Wrapper to ensure progress bar advances even if skipped
        return analyze_deposit(deposit, exchange_set)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                analyze_with_progress,
                deposit,
                exchange_set
            ): deposit for deposit in deposit_addresses
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="🔬 Analyzing deposits",
            unit="deposit",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
        ):
            if result := future.result():
                clusters.append(result)

    if not clusters:
        print("❌ No clusters identified")
        return []

    return sorted(clusters, key=lambda x: x['cluster_size'], reverse=True)

def display_results(clusters, exchange_labels=None):
    """Professional results presentation with exchange labels"""
    if not clusters:
        print("\n💡 No deposit clusters found")
        return
    print(f"\n🎯 Found {len(clusters)} clusters (showing top 10)")
    print("═" * 60)
    for i, cluster in enumerate(clusters[:10], 1):
        print(f"\n🏷️  Cluster #{i} (Size: {cluster['cluster_size']})")
        print(f"📍 Deposit: {cluster['deposit']}")
        exchange = cluster['exchange']
        # More explicit label logic: prefer Label, then Exchange Name, then address
        label = None
        if exchange_labels:
            # If the label is present and not equal to the address, use it
            label_candidate = exchange_labels.get(exchange.lower())
            if label_candidate and label_candidate != exchange:
                label = label_candidate
            else:
                label = None
        # If no label, fallback to Exchange Name (guaranteed present in CSV logic)
        if not label:
            # Try to get Exchange Name directly from the CSV mapping if possible
            # (Assume exchange_labels was built with Label or Exchange Name)
            label = exchange_labels.get(exchange.lower(), exchange)
        print(f"🏦 Exchange: {label} ({exchange})")
        print("\n👥 Related addresses:")
        for j, addr in enumerate(cluster['related_users'][:10], 1):
            print(f"  {j}. {addr}")
        if len(cluster['related_users']) > 10:
            print(f"  ... and {len(cluster['related_users']) - 10} more")
        print("─" * 40)

def main():
    print("\n" + "═" * 60)
    print("🔗 Ethereum Deposit Clustering Tool".center(60))
    print("═" * 60 + "\n")
    print("📂 Loading exchange addresses...")
    exchange_addresses, exchange_labels = load_exchange_addresses(CSV_FILE)
    if not exchange_addresses:
        print("❌ Critical: No exchange addresses loaded. Check CSV file.")
        return
    while True:
        user_address = input("\n🔢 Enter Ethereum address (or 'quit'): ").strip().lower()
        if user_address in ('quit', 'exit'):
            print("\n🛑 Session ended")
            break
        if not user_address.startswith('0x') or len(user_address) != 42:
            print("⚠️ Invalid Ethereum address format")
            continue
        print("\n" + "─" * 60)
        start_time = time.time()
        clusters = cluster_addresses(user_address, exchange_addresses)
        display_results(clusters, exchange_labels)
        elapsed = time.time() - start_time
        print(f"\n⏱️  Analysis completed in {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        print("─" * 60)

if __name__ == '__main__':
    main()