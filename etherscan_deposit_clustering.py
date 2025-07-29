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

# Diese Funktion lädt Exchange-Adressen und deren Labels aus einer CSV-Datei.
# Sie öffnet die Datei, prüft die Spaltennamen (z.B. 'Address', 'Label', 'Exchange Name'),
# liest jede Zeile ein, wandelt die Adressen in Kleinbuchstaben um und speichert sie in einer Menge.
# Für jede Adresse wird außerdem ein Label (Name der Exchange oder Adresse selbst) gespeichert.
# Am Ende gibt sie die Menge aller Adressen und ein Dictionary mit Labels zurück.

def load_exchange_addresses(csv_file):
    """Load exchange addresses and their labels from CSV with robust error handling"""
    exchange_addresses = set()
    exchange_labels = dict()
    try:
        # Öffne die CSV-Datei und lese sie zeilenweise ein
        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("CSV file is empty or malformed")

            # Prüfe, wie die Spalten heißen
            address_column = 'Address' if 'Address' in reader.fieldnames else 'address'
            label_column = 'Label' if 'Label' in reader.fieldnames else None
            name_column = 'Exchange Name' if 'Exchange Name' in reader.fieldnames else None
            for row in reader:
                # Hole die Adresse aus der Zeile
                if address := row.get(address_column, '').strip():
                    address = address.lower()
                    exchange_addresses.add(address)
                    # Hole das Label oder den Namen, falls vorhanden
                    label = row.get(label_column, '').strip() if label_column else ''
                    name = row.get(name_column, '').strip() if name_column else ''
                    # Bevorzuge Label, dann Name, dann Adresse selbst
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

# Diese Funktion ruft Daten von der Etherscan-API ab.
# Sie baut die Anfrage mit den übergebenen Parametern und schickt sie an die API.
# Wenn die Antwort nicht wie erwartet ist oder ein Fehler auftritt, wird die Anfrage bis zu 3 Mal wiederholt (mit Wartezeit dazwischen).
# Sie prüft, ob die Antwort ein Dictionary ist und ob der Status stimmt.
# Bei Fehlern gibt sie eine Fehlermeldung aus und wirft eine Exception.

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_etherscan_data(params):
    """Robust API fetcher with timeout and validation"""
    try:
        # Sende die Anfrage an die Etherscan-API
        response = requests.get(ETHERSCAN_API_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # Prüfe, ob die Antwort ein Dictionary ist
        if not isinstance(data, dict):
            raise ValueError("Invalid API response format")
            
        # Prüfe, ob der Status stimmt oder keine Transaktionen gefunden wurden
        if data.get('status') != '1' and data.get('message') != 'No transactions found':
            # Mehr Details für Debugging ausgeben
            print(f"[Etherscan API Error] message: {data.get('message')}, result: {data.get('result')}, params: {params}")
            raise ValueError(data.get('message', 'Unknown API error'))
            
        return data
    except Exception as e:
        print(f"⚠️ API request failed for {params.get('action')}: {str(e)}")
        raise


# Diese Funktion holt alle Transaktionen (normal und intern) für eine Adresse.
# Sie arbeitet mit Seiten (Pagination), weil Etherscan pro Anfrage maximal 1000 Ergebnisse liefert.
# Sie fragt so lange neue Seiten ab, bis weniger als 1000 Transaktionen zurückkommen oder das 10.000er-Limit erreicht ist.
# Alle Transaktionen werden in einer Liste gesammelt und am Ende zurückgegeben.
def get_all_transactions(address, action):
    """Get complete transaction history with pagination, respecting Etherscan's 10,000 result window limit"""
    all_txs = []
    page = 1
    # Baue die Parameter für die API-Anfrage
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
        # Prüfe, ob das 10.000er-Limit erreicht ist
        if page * MAX_RESULTS > 10000:
            print(f"⚠️ Etherscan pagination limit reached for {address} ({action}), only partial data fetched.")
            break
        try:
            # Hole die Transaktionen für die aktuelle Seite
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

# Diese Funktion prüft, ob eine Adresse ein Smart Contract ist.
# Sie fragt bei Etherscan nach dem Quellcode der Adresse.
# Wenn ein ContractName vorhanden ist, gilt die Adresse als Smart Contract.
# Die Ergebnisse werden zwischengespeichert (Cache), damit nicht mehrfach dieselbe Adresse geprüft wird.
# Bei Fehlern wird vorsichtshalber angenommen, dass es ein Contract ist.

@lru_cache(maxsize=2048)
def is_contract(address):
    """Check if an address is a smart contract using Etherscan's getsourcecode API."""
    try:
        # Baue die Anfrage für den Contract-Check
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
            # Wenn ContractName nicht leer ist, ist es ein Contract
            if contract_info.get('ContractName'):
                return True
        return False
    except Exception as e:
        print(f"⚠️ Failed to check contract status for {address}: {str(e)}")
        # Im Zweifel als Contract behandeln
        return True

# Diese Funktion analysiert eine Deposit-Adresse, um mögliche Cluster zu finden.
# Sie überspringt Smart Contracts (außer sie sind als Exchange bekannt).
# Sie holt alle Transaktionen (normal und intern) für die Adresse.
# Wenn es zu viele Transaktionen gibt (>=10.000), wird die Adresse übersprungen (wahrscheinlich Service/Exchange).
# Sie sammelt alle eindeutigen Absender, die an diese Adresse eingezahlt haben (außer Exchanges und die Adresse selbst).
# Wenn es zu viele verschiedene Absender gibt (> sender_threshold), wird die Adresse übersprungen.
# Dann prüft sie, ob die Adresse Geld an eine bekannte Exchange weitergeleitet hat.
# Wenn ja und es mehr als einen Absender gibt, wird ein Cluster mit allen Infos zurückgegeben.

def analyze_deposit(deposit, exchange_set, sender_threshold=1000):
    """Analyze deposit address for clustering: multiple senders to deposit, deposit forwards to exchange"""
    # Überspringe Smart Contracts, außer sie sind als Exchange bekannt
    if deposit not in exchange_set and is_contract(deposit):
        print(f"⏩ Skipping contract address {deposit}")
        return None
    try:
        if not deposit:
            return None
        print(f"⌛ Analyzing deposit: {deposit[:8]}...", end='\r')

        # Hole alle Transaktionen für die Deposit-Adresse
        normal_txs = get_all_transactions(deposit, 'txlist')
        internal_txs = get_all_transactions(deposit, 'txlistinternal')
        all_txs = normal_txs + internal_txs

        # Überspringe Adressen mit zu vielen Transaktionen
        if len(all_txs) >= 10000:
            print(f"⏩ Skipping high-activity address {deposit} (>=10,000 transactions, likely a service)")
            return None

        # Sammle alle eindeutigen Absender, die an diese Adresse eingezahlt haben
        incoming_senders = set()
        for tx in all_txs:
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            # Nur echte Einzahlungen von Nicht-Exchanges und nicht von sich selbst
            if tx_to == deposit and tx_from not in exchange_set and tx_from != deposit:
                incoming_senders.add(tx_from)

        # Überspringe Adressen mit zu vielen verschiedenen Absendern
        if len(incoming_senders) > sender_threshold:
            print(f"⏩ Skipping high-activity address {deposit[:8]} ({len(incoming_senders)} unique senders)")
            return None

        # Prüfe, ob die Deposit-Adresse Geld an eine bekannte Exchange weitergeleitet hat
        forwarded_to_exchange = None
        for tx in all_txs:
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            if tx_from == deposit and tx_to in exchange_set:
                forwarded_to_exchange = tx_to
                break

        # Wenn ein Cluster gefunden wurde, gib die Infos zurück
        if forwarded_to_exchange and len(incoming_senders) > 1:
            print(f"✓ Found cluster at {deposit}".ljust(40))
            return {
                'deposit': deposit,
                'exchange': forwarded_to_exchange,
                'related_users': list(incoming_senders),
                'cluster_size': len(incoming_senders)
            }
    except Exception as e:
        print(f"⚠️ Failed to analyze {deposit[:8]}: {str(e)[:50]}".ljust(40))
    return None

# Diese Funktion sucht für eine Nutzeradresse nach möglichen Clustern.
# Sie holt alle Transaktionen der Nutzeradresse und extrahiert alle Adressen, an die der Nutzer Geld geschickt hat (Deposits).
# Für jede dieser Deposit-Adressen wird (ggf. parallel) geprüft, ob sie ein Cluster bildet (siehe analyze_deposit).
# Das Ganze läuft mit Fortschrittsbalken, damit man sieht, wie weit die Analyse ist.
# Am Ende werden alle gefundenen Cluster nach Größe sortiert zurückgegeben.

def cluster_addresses(user_address, exchange_addresses):
    """Parallel clustering engine with progress tracking (new heuristic)"""
    user_address = user_address.lower()
    print(f"\n🔍 Analyzing {user_address}")

    # Hole alle Transaktionen der Nutzeradresse
    print("📥 Fetching transactions...")
    normal_txs = get_all_transactions(user_address, 'txlist')
    internal_txs = get_all_transactions(user_address, 'txlistinternal')
    all_txs = normal_txs + internal_txs

    if not all_txs:
        print("❌ No transactions found for this address")
        return []

    print(f"✅ Found {len(all_txs)} total transactions")

    # Extrahiere alle Adressen, an die der Nutzer Geld geschickt hat
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
        # Wrapper, damit der Fortschrittsbalken auch bei übersprungenen Adressen weiterläuft
        return analyze_deposit(deposit, exchange_set)

    # Analysiere alle Deposit-Adressen (ggf. parallel, je nach MAX_WORKERS)
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

    # Sortiere die Cluster nach Größe (absteigend)
    return sorted(clusters, key=lambda x: x['cluster_size'], reverse=True)

# Diese Funktion zeigt die gefundenen Cluster übersichtlich an.
# Für jeden Cluster werden die Größe, die Deposit-Adresse, die zugehörige Exchange (mit Label, falls vorhanden)
# und die ersten 10 zugehörigen Nutzeradressen ausgegeben.
# Wenn es mehr als 10 Nutzer gibt, wird das ebenfalls angezeigt.

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
        # Bestimme das Label der Exchange, falls vorhanden
        label = None
        if exchange_labels:
            label_candidate = exchange_labels.get(exchange.lower())
            if label_candidate and label_candidate != exchange:
                label = label_candidate
            else:
                label = None
        if not label:
            label = exchange_labels.get(exchange.lower(), exchange)
        print(f"🏦 Exchange: {label} ({exchange})")
        print("\n👥 Related addresses:")
        for j, addr in enumerate(cluster['related_users'][:10], 1):
            print(f"  {j}. {addr}")
        if len(cluster['related_users']) > 10:
            print(f"  ... and {len(cluster['related_users']) - 10} more")
        print("─" * 40)

# Das ist die Hauptfunktion des Programms.
# Sie lädt die Exchange-Adressen und Labels, fragt den Nutzer nach einer Ethereum-Adresse,
# startet die Analyse (Cluster-Suche) und zeigt die Ergebnisse an.
# Das Ganze läuft in einer Schleife, bis der Nutzer 'quit' eingibt.

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
        # Frage den Nutzer nach einer Ethereum-Adresse
        user_address = input("\n🔢 Enter Ethereum address (or 'quit'): ").strip().lower()
        if user_address in ('quit', 'exit'):
            print("\n🛑 Session ended")
            break
        if not user_address.startswith('0x') or len(user_address) != 42:
            print("⚠️ Invalid Ethereum address format")
            continue
        print("\n" + "─" * 60)
        start_time = time.time()
        # Starte die Cluster-Analyse
        clusters = cluster_addresses(user_address, exchange_addresses)
        # Zeige die Ergebnisse an
        display_results(clusters, exchange_labels)
        elapsed = time.time() - start_time
        print(f"\n⏱️  Analysis completed in {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        print("─" * 60)

if __name__ == '__main__':
    main()