# Ethereum Address Clustering and Funding Source Analysis Tool

## Introduction

This tool helps you analyze Ethereum blockchain addresses to uncover connections and understand money flows. It offers two main analysis modes:

1.  **Forward-Clustering:** Identifies groups ("clusters") of users who might be using the same deposit address to send money to an exchange (like Binance, Coinbase, etc.). This is useful for understanding how multiple individuals might be sharing an intermediary address before funds reach a centralized exchange.

2.  **Backward-Clustering (Funding Source Analysis):** Traces the origins of funds received by a specific address, pinpointing transactions from known exchange wallets. This helps in understanding where an address has received money from, especially from exchanges.

This tool is valuable for blockchain research, anti-money laundering efforts, or simply gaining insights into transaction patterns on the Ethereum network.

---

## Features

* **Flexible Analysis Modes:** Choose between "Forward-Clustering" to find shared deposit addresses or "Backward-Clustering" to identify funding sources from exchanges.
* **Exchange Address Loading:** Automatically loads a list of known exchange addresses and their labels from a CSV file (`collected_addresses.csv`).
* **User Input:** Prompts you to enter an Ethereum address for analysis.
* **Comprehensive Transaction Fetching:** Retrieves both normal and internal transactions for a given address from the Etherscan API.
* **Intelligent Deposit Address Analysis (Forward-Clustering):**
    * Identifies all addresses to which the analyzed user sent money ("deposit addresses").
    * Checks if these deposit addresses are used by multiple senders.
    * Determines if these deposit addresses forward funds to a known exchange.
    * Filters out smart contracts (unless they are known exchanges) and high-activity addresses (e.g., popular services) to focus on individual deposit patterns.
    * Groups related addresses into "clusters" and presents detailed results, including transaction counts and total ETH amounts for each clustered user.
* **Funding Source Identification (Backward-Clustering):**
    * Analyzes incoming transactions to a target address.
    * Identifies and categorizes deposits originating from known exchange addresses.
    * Provides detailed metrics for each funding source, including transaction count, total amount, first/last seen timestamps, and an activity timeline.
* **Robust API Handling:** Implements retry mechanisms and delays to gracefully handle Etherscan API rate limits and errors.
* **Progress Tracking:** Displays progress bars during intensive analysis to keep you informed.
* **Clear and Detailed Output:** Presents analysis results in an organized and easy-to-understand format.

---

## How It Works (Step-by-Step)

### 1. **Load Exchange Addresses**
    * The program reads a CSV file (`collected_addresses.csv`) that lists known exchange addresses and their associated labels (e.g., "Binance"). This data is crucial for identifying exchange-related transactions.

### 2. **Select Analysis Mode**
    * You choose between "Forward-Clustering" or "Backward-Clustering" based on your research goal.

### 3. **Ask for User Input**
    * You're prompted to enter the Ethereum address you wish to analyze (a 42-character string starting with `0x`). You can analyze multiple addresses, one at a time.

### 4. **Fetch Transactions**
    * For the entered address, the tool downloads all normal and internal transactions from the Etherscan API, combining them into a single list for comprehensive analysis.

### 5. **Mode-Specific Analysis**

#### **Forward-Clustering (Mode 1)**
    * **Find Deposit Addresses:** The tool identifies all addresses to which the entered user address has sent money.
    * **Analyze Each Deposit Address:** For each identified deposit address, the tool performs the following:
        * Checks if it's a smart contract (and skips it unless it's a recognized exchange address).
        * Downloads all transactions associated with that deposit address.
        * Skips addresses with an excessive number of transactions (likely indicating a service rather than a personal deposit).
        * Identifies all unique senders to that deposit address (excluding exchanges and the deposit address itself).
        * Skips addresses with too many unique senders (again, to filter out large services).
        * Determines if the deposit address subsequently forwards funds to a known exchange.
        * If a "cluster" is identified (multiple users sending to a shared deposit address that then forwards to an exchange), this information is saved, including transaction counts and ETH amounts for each sender.

#### **Backward-Clustering (Mode 2)**
    * **Identify Incoming Transactions:** The tool examines all transactions where the entered address is the recipient.
    * **Pinpoint Exchange Sources:** It specifically identifies transactions where the sender is a known exchange address.
    * **Gather Funding Details:** For each identified funding source from an exchange, it aggregates data such as the number of transactions, total ETH received, and timestamps of the first and last interactions.

### 6. **Show Results**
    * The tool prints out the analysis results in a clear format:
        * **For Forward-Clustering:** Details on each cluster, including the deposit address, the linked exchange (with label), the associated user addresses (up to 10 with full address, transaction count, and total ETH), and the cluster size.
        * **For Backward-Clustering:** Information on each funding source from an exchange, including the exchange label, total transactions, total and average ETH amounts, first/last seen dates, and a visual activity bar.

### 7. **Repeat or Quit**
    * You can choose to analyze another address or type `quit` to exit the program.

---

## Program Flow (Visual)

```mermaid
graph TD;
    A[Start Program] --> B{Load Exchange Addresses\nfrom CSV};
    B --> C[Display Mode Options];
    C --> D{User Selects Mode};

    D -- "Mode 1: Forward-Clustering" --> E[Ask User for\nEthereum Address (User Address)];
    E --> F[Fetch All Transactions\nfor User Address];
    F --> G[Find All Deposit Addresses\n(where User sent money)];
    G --> H{For Each Deposit Address: Analyze};
    H -- "Is it a contract\nor high-activity?" --> I[Skip];
    H -- "No" --> J[Find Unique Senders\nto Deposit Address];
    J --> K{Does Deposit Forward\nto Exchange?};
    K -- "Yes & >1 Sender" --> L[Save as Cluster];
    K -- "No or <=1 Sender" --> M[Skip];
    L --> N[Show Forward-Clustering Results];
    M --> N;
    I --> N;

    D -- "Mode 2: Backward-Clustering" --> O[Ask User for\nEthereum Address (Target Address)];
    O --> P[Fetch All Transactions\nfor Target Address];
    P --> Q[Identify Incoming Transactions\nfrom Known Exchanges];
    Q --> R[Aggregate Funding Source Details];
    R --> S[Show Backward-Clustering Results];

    N --> T{Analyze Another\nAddress?};
    S --> T;
    T -- "Yes" --> C;
    T -- "No" --> U[End];
```

---

## Setup Instructions

1. **Install Python**
   - Make sure you have Python 3.7 or newer installed. You can download it from [python.org](https://www.python.org/downloads/).

2. **Install Required Libraries**
   - Open a terminal (Command Prompt or PowerShell on Windows).
   - Navigate to the folder with the script.
   - Run:
     ```bash
     pip install requests tenacity tqdm
     ```

3. **Prepare the CSV File**
   - Make sure you have a file called `collected_addresses.csv` in the same folder.
   - This file should have at least a column for addresses, and optionally labels or exchange names.

4. **Check Your API Key**
   - The script uses a default Etherscan API key. For heavy use, get your own free key from [Etherscan.io](https://etherscan.io/myapikey) and replace the value in the script.

---

## How to Use

1. Open a terminal and navigate to the folder with the script.
2. Run the script:
   ```bash
   python etherscan_deposit_clustering.py
   ```
3. When prompted, enter an Ethereum address.
4. Wait for the analysis to complete. The tool will show you any clusters it finds.
5. Enter another address or type `quit` to exit.

---

## License

This tool is for educational and research purposes. Use responsibly!
