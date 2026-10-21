# Data Freshness Guarantees ⏱️

This guide outlines the data freshness model, caching tiers, maximum staleness guarantees, and cache-busting mechanisms across all StellarKit API endpoints. It is designed to help developers build real-time applications, balance freshness against latency, and choose the optimal polling or streaming strategy.

**See also:**
- [Performance Guide](performance.md) — latency expectations, cache tuning, and Horizon optimization
- [Caching Strategy](caching-strategy.md) — per-endpoint TTL rationale and memory management
- [Real-Time Guide](real-time-guide.md) — building real-time applications with SSE, WebSockets, and polling

---

## Table of Contents

1. [Architectural Overview](#1-architectural-overview)
2. [Data Freshness Categories](#2-data-freshness-categories)
3. [Master Data Freshness Reference Table](#3-master-data-freshness-reference-table)
4. [Bypassing the Cache with ?fresh=true](#4-bypassing-the-cache-with-freshtrue)
5. [Configuring TTL Environment Variables](#5-configuring-ttl-environment-variables)
6. [Polling Strategy Recommendations](#6-polling-strategy-recommendations)

---

## 1. Architectural Overview

StellarKit API acts as an intelligent intermediary between client applications and the upstream Stellar network (Horizon and Soroban RPC). 

```
+------------------+         +--------------------+         +---------------------+
|  Client / dApp   | ------> |   StellarKit API   | ------> |   Stellar Horizon   |
|                  |         |  (In-Memory Cache) |         |    & Soroban RPC    |
+------------------+         +--------------------+         +---------------------+
                                       |                               |
                              Served in < 5 ms               Ledger Close Cycle
                            (TTL bounded staleness)              (~5 seconds)
```

Data freshness in StellarKit is shaped by three key realities:

1. **Stellar Ledger Resolution:** Stellar closes a consensus ledger roughly every **5 seconds**. Data on the network does not update continuously in milliseconds; state shifts occur in discrete batches at each ledger close.
2. **In-Memory TTL Caching:** To prevent upstream Horizon rate-limiting and minimize latency (< 5 ms for cache hits), read-intensive endpoints are cached using an in-memory TTL store.
3. **Explicit Cache Invalidation:** Any cached endpoint supporting read queries provides a `?fresh=true` query parameter, giving callers immediate, authoritative access to live upstream data on demand.

---

## 2. Data Freshness Categories

### 2.1 Live / Uncached Endpoints (Zero Cache Staleness)
These endpoints interact directly with Horizon or Soroban RPC on every single invocation and never cache responses:
- **Transaction Submission:** `POST /transactions/submit` or `POST /transactions` — transactions are evaluated and submitted immediately to the Stellar consensus network.
- **Event & SSE Streams:** `/stream/*` — server-sent event (SSE) streams and WebSocket feeds stream ledger events in real time as Horizon ingests them.
- **Batch Processing Operations:** Endpoints with write or volatile state assertions bypass the cache to guarantee atomic state validation.

### 2.2 Short TTL Endpoints (High Volatility: ≤ 5–10 seconds)
These endpoints serve data that evolves with each ledger close or fluctuates with market forces:
- **Fee Estimates & Surge Status:** Changes as pending transaction queues expand or contract during high-load periods.
- **Base Fee & Network Status:** Reflects current consensus base fee and latest ledger sequence.
- **Asset Pricing & DEX Arbitrage:** Fast-moving order book spreads and price indices.
- **Single Trustline Balances:** Single asset balance lookups where precision is critical for payment workflows.

### 2.3 Medium TTL Endpoints (Moderate Volatility: 15–30 seconds)
These endpoints serve data that updates upon specific user-triggered actions:
- **Account Summaries & Balances:** Account balances, sequence numbers, and subentry counts.
- **Claimable Balances & Sponsor Balances:** Balances created, claimed, or clawed back.
- **Historical Ledger Effects:** Ledger effects are append-only and immutable once committed.
- **Account Signers & Freeze Status:** Auth flags and threshold configurations change only when explicit administrative transactions are submitted.
- **Soroban Instance Storage:** Smart contract persistent/instance storage entries.

### 2.4 Long TTL Endpoints (Low Volatility: 1–5 minutes)
These endpoints serve structural or aggregated data that remains stable over long periods:
- **Network Validators & Quorum Sets:** The validator set and quorum map change infrequently.
- **DEX Top Markets & Volume Leaders:** 24-hour volume rankings and historical market metrics.
- **Soroban Contract Functions (ABI):** Contract interfaces are immutable for a given WASM executable hash.
- **Stellar TOML Files:** Domain `.well-known/stellar.toml` metadata files are cached for 1 hour by default.

---

## 3. Master Data Freshness Reference Table

| Category | Endpoint(s) | Mode | Default TTL | Configurable Env Var | Supports `?fresh=true` | Max Staleness |
| :--- | :--- | :---: | :---: | :--- | :---: | :---: |
| **Network** | `GET /network-status` | Cached | 10 s | `CACHE_TTL_NETWORK_STATUS_MS` | ✅ Yes | 10 s |
| **Network** | `GET /network/base-fee` | Cached | 5 s | `CACHE_TTL_BASE_FEE_MS` | ✅ Yes | 5 s |
| **Network** | `GET /network/validators` | Cached | 300 s (5m) | `CACHE_TTL_VALIDATORS_MS` | ✅ Yes | 300 s |
| **Network** | `GET /network/validator-quorum` | Cached | 300 s (5m) | `CACHE_TTL_VALIDATORS_MS` | ✅ Yes | 300 s |
| **Network** | `GET /network/fee-percentiles` | Cached | 5 s | `CACHE_TTL_FEE_PERCENTILES_MS` | ✅ Yes | 5 s |
| **Fees** | `GET /fee-estimate` | Cached | 5 s | `CACHE_TTL_FEE_ESTIMATE_MS` | ✅ Yes | 5 s |
| **Fees** | `GET /fee-estimate/surge-status` | Cached | 5 s | `CACHE_TTL_FEE_ESTIMATE_MS` | ✅ Yes | 5 s |
| **Account** | `GET /account/:id` | Cached | 5 s | `CACHE_TTL_MS` (global fallback) | ✅ Yes | 5 s |
| **Account** | `GET /account/:id/summary` | Cached | 5 s | `CACHE_TTL_MS` (global fallback) | ✅ Yes | 5 s |
| **Account** | `GET /account/:id/sequence` | Cached | 20 s | `CACHE_TTL_SEQUENCE_MS` | ✅ Yes | 20 s |
| **Account** | `GET /account/:id/asset-balance/:code/:issuer` | Cached | 10 s | `CACHE_TTL_ASSET_BALANCE_MS` | ✅ Yes | 10 s |
| **Account** | `GET /account/:id/claimable-balances` | Cached | 20 s | `CACHE_TTL_CLAIMABLE_BALANCES_MS` | ✅ Yes | 20 s |
| **Account** | `GET /account/:id/signing-keys` | Cached | 20 s | `CACHE_TTL_SIGNING_KEYS_MS` | ✅ Yes | 20 s |
| **Account** | `GET /account/:id/freeze-status` | Cached | 30 s | `CACHE_TTL_FREEZE_CHECK_MS` | ✅ Yes | 30 s |
| **Account** | `GET /account/:id/effects` | Cached | 30 s | `CACHE_TTL_EFFECTS_MS` | ✅ Yes | 30 s |
| **Account** | `GET /account/:id/trades` | Cached | 5 s | `CACHE_TTL_TRADES_MS` | ✅ Yes | 5 s |
| **Account** | `GET /account/:id/pool-positions` | Cached | 15 s | `CACHE_TTL_POOL_POSITIONS_MS` | ✅ Yes | 15 s |
| **Account** | `GET /account/:id/liquidity-contribution` | Cached | 15 s | `CACHE_TTL_LIQUIDITY_CONTRIBUTION_MS` | ✅ Yes | 15 s |
| **Assets** | `GET /asset/:code/:issuer` | Cached | 30 s | `CACHE_TTL_ASSET_MS` | ✅ Yes | 30 s |
| **Assets** | `GET /asset/price` | Cached | 5 s | `CACHE_TTL_ASSET_PRICE_MS` | ✅ Yes | 5 s |
| **Assets** | `GET /assets/overview` | Cached | 30 s | `CACHE_TTL_ASSET_MS` | ✅ Yes | 30 s |
| **Liquidity Pools** | `GET /liquidity-pools/:id` | Cached | 15 s | `CACHE_TTL_POOL_POSITIONS_MS` | ✅ Yes | 15 s |
| **Liquidity Pools** | `GET /liquidity-pools/:id/trades` | Cached | 5 s | `CACHE_TTL_POOL_TRADES_MS` | ✅ Yes | 5 s |
| **DEX** | `GET /dex/arbitrage` | Cached | 5 s | `CACHE_TTL_ARBITRAGE_MS` | ✅ Yes | 5 s |
| **DEX** | `GET /dex/top-markets` | Cached | 60 s | `CACHE_TTL_TOP_MARKETS_MS` | ✅ Yes | 60 s |
| **DEX** | `GET /dex/volume-leaders` | Cached | 300 s (5m) | `CACHE_TTL_VOLUME_LEADERS_MS` | ✅ Yes | 300 s |
| **Soroban** | `GET /soroban/contract/:id/storage` | Cached | 15 s | `CACHE_TTL_CONTRACT_STORAGE_MS` | ✅ Yes | 15 s |
| **Soroban** | `GET /soroban/contract/:id/functions` | Cached | 60 s | `CACHE_TTL_CONTRACT_FUNCTIONS_MS` | ✅ Yes | 60 s |
| **TOML** | `GET /stellar-toml/:domain` | Cached | 3600 s (1h)| N/A (Internal HTTP Cache) | ✅ Yes | 3600 s |
| **Transactions** | `POST /transactions/submit` | **Live** | 0 s (None) | None | N/A | **0 s (Real-time)** |
| **Streams** | `GET /stream/*` | **Live** | 0 s (None) | None | N/A | **0 s (Push)** |

---

## 4. Bypassing the Cache with ?fresh=true

When your application requires strictly zero-staleness verification (such as directly before signing a payment or validating a multi-sig threshold), append `?fresh=true` to any cached endpoint:

```bash
# Standard lookup (may return cached response if requested within 10s)
curl "https://api.stellarkit.io/account/GABCD.../asset-balance/USDC/GA5Z...?"

# Forced live lookup (bypasses cache and updates cached entry with fresh data)
curl "https://api.stellarkit.io/account/GABCD.../asset-balance/USDC/GA5Z...?fresh=true"
```

### Inspecting Freshness via Response Headers
StellarKit returns caching diagnostic headers on all requests:
- `X-Cache: HIT` — Response was resolved directly from the local in-memory cache without hitting Horizon.
- `X-Cache: MISS` — Response required a live upstream fetch from Horizon or Soroban RPC.
- `X-Response-Time: 3.2ms` — Execution duration. Cache hits typically complete in < 5 ms.

---

## 5. Configuring TTL Environment Variables

Every cache TTL can be customized in milliseconds via deployment environment variables. If an individual variable is not set, StellarKit falls back to the specified default or the global `CACHE_TTL_MS` fallback (5,000 ms).

```env
# Global fallback for unconfigured cached endpoints
CACHE_TTL_MS=5000

# High-frequency market data
CACHE_TTL_FEE_ESTIMATE_MS=3000
CACHE_TTL_BASE_FEE_MS=3000
CACHE_TTL_ASSET_PRICE_MS=2000
CACHE_TTL_ARBITRAGE_MS=2000

# Account state
CACHE_TTL_NETWORK_STATUS_MS=10000
CACHE_TTL_CLAIMABLE_BALANCES_MS=15000
CACHE_TTL_EFFECTS_MS=30000
CACHE_TTL_SIGNING_KEYS_MS=15000
CACHE_TTL_FREEZE_CHECK_MS=20000

# Long-lived metadata
CACHE_TTL_VALIDATORS_MS=600000
CACHE_TTL_CONTRACT_FUNCTIONS_MS=120000
```

---

## 6. Polling Strategy Recommendations

To build resilient and responsive applications without saturating your API quotas, follow these patterns:

| App Type | Recommended Pattern | Rationale |
| :--- | :--- | :--- |
| **Trading & Arbitrage** | Stream order books via WebSocket/SSE; poll `/fee-estimate` every 5 s. | Market spreads change rapidly, but base network fees only advance with ledgers (~5 s). |
| **Wallet & Balances** | Poll `/account/:id` every 10–15 s; call `?fresh=true` immediately before transaction submission. | Users do not need sub-second balance refreshes during idle periods, but pre-flight submissions require verified sequence numbers. |
| **Explorer & Indexers** | Consume `/stream/ledgers` or `/stream/transactions` sequentially using cursors. | Push streaming avoids missing transactions and eliminates polling latency entirely. |
| **Compliance Monitoring** | Query `/account/:id/freeze-status` with standard TTL; bypass with `?fresh=true` only upon high-value risk triggers. | Issuer freeze flags are set infrequently; aggressive polling generates redundant RPC traffic. |
