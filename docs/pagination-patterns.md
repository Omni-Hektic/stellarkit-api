# Advanced Pagination Patterns 📜

This guide outlines advanced pagination architectures for high-volume data fetching, historical reconciliation pipelines, and large-scale data ingestion across StellarKit API endpoints.

**See also:**
- [Response Format Guide](response-format.md) — standard pagination envelope (`meta`, `cursor`, `nextCursor`, `hasMore`)
- [Data Freshness Guarantees](data-freshness.md) — cache TTLs, live vs cached data, and ?fresh=true
- [Rate Limiting Strategies](rate-limiting-strategies.md) — request batching and backoff algorithms

---

## Table of Contents

1. [The Cursor Pagination Model](#1-the-cursor-pagination-model)
2. [Sequential Pagination with Cursor Resumption](#2-sequential-pagination-with-cursor-resumption)
3. [Parallel Pagination Across Multiple Accounts](#3-parallel-pagination-across-multiple-accounts)
4. [Detecting Stream Termination (Last Page Invariants)](#4-detecting-stream-termination-last-page-invariants)
5. [Recommended Page Sizes by Use Case](#5-recommended-page-sizes-by-use-case)
6. [Complete Production Example: Streaming Account Transactions](#6-complete-production-example-streaming-account-transactions)

---

## 1. The Cursor Pagination Model

StellarKit uses opaque, deterministic cursors derived from Stellar ledger sequences and operation order indices rather than integer offsets (e.g., `OFFSET 100`).

```
Request:  GET /account/:id/transactions?limit=50&order=asc
Response: {
  "success": true,
  "data": [ ... 50 records ... ],
  "meta": {
    "count": 50,
    "limit": 50,
    "order": "asc",
    "cursor": "1284920492840",
    "nextCursor": "1284920495932",
    "hasMore": true
  }
}
```

### Why Cursor-Based Pagination?
- **Immunity to Drift:** Adding or removing records does not shift page windows or cause duplicate entries.
- **Constant Performance (O(1)):** Lookups on indexed Horizon cursors take constant time regardless of whether you are reading record 10 or record 10,000,000.
- **Restartability:** A cursor is a self-contained bookmark that can be persisted to disk or a database to resume execution at any time.

---

## 2. Sequential Pagination with Cursor Resumption

For ETL tasks, backup processes, or audit logs, jobs must survive network disconnections and process restarts without re-ingesting previous data.

### Checkpointing Pattern
1. Ingest page $N$.
2. Process/persist records in a transactional boundary.
3. Save `nextCursor` to persistent storage (e.g., PostgreSQL, Redis, or local checkpoint file).
4. Request page $N+1$ using `?cursor=${savedCursor}`.

```
+------------+       GET ?cursor=C0        +----------------+
| Ingestion  | --------------------------> | StellarKit API |
|   Worker   | <-------------------------- |                |
+------------+      Page 1 + nextCursor C1 +----------------+
      |
      | 1. Process Batch
      | 2. Persist Checkpoint(C1)
      v
+------------+       GET ?cursor=C1        +----------------+
|  Database  | --------------------------> | StellarKit API |
| Checkpoint | <-------------------------- |                |
+------------+      Page 2 + nextCursor C2 +----------------+
```

---

## 3. Parallel Pagination Across Multiple Accounts

When synchronizing transaction history for hundreds or thousands of institutional wallets, paginating sequentially across all accounts in a single thread creates massive latency.

### Concurrency Throttling Pattern
Run parallel pagination streams per account, bounded by a concurrency pool (e.g., `p-limit` or worker queues) to respect StellarKit rate limits:

```typescript
import pLimit from 'p-limit';

const CONCURRENT_ACCOUNTS = 5;
const limit = pLimit(CONCURRENT_ACCOUNTS);

async function syncAllAccounts(accountIds: string[]) {
  const tasks = accountIds.map(accountId => 
    limit(() => streamAccountToCompletion(accountId))
  );
  await Promise.all(tasks);
}
```

**Key Safeguards:**
- Use distinct cursor states per account.
- Spread requests across worker threads or separate client instances.
- Back off exponentially if an HTTP `429 Too Many Requests` is returned.

---

## 4. Detecting Stream Termination (Last Page Invariants)

To reliably detect when pagination has reached the end of a dataset, evaluate both metadata flags and record count:

| Check | Condition | Meaning |
| :--- | :--- | :--- |
| **Primary Check** | `meta.hasMore === false` | Upstream has confirmed no further records exist in this direction. |
| **Cursor Check** | `meta.nextCursor === null` | No subsequent pagination token is available. |
| **Payload Check** | `data.length === 0` | Empty page returned; stream is completely drained. |
| **Sub-Limit Check** | `data.length < meta.limit` | Received fewer records than requested; reached terminal page. |

### Idiomatic Termination Logic:
```javascript
const isLastPage = !meta.hasMore || meta.nextCursor === null || data.length === 0;
if (isLastPage) {
  // Exit pagination loop
  break;
}
```

---

## 5. Recommended Page Sizes by Use Case

| Use Case | Recommended `limit` | Rationale |
| :--- | :---: | :--- |
| **Interactive UI / Mobile Apps** | `10 – 25` | Minimizes response payload size (< 15 KB), renders instantaneously, and conserves mobile data. |
| **Background Account Sync** | `50 – 100` | Optimal balance between network overhead and memory consumption per page. |
| **Bulk ETL & Compliance Data Lake** | `200` (Max) | Minimizes round-trip HTTP overhead when extracting hundreds of thousands of historical records. |
| **Real-Time Polling for New Items** | `10` | Only checking for recently arrived entries since last cursor; smaller payloads reduce JSON parsing costs. |

---

## 6. Complete Production Example: Streaming Account Transactions

The following complete Node.js utility implements an async generator that streams all transactions for a given account. It features automatic cursor progression, checkpoint resumption, and exponential backoff retry handling.

```javascript
/**
 * stream-transactions.js
 * 
 * Production async generator streaming all transactions for an account.
 */

const axios = require('axios');

/**
 * Async generator yielding individual transactions page by page.
 * 
 * @param {string} baseUrl - StellarKit API root URL (e.g. 'http://localhost:3000')
 * @param {string} accountId - Stellar Public Key (G...)
 * @param {object} options - Pagination options
 * @param {string} [options.initialCursor] - Optional checkpoint cursor to resume from
 * @param {number} [options.pageSize=100] - Records per page (1-200)
 * @param {string} [options.order='asc'] - Sort order ('asc' for oldest first, 'desc' for newest)
 */
async function* iterateAccountTransactions(baseUrl, accountId, options = {}) {
  const {
    initialCursor = null,
    pageSize = 100,
    order = 'asc',
    maxRetries = 5,
  } = options;

  let currentCursor = initialCursor;
  let hasMore = true;

  while (hasMore) {
    const params = {
      limit: pageSize,
      order: order,
    };
    if (currentCursor) {
      params.cursor = currentCursor;
    }

    let pageData = null;
    let attempt = 0;

    // Retry loop with exponential backoff
    while (attempt < maxRetries) {
      try {
        const response = await axios.get(`${baseUrl}/account/${accountId}/transactions`, {
          params,
          timeout: 10000,
        });

        if (response.data && response.data.success) {
          pageData = response.data;
          break;
        } else {
          throw new Error(response.data?.error?.message || 'Unknown API failure');
        }
      } catch (err) {
        attempt++;
        const status = err.response?.status;
        const delay = Math.pow(2, attempt) * 500 + Math.random() * 200;

        if (status === 429 || (status >= 500 && status < 600)) {
          console.warn(`[Pagination] Transient error ${status}. Retrying in ${Math.round(delay)}ms...`);
          await new Promise(res => setTimeout(res, delay));
        } else {
          throw err; // Non-retryable error (e.g. 400 Bad Request, 404 Account Not Found)
        }
      }
    }

    if (!pageData) {
      throw new Error(`Failed to fetch page after ${maxRetries} attempts.`);
    }

    const { data: records, meta } = pageData;

    // Yield records one by one to caller
    for (const record of records) {
      yield {
        record,
        cursor: record.paging_token || record.cursor,
      };
    }

    // Determine termination conditions
    if (!meta.hasMore || !meta.nextCursor || records.length === 0 || records.length < pageSize) {
      hasMore = false;
    } else {
      currentCursor = meta.nextCursor;
    }
  }
}

// === Runner / Consumer Example ===
async function main() {
  const API_URL = process.env.STELLARKIT_URL || 'http://localhost:3000';
  const ACCOUNT_ID = 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';

  console.log(`Starting pagination stream for: ${ACCOUNT_ID}`);
  let count = 0;
  let lastCursor = null;

  try {
    for await (const { record, cursor } of iterateAccountTransactions(API_URL, ACCOUNT_ID, {
      pageSize: 100,
      order: 'asc',
    })) {
      count++;
      lastCursor = cursor;

      if (count % 100 === 0) {
        console.log(`Processed ${count} transactions... Last checkpoint cursor: ${lastCursor}`);
      }
    }

    console.log(`\n✅ Completed stream! Total transactions ingested: ${count}`);
    console.log(`Final checkpoint cursor: ${lastCursor}`);
  } catch (error) {
    console.error('Fatal streaming error:', error.message);
  }
}

if (require.main === module) {
  main();
}

module.exports = { iterateAccountTransactions };
```
