# FlexQL — Persistent SQL-like Database Engine

## Project Structure

```
flexql/
├── Makefile                        ← build + run targets
├── README.md                       ← this file
├── FlexQL_Design_Document.docx     ← submission design document
├── include/
│   └── flexql.h                    ← public API header (opaque handle + 4 functions)
├── src/
│   ├── server/
│   │   └── server.cpp              ← database engine (WAL + HeapFile + executor)
│   └── client/
│       ├── flexql_api.cpp          ← client API implementation (TCP + wire protocol)
│       └── repl.cpp                ← interactive REPL terminal
└── tests/
    └── benchmark_flexql.cpp        ← benchmark + unit tests (PUT benchmark.zip FILE HERE)
```

## Quick Start

### 1. Build
```bash
make
```

### 2. Run — choose ONE target:
```bash
make run-unit    # unit tests only     (~5 seconds)
make run-1m      # 1 million rows      (~60 seconds)
make run-10m     # 10 million rows     (~10 minutes)
```

### 3. Interactive client (optional)
```bash
# Terminal 1
./bin/flexql-server 9000

# Terminal 2
./bin/flexql-client 127.0.0.1 9000
```

## Benchmark File Setup

**Use the file:** `benchmark.zip` (the latest version provided)

**Extract and copy ONE file** into your project:
```
benchmark.zip → FlexQL_Benchmark_Unit_Tests-main/benchmark_flexql.cpp
                              ↓
                copy to → flexql/tests/benchmark_flexql.cpp
```
Then run `make` again to rebuild the benchmark binary.

## Persistence

Every run creates `flexql_data/` containing:
- `wal.log`        — Write-Ahead Log (binary, group-committed every 512 ops)
- `<TABLE>.heap`   — Per-table binary row store

Run `make clean` to wipe both build artifacts and data files.
