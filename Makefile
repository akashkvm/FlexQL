# ============================================================
# FlexQL — Makefile
# ============================================================
#
# Step 1: Build
#   make
#
# Step 2: Clean build files
#   make clean
#
# Step 3: Pick ONE run option below.
#         Comment out the two you don't need.
#         Then run:  make run
#
# ============================================================

CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O3 -flto -march=native -DFLEXQL_DISABLE_TIMING
INCLUDES := -Iinclude
PORT     := 9000

.PHONY: all clean test run run-unit run-1m run-10m

# ── Build all binaries ───────────────────────────────────────
all: bin/flexql-server bin/flexql-client bin/benchmark

bin/flexql-server: src/server/server.cpp include/flexql.h | bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $<
	@echo "[OK] Built bin/flexql-server"

bin/flexql-client: src/client/repl.cpp src/client/flexql_api.cpp include/flexql.h | bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ src/client/repl.cpp src/client/flexql_api.cpp
	@echo "[OK] Built bin/flexql-client"

bin/benchmark: tests/benchmark_flexql.cpp src/client/flexql_api.cpp include/flexql.h | bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ tests/benchmark_flexql.cpp src/client/flexql_api.cpp
	@echo "[OK] Built bin/benchmark"

bin:
	mkdir -p bin

# ── Clean ────────────────────────────────────────────────────
clean:
	rm -rf bin/ flexql_data/
	@echo "[OK] Cleaned"

# ============================================================
# RUN OPTIONS
#
# Uncomment EXACTLY ONE of the three lines below,
# keep the other two commented out.
# Then type:  make run
#
# OPTION 1 — unit tests only   (~5 seconds)
# RUN_TARGET = run-unit
#
# OPTION 2 — 1 million rows    (~60 seconds)
# RUN_TARGET = run-1m
#
# OPTION 3 — 10 million rows   (~10 minutes)
RUN_TARGET = run-10m
#
# ============================================================

# "make test" and "make run" both trigger the selected RUN_TARGET
test run: all
	@$(MAKE) $(RUN_TARGET)

# ── Unit tests only ──────────────────────────────────────────
run-unit: all
	@echo ""
	@echo "=============================="
	@echo "  FlexQL — Unit Tests"
	@echo "=============================="
	@./bin/flexql-server $(PORT) &                        \
	 SERVER_PID=$$!;                                      \
	 sleep 0.8;                                           \
	 FLEXQL_PORT=$(PORT) ./bin/benchmark --unit-test;     \
	 EXIT=$$?;                                            \
	 kill $$SERVER_PID 2>/dev/null;                       \
	 wait $$SERVER_PID 2>/dev/null;                       \
	 echo "=============================="; exit $$EXIT

# ── 1 million rows ───────────────────────────────────────────
run-1m: all
	@echo ""
	@echo "=============================="
	@echo "  FlexQL — 1 Million Rows"
	@echo "=============================="
	@./bin/flexql-server $(PORT) &                        \
	 SERVER_PID=$$!;                                      \
	 sleep 0.8;                                           \
	 FLEXQL_PORT=$(PORT) ./bin/benchmark 1000000;         \
	 EXIT=$$?;                                            \
	 kill $$SERVER_PID 2>/dev/null;                       \
	 wait $$SERVER_PID 2>/dev/null;                       \
	 echo "=============================="; exit $$EXIT

# ── 10 million rows ──────────────────────────────────────────
run-10m: all
	@echo ""
	@echo "=============================="
	@echo "  FlexQL — 10 Million Rows"
	@echo "=============================="
	@./bin/flexql-server $(PORT) &                        \
	 SERVER_PID=$$!;                                      \
	 sleep 0.8;                                           \
	 FLEXQL_PORT=$(PORT) ./bin/benchmark 10000000;        \
	 EXIT=$$?;                                            \
	 kill $$SERVER_PID 2>/dev/null;                       \
	 wait $$SERVER_PID 2>/dev/null;                       \
	 echo "=============================="; exit $$EXIT
