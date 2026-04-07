/* ============================================================
 * repl.cpp — FlexQL Interactive Terminal
 *
 * Displays SELECT results in MySQL tabular format:
 *
 *   +----+----------+---------------------------+---------+
 *   | ID | NAME     | EMAIL                     | BALANCE |
 *   +----+----------+---------------------------+---------+
 *   |  1 | user1    | user1@mail.com             | 1001.00 |
 *   |  2 | user2    | user2@mail.com             | 1002.00 |
 *   +----+----------+---------------------------+---------+
 *   2 rows in set (1 ms)
 *
 * Non-SELECT operations print:
 *   Query OK (0 ms)
 * ============================================================ */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <chrono>

#include "../../include/flexql.h"

/* ============================================================
 * Result collector — accumulates all rows for tabular display
 * ============================================================ */
struct ResultSet {
    std::vector<std::string>              col_names;
    std::vector<std::vector<std::string>> rows;
    bool headers_set = false;
};

/* Callback: fired once per result row by flexql_exec */
static int collect_callback(void* data, int argc,
                             char** argv, char** col_names)
{
    ResultSet* rs = static_cast<ResultSet*>(data);

    /* First row — capture column names */
    if (!rs->headers_set) {
        rs->col_names.clear();
        for (int i = 0; i < argc; i++)
            rs->col_names.push_back(col_names[i] ? col_names[i] : "");
        rs->headers_set = true;
    }

    std::vector<std::string> row;
    row.reserve(argc);
    for (int i = 0; i < argc; i++)
        row.push_back(argv[i] ? argv[i] : "NULL");
    rs->rows.push_back(std::move(row));
    return 0;
}

/* ============================================================
 * Print one horizontal border line, e.g.:
 *   +------+----------+--------+
 * ============================================================ */
static void print_border(const std::vector<int>& widths) {
    printf("+");
    for (int w : widths) {
        for (int i = 0; i < w + 2; i++) printf("-");
        printf("+");
    }
    printf("\n");
}

/* ============================================================
 * Render a complete ResultSet in MySQL tabular format
 * ============================================================ */
static void print_table(const ResultSet& rs, long long elapsed_ms) {
    int ncols = (int)rs.col_names.size();
    if (ncols == 0) return;

    /* Calculate column widths: max(header, data) */
    std::vector<int> widths(ncols, 0);
    for (int i = 0; i < ncols; i++)
        widths[i] = (int)rs.col_names[i].size();

    for (const auto& row : rs.rows) {
        for (int i = 0; i < ncols && i < (int)row.size(); i++)
            widths[i] = std::max(widths[i], (int)row[i].size());
    }

    /* Top border */
    print_border(widths);

    /* Header row */
    printf("|");
    for (int i = 0; i < ncols; i++) {
        printf(" %-*s |", widths[i], rs.col_names[i].c_str());
    }
    printf("\n");

    /* Header / data separator */
    print_border(widths);

    /* Data rows */
    for (const auto& row : rs.rows) {
        printf("|");
        for (int i = 0; i < ncols; i++) {
            const char* val = (i < (int)row.size()) ? row[i].c_str() : "";
            /* Right-align if purely numeric, left-align otherwise */
            bool numeric = true;
            for (const char* p = val; *p; p++)
                if (!std::isdigit((unsigned char)*p) &&
                    *p != '.' && *p != '-' && *p != '+')
                    { numeric = false; break; }
            if (*val == '\0') numeric = false;
            if (numeric)
                printf(" %*s |",  widths[i], val);   /* right-align */
            else
                printf(" %-*s |", widths[i], val);   /* left-align  */
        }
        printf("\n");
    }

    /* Bottom border */
    print_border(widths);

    /* Row count + timing */
    int nrows = (int)rs.rows.size();
    if (nrows == 1)
        printf("1 row in set (%lld ms)\n", elapsed_ms);
    else
        printf("%d rows in set (%lld ms)\n", nrows, elapsed_ms);
}

/* ============================================================
 * Main REPL loop
 * ============================================================ */
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr,
            "Usage: %s <host> <port>\n"
            "  e.g: %s 127.0.0.1 9000\n",
            argv[0], argv[0]);
        return 1;
    }

    const char* host = argv[1];
    int         port = std::atoi(argv[2]);

    FlexQL* db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        fprintf(stderr,
            "Cannot connect to FlexQL server at %s:%d\n"
            "  Make sure the server is running: ./bin/flexql-server %d\n",
            host, port, port);
        return 1;
    }
    printf("Connected to FlexQL server at %s:%d\n\n", host, port);

    char        line_buf[65536];
    std::string accumulated;

    while (true) {
        /* Prompt */
        printf(accumulated.empty() ? "flexql> " : "     -> ");
        fflush(stdout);

        if (!fgets(line_buf, (int)sizeof(line_buf), stdin)) {
            printf("\nConnection closed\n");
            break;
        }

        std::string input(line_buf);
        while (!input.empty() &&
               (input.back() == '\n' || input.back() == '\r'))
            input.pop_back();

        /* Meta-commands */
        std::string cmd = input;
        while (!cmd.empty() && cmd.front() == ' ') cmd.erase(0,1);
        if (cmd == ".exit" || cmd == "exit" || cmd == "quit") {
            printf("Connection closed\n");
            break;
        }
        if (cmd.empty()) continue;

        accumulated += input + " ";

        /* Only dispatch when a ';' is present */
        if (accumulated.find(';') == std::string::npos) continue;

        /* Detect SELECT vs other for display */
        bool is_select = false;
        {
            size_t first = accumulated.find_first_not_of(" \t\r\n");
            if (first != std::string::npos &&
                accumulated.size() - first >= 6) {
                char kw[7] = {};
                for (int k = 0; k < 6 &&
                     first + k < accumulated.size(); k++)
                    kw[k] = toupper((unsigned char)accumulated[first+k]);
                is_select = (strncmp(kw, "SELECT", 6) == 0);
            }
        }

        /* Execute and measure */
        ResultSet rs;
        char* errmsg = nullptr;

        auto t0 = std::chrono::high_resolution_clock::now();
        int rc = flexql_exec(db, accumulated.c_str(),
                             is_select ? collect_callback : nullptr,
                             is_select ? &rs : nullptr,
                             &errmsg);
        auto t1 = std::chrono::high_resolution_clock::now();
        long long ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(t1 - t0).count();

        if (rc != FLEXQL_OK) {
            fprintf(stderr, "Error: %s\n",
                    errmsg ? errmsg : "unknown error");
            flexql_free(errmsg);
        } else if (is_select && !rs.col_names.empty()) {
            print_table(rs, ms);
        } else {
            printf("Query OK (%lld ms)\n", ms);
        }
        printf("\n");

        accumulated.clear();
    }

    flexql_close(db);
    return 0;
}
