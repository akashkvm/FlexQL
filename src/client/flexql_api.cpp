#include "../../include/flexql.h"
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

struct FlexQL {
    int sock;
};

static bool parse_length_prefixed_token(const std::string &text, size_t &pos, std::string &out) {
    size_t colon = text.find(':', pos);
    if (colon == std::string::npos || colon == pos) {
        return false;
    }

    for (size_t i = pos; i < colon; ++i) {
        if (text[i] < '0' || text[i] > '9') {
            return false;
        }
    }

    size_t len = 0;
    try {
        len = std::stoul(text.substr(pos, colon - pos));
    } catch (...) {
        return false;
    }

    size_t start = colon + 1;
    if (start + len > text.size()) {
        return false;
    }

    out.assign(text, start, len);
    pos = start + len;
    return true;
}

static bool parse_row_payload(
    const std::string &payload,
    std::vector<std::string> &values,
    std::vector<std::string> &columnNames
) {
    values.clear();
    columnNames.clear();

    if (payload.empty()) {
        return false;
    }

    size_t pos = 0;
    size_t countEnd = payload.find(' ');
    if (countEnd == std::string::npos || countEnd == 0) {
        return false;
    }

    for (size_t i = 0; i < countEnd; ++i) {
        if (payload[i] < '0' || payload[i] > '9') {
            return false;
        }
    }

    int columnCount = 0;
    try {
        columnCount = std::stoi(payload.substr(0, countEnd));
    } catch (...) {
        return false;
    }

    if (columnCount < 0) {
        return false;
    }

    pos = countEnd + 1;
    values.reserve(static_cast<size_t>(columnCount));
    columnNames.reserve(static_cast<size_t>(columnCount));

    for (int i = 0; i < columnCount; ++i) {
        std::string name;
        std::string value;
        if (!parse_length_prefixed_token(payload, pos, name)) {
            return false;
        }
        if (!parse_length_prefixed_token(payload, pos, value)) {
            return false;
        }
        columnNames.push_back(name);
        values.push_back(value);
    }

    return pos == payload.size();
}

int flexql_open(const char *host, int port, FlexQL **outDb) {
    FlexQL *db = (FlexQL*)malloc(sizeof(FlexQL));

    db->sock = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    inet_pton(AF_INET, host, &serv_addr.sin_addr);

    if (connect(db->sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        free(db);
        return FLEXQL_ERROR;
    }

    *outDb = db;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    close(db->sock);
    free(db);
    return FLEXQL_OK;
}

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *arg,
    char **errmsg
) {
    if (send(db->sock, sql, strlen(sql), MSG_NOSIGNAL) < 0) {
        if (errmsg) {
            const char *msg = "send failed (socket closed by server)";
            *errmsg = (char*)malloc(strlen(msg) + 1);
            if (*errmsg) {
                strcpy(*errmsg, msg);
            }
        }
        return FLEXQL_ERROR;
    }

    std::string pending;
    char buffer[4096 + 1];
    int valread;
    bool done = false;
    bool hasError = false;
    std::string errorText;

    while (!done && (valread = read(db->sock, buffer, 4096)) > 0) {
        buffer[valread] = '\0';
        pending.append(buffer, valread);

        size_t newlinePos;
        while ((newlinePos = pending.find('\n')) != std::string::npos) {
            std::string line = pending.substr(0, newlinePos);
            pending.erase(0, newlinePos + 1);

            if (line == "END") {
                done = true;
                break;
            }

            if (line.rfind("ERROR:", 0) == 0) {
                hasError = true;
                errorText = line;
                continue;
            }

            if (callback && line.rfind("ROW ", 0) == 0) {
                std::string rowValue = line.substr(4);
                std::vector<std::string> values;
                std::vector<std::string> columnNames;

                if (parse_row_payload(rowValue, values, columnNames)) {
                    std::vector<char*> argv(values.size(), nullptr);
                    std::vector<char*> col(columnNames.size(), nullptr);

                    for (size_t i = 0; i < values.size(); ++i) {
                        argv[i] = (char*)values[i].c_str();
                        col[i] = (char*)columnNames[i].c_str();
                    }
                    callback(arg, static_cast<int>(argv.size()), argv.data(), col.data());
                } else {
                    // Backward compatibility with older servers: pass raw row payload as a single value.
                    char *argv[1];
                    char *col[1];
                    argv[0] = (char*)rowValue.c_str();
                    col[0] = (char*)"row";
                    callback(arg, 1, argv, col);
                }
            }
        }
    }

    if (!done && valread < 0) {
        if (errmsg) {
            const char *msg = "read failed";
            *errmsg = (char*)malloc(strlen(msg) + 1);
            if (*errmsg) {
                strcpy(*errmsg, msg);
            }
        }
        return FLEXQL_ERROR;
    }

    if (!done) {
        if (errmsg) {
            const char *msg = "connection closed before END";
            *errmsg = (char*)malloc(strlen(msg) + 1);
            if (*errmsg) {
                strcpy(*errmsg, msg);
            }
        }
        return FLEXQL_ERROR;
    }

    if (hasError) {
        if (errmsg) {
            *errmsg = (char*)malloc(errorText.size() + 1);
            if (*errmsg) {
                strcpy(*errmsg, errorText.c_str());
            }
        }
        return FLEXQL_ERROR;
    }

    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    free(ptr);
}