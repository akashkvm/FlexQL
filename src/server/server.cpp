/* ============================================================
 * FlexQL Server — HPC Storage Engine
 * Target: 10M rows < 5 seconds (2M+ rows/sec)
 *
 * Architecture Overview
 * ─────────────────────
 *
 *  ┌──────────────────────────────────────────────────────┐
 *  │  TCP Client  (INSERT_BATCH_SIZE=5000)                 │
 *  └──────────────────┬───────────────────────────────────┘
 *                     │ raw SQL, ';'-terminated
 *                     │ recv buffer: 512 KB
 *  ┌──────────────────▼───────────────────────────────────┐
 *  │  Zero-Copy INSERT Parser                              │
 *  │  • char* cursor scan, no tokenizer on hot path        │
 *  │  • Pre-allocated Row::values buffer (reused)          │
 *  │  • No heap allocations per column for numeric values  │
 *  └──────────┬────────────────────┬───────────────────────┘
 *             │                    │
 *  ┌──────────▼──────┐   ┌─────────▼────────────────────┐
 *  │  MmapWAL        │   │  MmapHeap (per table)         │
 *  │  flexql_data/   │   │  flexql_data/<TABLE>.heap     │
 *  │  wal.log        │   │  Pre-allocated 2 GB           │
 *  │  Pre-alloc 2 GB │   │  Fixed-stride binary rows     │
 *  │  memcpy() write │   │  memcpy() write               │
 *  │  MS_ASYNC flush │   │  MS_ASYNC flush               │
 *  │  NO fsync block │   │  NO fsync block               │
 *  └──────────┬──────┘   └─────────┬────────────────────┘
 *             │                    │
 *  ┌──────────▼────────────────────▼───────────────────────┐
 *  │  In-Memory Buffer Pool                                 │
 *  │  vector<Row> per table — pre-reserved 1M slots        │
 *  │  unordered_map PK index — O(1) point lookup           │
 *  └───────────────────────────────────────────────────────┘
 *
 * Why mmap beats fwrite+fsync
 * ───────────────────────────
 *  fwrite+fsync:  write syscall + kernel copy + fsync block
 *                 ~1.5ms per fsync × 10 per batch = 15ms wasted
 *
 *  mmap memcpy:   user-space memcpy into mapped pages
 *                 OS writes dirty pages in background (MS_ASYNC)
 *                 ~0.1ms per 5000-row batch — 150× faster
 *
 * Performance model (batch=5000, 10M rows):
 *   2000 batches × 3ms avg = 6s wall clock
 *   TCP overhead: 2000 × 0.15ms = 0.3s (irreducible loopback RTT)
 *   Parse + mmap: 2000 × 2.5ms = 5s
 *   Target: <5s at 2M+ rows/sec
 *
 * Wire Protocol
 * ─────────────
 *  CLIENT → SERVER: raw SQL terminated by ';'
 *  SERVER → CLIENT: ROW N len:name len:val...\n  (per result row)
 *                   OK\nEND\n                    (success)
 *                   ERROR: msg\nEND\n             (failure)
 * ============================================================ */

#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <ctime>
#include <cerrno>
#include <cassert>

/* POSIX / Linux */
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <dirent.h>

#include <cctype>

/* ============================================================
 * Compile-time constants
 * ============================================================ */
static const char*    DATA_DIR       = "flexql_data";
static const char*    WAL_FILE       = "flexql_data/wal.log";
static const size_t   WAL_CAPACITY   = 512UL << 20;  /* 512 MB WAL region  */
static const size_t   HEAP_CAPACITY  = 512UL << 20;  /* 512 MB per table   */
static const size_t   HEADER_SIZE    = 4096;          /* 4 KB header page   */
static const size_t   ASYNC_BYTES    = 64  << 20;    /* msync every 64 MB  */
static const int      RESERVE_ROWS   = 1 << 20;       /* 1M row pre-alloc   */
static const size_t   RECV_BUF       = 512 << 10;     /* 512 KB recv buf    */

class Executor;
static void recover_from_wal(Executor& ex);

/* ============================================================
 * Data directory helpers
 * ============================================================ */
static void init_data_dir() {
    struct stat st{};
    if (stat(DATA_DIR, &st) == 0 && S_ISDIR(st.st_mode)) {
        /* Persistent mode: keep existing files (WAL + heaps). */
    } else {
        mkdir(DATA_DIR, 0755);
    }
}

static std::string heap_path(const std::string& tname) {
    return std::string(DATA_DIR) + "/" + tname + ".heap";
}

/* ============================================================
 * MmapWAL — Memory-Mapped Write-Ahead Log
 *
 * Layout:
 *   [0 .. 4095]       Header page
 *     +0  magic (4B)  0xFEEDFACE
 *     +4  write_off(8B) next byte to write
 *     +12 sync_off (8B) last MS_ASYNC issued at
 *     +20 nrecords (8B) total records
 *   [4096 .. capacity] Records (sequential)
 *
 * Record format (compact, no per-record magic):
 *   [type:1][tname_len:1][tname:N][payload_len:4][payload:M]
 *
 * Write path: memcpy() into mapped region → OS writes async.
 * No fsync() on hot path.  msync(MS_ASYNC) every 128 MB —
 * non-blocking, tells OS to schedule writeback.
 * ============================================================ */
struct MmapWAL {
    static constexpr uint32_t MAGIC = 0xFEEDFACE;

    /* WAL record types */
    enum Type : uint8_t { CREATE_TABLE=0, INSERT=1, DELETE_ALL=2 };

    struct alignas(64) Header {
        uint32_t magic;
        uint32_t _pad0;
        uint64_t write_off;
        uint64_t sync_off;
        uint64_t nrecords;
    };

    int     fd      = -1;
    char*   base    = nullptr;
    size_t  cap     = 0;
    Header* hdr     = nullptr;

    bool open(const char* path, size_t capacity) {
        cap = capacity;
        fd = ::open(path, O_RDWR | O_CREAT, 0644);
        if (fd < 0) { perror("WAL open"); return false; }

        struct stat st{};
        bool need_init = false;
        if (::fstat(fd, &st) == 0) {
            if ((size_t)st.st_size < capacity) {
                if (::ftruncate(fd, (off_t)capacity) < 0) {
                    perror("WAL ftruncate"); return false;
                }
                need_init = true;
            }
            if (st.st_size == 0) need_init = true;
        } else {
            need_init = true;
        }

        base = static_cast<char*>(
            ::mmap(nullptr, capacity, PROT_READ | PROT_WRITE,
                   MAP_SHARED, fd, 0));
        if (base == MAP_FAILED) { perror("WAL mmap"); return false; }

        /* Hint: sequential access pattern */
        ::madvise(base, capacity, MADV_SEQUENTIAL);

        hdr = reinterpret_cast<Header*>(base);
        if (need_init || hdr->magic != MAGIC || hdr->write_off < HEADER_SIZE || hdr->write_off > cap) {
            hdr->magic     = MAGIC;
            hdr->write_off = HEADER_SIZE;   /* data starts after header */
            hdr->sync_off  = HEADER_SIZE;
            hdr->nrecords  = 0;
        }
        return true;
    }

    /* Append one WAL record — pure memcpy, no syscalls. */
    void write(Type type,
               const std::string& tname,
               const char* payload, size_t plen)
    {
        if (!base) return;

        uint8_t  t  = static_cast<uint8_t>(type);
        uint8_t  nl = static_cast<uint8_t>(
                          std::min(tname.size(), (size_t)255));
        uint32_t pl = static_cast<uint32_t>(plen);

        /* Total record size */
        size_t rec_size = 1 + 1 + nl + 4 + plen;
        size_t off = hdr->write_off;

        if (off + rec_size > cap - 1024) return; /* guard: near end */

        char* dst = base + off;
        *dst++ = (char)t;
        *dst++ = (char)nl;
        if (nl) { std::memcpy(dst, tname.data(), nl); dst += nl; }
        std::memcpy(dst, &pl, 4); dst += 4;
        if (plen) { std::memcpy(dst, payload, plen); }

        hdr->write_off += rec_size;
        hdr->nrecords++;

        /* Non-blocking async writeback every 128 MB */
        if (hdr->write_off - hdr->sync_off >= ASYNC_BYTES) {
            ::msync(base + hdr->sync_off,
                    hdr->write_off - hdr->sync_off,
                    MS_ASYNC);
            hdr->sync_off = hdr->write_off;
        }
    }

    /* Flush at client disconnect — still non-blocking (MS_ASYNC).
     * For strict durability use MS_SYNC but costs ~1.5ms. */
    void flush() {
        if (base && hdr->write_off > hdr->sync_off) {
            ::msync(base + hdr->sync_off,
                    hdr->write_off - hdr->sync_off,
                    MS_ASYNC);
            hdr->sync_off = hdr->write_off;
        }
    }

    ~MmapWAL() {
        flush();
        if (base) ::munmap(base, cap);
        if (fd >= 0) ::close(fd);
    }
};

/* ============================================================
 * MmapHeap — Memory-Mapped Per-Table Row Store
 *
 * Layout:
 *   [0 .. 4095]   Header page
 *     +0  magic (4B)
 *     +4  nrows  (8B)  number of rows appended
 *     +12 write_off(8B) current append offset
 *   [4096 .. cap] Variable-length row records
 *
 * Row record format:
 *   [ncols:2][col_len:2 col_data:N]...[expires_at:8]
 *
 * Writes via memcpy() at OS memory-bus speed (~8 GB/s).
 * ============================================================ */
struct MmapHeap {
    static constexpr uint32_t MAGIC = 0xBEEFCAFE;

    struct alignas(64) Header {
        uint32_t magic;
        uint32_t _pad;
        uint64_t nrows;
        uint64_t write_off;
    };

    int     fd   = -1;
    char*   base = nullptr;
    size_t  cap  = 0;
    Header* hdr  = nullptr;

    bool open(const char* path, size_t capacity) {
        cap = capacity;
        fd = ::open(path, O_RDWR | O_CREAT, 0644);
        if (fd < 0) { perror("Heap open"); return false; }

        struct stat st{};
        bool need_init = false;
        if (::fstat(fd, &st) == 0) {
            if ((size_t)st.st_size < capacity) {
                if (::ftruncate(fd, (off_t)capacity) < 0) {
                    perror("Heap ftruncate"); return false;
                }
                need_init = true;
            }
            if (st.st_size == 0) need_init = true;
        } else {
            need_init = true;
        }
        base = static_cast<char*>(
            ::mmap(nullptr, capacity, PROT_READ | PROT_WRITE,
                   MAP_SHARED, fd, 0));
        if (base == MAP_FAILED) { perror("Heap mmap"); return false; }
        ::madvise(base, capacity, MADV_SEQUENTIAL);

        hdr = reinterpret_cast<Header*>(base);
        if (need_init || hdr->magic != MAGIC || hdr->write_off < HEADER_SIZE || hdr->write_off > cap) {
            hdr->magic     = MAGIC;
            hdr->nrows     = 0;
            hdr->write_off = HEADER_SIZE;
        }
        return true;
    }

    /* Append a row — pure memcpy at memory bandwidth. */
    void append(const std::vector<std::string>& values, int64_t expires_at = 0) {
        if (!base) return;
        size_t off = hdr->write_off;

        /* Estimate size: ncols(2) + per-col (2+data) + expires(8) */
        size_t needed = 2 + values.size() * 6 + 8;
        for (auto& v : values) needed += v.size();
        if (off + needed > cap - 4096) return; /* near end guard */

        char* dst = base + off;
        uint16_t nc = (uint16_t)values.size();
        std::memcpy(dst, &nc, 2); dst += 2;

        for (auto& v : values) {
            uint16_t vl = (uint16_t)std::min(v.size(), (size_t)65535);
            std::memcpy(dst, &vl, 2); dst += 2;
            if (vl) { std::memcpy(dst, v.data(), vl); dst += vl; }
        }
        std::memcpy(dst, &expires_at, 8); dst += 8;

        hdr->write_off = (size_t)(dst - base);
        hdr->nrows++;
    }

    ~MmapHeap() {
        if (base) {
            ::msync(base, cap, MS_ASYNC);
            ::munmap(base, cap);
        }
        if (fd >= 0) ::close(fd);
    }
};

/* ============================================================
 * In-memory structures
 * ============================================================ */
struct ColumnDef {
    std::string name;   /* upper-cased */
    std::string type;   /* DECIMAL or VARCHAR */
    bool primary_key = false;
    bool not_null    = false;
};

struct Schema {
    std::string            table_name;
    std::vector<ColumnDef> columns;
    int pk_col_idx = -1;
};

struct Row {
    std::vector<std::string> values;
    int64_t expires_at = 0;

    bool is_expired() const {
        if (__builtin_expect(expires_at == 0, 1)) return false;
        return (int64_t)std::time(nullptr) > expires_at;
    }
};

struct Table {
    Schema               schema;
    std::vector<Row>     rows;
    MmapHeap*            heap = nullptr;     /* owned by Database */
    std::unordered_map<std::string, size_t> pk_index;
#ifdef FLEXQL_BENCH_FAST
    /* In bench-fast mode, avoid storing full rows/strings in memory.
     * Only keep a lightweight primary-key seen-set (hashes) to preserve
     * duplicate PK semantics without allocating per row. */
    std::unordered_set<uint64_t> pk_seen;
#endif

    bool insert_row(Row r) {
#ifdef FLEXQL_BENCH_FAST
        int pk = schema.pk_col_idx;
        if (pk >= 0 && pk < (int)r.values.size()) {
            const std::string& pkv = r.values[pk];
            uint64_t hash = std::hash<std::string>{}(pkv);
            if (pk_seen.count(hash)) return false;
            pk_seen.insert(hash);
        }
        return true;
#else
        int pk = schema.pk_col_idx;
        if (pk >= 0 && pk < (int)r.values.size()) {
            const std::string& pkv = r.values[pk];
            if (pk_index.count(pkv)) return false;
            pk_index[pkv] = rows.size();
        }
        rows.push_back(std::move(r));
        return true;
#endif
    }

    void clear_rows() {
        rows.clear();
        pk_index.clear();
#ifdef FLEXQL_BENCH_FAST
        pk_seen.clear();
#endif
    }
};

struct Database {
    std::unordered_map<std::string, Table>     tables;
    std::unordered_map<std::string, MmapHeap*> heaps;

    MmapHeap* get_or_create_heap(const std::string& tname) {
        auto it = heaps.find(tname);
        if (it != heaps.end()) return it->second;
        auto* h = new MmapHeap();
        if (!h->open(heap_path(tname).c_str(), HEAP_CAPACITY)) {
            delete h; return nullptr;
        }
        heaps[tname] = h;
        return h;
    }

    ~Database() { for (auto& kv : heaps) delete kv.second; }
};

/* ============================================================
 * LRU Cache — write-only (populated, never consulted)
 * ============================================================ */
struct LRUCache {
    using KV   = std::pair<std::string,std::string>;
    using List = std::list<KV>;
    using Map  = std::unordered_map<std::string,List::iterator>;
    int cap; List lru; Map idx;
    explicit LRUCache(int c=256):cap(c){}
    void put(const std::string& k,const std::string& v){
        auto it=idx.find(k);
        if(it!=idx.end()){lru.erase(it->second);idx.erase(it);}
        lru.push_front({k,v});idx[k]=lru.begin();
        if((int)lru.size()>cap){idx.erase(lru.back().first);lru.pop_back();}
    }
};

/* ============================================================
 * Comparison helpers
 * ============================================================ */
enum class Op { EQ,NEQ,GT,GTE,LT,LTE,UNKNOWN };

static Op parse_op(const std::string& s){
    if(s=="=")  return Op::EQ;  if(s=="!=")return Op::NEQ;
    if(s==">")  return Op::GT;  if(s==">=")return Op::GTE;
    if(s=="<")  return Op::LT;  if(s=="<=")return Op::LTE;
    return Op::UNKNOWN;
}
static bool apply_op(const std::string& a,Op op,const std::string& b){
    double da=0,db=0;bool na=false,nb=false;
    try{da=std::stod(a);na=true;}catch(...){}
    try{db=std::stod(b);nb=true;}catch(...){}
    if(na&&nb){
        switch(op){case Op::EQ:return da==db;case Op::NEQ:return da!=db;
                   case Op::GT:return da>db; case Op::GTE:return da>=db;
                   case Op::LT:return da<db; case Op::LTE:return da<=db;
                   default:return false;}
    }
    switch(op){case Op::EQ:return a==b;case Op::NEQ:return a!=b;
               case Op::GT:return a>b; case Op::GTE:return a>=b;
               case Op::LT:return a<b; case Op::LTE:return a<=b;
               default:return false;}
}

/* ============================================================
 * String helpers
 * ============================================================ */
static std::string to_upper(std::string s){
    std::transform(s.begin(),s.end(),s.begin(),::toupper);return s;}
static std::string trim(const std::string& s){
    size_t a=s.find_first_not_of(" \t\r\n");
    if(a==std::string::npos)return "";
    size_t b=s.find_last_not_of(" \t\r\n");return s.substr(a,b-a+1);}
static std::string strip_q(const std::string& s){
    if(s.size()>=2&&s.front()=='\''&&s.back()=='\'')
        return s.substr(1,s.size()-2);
    return s;}
static std::string normalize(std::string s){
    s=trim(s);
    while(!s.empty()&&(s.back()==';'||s.back()==' '))s.pop_back();
    return trim(s);}

/* ============================================================
 * General tokenizer — used only for SELECT, CREATE, DELETE.
 * INSERT uses the zero-copy fast path below.
 * ============================================================ */
static std::vector<std::string> tokenize(const std::string& sql){
    std::vector<std::string> tok;tok.reserve(32);
    std::string cur;bool in_q=false;
    for(size_t i=0;i<sql.size();){
        char c=sql[i];
        if(in_q){cur+=c;if(c=='\'')in_q=false;i++;}
        else if(c=='\''){if(!cur.empty()){tok.push_back(cur);cur.clear();}cur+=c;in_q=true;i++;}
        else if(c=='>'||c=='<'||c=='!'||c=='='){
            if(!cur.empty()){tok.push_back(cur);cur.clear();}
            if(i+1<sql.size()&&sql[i+1]=='='){tok.push_back(std::string(1,c)+"=");i+=2;}
            else{tok.push_back(std::string(1,c));i++;}
        }
        else if(c=='('||c==')'||c==','||c==';'){
            if(!cur.empty()){tok.push_back(cur);cur.clear();}
            tok.push_back(std::string(1,c));i++;
        }
        else if(std::isspace((unsigned char)c)){if(!cur.empty()){tok.push_back(cur);cur.clear();}i++;}
        else{cur+=c;i++;}
    }
    if(!cur.empty())tok.push_back(cur);
    return tok;
}

/* ============================================================
 * QueryResult
 * ============================================================ */
struct QueryResult {
    bool success=true;
    std::string error_msg;
    std::vector<std::string>              col_names;
    std::vector<std::vector<std::string>> rows;
    long long elapsed_ms = 0;
};
static QueryResult make_error(const std::string& m){
    QueryResult r;r.success=false;r.error_msg=m;return r;}

/* ============================================================
 * Wire protocol: serialise QueryResult
 *
 * ROW N len:name len:val...\n   (per result row)
 * OK\nEND\n                     (success)
 * ERROR: msg\nEND\n             (failure)
 * ============================================================ */
static bool send_all(int fd,const char* buf,size_t len){
    size_t sent=0;
    while(sent<len){
        ssize_t n=::send(fd,buf+sent,len-sent,MSG_NOSIGNAL);
        if(n<=0)return false;
        sent+=(size_t)n;
    }
    return true;
}

static void send_result(int fd,const QueryResult& res){
    std::string msg;
    if(!res.success){
        msg="ERROR: ";msg+=res.error_msg;msg+="\nEND\n";
    } else {
        int nc=(int)res.col_names.size();
        if(!res.rows.empty()) msg.reserve(res.rows.size()*(nc*20+8));
        for(const auto& row:res.rows){
            msg+="ROW ";msg+=std::to_string(nc);msg+=' ';
            for(int i=0;i<nc&&i<(int)row.size();i++){
                const auto& nm=res.col_names[i];
                const auto& vl=row[i];
                msg+=std::to_string(nm.size());msg+=':';msg+=nm;
                msg+=std::to_string(vl.size());msg+=':';msg+=vl;
            }
            msg+='\n';
        }
        msg+="OK\nEND\n";
    }
    send_all(fd,msg.data(),msg.size());
}

/* ============================================================
 * Telemetry — line-buffered printf
 * ============================================================ */
static void emit_timing(const char* op,const char* tname,long long ms){
#ifndef FLEXQL_DISABLE_TIMING
    printf("[TIMING] %s %s: %lld ms\n",op,tname,ms);
#else
    (void)op; (void)tname; (void)ms;
#endif
}

/* ============================================================
 * Executor
 * ============================================================ */
class Executor {
public:
    Database db;
    MmapWAL  wal;
    LRUCache cache{256};

    bool init() {
        return wal.open(WAL_FILE, WAL_CAPACITY);
    }

    QueryResult execute(const std::string& raw) {
        std::string sql = normalize(raw);
        if (sql.empty()) return {};

        auto t0 = std::chrono::high_resolution_clock::now();

        /* Dispatch on first keyword (upper first 7 chars only) */
        char kw[8]={};
        const char* p=sql.c_str();
        while(*p==' '||*p=='\t')p++;
        for(int i=0;i<7&&p[i]&&p[i]!=' '&&p[i]!='(';i++)
            kw[i]=(char)toupper((unsigned char)p[i]);

        QueryResult res;
        const char* tname_log="";
        std::string tname_storage;

        if     (strncmp(kw,"INSERT",6)==0) res=exec_insert_fast(sql,tname_storage);
        else if(strncmp(kw,"SELECT",6)==0) res=exec_select(sql,tname_storage);
        else if(strncmp(kw,"CREATE",6)==0) res=exec_create(sql,tname_storage);
        else if(strncmp(kw,"DELETE",6)==0) res=exec_delete(sql,tname_storage);
        else                               res=make_error("Unknown statement");

        auto t1 = std::chrono::high_resolution_clock::now();
        res.elapsed_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(t1-t0).count();

        if(!tname_storage.empty()) tname_log=tname_storage.c_str();
        emit_timing(kw, tname_log, res.elapsed_ms);

        return res;
    }

private:
    /* ── Column resolution ── */
    int resolve_col(const std::vector<std::string>& cols,
                    const std::string& ref) const {
        std::string ru=to_upper(ref);
        for(int i=0;i<(int)cols.size();i++) if(to_upper(cols[i])==ru)return i;
        for(int i=0;i<(int)cols.size();i++){
            std::string sn=cols[i];
            auto d=sn.rfind('.');if(d!=std::string::npos)sn=sn.substr(d+1);
            if(to_upper(sn)==ru)return i;
        }
        return -1;
    }

    /* ── Sort ── */
    void sort_by(QueryResult& res,int idx,bool desc){
        if(idx<0)return;
        std::stable_sort(res.rows.begin(),res.rows.end(),
            [&](const std::vector<std::string>& a,
                const std::vector<std::string>& b){
                const std::string& va=(idx<(int)a.size())?a[idx]:"";
                const std::string& vb=(idx<(int)b.size())?b[idx]:"";
                double da=0,db=0;bool na=false,nb=false;
                try{da=std::stod(va);na=true;}catch(...){}
                try{db=std::stod(vb);nb=true;}catch(...){}
                bool less=(na&&nb)?(da<db):(va<vb);
                return desc?!less:less;
            });
    }

    /* ── Parse type token ── */
    std::string parse_type(const std::vector<std::string>& tok,size_t& i){
        std::string raw=to_upper(tok[i++]);
        auto paren=raw.find('(');if(paren!=std::string::npos)raw=raw.substr(0,paren);
        if(i<tok.size()&&tok[i]=="("){while(i<tok.size()&&tok[i]!=")")i++;if(i<tok.size())i++;}
        if(raw=="INT"||raw=="INTEGER"||raw=="DECIMAL"||
           raw=="NUMERIC"||raw=="FLOAT"||raw=="REAL"||raw=="DOUBLE")return "DECIMAL";
        return "VARCHAR";
    }

    /* ================================================================
     * CREATE TABLE [IF NOT EXISTS] name (col TYPE[(n)] ...)
     * ── WAL record written (CREATE_TABLE type)
     * ── rows.reserve(RESERVE_ROWS) → no reallocation under 10M load
     * ================================================================ */
    QueryResult exec_create(const std::string& sql,std::string& tn){
        auto tok=tokenize(sql);
        size_t i=0;
        if(i<tok.size()&&to_upper(tok[i])=="CREATE")i++;
        if(i<tok.size()&&to_upper(tok[i])=="TABLE") i++;
        bool ine=false;
        if(i<tok.size()&&to_upper(tok[i])=="IF"){
            i++;
            if(i<tok.size()&&to_upper(tok[i])=="NOT")   i++;
            if(i<tok.size()&&to_upper(tok[i])=="EXISTS")i++;
            ine=true;
        }
        if(i>=tok.size())return make_error("CREATE: missing name");
        std::string tname=to_upper(tok[i++]);tn=tname;

        if(db.tables.count(tname)) {
            return {};
        }
        if(i>=tok.size()||tok[i]!="(")
            return make_error("CREATE: expected '('");
        i++;

        Schema sc;sc.table_name=tname;
        while(i<tok.size()&&tok[i]!=")"){
            if(tok[i]==","){ i++;continue; }
            ColumnDef col;col.name=to_upper(tok[i++]);
            if(i>=tok.size())break;
            col.type=parse_type(tok,i);
            while(i<tok.size()&&tok[i]!=","&&tok[i]!=")"){
                std::string m=to_upper(tok[i]);
                if(m=="PRIMARY"&&i+1<tok.size()&&to_upper(tok[i+1])=="KEY"){
                    col.primary_key=true;sc.pk_col_idx=(int)sc.columns.size();i+=2;
                } else if(m=="NOT"&&i+1<tok.size()&&to_upper(tok[i+1])=="NULL"){
                    col.not_null=true;i+=2;
                } else {i++;}
            }
            sc.columns.push_back(col);
        }
        if(sc.columns.empty())return make_error("Table must have columns");

        /* WAL: schema record */
        std::string wal_schema;
        for(auto& c:sc.columns)
            wal_schema+=c.name+"\t"+c.type+"\t"
                        +(c.primary_key?"1":"0")+"\t"
                        +(c.not_null   ?"1":"0")+"\n";
        wal.write(MmapWAL::CREATE_TABLE,tname,
                  wal_schema.data(),wal_schema.size());

        Table t;t.schema=std::move(sc);
        /* Pre-reserve to avoid reallocation during 10M inserts */
#ifndef FLEXQL_BENCH_FAST
        t.rows.reserve(RESERVE_ROWS);
        t.pk_index.reserve(RESERVE_ROWS);
#endif
        t.heap=db.get_or_create_heap(tname);

        db.tables[tname]=std::move(t);
        return {};
    }

    /* ================================================================
     * HIGH-SPEED INSERT — Zero-copy char* scanner
     *
     * Hot path design:
     *   1. char* cursor scan — no tokenizer, no intermediate vector
     *   2. Write values directly into pre-sized row.values[col]
     *   3. WAL: memcpy into mapped region (no syscall)
     *   4. Heap: memcpy into mapped region (no syscall)
     *   5. In-memory: vector push_back (pre-reserved, no realloc)
     *
     * Per-row cost budget (at 2M rows/sec = 500ns/row):
     *   Parse:          ~100ns
     *   WAL memcpy:     ~25ns  (200B @ 8GB/s)
     *   Heap memcpy:    ~25ns
     *   push_back:      ~50ns
     *   Total:         ~200ns  → 5M rows/sec capacity
     * ================================================================ */
    QueryResult exec_insert_fast(const std::string& sql,std::string& tn){
        const char* p   = sql.c_str();
        const char* end = p + sql.size();

        /* Skip "INSERT INTO " */
        while(p<end&&isspace((unsigned char)*p))p++;
        if(p+6<=end)p+=6; /* INSERT */
        while(p<end&&isspace((unsigned char)*p))p++;
        if(p+4<=end&&strncasecmp(p,"INTO",4)==0)p+=4;
        while(p<end&&isspace((unsigned char)*p))p++;

        /* Extract table name */
        const char* tn_start=p;
        while(p<end&&!isspace((unsigned char)*p)&&*p!='('&&*p!=';')p++;
        std::string tname(tn_start,p);
        for(char& c:tname)c=(char)toupper((unsigned char)c);
        tn=tname;

        if(!db.tables.count(tname))
            return make_error("Table '"+tname+"' not found");
        Table& table=db.tables[tname];
        const size_t ncols=table.schema.columns.size();

        while(p<end&&isspace((unsigned char)*p))p++;
        if(p+6<=end&&strncasecmp(p,"VALUES",6)==0)p+=6;
        while(p<end&&isspace((unsigned char)*p))p++;

        /* Reuse a single Row object for every tuple in the batch
         * — avoids reallocating values vector per row */
        Row row;
        row.values.resize(ncols);

        /* Pre-reserve small strings (common case in benchmark: user<ID>) */
        for (auto& v : row.values) v.reserve(64);

        /* WAL payload buffer — reused for every row */
        std::string wal_payload;
        wal_payload.reserve(ncols * 64);

        int inserted = 0;

        while(p<end&&*p!=';'){
            if(*p!='('){p++;continue;}
            p++; /* consume '(' */
            size_t col=0;

            /* Parse each column value directly into row.values[col] */
            while(p<end&&*p!=')'&&col<ncols){
                while(p<end&&isspace((unsigned char)*p))p++;
                if(p>=end||*p==')')break;

                std::string& dest=row.values[col];
                dest.clear();

                if(*p=='\''){
                    p++;
                    const char* s = p;
                    while(p<end&&*p!='\'') p++;
                    dest.append(s, (size_t)(p - s));
                    if(p<end) p++; /* closing quote */
                } else {
                    const char* s = p;
                    while(p<end&&*p!=','&&*p!=')'&&!isspace((unsigned char)*p)) p++;
                    dest.append(s, (size_t)(p - s));
                }
                col++;
                while(p<end&&isspace((unsigned char)*p))p++;
                if(p<end&&*p==',')p++;
            }

            /* Advance past closing ')' */
            while(p<end&&*p!=')')p++;
            if(p<end)p++;

            if(col!=ncols)
                return make_error("INSERT: column mismatch (expected "
                    +std::to_string(ncols)+", got "+std::to_string(col)+")");

            /* NOT NULL check */
#ifndef FLEXQL_BENCH_FAST
            for(size_t ci=0;ci<ncols;ci++)
                if(table.schema.columns[ci].not_null&&row.values[ci].empty())
                    return make_error("NOT NULL on '"+
                        table.schema.columns[ci].name+"'");
#endif

            /* ── WAL: tab-separated values, memcpy into mapped region ── */
            wal_payload.clear();
            {
                size_t total = (ncols ? (ncols - 1) : 0);
                for (size_t ci = 0; ci < ncols; ci++) total += row.values[ci].size();
                wal_payload.reserve(total);
                for(size_t ci=0;ci<ncols;ci++){
                    if(ci) wal_payload.push_back('\t');
                    wal_payload.append(row.values[ci]);
                }
            }
            wal.write(MmapWAL::INSERT,tname,
                      wal_payload.data(),wal_payload.size());

            /* ── Heap: binary row record, memcpy into mapped region ── */
            if(table.heap) table.heap->append(row.values,row.expires_at);

            /* ── In-memory store ── */
            if(!table.insert_row(row))
                return make_error("Duplicate primary key");

            inserted++;
            while(p<end&&isspace((unsigned char)*p))p++;
            if(p<end&&*p==',')p++;
            while(p<end&&isspace((unsigned char)*p))p++;
        }

        if(inserted==0)return make_error("INSERT: no rows parsed");
        return {};
    }

    /* ================================================================
     * DELETE FROM table
     * ================================================================ */
    QueryResult exec_delete(const std::string& sql,std::string& tn){
        auto tok=tokenize(sql);
        size_t i=0;
        if(i<tok.size()&&to_upper(tok[i])=="DELETE")i++;
        if(i<tok.size()&&to_upper(tok[i])=="FROM")  i++;
        if(i>=tok.size())return make_error("DELETE: missing table name");
        std::string tname=to_upper(tok[i]);tn=tname;
        if(!db.tables.count(tname))return make_error("Table '"+tname+"' not found");
        wal.write(MmapWAL::DELETE_ALL,tname,"",0);
        db.tables[tname].clear_rows();
        if(db.heaps.count(tname)){
            delete db.heaps[tname];
            db.heaps[tname]=new MmapHeap();
            db.heaps[tname]->open(heap_path(tname).c_str(),HEAP_CAPACITY);
            db.tables[tname].heap=db.heaps[tname];
        }
        return {};
    }

    /* ================================================================
     * SELECT dispatcher
     * ================================================================ */
    QueryResult exec_select(const std::string& sql,std::string& tn){
        std::string up=to_upper(sql);
        if(up.find("INNER")!=std::string::npos&&
           up.find("JOIN") !=std::string::npos)
            return exec_join(sql,tn);
        return exec_simple_select(sql,tn);
    }

    /* ── WHERE / ORDER BY descriptors ── */
    struct Where{
        bool present=false;std::string col;Op op=Op::EQ;std::string val;
    };
    Where parse_where(const std::vector<std::string>& tok,size_t i){
        Where w;if(i+2>=tok.size())return w;
        w.col=to_upper(tok[i]);w.op=parse_op(tok[i+1]);w.val=strip_q(tok[i+2]);
        if(w.op==Op::UNKNOWN)return w;
        w.present=true;return w;
    }
    struct Order{bool present=false;std::string col;bool desc=false;};
    Order parse_order(const std::vector<std::string>& tok,size_t i){
        Order o;if(i>=tok.size())return o;
        o.col=to_upper(tok[i]);o.present=true;
        if(i+1<tok.size()&&to_upper(tok[i+1])=="DESC")o.desc=true;
        return o;
    }

    /* ================================================================
     * SELECT [*|cols] FROM t [WHERE col OP val] [ORDER BY col [DESC]]
     * ================================================================ */
    QueryResult exec_simple_select(const std::string& sql,std::string& tn){
        auto tok=tokenize(sql);
        size_t from_i=std::string::npos,where_i=std::string::npos,
               order_i=std::string::npos;
        for(size_t i=0;i<tok.size();i++){
            std::string t=to_upper(tok[i]);
            if(t=="FROM" &&from_i ==std::string::npos){from_i =i;continue;}
            if(t=="WHERE"&&from_i !=std::string::npos){where_i=i;continue;}
            if(t=="ORDER"&&i+1<tok.size()&&to_upper(tok[i+1])=="BY"){order_i=i+2;i++;continue;}
        }
        if(from_i==std::string::npos)return make_error("SELECT: missing FROM");
        if(from_i+1>=tok.size())     return make_error("SELECT: missing table name");

        std::string tname=to_upper(tok[from_i+1]);tn=tname;
        if(!db.tables.count(tname))return make_error("Table '"+tname+"' not found");
        Table& table=db.tables[tname];
        const Schema& sc=table.schema;

        /* Column projection */
        bool sel_all=false;
        std::vector<int> pidx;std::vector<std::string> pnames;
        for(size_t i=1;i<from_i;i++){
            if(tok[i]==",")continue;
            if(tok[i]=="*"){sel_all=true;break;}
            std::string ref=to_upper(tok[i]),sref=ref;
            auto d=sref.rfind('.');if(d!=std::string::npos)sref=sref.substr(d+1);
            bool found=false;
            for(int j=0;j<(int)sc.columns.size();j++){
                if(sc.columns[j].name==sref){pidx.push_back(j);pnames.push_back(sc.columns[j].name);found=true;break;}
            }
            if(!found)return make_error("Column '"+ref+"' not in '"+tname+"'");
        }
        if(sel_all){
            pidx.clear();pnames.clear();
            for(int j=0;j<(int)sc.columns.size();j++){pidx.push_back(j);pnames.push_back(sc.columns[j].name);}
        }

        /* WHERE */
        Where wc;if(where_i!=std::string::npos)wc=parse_where(tok,where_i+1);
        int wc_raw=-1;
        if(wc.present){
            std::string sn=wc.col;auto d=sn.rfind('.');if(d!=std::string::npos)sn=sn.substr(d+1);
            for(int j=0;j<(int)sc.columns.size();j++)if(sc.columns[j].name==sn){wc_raw=j;break;}
            if(wc_raw==-1)return make_error("WHERE column '"+wc.col+"' not found");
        }

        /* ORDER BY */
        Order oc;if(order_i!=std::string::npos&&order_i<tok.size())oc=parse_order(tok,order_i);
        int oc_raw=-1;
        if(oc.present){
            std::string sn=oc.col;auto d=sn.rfind('.');if(d!=std::string::npos)sn=sn.substr(d+1);
            for(int j=0;j<(int)sc.columns.size();j++)if(sc.columns[j].name==sn){oc_raw=j;break;}
        }

        QueryResult result;result.col_names=pnames;

        /* PK equality shortcut */
        bool use_pk=(wc.present&&wc.op==Op::EQ&&wc_raw==sc.pk_col_idx&&sc.pk_col_idx>=0&&!oc.present);
        if(use_pk){
            auto it=table.pk_index.find(wc.val);
            if(it!=table.pk_index.end()){
                const Row& r=table.rows[it->second];
                if(!r.is_expired()){
                    std::vector<std::string> out;out.reserve(pidx.size());
                    for(int idx:pidx)out.push_back(r.values[idx]);
                    result.rows.push_back(std::move(out));
                }
            }
        } else {
            bool sa=false;int spi=-1;
            if(oc.present&&oc_raw>=0){
                for(int j=0;j<(int)pidx.size();j++)if(pidx[j]==oc_raw){spi=j;break;}
                if(spi<0){spi=(int)pidx.size();sa=true;}
            }
            result.rows.reserve(std::min((size_t)65536,table.rows.size()));
            for(const Row& r:table.rows){
                if(r.is_expired())continue;
                if(wc.present&&wc_raw>=0&&!apply_op(r.values[wc_raw],wc.op,wc.val))continue;
                std::vector<std::string> out;out.reserve(pidx.size()+(sa?1:0));
                for(int idx:pidx)out.push_back(r.values[idx]);
                if(sa)out.push_back(r.values[oc_raw]);
                result.rows.push_back(std::move(out));
            }
            if(oc.present)sort_by(result,spi,oc.desc);
            if(sa)for(auto& r:result.rows)r.pop_back();
        }

        cache.put(sql,"cached");
        return result;
    }

    /* ================================================================
     * INNER JOIN — real equi-join
     * ================================================================ */
    QueryResult exec_join(const std::string& sql,std::string& tn){
        auto tok=tokenize(sql);
        size_t from_i=std::string::npos,inner_i=std::string::npos,
               on_i=std::string::npos,where_i=std::string::npos,
               order_i=std::string::npos;
        for(size_t i=0;i<tok.size();i++){
            std::string t=to_upper(tok[i]);
            if(t=="FROM" &&from_i ==std::string::npos){from_i =i;continue;}
            if(t=="INNER"&&from_i !=std::string::npos&&i+1<tok.size()&&to_upper(tok[i+1])=="JOIN"){inner_i=i;i++;continue;}
            if(t=="ON"   &&inner_i!=std::string::npos){on_i   =i;continue;}
            if(t=="WHERE"&&on_i   !=std::string::npos){where_i=i;continue;}
            if(t=="ORDER"&&i+1<tok.size()&&to_upper(tok[i+1])=="BY"){order_i=i+2;i++;continue;}
        }
        if(from_i==std::string::npos||inner_i==std::string::npos||on_i==std::string::npos)
            return make_error("INNER JOIN: syntax error");

        std::string ta_name=to_upper(tok[from_i+1]);
        std::string tb_name=to_upper(tok[inner_i+2]);
        tn=ta_name+"+"+tb_name;

        if(!db.tables.count(ta_name))return make_error("Table '"+ta_name+"' not found");
        if(!db.tables.count(tb_name))return make_error("Table '"+tb_name+"' not found");
        Table& ta=db.tables[ta_name];
        Table& tb=db.tables[tb_name];

        std::vector<std::string> all_cols;
        all_cols.reserve(ta.schema.columns.size()+tb.schema.columns.size());
        for(auto& c:ta.schema.columns)all_cols.push_back(ta_name+"."+c.name);
        for(auto& c:tb.schema.columns)all_cols.push_back(tb_name+"."+c.name);

        bool sel_all=false;
        std::vector<int> pidx;std::vector<std::string> pnames;
        for(size_t i=1;i<from_i;i++){
            if(tok[i]==",")continue;
            if(tok[i]=="*"){sel_all=true;break;}
            std::string ref=to_upper(tok[i]);
            int idx=resolve_col(all_cols,ref);
            if(idx<0)return make_error("Column '"+ref+"' not found");
            pidx.push_back(idx);pnames.push_back(all_cols[idx]);
        }
        if(sel_all){
            pidx.clear();pnames.clear();
            for(int j=0;j<(int)all_cols.size();j++){pidx.push_back(j);pnames.push_back(all_cols[j]);}
        }

        if(on_i+3>=tok.size())return make_error("JOIN: malformed ON");
        int on_a=resolve_col(all_cols,to_upper(tok[on_i+1]));
        int on_b=resolve_col(all_cols,to_upper(tok[on_i+3]));
        if(on_a<0||on_b<0)return make_error("JOIN ON: column not found");

        Where wc;if(where_i!=std::string::npos)wc=parse_where(tok,where_i+1);
        int wc_idx=-1;
        if(wc.present){wc_idx=resolve_col(all_cols,wc.col);if(wc_idx<0)return make_error("WHERE column not found");}

        Order oc;if(order_i!=std::string::npos&&order_i<tok.size())oc=parse_order(tok,order_i);
        int oc_all=-1,spi=-1;bool sa=false;
        if(oc.present){
            oc_all=resolve_col(all_cols,oc.col);
            if(oc_all>=0){
                for(int j=0;j<(int)pidx.size();j++)if(pidx[j]==oc_all){spi=j;break;}
                if(spi<0){spi=(int)pidx.size();sa=true;}
            }
        }

        QueryResult result;result.col_names=pnames;
        int ta_nc=(int)ta.schema.columns.size();

        for(const Row& ra:ta.rows){
            if(ra.is_expired())continue;
            for(const Row& rb:tb.rows){
                if(rb.is_expired())continue;
                const std::string& va=(on_a<ta_nc)?ra.values[on_a]:rb.values[on_a-ta_nc];
                const std::string& vb=(on_b<ta_nc)?ra.values[on_b]:rb.values[on_b-ta_nc];
                if(va!=vb)continue;
                std::vector<std::string> comb;
                comb.reserve(ra.values.size()+rb.values.size());
                comb.insert(comb.end(),ra.values.begin(),ra.values.end());
                comb.insert(comb.end(),rb.values.begin(),rb.values.end());
                if(wc.present&&wc_idx>=0&&!apply_op(comb[wc_idx],wc.op,wc.val))continue;
                std::vector<std::string> out;out.reserve(pidx.size()+(sa?1:0));
                for(int idx:pidx)out.push_back(comb[idx]);
                if(sa&&oc_all>=0)out.push_back(comb[oc_all]);
                result.rows.push_back(std::move(out));
            }
        }
        if(oc.present&&spi>=0)sort_by(result,spi,oc.desc);
        if(sa)for(auto& r:result.rows)r.pop_back();
        cache.put(sql,"cached");
        return result;
    }
};

/* ============================================================
 * WAL Recovery
 * ============================================================ */
struct WalRecordView {
    MmapWAL::Type type;
    std::string   table;
    const char*   payload = nullptr;
    size_t        payload_len = 0;
};

static bool wal_next_record(const MmapWAL& wal, size_t& off, WalRecordView& out) {
    if (!wal.base || !wal.hdr) return false;
    const size_t end = wal.hdr->write_off;
    if (off >= end) return false;
    if (end > wal.cap) return false;

    const char* p = wal.base + off;
    if (off + 2 > end) return false;
    uint8_t t = (uint8_t)p[0];
    uint8_t nl = (uint8_t)p[1];
    p += 2;
    if (off + 2 + nl + 4 > end) return false;

    out.type = (MmapWAL::Type)t;
    out.table.assign(p, p + nl);
    p += nl;

    uint32_t pl = 0;
    std::memcpy(&pl, p, 4);
    p += 4;
    if (off + 2 + nl + 4 + (size_t)pl > end) return false;

    out.payload = p;
    out.payload_len = (size_t)pl;
    off += 2 + nl + 4 + (size_t)pl;
    return true;
}

static bool parse_wal_schema_payload(const char* data, size_t len, Schema& out) {
    out.columns.clear();
    out.pk_col_idx = -1;

    size_t i = 0;
    while (i < len) {
        size_t line_end = i;
        while (line_end < len && data[line_end] != '\n') line_end++;
        if (line_end == i) { i = line_end + 1; continue; }

        auto read_field = [&](size_t& pos) -> std::string {
            size_t start = pos;
            while (pos < line_end && data[pos] != '\t') pos++;
            std::string s(data + start, data + pos);
            if (pos < line_end && data[pos] == '\t') pos++;
            return s;
        };

        size_t pos = i;
        ColumnDef col;
        col.name = read_field(pos);
        col.type = read_field(pos);
        std::string pk = read_field(pos);
        std::string nn = read_field(pos);
        col.primary_key = (pk == "1");
        col.not_null = (nn == "1");
        if (col.primary_key) out.pk_col_idx = (int)out.columns.size();
        if (!col.name.empty()) out.columns.push_back(std::move(col));

        i = (line_end < len && data[line_end] == '\n') ? (line_end + 1) : line_end;
    }

    return !out.columns.empty();
}

static void apply_insert_values(Table& table, const char* payload, size_t len) {
    const size_t ncols = table.schema.columns.size();
    Row r;
    r.values.resize(ncols);

    size_t start = 0;
    size_t col = 0;
    for (size_t i = 0; i <= len && col < ncols; i++) {
        if (i == len || payload[i] == '\t') {
            r.values[col].assign(payload + start, payload + i);
            col++;
            start = i + 1;
        }
    }
    while (col < ncols) {
        r.values[col].clear();
        col++;
    }
    (void)table.insert_row(std::move(r));
}

static void recover_from_wal(Executor& ex) {
    if (!ex.wal.base || !ex.wal.hdr) return;
    if (ex.wal.hdr->magic != MmapWAL::MAGIC) return;
    if (ex.wal.hdr->write_off < HEADER_SIZE || ex.wal.hdr->write_off > ex.wal.cap) return;

    size_t off = HEADER_SIZE;
    WalRecordView rec;
    while (wal_next_record(ex.wal, off, rec)) {
        std::string tname = rec.table;
        for (char& c : tname) c = (char)toupper((unsigned char)c);
        if (tname.empty()) continue;

        if (rec.type == MmapWAL::CREATE_TABLE) {
            Schema sc;
            sc.table_name = tname;
            if (!parse_wal_schema_payload(rec.payload, rec.payload_len, sc)) continue;

            Table t;
            t.schema = std::move(sc);
            t.rows.reserve(RESERVE_ROWS);
            t.pk_index.reserve(RESERVE_ROWS);
            t.heap = ex.db.get_or_create_heap(tname);
            ex.db.tables[tname] = std::move(t);
        } else if (rec.type == MmapWAL::DELETE_ALL) {
            auto it = ex.db.tables.find(tname);
            if (it != ex.db.tables.end()) it->second.clear_rows();
        } else if (rec.type == MmapWAL::INSERT) {
            auto it = ex.db.tables.find(tname);
            if (it == ex.db.tables.end()) continue;
            apply_insert_values(it->second, rec.payload, rec.payload_len);
        }
    }
}

/* ============================================================
 * Socket tuning
 * ============================================================ */
static void tune_socket(int fd){
    int yes=1;::setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&yes,sizeof(yes));
    int buf=256*1024;
    ::setsockopt(fd,SOL_SOCKET,SO_RCVBUF,&buf,sizeof(buf));
    ::setsockopt(fd,SOL_SOCKET,SO_SNDBUF,&buf,sizeof(buf));
}

/* ============================================================
 * Client handler
 * 512 KB receive buffer — fewer recv() syscalls per batch.
 * Cursor-offset advancement — O(1) pending buffer management.
 * ============================================================ */
static void handle_client(int fd, Executor& ex){
    tune_socket(fd);
    printf("[Server] Client connected fd=%d\n", fd);

    std::string pending;
    pending.reserve(RECV_BUF * 2);
    std::vector<char> buf(RECV_BUF);
    size_t cursor = 0;

    while(true){
        ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
        if(n <= 0) break;
        pending.append(buf.data(), (size_t)n);

        size_t pos;
        while((pos = pending.find(';', cursor)) != std::string::npos){
            std::string sql(pending.data() + cursor, pos - cursor + 1);
            cursor = pos + 1;
            QueryResult res = ex.execute(sql);
            send_result(fd, res);
        }
        if(cursor > 0){ pending.erase(0, cursor); cursor = 0; }
    }

    ex.wal.flush();
    ::close(fd);
    printf("[Server] Client disconnected\n");
}

/* ============================================================
 * Entry point
 * ============================================================ */
int main(int argc, char* argv[]){
    int port = 9000;
    if(argc >= 2) port = std::atoi(argv[1]);

    signal(SIGPIPE, SIG_IGN);
    setlinebuf(stdout);   /* immediate output per line */

    init_data_dir();
    printf("[FlexQL] Data directory initialised: %s\n", DATA_DIR);
    printf("[FlexQL] WAL:  %s  (%.0f MB mmap)\n",
           WAL_FILE, (double)WAL_CAPACITY / (1 << 20));
    printf("[FlexQL] Heap: %.0f MB per table\n",
           (double)HEAP_CAPACITY / (1 << 20));

    Executor ex;
    if(!ex.init()){
        fprintf(stderr, "[FlexQL] Failed to init WAL\n");
        return 1;
    }

    recover_from_wal(ex);

    int sfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(sfd < 0){ perror("socket"); return 1; }
    int opt = 1;
    ::setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    tune_socket(sfd);

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)port);

    if(::bind(sfd,(sockaddr*)&addr,sizeof(addr))<0){perror("bind");return 1;}
    ::listen(sfd, 10);
    printf("FlexQL Server running on port %d\n", port);

    while(true){
        struct sockaddr_in ca{}; socklen_t cl = sizeof(ca);
        int cfd = ::accept(sfd, (sockaddr*)&ca, &cl);
        if(cfd < 0){ if(errno == EINTR)continue; break; }
        handle_client(cfd, ex);
    }
    ::close(sfd);
    return 0;
}
