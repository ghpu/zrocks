// Opens a DB directory with REAL RocksDB (read-only) and runs a multi-check digest.
//
// Usage: verify_open <db_dir>
//   Prints "OPEN_OK count=N" then "k=v" lines on success, or "OPEN_FAIL: <status>".
//
// Checks performed (all failures print FAIL and exit 1):
//   1. OpenForReadOnly
//   2. Full forward scan (SeekToFirst -> end)   — exercises bloom filter + data blocks
//   3. Point Get on up to 3 sampled live keys   — exercises Get path + bloom probe
//   4. Point Get on a synthetic absent key       — must be NotFound
//   5. Seek to a mid-key + forward range scan   — exercises Seek + partial scan
//
// The widened checks mean filters, range-tombstones, and Get paths are all
// exercised, not just a single SeekToFirst walk.
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <memory>
#include <cstdio>
#include <string>
#include <vector>
using namespace rocksdb;

int main(int argc, char** argv) {
    if (argc < 2) { printf("usage: verify_open <dir>\n"); return 2; }

    Options o;
    o.create_if_missing = false;
    o.error_if_exists = false;

    std::unique_ptr<DB> db;
    Status s = DB::OpenForReadOnly(o, argv[1], &db);
    if (!s.ok()) {
        printf("OPEN_FAIL: %s\n", s.ToString().c_str());
        return 1;
    }

    ReadOptions ro;

    // ---- 1. Full forward scan (SeekToFirst -> end) ----
    std::vector<std::string> live_keys;
    std::vector<std::string> live_vals;
    {
        std::unique_ptr<Iterator> it(db->NewIterator(ro));
        long n = 0;
        std::string out;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            n++;
            std::string k = it->key().ToString();
            std::string v = it->value().ToString();
            live_keys.push_back(k);
            live_vals.push_back(v);
            if (n <= 200) out += k + "=" + v + "\n";
        }
        if (!it->status().ok()) {
            printf("ITER_FAIL: %s\n", it->status().ToString().c_str());
            return 1;
        }
        // Print the canonical summary line first.
        printf("OPEN_OK count=%ld\n%s", n, out.c_str());
    }

    // ---- 2. Point Get on up to 3 sampled live keys ----
    {
        size_t n = live_keys.size();
        std::vector<size_t> idxs;
        if (n > 0) idxs.push_back(0);
        if (n > 2) idxs.push_back(n / 2);
        if (n > 1) idxs.push_back(n - 1);

        for (size_t i : idxs) {
            std::string got;
            Status gs = db->Get(ro, live_keys[i], &got);
            if (!gs.ok()) {
                printf("GET_FAIL key=%s status=%s\n",
                       live_keys[i].c_str(), gs.ToString().c_str());
                return 1;
            }
            if (got != live_vals[i]) {
                printf("GET_MISMATCH key=%s expected=%s got=%s\n",
                       live_keys[i].c_str(), live_vals[i].c_str(), got.c_str());
                return 1;
            }
        }
    }

    // ---- 3. Point Get on a synthetic absent key (must be NotFound) ----
    {
        // Pick a key that cannot be in any reasonable test DB.
        const std::string absent = "\xff\xff\xff\xff__absent__";
        std::string got;
        Status gs = db->Get(ro, absent, &got);
        if (!gs.IsNotFound()) {
            printf("GET_ABSENT_FAIL: expected NotFound, got %s\n",
                   gs.ToString().c_str());
            return 1;
        }
    }

    // ---- 4. Seek to a mid-key + forward range scan ----
    {
        size_t n = live_keys.size();
        if (n >= 2) {
            // Seek to the key at n/2 and scan to the end; count must match.
            size_t seek_idx = n / 2;
            std::unique_ptr<Iterator> it(db->NewIterator(ro));
            it->Seek(live_keys[seek_idx]);
            if (!it->Valid()) {
                printf("SEEK_FAIL: Seek(%s) not valid\n",
                       live_keys[seek_idx].c_str());
                return 1;
            }
            if (it->key().ToString() != live_keys[seek_idx]) {
                printf("SEEK_KEY_MISMATCH: expected %s got %s\n",
                       live_keys[seek_idx].c_str(),
                       it->key().ToString().c_str());
                return 1;
            }
            long scan_count = 0;
            for (; it->Valid(); it->Next()) {
                scan_count++;
            }
            if (!it->status().ok()) {
                printf("SEEK_SCAN_FAIL: %s\n", it->status().ToString().c_str());
                return 1;
            }
            long expected = (long)(n - seek_idx);
            if (scan_count != expected) {
                printf("SEEK_COUNT_FAIL: expected %ld got %ld\n",
                       expected, scan_count);
                return 1;
            }
        }
    }

    return 0;
}
