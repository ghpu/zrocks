// Opens a DB directory with REAL RocksDB (read-only) and dumps a digest.
// Usage: verify_open <db_dir>   -> prints "OPEN_OK count=N" then "k=v" lines, or "OPEN_FAIL: <status>"
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <memory>
#include <cstdio>
#include <string>
using namespace rocksdb;
int main(int argc, char** argv){
  if(argc<2){printf("usage: verify_open <dir>\n");return 2;}
  Options o; o.create_if_missing=false; o.error_if_exists=false;
  std::unique_ptr<DB> db;
  Status s=DB::OpenForReadOnly(o, argv[1], &db);
  if(!s.ok()){printf("OPEN_FAIL: %s\n", s.ToString().c_str());return 1;}
  std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
  long n=0;
  std::string out;
  for(it->SeekToFirst(); it->Valid(); it->Next()){
    n++;
    if(n<=200) out += it->key().ToString() + "=" + it->value().ToString() + "\n";
  }
  if(!it->status().ok()){printf("ITER_FAIL: %s\n", it->status().ToString().c_str());return 1;}
  printf("OPEN_OK count=%ld\n%s", n, out.c_str());
  return 0;
}
