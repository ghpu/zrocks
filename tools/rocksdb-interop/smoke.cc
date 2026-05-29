#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/options.h>
#include <memory>
#include <cstdio>
using namespace rocksdb;
int main(){
  Options o; o.create_if_missing=true; o.compression=kNoCompression;
  BlockBasedTableOptions t; t.format_version=5; t.whole_key_filtering=true;
  o.table_factory.reset(NewBlockBasedTableFactory(t));
  std::unique_ptr<DB> db;
  Status s=DB::Open(o,"/tmp/rdb_smoke",&db);
  if(!s.ok()){printf("open fail: %s\n",s.ToString().c_str());return 1;}
  db->Put(WriteOptions(),"hello","world");
  db->Put(WriteOptions(),"foo","bar");
  FlushOptions fo; fo.wait=true; db->Flush(fo);
  std::string v; s=db->Get(ReadOptions(),"hello",&v);
  printf("get hello -> %s (%s)\n", v.c_str(), s.ToString().c_str());
  printf("rocksdb %d.%d.%d OK\n", ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH);
  return 0;
}
