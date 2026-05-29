#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/options.h>
#include <memory>
#include <cstdio>
#include <string>
using namespace rocksdb;
int main(int argc, char** argv){
  std::string dir = argc>1 ? argv[1] : "/tmp/rdb_fix";
  Options o; o.create_if_missing=true; o.error_if_exists=true; o.compression=kNoCompression;
  BlockBasedTableOptions t;
  t.format_version=5; t.whole_key_filtering=true; t.block_size=256; // small -> many data blocks
  t.no_block_cache=true;
  o.table_factory.reset(NewBlockBasedTableFactory(t));
  std::unique_ptr<DB> db; Status s=DB::Open(o,dir,&db);
  if(!s.ok()){printf("open fail: %s\n",s.ToString().c_str());return 1;}
  char k[16],v[24];
  for(int i=0;i<100;i++){snprintf(k,sizeof k,"key%03d",i);snprintf(v,sizeof v,"value-%03d",i);db->Put(WriteOptions(),k,v);}
  db->Delete(WriteOptions(),"key050");
  FlushOptions fo; fo.wait=true; db->Flush(fo);
  std::string got; printf("verify key000=%s key099=%s del key050 found=%d\n",
    (db->Get(ReadOptions(),"key000",&got).ok()?got.c_str():"?"),
    (db->Get(ReadOptions(),"key099",&got).ok()?got.c_str():"?"),
    db->Get(ReadOptions(),"key050",&got).ok());
  return 0;
}
