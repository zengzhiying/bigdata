package main

import (
	"fmt"
	"time"

	"github.com/tecbot/gorocksdb"
)

// 编译时:
// export CGO_CFLAGS="-I/usr/local/rocksdb/include"
// export CGO_LDFLAGS="-L/usr/local/rocksdb/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -llz4 -lzstd"
// go build
// 运行时:
// export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/rocksdb/lib
// ./rocksdb-example


func main() {
	// new db
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))

	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, "data/db")
	if err != nil {
		fmt.Println("open db error:", err)
		return
	}
	defer db.Close()
	defer opts.Destroy()
	defer bbto.Destroy()

	// write
	wOpts := gorocksdb.NewDefaultWriteOptions()
	defer wOpts.Destroy()
	err = db.Put(wOpts, []byte("key1"), []byte("value1"))
	if err != nil {
		fmt.Println("db put error:", err)
	}
	err = db.Put(wOpts, []byte("key2"), []byte("value2"))
	if err != nil {
		fmt.Println("db put error:", err)
	}
	
	// read
	rOpts := gorocksdb.NewDefaultReadOptions()
	defer rOpts.Destroy()

	value, err := db.Get(rOpts, []byte("key1"))
	if err != nil {
		fmt.Println("get data1 err:", err)
	}
	if value.Exists() {
		fmt.Println("get data1:", value.Data(), string(value.Data()))
	}
	value.Free()
	value, err = db.Get(rOpts, []byte("key2"))
	if err != nil {
		fmt.Println("get data2 err:", err)
	}
	if value.Exists() {
		fmt.Println("get data2:", value.Data(), string(value.Data()))
	}
	value.Free()

	// delete
	err = db.Delete(wOpts, []byte("key2"))
	if err != nil {
		fmt.Println("delete key2 error:", err)
	}
	value, err = db.Get(rOpts, []byte("key2"))
	if err != nil {
		fmt.Println("get key2 err:", err)
	}
	fmt.Println("key2 status", value.Exists(), value.Data())
	value.Free()

	// batch write
	wBatch := gorocksdb.NewWriteBatch()
	defer wBatch.Destroy()
	wBatch.Delete([]byte("kkkk"))
	wBatch.Put([]byte("number1"), []byte("1"))
	wBatch.Put([]byte("number2"), []byte("2"))
	err = db.Write(wOpts, wBatch)
	if err != nil {
		fmt.Println("batch write err:", err)
	}

	value, err = db.Get(rOpts, []byte("number2"))
	if err != nil {
		fmt.Println("number2 get err:", err)
	}
	fmt.Println(value.Exists(), string(value.Data()))
	value.Free()


	// scan iterator
	// 用read options
	iterOpts := gorocksdb.NewDefaultReadOptions()
	iterOpts.SetFillCache(false)
	defer iterOpts.Destroy()
	it := db.NewIterator(iterOpts)
	defer it.Close()

	// seek到大于或等于指定键的第一个键
	// it.Seek([]byte("aaa"))
	// seek到小于或等于指定键的最后一个键
	// it.SeekForPrev([]byte("bbb"))
	// seek到第一个键
	// it.SeekToFirst()
	// seek到最后一个键
	it.SeekToLast()

	// 迭代
	for it = it; it.Valid(); it.Next() {
		k, v := it.Key(), it.Value()
		fmt.Println(string(k.Data()), string(v.Data()))
		k.Free()
		v.Free()
	}
	if err := it.Err(); err != nil {
		fmt.Println("it err:", err)
	}


	// put benchmark
	t1 := time.Now().UnixNano()
	for i := 0; i < 1000000; i++ {
		pKey := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		err := db.Put(wOpts, pKey, []byte{1})
		if err != nil {
			fmt.Println("put benchmark err:", err)
		}
	}
	t2 := time.Now().UnixNano()
	useTime := float32(t2 - t1) / 1.0e9
	fmt.Printf("put benchmark time: %.3fs\n", useTime)

	// get benchmark
	t1 = time.Now().UnixNano()
	for i := 0; i < 1000000; i++ {
		pKey := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		value, err := db.Get(rOpts, pKey)
		if err != nil {
			fmt.Println("get benchmark err:", err)
			continue
		}
		if value.Data()[0] != 1 {
			fmt.Println("value err:", value.Data())
		}
		value.Free()
	}
	t2 = time.Now().UnixNano()
	useTime = float32(t2 - t1) / 1.0e9
	fmt.Printf("get benchmark time: %.3fs\n", useTime)

	// batch put benchmark
	t1 = time.Now().UnixNano()
	batchSize := 10
	for i := 1000000; i < 2000000; i++ {
		pKey := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		wBatch.Put(pKey, []byte{1})
		if wBatch.Count() >= batchSize {
			err := db.Write(wOpts, wBatch)
			if err != nil {
				fmt.Println("batch put benchmark err:", err)
			}
			wBatch.Clear()
		}
	}
	if wBatch.Count() > 0 {
		err := db.Write(wOpts, wBatch)
		if err != nil {
			fmt.Println("batch put benchmark err1:", err)
		}
		wBatch.Clear()
	}
	t2 = time.Now().UnixNano()
	useTime = float32(t2 - t1) / 1.0e9
	fmt.Printf("batch put benchmark time: %.3fs\n", useTime)

	// 数据变化 new iterator
	it1 := db.NewIterator(iterOpts)
	defer it1.Close()
	// count key
	it1.SeekToFirst()
	numKey := 0
	for it1 = it1; it1.Valid(); it1.Next() {
		k, v := it1.Key(), it1.Value()
		// fmt.Println(string(k.Data()), string(v.Data()))
		numKey++
		if numKey % 100000 == 0 {
			fmt.Printf("key: %v, value: %v\n", k.Data(), v.Data())
		}
		k.Free()
		v.Free()
	}
	if err := it1.Err(); err != nil {
		fmt.Println("it1 err:", err)
	}
	fmt.Println("total number keys:", numKey)
}
