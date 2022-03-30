package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/ClickHouse/clickhouse-go/lib/data"
)

type Ring [][2]float64

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9001?debug=true&username=default&password=123456")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	defer connect.Close()

	rows, err := connect.Query("SELECT ring, start, end FROM geo_ring PREWHERE origin='uzcxfrbrzp' AND destination='uzcxfpyxzr' LIMIT 1")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			ring  [][]interface{}
			start string
			end   string
		)
		if err := rows.Scan(&ring, &start, &end); err != nil {
			log.Fatal(err)
		}
		log.Printf("Ring: %v, Start: %s, End: %s\n", ring, start, end)
		for _, point := range ring {
			fmt.Printf("lat: %f, lng: %f\n", point[0], point[1])
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	// 写入Ring类型 采用OpenDirect直连方式，发送block写入
	connect1, err := clickhouse.OpenDirect("tcp://127.0.0.1:9001?debug=true&username=default&password=123456")

	if err != nil {
		log.Fatal(err)
	}

	defer connect1.Close()

	if _, err = connect1.Begin(); err != nil {
		fmt.Println(err)
		return
	}
	stmt, err := connect1.Prepare("INSERT INTO geo_ring (ring, start, end) VALUES (?, ?, ?)")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer stmt.Close()

	rings := make([]Ring, 2)

	rings[0] = make(Ring, 0)
	rings[0] = append(rings[0], [2]float64{120.156833, 35.978535})
	rings[0] = append(rings[0], [2]float64{120.116014, 35.957503})
	rings[0] = append(rings[0], [2]float64{120.196502, 35.966851})

	rings[1] = make(Ring, 0)
	rings[1] = append(rings[1], [2]float64{120.166833, 35.977535})
	rings[1] = append(rings[1], [2]float64{120.126014, 35.857503})
	rings[1] = append(rings[1], [2]float64{120.296502, 35.936851})

	block, err := connect1.Block()

	if err != nil {
		fmt.Println(err)
		return
	}

	block.Reserve()
	block.NumRows += 2

	// 列式存储 多条数据按照列放一块分别写入
	writeBatch(block, 0, rings)

	block.WriteFixedString(1, []byte("wwmkwjxw1j"))
	block.WriteFixedString(1, []byte("wwmkwmwb64"))

	block.WriteFixedString(2, []byte("wwmkx5s6j6"))
	block.WriteFixedString(2, []byte("wwms3qj5rd"))

	if err := connect1.WriteBlock(block); err != nil {
		fmt.Printf("Write block error: %s\n", err)
		return
	}

	if err := connect1.Commit(); err != nil {
		fmt.Printf("Commit error: %s\n", err)
		return
	}

	fmt.Printf("Commit success!")

}

// 写入格式
// uint64 row1 offset + row2 offset + ... + uint64 longitude1 + uint64 longitude2 + .. + uint64 latitude1 + uint64 latitude2 + ...
// 写入字节方式均为小端
func writeBatch(block *data.Block, idx int, rings []Ring) {
	// 每个数据的偏移放在一起 clickhouse依赖偏移分别截取数据
	offset := 0
	for _, r := range rings {
		offset += len(r)
		block.WriteUInt64(idx, uint64(offset))
	}

	// tuple每个位置的元素压到一起
	for _, r := range rings {
		for _, p := range r {
			block.WriteUInt64(idx, math.Float64bits(p[0]))
		}
	}

	for _, r := range rings {
		for _, p := range r {
			block.WriteUInt64(idx, math.Float64bits(p[1]))
		}
	}
}
