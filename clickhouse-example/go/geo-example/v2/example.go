package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/paulmach/orb"
)

/**
test sql:
INSERT INTO geo_ring VALUES ([(120.197221,35.953822), (120.200383,35.955692), (120.205557,35.957386)], 'uzcxfrbrzp', 'uzcxfpyxzr')
*/

func main() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9001"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "123456",
		},
		//Debug:           true,
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})

	if err != nil {
		fmt.Printf("Connect to Clickhouse error: %s\n", err)
		return
	}

	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 10,
	}), clickhouse.WithProgress(func(p *clickhouse.Progress) {
		fmt.Println("progress: ", p)
	}))
	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return
	}

	var (
		ring  orb.Ring
		start string
		end   string
	)

	if err := conn.QueryRow(ctx, "SELECT ring, start, end FROM geo_ring PREWHERE start='uzcxfrbrzp' AND end='uzcxfpyxzr' LIMIT 1").Scan(
		&ring,
		&start,
		&end,
	); err != nil {
		fmt.Printf("Query error: %s\n", err)
		return
	}

	fmt.Println(ring)
	fmt.Println(start)
	fmt.Println(end)

	for _, p := range ring {
		fmt.Println(p.X(), p.Y())
	}

	conn.Close()
}
