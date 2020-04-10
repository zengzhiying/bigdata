package main

import (
	"fmt"
	"context"

	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

func main() {
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://192.168.1.37:8529"},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	client, err := driver.NewClient(driver.ClientConfig{
		Connection: conn,
		Authentication: driver.BasicAuthentication("root", "123456"),
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx := context.Background()
	db, err := client.Database(ctx, "example")
	if err != nil {
		fmt.Println(err)
		return
	}
	query := "for v, e in 1..2  any 'airport/JK0331' air_relation return MERGE (v, {'e':e})"
	cursor, err := db.Query(ctx, query, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cursor.Close()
	for {
		var doc map[string]interface{}
		meta, err := cursor.ReadDocument(ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			// handle other errors
			fmt.Println("cursor: ", err)
		}
		if doc == nil {
			continue
		}
		fmt.Printf("Got doc with id '%s' from query\n", doc["_id"])
		fmt.Println(doc, doc == nil)
		fmt.Println(meta)
	}
}
