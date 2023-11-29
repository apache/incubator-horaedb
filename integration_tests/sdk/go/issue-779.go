package main

import (
	"context"

	"github.com/CeresDB/horaedb-client-go/ceresdb"
)

func checkAutoAddColumnsWithCreateTable(ctx context.Context, client ceresdb.Client) error {
	timestampName := "timestamp"

	err := dropTable(ctx, client, table)
	if err != nil {
		return err
	}

	err = createTable(ctx, client, timestampName)
	if err != nil {
		return err
	}

	err = writeAndQuery(ctx, client, timestampName)
	if err != nil {
		return err
	}

	return writeAndQueryWithNewColumns(ctx, client, timestampName)
}
