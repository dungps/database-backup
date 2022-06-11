package pg

import (
	"context"
	"react-web-backup/database"
	"testing"
)

func TestDB_Backup(t *testing.T) {
	ctx := context.Background()
	db := &DB{}

	err := db.Connect(ctx, &database.Connection{
		Username: "dev",
		Password: "nX2u2zI5BFN7",
		Name:     "kevin",
		Port:     5432,
		Host:     "localhost",
		Schema:   "public",
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	s, err := db.Backup(ctx)
	if err != nil {
		t.Fatal(err)
		return
	}

	t.Log(s)
}
