package main

import (
	"context"
	"flag"
	"fmt"
	"react-web-backup/database"
	_ "react-web-backup/database/mysql"
	"react-web-backup/storage"
	_ "react-web-backup/storage/file"
	"time"
)

var (
	configPath string
)

func init() {
	flag.StringVar(&configPath, "f", "./config.yml", "Restore file")
}

func main() {
	flag.Parse()

	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c, err := getConfig(configPath)
	if err != nil {
		panic(err)
		return
	}

	if c.Storage == nil {
		panic("missing storage config")
	}

	if c.Database == nil {
		panic("missing database config")
	}

	s, err := storage.GetStorage(c.Storage)
	if err != nil {
		panic(err)
	}

	if c.Tunnel != nil {
		c.Tunnel.Start()
	}

	db, err := database.GetDB(ctx, c.Database)
	if err != nil {
		panic(err)
	}

	switch c.Action {
	case "backup":
		backup(ctx, db, s, c)
	case "restore":
		restore(ctx, db, s, c)
	}
}

func backup(ctx context.Context, db database.Database, s storage.Storage, c *Config) {
	sql, err := db.Backup(ctx)
	if err != nil {
		panic(err)
	}

	currentTime := time.Now()
	fileName := fmt.Sprintf(
		"%d%d%d%d%d-%s.sql",
		currentTime.Day(),
		currentTime.Month(),
		currentTime.Year(),
		currentTime.Hour(),
		currentTime.Minute(),
		c.Database.Name,
	)

	filePath, err := s.Upload(fileName, sql)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Backup was saved at %s", filePath)
}

func restore(ctx context.Context, db database.Database, s storage.Storage, c *Config) {
	restoreFileContent, err := s.GetContent(c.RestoreVersion)
	if err != nil {
		panic(err)
	}
	err = db.Restore(ctx, restoreFileContent)
	if err != nil {
		panic(err)
	}
}
