package database

import (
	"context"
	"fmt"
	"strings"
)

type Connection struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
	Client   string `yaml:"client"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Schema   string `yaml:"schema,omitempty"`
}

type Database interface {
	Name() string
	Connect(ctx context.Context, c *Connection) error
	Backup(ctx context.Context) (string, error)
	Restore(ctx context.Context, filePath string) error
}

var registeredDB = make(map[string]Database)

func RegisterDb(db Database) {
	if db == nil {
		panic("cannot register database")
	}
	if db.Name() == "" {
		panic("cannot register database with empty string result for Name()")
	}
	registeredDB[strings.ToLower(db.Name())] = db
}

func GetDB(ctx context.Context, c *Connection) (Database, error) {
	if db, ok := registeredDB[c.Client]; ok {
		err := db.Connect(ctx, c)
		if err != nil {
			return nil, err
		}

		return db, nil
	}

	return nil, fmt.Errorf("cannot find driver for %s", c.Client)
}
