package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"os"
	"path"
	"react-web-backup/database"
	"react-web-backup/storage"
	"react-web-backup/tunnel"
)

type Config struct {
	Action         string                 `yaml:"action"`
	RestoreVersion string                 `yaml:"restore_version,omitempty"`
	Storage        *storage.StorageConfig `yaml:"storage,omitempty"`
	Database       *database.Connection   `yaml:"database,omitempty"`
	Tunnel         *tunnel.Tunnel         `yaml:"tunnel,omitempty"`
}

func getConfig(filePath string) (*Config, error) {
	if !path.IsAbs(filePath) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		filePath = path.Join(cwd, filePath)
	}

	stat, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}

	if stat.IsDir() {
		return nil, fmt.Errorf("%s is not a file", filePath)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	config := &Config{}
	err = yaml.NewDecoder(f).Decode(&config)
	return config, err
}
