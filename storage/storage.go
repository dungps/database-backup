package storage

import "fmt"

type Options struct {
	APIKey      string `yaml:"api_key,omitempty"`
	APISecret   string `yaml:"api_secret,omitempty"`
	StoragePath string `yaml:"storage_path,omitempty"`
}

type StorageConfig struct {
	Client  string  `yaml:"client"`
	Options Options `yaml:"options"`
}

type Storage interface {
	Name() string
	Init(options Options) error
	Upload(name string, fileContent string) (string, error)
	GetContent(name string) (string, error)
}

var storages = make(map[string]Storage)

func RegisterStorage(s Storage) {
	if s == nil {
		panic("cannot register storage")
	}
	if s.Name() == "" {
		panic("cannot register storage with empty result for Name()")
	}
	storages[s.Name()] = s
}

func GetStorage(o *StorageConfig) (Storage, error) {
	if s, ok := storages[o.Client]; ok {
		err := s.Init(o.Options)
		return s, err
	}

	return nil, fmt.Errorf("cannot find storage for %s type", o.Client)
}
