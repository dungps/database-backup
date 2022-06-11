package file

import (
	"errors"
	"os"
	"path"
	"react-web-backup/storage"
)

func init() {
	storage.RegisterStorage(&File{})
}

type File struct {
	storagePath string
}

func (f *File) Name() string {
	return "file"
}

func (f *File) Init(c storage.Options) error {
	f.storagePath = c.StoragePath
	if !path.IsAbs(f.storagePath) {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		f.storagePath = path.Join(cwd, f.storagePath)
	}

	_, err := os.Stat(f.storagePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = os.MkdirAll(f.storagePath, os.ModePerm)
			if err != nil {
				return err
			}
		}
		return err
	}

	return nil
}

func (f *File) Upload(name string, content string) (string, error) {
	filePath := path.Join(f.storagePath, name)
	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = file.Close()
	}()
	_, err = file.WriteString(content)
	return filePath, err
}

func (f *File) GetContent(name string) (string, error) {
	filePath := path.Join(f.storagePath, name)
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}

	return string(content), nil
}
