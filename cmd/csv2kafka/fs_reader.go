package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

type FilesystemReader struct {
	inputDir     string
	readyDir     string
	waitInterval int

	files  []os.FileInfo
	reader *GzipReader
	index  int
}

func NewFilesystemReader(cfg *config) (*FilesystemReader, error) {
	return &FilesystemReader{
		inputDir:     cfg.InputDir,
		readyDir:     cfg.ReadyDir,
		waitInterval: cfg.WaitInterval,
		index:        -1,
	}, nil
}

func (r *FilesystemReader) Read() ([]string, error) {
	if len(r.files) == 0 {
		i := 0
		log.Println("Scanning for files in dir", r.inputDir)
		files, err := readDir(r.inputDir)
		if err != nil {
			log.Printf("Failed to read input dir: %v", err)
			return nil, err
		}
		for ; i < len(files); i++ {
			name := filepath.Join(r.inputDir, files[i].Name())
			log.Println("Reading file", name)
			reader, err := NewGzipReader(name)
			if err == nil {
				r.reader = reader
				r.index = i
				r.files = files
				break
			}
		}
		if i == len(files) {
			log.Println("No input file found")
			log.Printf("Waiting for %v seconds", r.waitInterval)
			time.Sleep(time.Duration(r.waitInterval) * time.Second)
			return r.Read()
		}
	}
	record, err := r.reader.Read()
	if record == nil {
		err := r.Close()
		currentName := r.files[r.index].Name()
		if err != nil {
			fmt.Printf("Error closing file %v: %v", currentName, err)
		} else {
			log.Println("Closed file", currentName)
		}
		r.files = r.files[1:]
		r.PostProcess(currentName)
		return r.Read()
	}
	return record, err
}

func (r *FilesystemReader) Close() error {
	return r.reader.Close()
}

func (r *FilesystemReader) PostProcess(name string) {
	from := filepath.Join(r.inputDir, name)
	to := filepath.Join(r.readyDir, name)
	err := os.Rename(from, to)
	if err != nil {
		fmt.Printf("Failed to rename %v to %v: %v", from, to, err)
	} else {
		log.Println("Processed", name)
	}
}

func readDir(path string) ([]os.FileInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	filesInfo, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}
	return filesInfo, nil
}

// There is no method to close the gzip reader.
type GzipReader struct {
	s    *csv.Reader
	file *os.File
}

func NewGzipReader(filePath string) (*GzipReader, error) {
	r := &GzipReader{}
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	g, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	s := csv.NewReader(g)
	r.s = s
	r.file = f
	return r, err
}

func (r *GzipReader) Read() ([]string, error) {
	return r.s.Read()
	/*
		if r.s.Scan() {
			return string(r.s.Text()), nil
		}
		return "", errors.New("failed to scan")
	*/
}
func (r *GzipReader) Close() error {
	return r.file.Close()
}
