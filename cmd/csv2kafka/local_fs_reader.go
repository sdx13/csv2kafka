package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

type LocalFilesystemReader struct {
	inputDir     string
	readyDir     string
	waitInterval int

	files  []os.FileInfo
	reader *GzipReader
	index  int
	f      *os.File
}

func (r *LocalFilesystemReader) Read() ([]string, error) {
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
			f, err := os.Open(name)
			if err != nil {
				log.Println("Failed to open file", err)
				continue
			}
			reader, err := NewGzipReader(f)
			if err == nil {
				r.reader = reader
				r.index = i
				r.f = f
				r.files = files
				break
			} else {
				log.Println("Failed to create gzip reader", err)
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
		err := r.close()
		if err != nil {
			log.Println("Error while closing file", err)
		}
		currentName := r.files[r.index].Name()
		if err != nil {
			fmt.Printf("Error closing file %v: %v", currentName, err)
		} else {
			log.Println("Closed file", currentName)
		}
		r.files = r.files[1:]
		r.postProcess(currentName)
		return r.Read()
	}
	return record, err
}

func (r *LocalFilesystemReader) close() error {
	err := r.f.Close()
	if err != nil {
		return err
	}
	return r.reader.Close()
}

func (r *LocalFilesystemReader) postProcess(name string) {
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
