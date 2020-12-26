package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"log"
	"os"
)

// Abstract Entities
//
// We are dealing with at least two abstractions: data source and encoding.
// Data sources are files on local/SFTP filesystem or a network port. Encoding
// is about the data being in plaintext or gzip compressed.
//

// things like SFTP/local FS, network port (future)
type RecordSource struct {
}

// something that lets read a regular or gzip compressed file
type RecordReader struct {
}

// reads uncompressed files
type FileReader struct {
	s *bufio.Scanner
}

func NewFileReader(filePath string) (*FileReader, error) {
	r := &FileReader{}
	f, err := os.Open(filePath)
	if err == nil {
		s := bufio.NewScanner(f)
		if err == nil {
			r.s = s
		}
	}
	return r, err
}

func (r *FileReader) Read() (string, error) {
	if r.s.Scan() {
		return string(r.s.Text()), nil
	}
	return "", errors.New("failed to scan")
}

// reads gzip compressed files
type GzipReader struct {
	s *bufio.Scanner
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

	s := bufio.NewScanner(g)
	if err == nil {
		r.s = s
	}
	return r, err
}

func (r *GzipReader) Read() (string, error) {
	if r.s.Scan() {
		return string(r.s.Text()), nil
	}
	return "", errors.New("failed to scan")
}

func main() {
	fileName := "hits.csv.gz"
	//r, err := NewFileReader(fileName)
	r, err := NewGzipReader(fileName)
	if err != nil {
		log.Fatal("Could not open input file")
	}
	for {
		line, err := r.Read()
		if err != nil {
			break
		}
		fmt.Printf("%v\n", line)
	}
}
