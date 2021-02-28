package main

import (
	"compress/gzip"
	"encoding/csv"
	"os"
)

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
