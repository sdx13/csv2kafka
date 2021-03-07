package main

import (
	"compress/gzip"
	"encoding/csv"
	"io"
)

type GzipReader struct {
	s *csv.Reader
	z *gzip.Reader
}

func NewGzipReader(f io.Reader) (*GzipReader, error) {
	r := &GzipReader{}
	z, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	s := csv.NewReader(z)
	s.FieldsPerRecord = -1
	r.z = z
	r.s = s
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
	return r.z.Close()
}
