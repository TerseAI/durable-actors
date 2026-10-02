package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/superfly/ltx"
)

type file struct {
	Level uint8  `json:"level"`
	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
	Data  []byte `json:"data"`
}

func main() {
	if err := compact(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func compact(input io.Reader, output io.Writer) error {
	var files []file
	if err := json.NewDecoder(input).Decode(&files); err != nil {
		return err
	}
	readers := make([]io.Reader, len(files))
	for i, f := range files {
		readers[i] = bytes.NewReader(f.Data)
	}
	var data bytes.Buffer
	compactor, err := ltx.NewCompactor(&data, readers)
	if err != nil {
		return err
	}
	compactor.HeaderFlags = ltx.HeaderFlagNoChecksum
	if err := compactor.Compact(context.Background()); err != nil {
		return err
	}
	header := compactor.Header()
	if header.MinTXID != 1 {
		return fmt.Errorf("checkpoint must start at transaction one")
	}
	return json.NewEncoder(output).Encode(file{Level: 9, First: uint64(header.MinTXID), Last: uint64(header.MaxTXID), Data: data.Bytes()})
}
