package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"

	"github.com/pkg/errors"
)

func main() {
	numChunks := flag.Int("chunks", 16, "number of chunks")
	groups := flag.Int("groups", 32, "number of groups into which to put rows")
	rowsPerGroup := flag.Int("rows", 8000, "number of rows per group")
	padding := flag.Int("padding", 1000, "number of bytes to pad each row")
	flag.Parse()

	fmt.Printf("writing %d chunks each with %d rows...\n", *numChunks, *groups**rowsPerGroup)

	var wg sync.WaitGroup
	for chunk := 0; chunk < *numChunks; chunk++ {
		wg.Add(1)
		go writeChunk(&wg, chunk, *groups, *rowsPerGroup, *padding)
	}
	wg.Wait()
}

func writeChunk(wg *sync.WaitGroup, chunk, targetRanges, rowsPerRange, pad int) {
	defer wg.Done()
	dest, err := os.Create(fmt.Sprintf("idx.%d.csv", chunk))
	if err != nil {
		log.Fatal(err)
	}
	defer dest.Close()
	if err := writeChunkData(dest, chunk, targetRanges, rowsPerRange, pad); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote chunk %d\n", chunk)
}

func writeChunkData(dest io.Writer, chunk, groups, rowsPerGroup, pad int) error {
	random := rand.New(rand.NewSource(int64(6759853 + chunk)))
	padding := make([]byte, pad)
	for g := 0; g < groups; g++ {
		for row := 0; row < rowsPerGroup; row++ {
			if _, err := random.Read(padding); err != nil {
				return errors.Wrap(err, "reading random pad")
			}
			fmt.Fprintf(dest, "%d,%d,%d,%s\n", chunk, g, row, hex.EncodeToString(padding))
		}
	}
	return nil
}
