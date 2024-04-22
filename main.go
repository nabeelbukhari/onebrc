package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/profile"
)

type StationData struct {
	name                  string
	MaxTemp, MinTemp, Sum float64
	Count                 int
}

const (
	filePath   = "measurements_1B.txt"
	maxNameLen = 100
	maxNameNum = 10000
	mb         = 1024 * 1024 // bytes
)

func main() {
	// parse env vars and inputs
	shouldProfile := os.Getenv("PROFILE") == "true"
	if shouldProfile {
		defer profile.Start(profile.ProfilePath("./parallel-map-hash-parse")).Stop()
	}

	// start timer
	start := time.Now()

	// final results map
	stationData := make(map[string]*StationData, maxNameNum)

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to open %s file: %w", filePath, err))
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to read %s file: %w", filePath, err))
		return
	}

	parseChunkSize := 1 * mb
	numParsers := runtime.NumCPU()

	createWorkers(file, fileinfo, numParsers, parseChunkSize, stationData)
	printResults(stationData)
	elapsed := time.Since(start)
	log.Printf("Time took %s", elapsed)
}

func createWorkers(file *os.File, info os.FileInfo, numParsers int, parseChunkSize int, stationData map[string]*StationData) {

	// kick off "parser" workers
	wg := sync.WaitGroup{}
	wg.Add(numParsers)

	// buffered to not block on merging
	chunkOffsetCh := make(chan int64, numParsers)
	chunkStatsCh := make(chan *Map[string, *StationData], numParsers)

	go func() {
		i := 0
		for i < int(info.Size()) {
			chunkOffsetCh <- int64(i)
			i += parseChunkSize
		}
		close(chunkOffsetCh)
	}()

	for i := 0; i < numParsers; i++ {
		// WARN: w/ extra padding for line overflow. Each chunk should be read past
		// the intended size to the next new line. 128 bytes should be enough for
		// a max 100 byte name + the float value.
		buf := make([]byte, parseChunkSize+128)
		go func() {
			stationData := NewHashMap[string, *StationData](maxNameNum)
			for chunkOffset := range chunkOffsetCh {
				readUsingBuffer(file, buf, stationData, chunkOffset, parseChunkSize)
			}
			chunkStatsCh <- stationData
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(chunkStatsCh)
	}()

	for chunkStats := range chunkStatsCh {
		for _, s := range chunkStats.cache {
			if s == nil {
				continue
			}
			if ms, ok := stationData[s.name]; !ok {
				stationData[s.name] = s
			} else {
				if s.MinTemp < ms.MinTemp {
					ms.MinTemp = s.MinTemp
				}
				if s.MaxTemp > ms.MaxTemp {
					ms.MaxTemp = s.MaxTemp
				}
				ms.Sum += s.Sum
				ms.Count += s.Count
			}
		}
	}
}

func readUsingBuffer(file *os.File, buffer []byte, stationData *Map[string, *StationData], offset int64, size int) {

	bytesread, err := file.ReadAt(buffer, offset)

	if err != nil && err != io.EOF {
		log.Fatal(err)
	}

	if bytesread == 0 {
		return
	}

	fnv1aOffset64 := uint64(14695981039346656037)
	fnv1aPrime64 := uint64(1099511628211)

	pointer := 0
	extraLineRead := false

	// skip until first line
	for {
		if buffer[pointer] == '\n' {
			break
		}
		pointer++
	}
	pointer++

	for {
		semiColonIndex := -1
		idHash := uint64(fnv1aOffset64)

		// find the semi-colon and generate hash for name
		for i := pointer; i < len(buffer); i++ {
			ch := buffer[i]
			if ch == ';' {
				semiColonIndex = i
				break
			}
			// calculate FNV-1a hash
			idHash ^= uint64(ch)
			idHash *= fnv1aPrime64
		}

		// if no semi-colon found, it means we don't have a complete line
		if semiColonIndex == -1 {
			fmt.Println("no semi-colon found, skipping line")
			break
		}

		numberStartIndex := semiColonIndex + 1

		var temp float64
		// parse the number
		{
			negative := buffer[numberStartIndex] == '-'
			if negative {
				numberStartIndex++
			}

			var temps int64
			if buffer[numberStartIndex+1] == '.' {
				// 1.2\n
				temps = int64(buffer[numberStartIndex])*10 + int64(buffer[numberStartIndex+2]) - '0'*(10+1)
				// 12.3\n
			} else {
				temps = int64(buffer[numberStartIndex])*100 + int64(buffer[numberStartIndex+1])*10 + int64(buffer[numberStartIndex+3]) - '0'*(100+10+1)
			}

			if negative {
				temps = -temps
			}

			temp = float64(temps) / 10.0
		}

		newLineIndex := -1
		// find the newline
		for i := numberStartIndex + 3; i < len(buffer); i++ {
			if buffer[i] == '\n' {
				newLineIndex = i
				break
			}
		}

		// if no newline found, it means we don't have a complete line
		if newLineIndex == -1 {
			fmt.Println("no newline found, skipping line")
			break
		}

		station, found := stationData.GetUsingHash(idHash)
		if !found {
			name := string(buffer[pointer:semiColonIndex]) // actually allocate string
			stationData.SetUsingHash(idHash, &StationData{
				name:    name,
				MaxTemp: temp,
				MinTemp: temp,
				Sum:     temp,
				Count:   1,
			})
		} else {
			if temp < station.MinTemp {
				station.MinTemp = temp
			}
			if temp > station.MaxTemp {
				station.MaxTemp = temp
			}
			station.Sum += temp
			station.Count++
		}
		pointer = newLineIndex + 1

		if pointer >= size {
			if extraLineRead {
				break
			}
			extraLineRead = true
		}
	}
}

func printResults(stationData map[string]*StationData) { // doesn't help
	// sorted alphabetically for output
	names := make([]string, 0, len(stationData))
	for name := range stationData {
		names = append(names, name)
	}
	sort.Strings(names)

	var builder strings.Builder
	for i, name := range names {
		s := stationData[name]
		// gotcha: first round the sum to to remove float precision errors!
		avg := round(round(s.Sum) / float64(s.Count))
		builder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, s.MinTemp, avg, s.MaxTemp))
		if i < len(names)-1 {
			builder.WriteString(", ")
		}
	}

	writer := bufio.NewWriter(os.Stdout)
	fmt.Fprintf(writer, "{%s}\n", builder.String())
	writer.Flush()
}

// rounding floats to 1 decimal place with 0.05 rounding up to 0.1
func round(x float64) float64 {
	return math.Floor((x+0.05)*10) / 10
}
