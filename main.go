package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/pkg/profile"
)

type StationData struct {
	MaxTemp, MinTemp, Sum float64
	Count                 int
}

func main() {
	// parse env vars and inputs
	shouldProfile := os.Getenv("PROFILE") == "true"
	if shouldProfile {
		defer profile.Start(profile.ProfilePath("./sequential")).Stop()
	}
	start := time.Now()
	stationData := make(map[string]*StationData, 10000)

	buffersize := 1024 * 1024
	buffer := make([]byte, buffersize)

	file, err := os.Open("measurements_1B.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	readUsingBuffer(file, buffer, stationData)
	printResults(stationData)
	elapsed := time.Since(start)
	log.Printf("Time took %s", elapsed)
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

// parseFloatFast is a high performance float parser using the assumption that
// the byte slice will always have a single decimal digit.
func parseFloatFast(tempBS []byte) float64 {
	var startIndex int // is negative?
	if tempBS[0] == '-' {
		startIndex = 1
	}

	temperature := float64(tempBS[len(tempBS)-1]-'0') / 10 // single decimal digit
	place := 1.0
	for i := len(tempBS) - 3; i >= startIndex; i-- { // integer part
		temperature += float64(tempBS[i]-'0') * place
		place *= 10
	}

	if startIndex == 1 {
		temperature *= -1
	}
	return temperature
}

func readUsingBuffer(file *os.File, buffer []byte, stationData map[string]*StationData) {
	bytesread, err := file.Read(buffer)

	if err != nil {
		fmt.Println(err)
		return
	}

	remaining := make([]byte, 0)
	stationName := make([]byte, 100)

	for bytesread > 0 {
		pointer := 0
		for {
			newLineIndex := -1
			semiColonIndex := -1
			newLineFound := false
			for {
				if pointer+newLineIndex+1 >= len(buffer) {
					break
				}
				newLineIndex++
				if buffer[pointer+newLineIndex] == ';' {
					semiColonIndex = newLineIndex
					continue
				}
				if buffer[pointer+newLineIndex] == '\n' {
					newLineFound = true
					break
				}
			}
			// if no newline found, it means we don't have a complete line
			if newLineFound {
				currLine := buffer[pointer : pointer+newLineIndex]
				// how can we get rid of this
				if len(remaining) > 0 {
					currLine = append(remaining, currLine...)
					remaining = remaining[:0]
				}
				if semiColonIndex == -1 {
					semiColonIndex = bytes.IndexByte(currLine, ';')
				}
				if semiColonIndex > 0 {
					nameLen := copy(stationName, currLine[:semiColonIndex])
					temp := parseFloatFast(currLine[semiColonIndex+1:])
					nameUnsafe := unsafe.String(&stationName[0], nameLen)
					station, found := stationData[nameUnsafe]
					if !found {
						name := string(stationName[:nameLen]) // actually allocate string
						stationData[name] = &StationData{
							MaxTemp: temp,
							MinTemp: temp,
							Sum:     temp,
							Count:   1,
						}
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
				}
				pointer = pointer + newLineIndex + 1
			} else {
				remaining = append(remaining, buffer[pointer:]...)
				break
			}
		}

		bytesread, err = file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}

			log.Fatalf("read file line error: %v", err)
			return
		}
	}
}

func readCompleteFile(filename string) {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Bytes read: ", len(bytes))
}

func processFile(filename string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Fatalf("Stat: %v", err)
	}

	size := fi.Size()
	if size <= 0 || size != int64(int(size)) {
		log.Fatalf("Invalid file size: %d", size)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatalf("Mmap: %v", err)
	}

	defer func() {
		if err := syscall.Munmap(data); err != nil {
			log.Fatalf("Munmap: %v", err)
		}
	}()
}

func readWithScanner(file *os.File, stationData map[string]*StationData) {
	_, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}

	// buffersize := fileinfo.Size()
	buffersize := 1024 * 1024
	buffer := make([]byte, buffersize)
	// Create a scanner object that reads from the file
	scanner := bufio.NewScanner(file)
	scanner.Buffer(buffer, int(buffersize))
	scanner.Split(bufio.ScanLines)

	// Returns a boolean based on whether there's a next instance of `\n`
	// character in the IO stream. This step also advances the internal pointer
	// to the next position (after '\n') if it did find that token.
	read := scanner.Scan()

	for read {
		// fmt.Println("read string: ", scanner.Text())
		test := scanner.Text()
		splitted := strings.Split(test, ";")
		stationname := splitted[0]
		temp, error := strconv.ParseFloat(splitted[1], 32)
		if error != nil {
			fmt.Println("Error parsing float: ", error)
			return
		}
		station, found := stationData[stationname]
		if !found {
			stationData[stationname] = &StationData{
				MaxTemp: temp,
				MinTemp: temp,
				Sum:     temp,
				Count:   1,
			}
		} else {
			station.MaxTemp = math.Max(temp, station.MaxTemp)
			station.MinTemp = math.Min(temp, station.MinTemp)
			station.Sum = temp + station.Sum
			station.Count++
		}
		read = scanner.Scan()
	}
}
