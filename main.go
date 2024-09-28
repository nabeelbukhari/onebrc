package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"math/bits"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/pkg/profile"
)

type StationData struct {
	name                  string
	MaxTemp, MinTemp, Sum int64
	Count                 int
	nameAddress           uint64
	nameLength            int
}

type Scanner struct {
	pointer  unsafe.Pointer
	position uint64
	end      uint64
}

func movePointer(pointer unsafe.Pointer, pos uint64) unsafe.Pointer {
	return unsafe.Pointer(uintptr(pointer) + uintptr(pos))
}

func (s *Scanner) hasNext() bool {
	return s.position < s.end
}

func (s *Scanner) pos() uint64 {
	return s.position
}

func (s *Scanner) add(delta uint64) {
	s.position += delta
}

func (s *Scanner) getLong() uint64 {
	return *(*uint64)(movePointer(s.pointer, s.position))
}

func (s *Scanner) getLongAt(pos uint64) uint64 {
	return *(*uint64)(movePointer(s.pointer, pos))
}

func (s *Scanner) getByteAt(pos uint64) byte {
	return *(*byte)(movePointer(s.pointer, pos))
}

func (s *Scanner) getByteArrayAt(pos uint64) [maxNameLen]byte {
	return *(*[maxNameLen]byte)(movePointer(s.pointer, pos))
}

const (
	MIN_TEMP      = -999
	MAX_TEMP      = 999
	maxNameLen    = 100
	maxNameNum    = 10000
	mb            = 1024 * 1024 // bytes
	fnv1aOffset64 = uint64(14695981039346656037)
	fnv1aPrime64  = uint64(1099511628211)
)

var (
	filePath = "measurements.txt"
	MASK1    = [...]uint64{0xFF, 0xFFFF, 0xFFFFFF, 0xFFFFFFFF, 0xFFFFFFFFFF, 0xFFFFFFFFFFFF, 0xFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF,
		0xFFFFFFFFFFFFFFFF}
	MASK2 = [...]uint64{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFFFFFFFFFFFFFFFF}
)

func main() {
	// start timer
	start := time.Now()

	// parse env vars and inputs
	shouldProfile := os.Getenv("PROFILE") == "true"
	if shouldProfile {
		defer profile.Start(profile.ProfilePath("./profile")).Stop()
	}

	shouldPrintTimer := os.Getenv("TIMER") == "true"

	if len(os.Args) == 2 {
		filePath = os.Args[1]
	}

	// final results map
	finalResult := make(map[string]*StationData, maxNameNum)

	numParsers := runtime.NumCPU()

	createWorkers(numParsers, finalResult)
	printResults(finalResult)
	if shouldPrintTimer {
		elapsed := time.Since(start)
		log.Printf("Time took %s", elapsed)
	}
}

func createWorkers(numParsers int, finalResult map[string]*StationData) {

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to open %s file: %w", filePath, err))
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to read %s file: %w", filePath, err))
		return
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)

	if err != nil {
		log.Fatalf("Mmap: %v", err)
	}

	parseChunkSize := info.Size() / int64(numParsers)

	// kick off "parser" workers
	wg := sync.WaitGroup{}
	wg.Add(numParsers)

	// buffered to not block on merging
	chunkOffsetCh := make(chan int64, numParsers)
	chunkStatsCh := make(chan *Map[string, *StationData], numParsers)

	go func() {
		var i int64 = 0
		for i < info.Size() {
			if i+parseChunkSize < info.Size()+128 {
				chunkOffsetCh <- i
			}
			i += parseChunkSize
		}
		close(chunkOffsetCh)
	}()

	for i := 0; i < numParsers; i++ {
		go func() {
			results := NewHashMap[string, *StationData](maxNameNum)
			for chunkOffset := range chunkOffsetCh {
				maxAvailable := min(chunkOffset+parseChunkSize+128, info.Size())
				readUsingMMAP(data, results, uint64(chunkOffset), uint64(parseChunkSize), uint64(maxAvailable))
			}
			chunkStatsCh <- results
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(chunkStatsCh)
	}()

	scanner := &Scanner{pointer: unsafe.Pointer(&data[0]), position: 0, end: uint64(info.Size())}
	for chunkStats := range chunkStatsCh {
		for _, s := range chunkStats.cache {
			if s == nil {
				continue
			}
			byteArray := scanner.getByteArrayAt(s.nameAddress)
			s.name = string(byteArray[:s.nameLength])
			if ms, ok := finalResult[s.name]; !ok {
				finalResult[s.name] = s
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

	defer func() {
		if err := syscall.Munmap(data); err != nil {
			log.Fatalf("Munmap: %v", err)
		}
	}()

}

func readUsingMMAP(data []byte, results *Map[string, *StationData], offset uint64, bytesToRead uint64, maxAvailable uint64) {
	pointer := unsafe.Pointer(&data[0])
	scanner := &Scanner{pointer: pointer, position: offset, end: maxAvailable}
	segmentEnd := nextNewLine(scanner, min(maxAvailable-1, offset+bytesToRead))
	var segmentStart uint64
	if offset == 0 {
		segmentStart = offset
	} else {
		segmentStart = nextNewLine(scanner, offset) + 1
	}

	dist := (segmentEnd - segmentStart) / 4
	midPoint1 := nextNewLine(scanner, segmentStart+dist)
	midPoint2 := nextNewLine(scanner, segmentStart+dist+dist)
	midPoint3 := nextNewLine(scanner, segmentStart+dist+dist+dist)

	scanner1 := &Scanner{pointer: pointer, position: segmentStart, end: midPoint1}
	scanner2 := &Scanner{pointer: pointer, position: midPoint1 + 1, end: midPoint2}
	scanner3 := &Scanner{pointer: pointer, position: midPoint2 + 1, end: midPoint3}
	scanner4 := &Scanner{pointer: pointer, position: midPoint3 + 1, end: segmentEnd}

	for {
		if !scanner1.hasNext() {
			break
		}
		if !scanner2.hasNext() {
			break
		}
		if !scanner3.hasNext() {
			break
		}
		if !scanner4.hasNext() {
			break
		}
		word1 := scanner1.getLong()
		word2 := scanner2.getLong()
		word3 := scanner3.getLong()
		word4 := scanner4.getLong()
		delimiterMask1 := findDelimiter(word1)
		delimiterMask2 := findDelimiter(word2)
		delimiterMask3 := findDelimiter(word3)
		delimiterMask4 := findDelimiter(word4)
		word1b := scanner1.getLongAt(scanner1.pos() + 8)
		word2b := scanner2.getLongAt(scanner2.pos() + 8)
		word3b := scanner3.getLongAt(scanner3.pos() + 8)
		word4b := scanner4.getLongAt(scanner4.pos() + 8)
		delimiterMask1b := findDelimiter(word1b)
		delimiterMask2b := findDelimiter(word2b)
		delimiterMask3b := findDelimiter(word3b)
		delimiterMask4b := findDelimiter(word4b)
		station1 := findResult(word1, delimiterMask1, word1b, delimiterMask1b, scanner1, results)
		station2 := findResult(word2, delimiterMask2, word2b, delimiterMask2b, scanner2, results)
		station3 := findResult(word3, delimiterMask3, word3b, delimiterMask3b, scanner3, results)
		station4 := findResult(word4, delimiterMask4, word4b, delimiterMask4b, scanner4, results)
		temp1 := scanNumber(scanner1)
		temp2 := scanNumber(scanner2)
		temp3 := scanNumber(scanner3)
		temp4 := scanNumber(scanner4)
		record(station1, temp1)
		record(station2, temp2)
		record(station3, temp3)
		record(station4, temp4)
	}

	for scanner1.hasNext() {
		word := scanner1.getLong()
		pos := findDelimiter(word)
		wordB := scanner1.getLongAt(scanner1.pos() + 8)
		posB := findDelimiter(wordB)
		record(findResult(word, pos, wordB, posB, scanner1, results), scanNumber(scanner1))
	}

	for scanner2.hasNext() {
		word := scanner2.getLong()
		pos := findDelimiter(word)
		wordB := scanner2.getLongAt(scanner2.pos() + 8)
		posB := findDelimiter(wordB)
		record(findResult(word, pos, wordB, posB, scanner2, results), scanNumber(scanner2))
	}

	for scanner3.hasNext() {
		word := scanner3.getLong()
		pos := findDelimiter(word)
		wordB := scanner3.getLongAt(scanner3.pos() + 8)
		posB := findDelimiter(wordB)
		record(findResult(word, pos, wordB, posB, scanner3, results), scanNumber(scanner3))
	}

	for scanner4.hasNext() {
		word := scanner4.getLong()
		pos := findDelimiter(word)
		wordB := scanner4.getLongAt(scanner4.pos() + 8)
		posB := findDelimiter(wordB)
		record(findResult(word, pos, wordB, posB, scanner4, results), scanNumber(scanner4))
	}
}

func findResult(initialWord uint64, initialDelimiterMask uint64, wordB uint64, delimiterMaskB uint64, scanner *Scanner,
	stationData *Map[string, *StationData]) *StationData {
	word := initialWord
	delimiterMask := initialDelimiterMask
	var hash uint64
	var nameAddress = scanner.pos()
	var word2 = wordB
	var delimiterMask2 = delimiterMaskB
	if (delimiterMask | delimiterMask2) != 0 {
		letterCount1 := uint64(bits.TrailingZeros64(delimiterMask) >> 3)  // value between 1 and 8
		letterCount2 := uint64(bits.TrailingZeros64(delimiterMask2) >> 3) // value between 0 and 8
		mask := MASK2[letterCount1]
		word = word & MASK1[letterCount1]
		word2 = mask & word2 & MASK1[letterCount2]
		hash = word ^ word2
		existingResult, ok := stationData.GetUsingHash(hash)
		scanner.add(letterCount1 + (letterCount2 & mask))
		if ok {
			return existingResult
		}
	} else {
		// Slow-path for when the ';' could not be found in the first 16 bytes.
		hash = word ^ word2
		scanner.add(16)
		for {
			word = scanner.getLong()
			delimiterMask = findDelimiter(word)
			if delimiterMask != 0 {
				trailingZeros := bits.TrailingZeros64(delimiterMask)
				word = (word << (63 - trailingZeros))
				scanner.add(uint64(trailingZeros >> 3))
				hash ^= word
				break
			} else {
				scanner.add(8)
				hash ^= word
			}
		}
		existingResult, ok := stationData.GetUsingHash(hash)
		if ok {
			return existingResult
		}
	}

	// Save length of name for later.
	nameLength := int(scanner.pos() - nameAddress)
	result := &StationData{
		MinTemp:     MAX_TEMP,
		MaxTemp:     MIN_TEMP,
		Count:       0,
		nameAddress: nameAddress,
		nameLength:  nameLength,
	}
	stationData.SetUsingHash(hash, result)
	return result
}

func findDelimiter(word uint64) uint64 {
	input := word ^ 0x3B3B3B3B3B3B3B3B
	return (input - 0x0101010101010101) & ^input & 0x8080808080808080
}

func nextNewLine(scanner *Scanner, prev uint64) uint64 {
	for {
		currentWord := scanner.getLongAt(prev)
		input := currentWord ^ 0x0A0A0A0A0A0A0A0A
		pos := (input - 0x0101010101010101) & ^input & 0x8080808080808080
		if pos != 0 {
			prev += (uint64(bits.TrailingZeros64(uint64(pos))) >> 3)
			break
		} else {
			prev += 8
		}
	}
	return prev
}

func scanNumber(scanner *Scanner) int64 {
	numberWord := scanner.getLongAt(scanner.pos() + 1)
	decimalSepPos := bits.TrailingZeros64(^numberWord & 0x10101000)
	number := convertIntoNumber(decimalSepPos, int64(numberWord))
	scanner.add((uint64(decimalSepPos>>3) + 4))
	return number
}

// Special method to convert a number in the ascii number into an int without branches created by Quan Anh Mai.
func convertIntoNumber(decimalSepPos int, numberWord int64) int64 {
	shift := 28 - decimalSepPos
	// signed is -1 if negative, 0 otherwise
	signed := ^(numberWord << 59) >> 63
	designMask := ^(signed & 0xFF)
	// Align the number to a specific position and transform the ascii to digit value
	digits := ((numberWord & designMask) << shift) & 0x0F000F0F00
	// Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
	// 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
	// 0x000000UU00TTHH00 + 0x00UU00TTHH000000 * 10 + 0xUU00TTHH00000000 * 100
	absValue := ((digits * 0x640a0001) >> 32) & 0x3FF
	return (absValue ^ signed) - signed
}

func record(station *StationData, temp int64) {
	if temp < station.MinTemp {
		station.MinTemp = temp
	}
	if temp > station.MaxTemp {
		station.MaxTemp = temp
	}
	station.Sum += temp
	station.Count++
}

func getFloatValue(val int64) float64 {
	return float64(val) / 10
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
		avg := round(round(getFloatValue(s.Sum)) / float64(s.Count))
		builder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, getFloatValue(s.MinTemp), avg, getFloatValue(s.MaxTemp)))
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
