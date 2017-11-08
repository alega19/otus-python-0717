package main

import (
	"sync"
	"github.com/rainycape/memcache"
	"os"
	"compress/gzip"
	"bufio"
	"strings"
	"errors"
	"strconv"
	"github.com/golang/protobuf/proto"
	"path/filepath"
	"fmt"
)


type Message struct {
	item *memcache.Item
	errCh chan bool
}


type DeviceData struct {
	Type string
	Id   string
	Data []byte
}


type FileHandlerPool struct {
	filenamesCh chan string
	wg sync.WaitGroup
}

func NewFileHandlerPool(num int, memcacheClients map[string]*MemcacheClient) (hp *FileHandlerPool) {
	hp = &FileHandlerPool{
		filenamesCh: make(chan string),
	}
	for num > 0 {
		go hp.startHandler(memcacheClients)
		num--
	}
	return
}

func (hp *FileHandlerPool) TryStopAndWait() {
	close(hp.filenamesCh)
	hp.wg.Wait()
}

func (hp *FileHandlerPool) AddFile(filename string) {
	hp.filenamesCh <- filename
}

func (hp *FileHandlerPool) startHandler(memcacheClients map[string]*MemcacheClient) {
	hp.wg.Add(1)
	defer hp.wg.Done()
	for filename := range hp.filenamesCh {
		err := hp.handleFile(filename, memcacheClients)
		if err != nil {
			logger.Println(err)
			continue
		}
		newFilename := filepath.Dir(filename) + "/." + filepath.Base(filename)
		if err := os.Rename(filename, newFilename); err != nil {
			logger.Println(err)
		}
	}
}

func (hp *FileHandlerPool) handleFile(filename string, memcacheClients map[string]*MemcacheClient) (err error) {
	file, err := os.Open(filename); if err != nil {return}
	defer file.Close()

	gzReader, err := gzip.NewReader(file); if err != nil {return}
	defer gzReader.Close()

	var processed, errorsNum uint64 = 0, 0
	scanner := bufio.NewScanner(gzReader)
	errCh := make(chan bool, 1024)
	var counter uint64 = 0
	for scanner.Scan() {
		processed++
		line := scanner.Text()
		line = strings.TrimSpace(line)
		dev, err := parseAndSerialize(line); if err != nil {
			errorsNum++
			logger.Println(err)
			continue
		}
		mc := memcacheClients[dev.Type]
		if mc == nil {
			errorsNum++
			logger.Println(errors.New("unknown device type: " + dev.Type))
			continue
		}
		mc.Set(&Message{
			&memcache.Item{
				Key:   dev.Type + ":" + dev.Id,
				Value: dev.Data,
			},
			errCh,
		})
		counter++
		if counter == uint64(cap(errCh)) {
			errorsNum += collectErrors(errCh, counter)
			counter = 0
		}
	}
	errorsNum += collectErrors(errCh, counter)
	errRate := float64(errorsNum) / float64(processed)
	if errRate < NORMAL_ERR_RATE {
		logger.Println(fmt.Sprintf("Acceptable error rate (%f). Successfull load \"%s\"", errRate, filename))
	} else {
		logger.Println(fmt.Sprintf("High error rate (%f > %f). Failed load \"%s\"", errRate, NORMAL_ERR_RATE, filename))
	}
	err = scanner.Err()
	return
}

func collectErrors(errCh chan bool, counter uint64) (num uint64) {
	for counter > 0 {
		if <-errCh {
			num++
		}
		counter--
	}
	return
}

func parseAndSerialize(s string) (dev DeviceData, err error) {
	parts := strings.Split(s, "\t"); if len(parts) != 5 {
		err = errors.New("parsing error: " + s)
		return
	}
	dev.Type = parts[0]
	dev.Id = parts[1]

	lat, err := strconv.ParseFloat(parts[2], 64); if err != nil {
		err = errors.New("parsing error: " + s)
		return
	}

	lon, err := strconv.ParseFloat(parts[3], 64); if err != nil {
		err = errors.New("parsing error: " + s)
		return
	}

	var apps []uint32
	for _, elem := range strings.Split(parts[4], ",") {
		app, err := strconv.ParseUint(elem, 10, 32); if err != nil {
			continue
		}
		apps = append(apps, uint32(app))
	}
	ua := &UserApps {
		Lat: &lat,
		Lon: &lon,
		Apps: apps,
	}
	dev.Data, err = proto.Marshal(ua); if err != nil {
		err = errors.New("marshaling error")
		return
	}
	return
}
