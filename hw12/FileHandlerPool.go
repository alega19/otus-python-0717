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
)

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

func (hp *FileHandlerPool) handleFile(filename string, memcacheClients map[string]*MemcacheClient) error {
	file, err := os.Open(filename); if err != nil {
		return err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file); if err != nil {
		return err
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		dev, err := parseAndSerialize(line); if err != nil {
			logger.Println(err)
			continue
		}
		mc := memcacheClients[dev.type_]
		if mc == nil {
			logger.Println(errors.New("unknown device type: " + dev.type_))
			continue
		}
		mc.Set(&memcache.Item{
			Key: dev.type_ + ":" + dev.id,
			Value: dev.data,
		})
	}
	if err = scanner.Err(); err != nil {
		return err
	}
	return nil
}

func parseAndSerialize(s string) (dev DeviceData, err error) {
	parts := strings.Split(s, "\t"); if len(parts) != 5 {
		err = errors.New("parsing error: " + s)
		return
	}
	dev.type_ = parts[0]
	dev.id = parts[1]

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
	dev.data, err = proto.Marshal(ua); if err != nil {
		err = errors.New("marshaling error")
		return
	}
	return
}
