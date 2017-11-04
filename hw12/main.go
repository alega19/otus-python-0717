package main

import (
	"flag"
	"path/filepath"
	"time"
	"sort"
)


type DeviceData struct {
	type_ string
	id    string
	data  []byte
}

var logger = NewLogger()

func main() {
	defer logger.TryStopAndWait()

	var idfa, gaid, adid, dvid, pattern string
	var workerNum int

	flag.StringVar(&idfa, "idfa", "127.0.0.1:33013", "idfa")
	flag.StringVar(&gaid, "gaid", "127.0.0.1:33014", "gaid")
	flag.StringVar(&adid, "adid", "127.0.0.1:33015", "adid")
	flag.StringVar(&dvid, "dvid", "127.0.0.1:33016", "dvid")
	flag.StringVar(&pattern, "pattern", "", "pattern")
	flag.IntVar(&workerNum, "workers", 4, "workers")
	flag.Parse()

	if pattern == "" {
		logger.Println("'pattern' is required")
		return
	}

	memcacheClients := make(map[string]*MemcacheClient)
	for devType, ip := range map[string]string{
		"idfa": idfa,
		"gaid": gaid,
		"adid": adid,
		"dvid": dvid,
	} {
		mc, err := NewMemcacheClient(ip, time.Second)
		if err != nil {
			logger.Println(err)
			return
		}
		defer mc.Close()
		memcacheClients[devType] = mc
	}

	filenames, err := filepath.Glob(pattern)
	if err != nil {
		logger.Println(err)
		return
	}
	sort.Strings(filenames)

	hndPool := NewFileHandlerPool(workerNum, memcacheClients)
	defer hndPool.TryStopAndWait()
	for _, filename := range filenames {
		logger.Println("Processing " + filename)
		hndPool.AddFile(filename)
	}
}
