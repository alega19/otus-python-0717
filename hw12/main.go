package main

import (
	"flag"
	"path/filepath"
	"log"
	"compress/gzip"
	"os"
	"bufio"
	"strings"
	"strconv"
	"time"
	"sort"
	"errors"
	"github.com/golang/protobuf/proto"
	"./appsinstalled"
	"github.com/rainycape/memcache"
)


func main() {
	var idfa, gaid, adid, dvid, pattern string

	flag.StringVar(&idfa, "idfa", "127.0.0.1:33013", "idfa")
	flag.StringVar(&gaid, "gaid", "127.0.0.1:33014", "gaid")
	flag.StringVar(&adid, "adid", "127.0.0.1:33015", "adid")
	flag.StringVar(&dvid, "dvid", "127.0.0.1:33016", "dvid")
	flag.StringVar(&pattern, "pattern", "", "pattern")
	flag.Parse()

	if pattern == "" {
		log.Fatal("'pattern' is required")
	}

	mcmap, err := createMemClients(idfa, gaid, adid, dvid); if err != nil {
		log.Fatal(err)
	}
	defer mcmap["idfa"].Close()
	defer mcmap["gaid"].Close()
	defer mcmap["adid"].Close()
	defer mcmap["dvid"].Close()

	filenames, err := filepath.Glob(pattern); if err != nil {
		log.Fatal(err)
	}
	sort.Strings(filenames)

	for _, filename := range filenames {
		log.Println("Processing " + filename)
		file, err := os.Open(filename); if err != nil {
			log.Println(err)
			continue
		}

		gzreader, err := gzip.NewReader(file); if err != nil {
			log.Println(err)
			file.Close()
			continue
		}

		scanner := bufio.NewScanner(gzreader)
		for scanner.Scan() {
			line := scanner.Text()
			line = strings.TrimSpace(line)
			dev_type, dev_id, data, err := parseAndSerialize(line); if err != nil {
				continue
			}
			key := dev_type + ":" + dev_id
			err = insertIntoMemcache(mcmap, dev_type, &memcache.Item{
				Key: key,
				Value: data,
			})
			if err != nil {
				log.Println(err)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Println(err)
		}
		gzreader.Close()
		file.Close()
		new_filename := filepath.Dir(filename) + "/." + filepath.Base(filename)
		if err := os.Rename(filename, new_filename); err != nil {
			log.Fatal(err)
		}
	}
}


func createMemClients(idfa string, gaid string, adid string, dvid string) (map[string]*memcache.Client, error) {
	var err error
	mcmap := make(map[string]*memcache.Client)
	mcmap["idfa"], err = memcache.New(idfa); if err != nil {
		return nil, err
	}
	mcmap["gaid"], err = memcache.New(gaid); if err != nil {
                return nil, err
        }
	mcmap["adid"], err = memcache.New(adid); if err != nil {
                return nil, err
        }
	mcmap["dvid"], err = memcache.New(dvid); if err != nil {
                return nil, err
        }
	for _, mc := range mcmap {
		mc.SetTimeout(2 * time.Second)
	}
	return mcmap, nil
}


func parseAndSerialize(s string) (string, string, []byte, error) {
	parts := strings.Split(s, "\t"); if len(parts) != 5 {
		return "", "", nil, errors.New("parsing error")
	}

	lat, err := strconv.ParseFloat(parts[2], 64); if err != nil {
		return "", "", nil, errors.New("parsing error")
	}

	lon, err := strconv.ParseFloat(parts[3], 64); if err != nil {
		return "", "", nil, errors.New("parsing error")
	}

	var apps []uint32
	for _, elem := range strings.Split(parts[4], ",") {
		app, err := strconv.ParseUint(elem, 10, 32); if err != nil {
			continue
		}
		apps = append(apps, uint32(app))
	}
	ua := &appsinstalled.UserApps {
		Lat: &lat,
		Lon: &lon,
		Apps: apps,
	}
	data, err := proto.Marshal(ua); if err != nil {
		return "", "", nil, errors.New("marshaling error")
	}
	return parts[0], parts[1], data, nil
}


func insertIntoMemcache(mcmap map[string]*memcache.Client, dev_type string, item *memcache.Item) error {
	var err error
	for i := 0; i < 3; i++ {
		err = mcmap[dev_type].Set(item)
		if err == nil {
			break
		}
	}
	return err
}

