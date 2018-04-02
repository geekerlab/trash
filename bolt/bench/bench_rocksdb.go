package main

import (
    "crypto/md5"
    "encoding/hex"
    "fmt"
    "log"
    "math/rand"
    "os"
    "strconv"
    "time"

    "github.com/tecbot/gorocksdb"
)

func main() {
    concurrency, _ := strconv.Atoi(os.Args[1])
    tasks, _ := strconv.Atoi(os.Args[2])
    disks, _ := strconv.Atoi(os.Args[3])
    partitions, _ := strconv.Atoi(os.Args[4])

    dbPaths := listDBPaths('b', disks, partitions)
    dbs := openDBs(dbPaths)
    ropt := gorocksdb.NewDefaultReadOptions()
    wopt := gorocksdb.NewDefaultWriteOptions()

    wch := make(chan time.Duration, concurrency)
    wsuccess := make(chan int, concurrency)

    rch := make(chan time.Duration, concurrency)
    rsuccess := make(chan int, concurrency)
    srcs := prepareData(1000)

    keys := populateData(1000, srcs, dbs)

    for t := 0; t < 10; t++ {
        log.Printf("start to benchmark...")
        begin := time.Now()
        for i := 0; i < concurrency; i++ {
            go func(id int) {
                var rduration time.Duration
                var wduration time.Duration
                rsucc := 0
                wsucc := 0

                for j := 0; j < tasks; j++ {
                    di := rand.Intn(len(dbs))
                    db := dbs[di]

                    if rand.Intn(100) <= 20 {
                        key := generateKey()
                        data := srcs[rand.Intn(len(srcs))].Data

                        start := time.Now()
                        if err := db.Put(wopt, []byte(key), data); err == nil {
                            wduration += time.Since(start)
                            wsucc++
                        }
                    } else {
                        key := keys[di][rand.Intn(len(keys[di]))]

                        start := time.Now()
                        if _, err := db.Get(ropt, []byte(key)); err == nil {
                            rduration += time.Since(start)
                            rsucc++
                        }
                    }
                }

                rch <- rduration
                rsuccess <- rsucc
                wch <- wduration
                wsuccess <- wsucc
            }(i)
        }

        var relapsed, welapsed time.Duration
        var rcount, wcount int64
        for i := 0; i < concurrency; i++ {
            relapsed += <-rch
            welapsed += <-wch
            rcount += int64(<-rsuccess)
            wcount += int64(<-wsuccess)
        }

        d := time.Since(begin)

        log.Printf("For read requests:")
        log.Printf("it took %s", relapsed)
        log.Printf("success requests: %d", rcount)
        log.Printf("time cost per request: %.2fms", float64((relapsed/time.Millisecond))/float64(rcount))
        log.Printf("qps: %.2f\n\n", float64(rcount*1000)/float64(d/time.Millisecond))
        log.Printf("For write requests:")
        log.Printf("it took %s", welapsed)
        log.Printf("success requests: %d", wcount)
        log.Printf("time cost per request: %.2fms", float64((welapsed/time.Millisecond))/float64(wcount))
        log.Printf("qps: %.2f\n\n\n", float64(wcount*1000)/float64(d/time.Millisecond))

        time.Sleep(time.Second * 600)
    }

    for _, d := range dbs {
        d.Close()
    }
}

func generateKey() string {
    t := fmt.Sprintf("tenant%06d", rand.Intn(100))
    c := fmt.Sprintf("container%04d", rand.Intn(10))

    ts := time.Now()
    o := strconv.FormatInt(ts.UnixNano(), 10)

    return fmt.Sprintf("/%s/%s/%s", t, c, o)
}

type src struct {
    Data     []byte
    Checksum string
}

func prepareData(n int) []src {
    var srcs []src
    for i := 0; i < n; i++ {
        data := Bytes(256)
        checksum := md5.Sum(data)
        srcs = append(srcs, src{Data: data, Checksum: hex.EncodeToString(checksum[:])})
    }

    return srcs
}

func openDBs(paths []string) []*gorocksdb.DB {
    var dbs []*gorocksdb.DB

    opts := gorocksdb.NewDefaultOptions()
    opts.SetCreateIfMissing(true)
    opts.SetCompression(gorocksdb.NoCompression)
    opts.SetWriteBufferSize(671088640)

    for _, p := range paths {
        db, err := gorocksdb.OpenDb(opts, p)
        if err != nil {
            log.Fatal(err)
        }
        dbs = append(dbs, db)
    }

    return dbs
}

func listDBPaths(begin rune, disks int, partitions int) []string {
    var dbs []string

    for i := 0; i < disks; i++ {
        for j := 0; j < partitions; j++ {
            db := fmt.Sprintf("/srv/node/sd%c/%d", begin, j)
            dbs = append(dbs, db)
        }
        begin += 1
    }

    return dbs
}

func populateData(num int, srcs []src, dbs []*gorocksdb.DB) [][]string {
    keys := make([][]string, len(dbs))
    wopt := gorocksdb.NewDefaultWriteOptions()
    wopt.SetSync(true)

    for i, db := range dbs {
        var ks []string
        for j := 0; j < num; j++ {
            data := srcs[rand.Intn(len(srcs))].Data
            p := generateKey()

            if err := db.Put(wopt, []byte(p), data); err != nil {
                continue
            }
            ks = append(ks, p)
        }

        keys[i] = ks
    }

    return keys
}

func checksum(data []byte) string {
    checksum := md5.Sum(data)
    return hex.EncodeToString(checksum[:])
}

func Bytes(n int) []byte {
    d := make([]byte, n)
    rand.Read(d)

    return d
}

