package main

import (
//    "crypto/md5"
//    "encoding/hex"
    "fmt"
    "log"
//    "math/rand"
//    "os"
//    "strconv"
    "time"
    "sync"
    "errors"
    "github.com/boltdb/bolt"
)

const _FLAG_DELETE = "_FLAG_DELETE"

type TableSchema struct {
    columns map[string]bool

    rwlock_  sync.Mutex
}


type RowMutation struct {
    pairs map[string]string
    num int

    schema *TableSchema
}

type Table struct {
    schema_ *TableSchema
    path_  string

    rwlock_  sync.Mutex
    db_ *bolt.DB

    compact_channel_ chan bool
}

func (s *TableSchema) AddColumn(column string) error {
    s.rwlock_.Lock()
    defer s.rwlock_.Unlock()
    if ok := s.columns[column]; ok == true {
        return errors.New("column exist")
    }
    s.columns[column] = true
    return nil
}

func (s *TableSchema) IsColumn(column string) bool {
    s.rwlock_.Lock()
    defer s.rwlock_.Unlock()
    return s.columns[column]
}

func (s *TableSchema) PrintColumn() {
    for c := range s.columns {
        fmt.Println(c)
    }
}

func (s *TableSchema) NumColumns() int {
    s.rwlock_.Lock()
    defer s.rwlock_.Unlock()
    var list = []bool{}
    for _, v := range s.columns {
        list = append(list, v)
    }
    return len(list)
}

func (m *RowMutation) Write(key, column, value string) bool {
    if !m.schema.IsColumn(column) {
        return false
    }
    m.pairs[key + "/" + column] = value
    return true
}

func (m *RowMutation) Delete(key, column string) bool {
    if !m.schema.IsColumn(column) {
        return false
    }

    m.pairs[key + "/" + column] = _FLAG_DELETE
    return true
}

func (m *RowMutation) DeleteRow(key string) bool {
    for c:= range m.schema.columns {
      m.pairs[key + "/" + c] = _FLAG_DELETE
    }
    return true
}


func OpenTable(path string, table_schema *TableSchema) (*Table, error) {
    if table_schema.NumColumns() == 0 {
        return nil, errors.New("no column is defined")
    }
    var table = &Table{path_:path}
    db, err := bolt.Open(table.path_, 0600, &bolt.Options{Timeout: 1 * time.Second})
    if err != nil {
        log.Fatal(err)
        return nil, err
    }
    table.db_ = db
    table.schema_ = table_schema
    return table, nil
}

func (t *Table) NewRowMutation() (*RowMutation, error) {
    log.Printf("NewRowMutation")
    mutation := new(RowMutation)
    mutation.pairs = make(map[string]string)
    mutation.schema = t.schema_
    return mutation, nil
}

func (t *Table) UpdateMutation(mutation *RowMutation) error {
    log.Printf("UpdateMutation")
    t.rwlock_.Lock()
    defer t.rwlock_.Unlock()
    

    t.db_.Update(func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists([]byte("mybucket"))
        if err != nil {
            return err
        }
        return nil
    })
    for c, v := range mutation.pairs {
        if err := t.db_.Update(func(tx *bolt.Tx) error {
            log.Printf("UpdateMutation: IN TX:{%s,%s}", c, v)
            b := tx.Bucket([]byte("mybucket"))
            err := b.Put([]byte(c), []byte(v))
            if err != nil {
                log.Printf("wrong")
            }
            return err
        }); err == nil {

        }
    }
    return nil
}

func (t *Table) PrintRow(key string) (error) {
    log.Printf("PrintRow")
    t.rwlock_.Lock()
    defer t.rwlock_.Unlock()
    if err := t.db_.View(func(tx *bolt.Tx) error {
        log.Printf("In TX")
        b := tx.Bucket([]byte("mybucket"))
        for column, _ := range t.schema_.columns {
            internal_key := key + "/" + column
            fmt.Printf("read: %s --> ", internal_key)
            value := b.Get([]byte(internal_key))
            if string(value) != _FLAG_DELETE {
                if string(value) != "" {
                    fmt.Printf("{%s: %s}", column, value)
                }
                fmt.Printf("\n")
            } else {
                fmt.Printf("deleted\n")
            }
        }
        return nil
    }); err == nil {
        return err
    }
    return nil
}

func (t *Table) CompactionTask() {
    log.Printf("CompactionTask")

    t.rwlock_.Lock()
    defer t.rwlock_.Unlock()
    
    tx, err := t.db_.Begin(false)
    if err != nil {
        log.Fatal("fatal: open db iterator")
        return
    }
    deleted_keys := []string{}
    c := tx.Bucket([]byte("mybucket")).Cursor()
    for k, v := c.First(); k != nil; k, v = c.Next() {
        log.Printf("compact:(%s: %s)\n", k, v)
        if string(v) == _FLAG_DELETE {
            deleted_keys = append(deleted_keys, string(k))
        }
    }
    for _, key := range deleted_keys {
        log.Printf("deleted: %s\n", key) 
        if err := t.db_.Update(func(tx *bolt.Tx) error {
//            b := tx.Bucket([]byte("mybucket"))
//            err := b.Delete([]byte(key))
//            if err != nil {
//                log.Printf("wrong")
//            }
            return err
        }); err == nil {

        }
    
    }
}

func (t *Table) StartCompaction() bool {
    t.compact_channel_ = t.ScheduleTimer(t.CompactionTask, 15*time.Millisecond)
    return t.compact_channel_ != nil
}

func (t *Table) StopCompaction() {
    if t.compact_channel_ != nil {
        t.compact_channel_ <- true
    }
    t.compact_channel_ = nil
}

func (t *Table) ScheduleTimer(what func(), delay time.Duration) chan bool {
    stop := make(chan bool)

    go func() {
        for {
            what()
            select {
            case <-time.After(delay):
            case <-stop:
                return
            }
        }
    }()

    return stop
}



func main() {
    schema := new(TableSchema)
    schema.columns = make(map[string]bool)
    schema.AddColumn("c1")
    schema.AddColumn("c2")
    schema.AddColumn("c3")
    fmt.Println(schema.NumColumns())

    table, err := OpenTable("./test.db", schema)
    if err != nil {
        log.Fatal(err)
    }

    mutation, err := table.NewRowMutation()
    mutation.Write("k1", "c1", "v1")
    mutation.Write("k2", "c2", "v2")
    mutation.DeleteRow("k1")
    if err != nil {
        log.Fatal(err)
    }

    if err := table.UpdateMutation(mutation); err != nil {
        log.Fatal(err)
    }

    table.PrintRow("k1")
    table.PrintRow("k2")
    table.StartCompaction()
    defer table.StopCompaction()
    time.Sleep(50 * time.Millisecond)

    table.PrintRow("k1")
    table.PrintRow("k2")
}
