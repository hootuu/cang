package cang

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/hootuu/utils/errors"
	"github.com/hootuu/utils/logger"
	"github.com/hootuu/utils/sys"
	"go.uber.org/zap"
	"path/filepath"
	"regexp"
	"sync"
)

type Cang struct {
	name        string
	path        string
	ready       bool
	boltDB      *bolt.DB
	collections map[string]*Collection
	lock        sync.Mutex
}

func NameVerify(nameStr string) *errors.Error {
	matched, _ := regexp.MatchString("^[a-zA-Z0-9_]{1,108}$", nameStr)
	if !matched {
		return errors.Verify(fmt.Sprintf("invalid dbx.name: %s", nameStr))
	}
	return nil
}

func NewCang(name string, dbPath string) (*Cang, *errors.Error) {
	if err := NameVerify(name); err != nil {
		return nil, err
	}
	absPath := dbPath
	if !filepath.IsAbs(absPath) {
		absPath = filepath.Join(sys.WorkingDirectory, dbPath, name+".db")
	}
	if !doCheckAndPut(absPath) {
		return nil, errors.Verify("databases with the same path and name exist")
	}
	dbx := &Cang{
		name: name,
		path: absPath,
	}
	if err := dbx.doInit(); err != nil {
		return nil, err
	}
	return dbx, nil
}

func (dbx *Cang) Collection(name string) (*Collection, *errors.Error) {
	coll, ok := dbx.collections[name]
	if ok {
		return coll, nil
	}
	dbx.lock.Lock()
	defer dbx.lock.Unlock()
	coll, err := doNewCollection(dbx, name)
	if err != nil {
		return nil, err
	}
	dbx.collections[name] = coll
	return coll, nil
}

func (dbx *Cang) IsReady() bool {
	return dbx.ready
}

func (dbx *Cang) Close() *errors.Error {
	sys.Info("Close dbx.", dbx.name, " ......")
	if dbx.boltDB == nil {
		return nil
	}
	nErr := dbx.boltDB.Close()
	if nErr != nil {
		logger.Logger.Error("dbx.boltDB.Close() failed", zap.Error(nErr))
		return errors.Sys("dbx." + dbx.path + " close failed: " + nErr.Error())
	}
	return nil
}

func (dbx *Cang) doInit() *errors.Error {
	var nErr error
	dbx.boltDB, nErr = bolt.Open(dbx.path, 0600,
		&bolt.Options{ReadOnly: false})
	if nErr != nil {
		sys.Info("Linker Store Init Failed:", nErr.Error())
		return errors.Sys("init linker store failed")
	}
	dbx.ready = true
	return nil
}

var gDBDict = make(map[string]struct{})

func Dump(callback func(dbAbsPath string)) {
	for k, _ := range gDBDict {
		callback(k)
	}
}

func doCheckAndPut(dbAbsPath string) bool {
	_, exists := gDBDict[dbAbsPath]
	if exists {
		return false
	}
	gDBDict[dbAbsPath] = struct{}{}
	return true
}
