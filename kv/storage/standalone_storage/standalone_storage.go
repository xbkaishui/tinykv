package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := dbPath + "/kv"
	raftPath := dbPath + "/raft"

	kvEngine := engine_util.CreateDB(kvPath,false)
	raftEngine := engine_util.CreateDB(raftPath,false)
	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvEngine,raftEngine,kvPath,raftPath),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneReader(s.engine.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _,b := range batch{
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			err := engine_util.PutCF(s.engine.Kv,put.Cf,put.Key,put.Value)
			if err!=nil{
				return err
			}
		case storage.Delete:
			delete := b.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engine.Kv,delete.Cf,delete.Key)
			if err!=nil{
				return err
			}
		}
	}
	return nil
}

type StandAloneReader struct {
	Txn *badger.Txn
}

func NewStandAloneReader(Txn *badger.Txn) *StandAloneReader{
	return &StandAloneReader{
		Txn: Txn,
	}
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error){
	value, err := engine_util.GetCFFromTxn(s.Txn,cf,key)
	if err == badger.ErrKeyNotFound{
		return nil,nil
	}
	return value, err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator{
	return engine_util.NewCFIterator(cf,s.Txn)
}

func (s *StandAloneReader) Close(){
	s.Txn.Discard()
}