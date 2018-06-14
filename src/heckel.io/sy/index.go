package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"errors"
	"os"
	"io/ioutil"
	"path/filepath"
)

type index struct {
	root			string
	idx             map[string]bool
	db              *sql.DB
	tx              *sql.Tx
	fileInsert      *sql.Stmt
	fileDelete      *sql.Stmt
	fileSelect		*sql.Stmt
	fileChunkInsert *sql.Stmt
	fileChunkDelete *sql.Stmt
	chunkInsert     *sql.Stmt
}

func NewIndex(root string) (*index, error) {
	os.MkdirAll(root, 0744)
	os.MkdirAll(root + "/idx", 0744)

	db, err := sql.Open("sqlite3", root + "/db")

	if err != nil {
		return nil, err
	}

	db.Exec("create table file (file_id int primary key, file_checksum varchar(255), filename varchar(255) unique)")
	db.Exec("create table chunk (chunk_checksum varchar(255) primary key)")
	db.Exec("create table file_chunk (file_id int, chunk_checksum varchar(255), num int)")

	db.Exec("create index index_file_file_id on file_chunk.file_id")

	idx := make(map[string]bool)

	return &index{root: root, db: db, idx: idx}, nil
}

func (index *index) Begin() (err error) {
	index.tx, err = index.db.Begin()

	if err != nil {
		return err
	}

	index.fileInsert, err = index.tx.Prepare("insert into file (file_id, file_checksum, filename) values (?, ?, ?)")

	if err != nil {
		return err
	}

	index.fileDelete, err = index.tx.Prepare("delete from file where file_id = ?")

	if err != nil {
		return err
	}

	index.fileSelect, err = index.tx.Prepare("select file_id from file where filename = ?")

	if err != nil {
		return err
	}

	index.fileChunkInsert, err = index.tx.Prepare("insert into file_chunk (file_id, chunk_checksum, num) values (?, ?, ?)")

	if err != nil {
		return err
	}

	index.fileChunkDelete, err = index.tx.Prepare("delete from file_chunk where file_id = ?")

	if err != nil {
		return err
	}

	index.chunkInsert, err = index.tx.Prepare("insert into chunk (chunk_checksum) values (?)")

	if err != nil {
		return err
	}

	return nil
}

func (index *index) Load() error {
	rows, err := index.db.Query("select chunk_checksum from chunk")

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var checksum string
		err = rows.Scan(&checksum)

		if err == nil {
			index.idx[checksum] = true
		}
	}

	return nil
}

func (index *index) Exists(checksum string) bool {
	if _, ok := index.idx[checksum]; ok {
		return true
	} else {
		return false
	}
}

func (index *index) Close() {
	index.db.Close()
}

func (index *index) Commit() {
	index.tx.Commit()
}

func (index *index) AddChunk(checksum string) {
	index.idx[checksum] = true
	index.chunkInsert.Exec(checksum)
}

func (index *index) AddFileChunk(fileId int, checksum string, num int) {
	index.fileChunkInsert.Exec(fileId, checksum, num)
}

func (index *index) AddFile(fileId int, checksum string, filename string) {
	index.fileInsert.Exec(fileId, "", filename)
}

func (index *index) GetFileId(filename string) (int, error) {
	result, err := index.fileSelect.Query(filename)

	if err != nil {
		return 0, err
	}

	if !result.Next() {
		return 0, errors.New("Unknown file")
	}

	var fileId int
	result.Scan(&fileId)

	return fileId, nil
}


func (index *index) RemoveFile(filename string) error {
	fileId, err := index.GetFileId(filename)

	if err != nil {
		return err
	}

	_, err = index.fileDelete.Exec(fileId)

	if err != nil {
		return err
	}

	return nil
}

func (index *index) GetChunkPath(checksum string) string {
	prefix := checksum[:2]
	dir := index.root + "/idx/" + prefix

	return dir + "/" + checksum
}

func (index *index) WriteChunk(checksum string, b []byte) {
	chunkFile := index.GetChunkPath(checksum)

	if _, err := os.Stat(chunkFile); err != nil {
		os.Mkdir(filepath.Dir(chunkFile), 0744)
		ioutil.WriteFile(chunkFile, b, 0644)
	}
}

func (index *index) ReadChunk(checksum string) ([]byte, error) {
	chunkFile := index.GetChunkPath(checksum)
	return ioutil.ReadFile(chunkFile)
}

func (index *index) DeleteChunk(checksum string) {
	chunkFile := index.GetChunkPath(checksum)
	os.Remove(chunkFile)
}
