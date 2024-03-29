package goToolMSSqlHelper

import (
	"errors"
	"github.com/Deansquirrel/goToolMSSql"
	"github.com/Deansquirrel/goToolMSSql2000"
)

const (
	sqlGetDbId = "" +
		"select db_id()"
)

func GetDbId(dbConfig *goToolMSSql.MSSqlConfig) (int, error) {
	rows, err := GetRowsBySQL(dbConfig, sqlGetDbId)
	if err != nil {
		return -1, err
	}
	var id int
	flag := false
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return -1, err
		}
		flag = true
	}
	if rows.Err() != nil {
		return -1, rows.Err()
	}
	if flag {
		return id, nil
	} else {
		return -1, errors.New("get db id return empty")
	}
}

func GetDbId2000(dbConfig *goToolMSSql2000.MSSqlConfig) (int, error) {
	rows, err := GetRowsBySQL2000(dbConfig, sqlGetDbId)
	if err != nil {
		return -1, err
	}
	var id int
	flag := false
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return -1, err
		}
		flag = true
	}
	if rows.Err() != nil {
		return -1, rows.Err()
	}
	if flag {
		return id, nil
	} else {
		return -1, errors.New("get db id return empty")
	}
}
