package storage

import (
	"encoding/json"
	radix "github.com/mediocregopher/radix/v3"
)

type Redis struct {
	Host           string `yaml:"host" json:"host"`
	Port           string `yaml:"port" json:"port"`
	ConnectionType string `yaml:"connectionType" json:"connectionType"`
	Connections    int    `yaml:"connections" json:"connections"`
}

const (
	get     = "GET"
	set     = "SET"
	lpop    = "LPOP"
	lpush   = "LPUSH"
	Lset    = "LSET"
	rpop    = "RPOP"
	rpush   = "RPUSH"
	lrange  = "LRANGE"
	lindex  = "LINDEX"
	lrem    = "LREM"
	linsert = "LINSERT"
	llen    = "LLEN"
	hget    = "HGET"
	hset    = "HSET"
	hgetall = "HGETALL"
	hkeys   = "HKEYS"
	hlen    = "HLEN"
	hexists = "HEXISTS"
)

func (r Redis) getClient() (radix.Client, error) {
	client, err := radix.NewPool(r.ConnectionType, r.Host, r.Connections)

	if err != nil {
		return nil, err
	}

	return client, err
}

// Get Value from redis and return json string or error
func RedisGet(k string, c radix.Client) (string, error) {

	var val string

	err := c.Do(radix.Cmd(&val, get, k))

	if err != nil {
		return "", err
	}

	return val, nil
}

// Set Value as redis string
func RedisSet(k string, v string, c radix.Client) error {
	return c.Do(radix.Cmd(nil, set, k, v))
}

// Get Value from the top a redis list
func RedisLPop(k, f string, c radix.Client) ([]string, error) {
	return redisListPop(k, f, lpop, c)
}

// Prepend values to a redis list
func RedisLPush(k, f string, v string, c radix.Client) error {
	return c.Do(radix.Cmd(nil, lpush, k, f, v))
}

// Get values from the bottom of a redis list
func RedisRPop(k, f string, c radix.Client) ([]string, error) {
	return redisListPop(k, f, rpop, c)
}

// Append values to a redis list
func RedisRPush(k, f string, v interface{}, c radix.Client) error {

	s, err := marshalToString(v)

	if err != nil {
		return err
	}

	return c.Do(radix.Cmd(nil, rpush, k, f, s))
}

func RedisHGet(k, f string, c radix.Client) (map[string]string, error) {
	var m map[string]string

	err := c.Do(radix.Cmd(&m, hget, k, f))

	if err != nil {
		return nil, err
	}

	return m, nil
}

func RedisHGetAll(k, f string, c radix.Client) (map[string]string, error) {
	var m map[string]string

	err := c.Do(radix.Cmd(&m, hgetall, k, f))

	if err != nil {
		return nil, err
	}

	return m, nil
}

func RedisHSet(k, f string, v interface{}, c radix.Client) error {
	sJson, errMarshal := marshalToString(v)

	if errMarshal != nil {
		return errMarshal
	}

	err := c.Do(radix.Cmd(nil, hset, k, f, sJson))

	if err != nil {
		return err
	}

	return nil
}

func marshalToString(v interface{}) (string, error) {
	marshaller, err := json.Marshal(v)

	if err != nil {
		return "", err
	}

	return string(marshaller), nil
}

func unmarshalToMapOfStrings(s string) (map[string]string, error) {

	var v map[string]string

	err := json.Unmarshal([]byte(s), v)

	if err != nil {
		return nil, err
	}

	return v, nil

}

// Fetch values from a redis list either from the top or from the bottom.
func redisListPop(k string, f string, cmd string, c radix.Client) ([]string, error) {
	var list []string

	err := c.Do(radix.Cmd(&list, cmd, k, f))

	if err != nil {
		return nil, err
	}

	return list, nil
}