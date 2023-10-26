package main

import (
	"log"
	"sync"

	aperturekv "github.com/minimAluminiumalism/ApertureKV"
	aperture_redis "github.com/minimAluminiumalism/ApertureKV/redis"
	"github.com/tidwall/redcon"
)


const PORT string = ":6380"


type ApertureSvr struct {
	dbs		map[int]*aperture_redis.RedisDS
	server	*redcon.Server
	mu		sync.Mutex
}


func (svr *ApertureSvr) listen() {
	log.Println("aperture server is running on %v", PORT)
	log.Fatal(svr.server.ListenAndServe())
}

func (svr *ApertureSvr) accept(conn redcon.Conn) bool {
	cli := new(ApertureClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *ApertureSvr) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
}

func main() {
	addr := "localhost" + PORT
	redisDS, err := aperture_redis.NewRedisDS(aperturekv.DefaultOptions)
	if err != nil {
		panic(err)
	}
	apertureServer := &ApertureSvr{
		dbs: make(map[int]*aperture_redis.RedisDS),
	}
	apertureServer.dbs[0] = redisDS
	apertureServer.server = redcon.NewServer(addr, execClientCommand, apertureServer.accept, apertureServer.close)
	apertureServer.listen()
}