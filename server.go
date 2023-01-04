/*
 *
 * Copyright 2022 puzzlesessionserver authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	pb "github.com/dvaumoron/puzzlesessionservice"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"
)

// TODO configuration reading
var timeout time.Duration = 1260 * time.Second

// server is used to implement puzzlesessionservice.SessionServer.
type server struct {
	pb.UnimplementedSessionServer
	rdb *redis.Client
}

func (s *server) Generate(ctx context.Context, in *pb.SessionInfo) (*pb.SessionId, error) {
	var id uint64
	idStr := ""
	exists := true
	for exists {
		id := rand.Uint64()
		idStr := fmt.Sprint(id)
		nb, err := s.rdb.Exists(ctx, idStr).Result()
		exists = err != nil || nb == 1
	}
	_, err := s.rdb.HSet(ctx, idStr, "sessionCreationTime", time.Now().String()).Result()
	s.rdb.Expire(ctx, idStr, timeout)
	return &pb.SessionId{Id: id}, err
}

func (s *server) GetSessionInfo(ctx context.Context, in *pb.SessionId) (*pb.SessionInfo, error) {
	id := fmt.Sprint(in.Id)
	info, err := s.rdb.HGetAll(ctx, id).Result()
	s.rdb.Expire(ctx, id, timeout)
	return &pb.SessionInfo{Info: info}, err
}

func (s *server) UpdateSessionInfo(ctx context.Context, in *pb.SessionUpdate) (*pb.SessionError, error) {
	info := map[string]any{}
	keyToDelete := []string{}
	for k, v := range in.Info {
		if v == "" {
			keyToDelete = append(keyToDelete, k)
		} else {
			info[k] = v
		}
	}
	id := fmt.Sprint(in.Id)
	pipe := s.rdb.TxPipeline()
	pipe.HDel(ctx, id, keyToDelete...)
	pipe.HSet(ctx, id, info)
	cmds, err := pipe.Exec(ctx)
	if err == nil {
		for _, cmd := range cmds {
			err = cmd.Err()
			if err != nil {
				break
			}
		}
	}
	s.rdb.Expire(ctx, id, timeout)
	return &pb.SessionError{Err: err.Error()}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	s := grpc.NewServer()
	pb.RegisterSessionServer(s, &server{rdb: rdb})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
