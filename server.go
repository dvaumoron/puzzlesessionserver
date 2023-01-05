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
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	pb "github.com/dvaumoron/puzzlesessionservice"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

// server is used to implement puzzlesessionservice.SessionServer.
type server struct {
	pb.UnimplementedSessionServer
	rdb            *redis.Client
	sessionTimeout time.Duration
	retryNumber    int
}

func (s *server) Generate(ctx context.Context, in *pb.SessionInfo) (*pb.SessionId, error) {
	for i := 0; i < s.retryNumber; i++ {
		id := rand.Uint64()
		idStr := fmt.Sprint(id)
		nb, err := s.rdb.Exists(ctx, idStr).Result()
		if err == nil && nb == 0 {
			_, err := s.rdb.HSet(ctx, idStr, "sessionCreationTime", time.Now().String()).Result()
			s.rdb.Expire(ctx, idStr, s.sessionTimeout)
			return &pb.SessionId{Id: id}, err
		}
	}
	return nil, errors.New("increment reached maximum number of retries")
}

func (s *server) GetSessionInfo(ctx context.Context, in *pb.SessionId) (*pb.SessionInfo, error) {
	id := fmt.Sprint(in.Id)
	info, err := s.rdb.HGetAll(ctx, id).Result()
	s.rdb.Expire(ctx, id, s.sessionTimeout)
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
	s.rdb.Expire(ctx, id, s.sessionTimeout)
	return &pb.SessionError{Err: err.Error()}, nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Failed to load .env file")
	}

	sessionTimeoutSec, err := strconv.ParseInt(os.Getenv("SESSION_TIMEOUT"), 10, 64)
	if err != nil {
		log.Fatal("Failed to parse SESSION_TIMEOUT")
	}
	sessionTimeout := time.Duration(sessionTimeoutSec) * time.Second

	retryNumber, err := strconv.Atoi(os.Getenv("RETRY_NUMBER"))
	if err != nil {
		log.Fatal("Failed to parse RETRY_NUMBER")
	}

	dbNum, err := strconv.Atoi(os.Getenv("REDIS_SERVER_DB"))
	if err != nil {
		log.Fatal("Failed to parse REDIS_SERVER_DB")
	}

	lis, err := net.Listen("tcp", ":"+os.Getenv("SERVICE_PORT"))
	if err != nil {
		log.Fatalf("Failed to listen : %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_SERVER_ADDR"),
		Username: os.Getenv("REDIS_SERVER_USERNAME"),
		Password: os.Getenv("REDIS_SERVER_PASSWORD"),
		DB:       dbNum,
	})

	s := grpc.NewServer()
	pb.RegisterSessionServer(s, &server{
		rdb: rdb, sessionTimeout: sessionTimeout, retryNumber: retryNumber,
	})
	log.Printf("Listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve : %v", err)
	}
}
