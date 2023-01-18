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
package sessionserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	pb "github.com/dvaumoron/puzzlesessionservice"
	"github.com/go-redis/redis/v8"
)

// this key maintains the existence of the session when there is no other data,
// but it is never send to client nor updated by it
const creationTimeName = "sessionCreationTime"

// Server is used to implement puzzlesessionservice.SessionServer
type Server struct {
	pb.UnimplementedSessionServer
	rdb            *redis.Client
	sessionTimeout time.Duration
	retryNumber    int
}

func New(rdb *redis.Client, sessionTimeout time.Duration, retryNumber int) *Server {
	return &Server{rdb: rdb, sessionTimeout: sessionTimeout, retryNumber: retryNumber}
}

func (s *Server) updateWithDefaultTTL(ctx context.Context, id string) {
	err := s.rdb.Expire(ctx, id, s.sessionTimeout)
	if err != nil {
		log.Println("Failed to set TTL :", err)
	}
}

func (s *Server) Generate(ctx context.Context, in *pb.SessionInfo) (*pb.SessionId, error) {
	for i := 0; i < s.retryNumber; i++ {
		id := rand.Uint64()
		idStr := fmt.Sprint(id)
		nb, err := s.rdb.Exists(ctx, idStr).Result()
		if err == nil && nb == 0 {
			err := s.rdb.HSet(ctx, idStr, creationTimeName, time.Now().String()).Err()
			if err == nil {
				s.updateWithDefaultTTL(ctx, idStr)
			}
			return &pb.SessionId{Id: id}, err
		}
	}
	return nil, errors.New("generate reached maximum number of retries")
}

func (s *Server) GetSessionInfo(ctx context.Context, in *pb.SessionId) (*pb.SessionInfo, error) {
	id := fmt.Sprint(in.Id)
	info, err := s.rdb.HGetAll(ctx, id).Result()
	if err == nil {
		s.updateWithDefaultTTL(ctx, id)

		delete(info, creationTimeName)
	}
	return &pb.SessionInfo{Info: info}, err
}

func (s *Server) UpdateSessionInfo(ctx context.Context, in *pb.SessionUpdate) (*pb.SessionError, error) {
	info := map[string]any{}
	keyToDelete := []string{}
	for k, v := range in.Info {
		if k == creationTimeName {
			continue
		} else if v == "" {
			keyToDelete = append(keyToDelete, k)
		} else {
			info[k] = v
		}
	}
	id := fmt.Sprint(in.Id)
	pipe := s.rdb.TxPipeline()
	pipe.HDel(ctx, id, keyToDelete...)
	pipe.HSet(ctx, id, info)
	errStr := ""
	if _, err := pipe.Exec(ctx); err == nil {
		s.updateWithDefaultTTL(ctx, id)
	} else {
		errStr = err.Error()
	}
	return &pb.SessionError{Err: errStr}, nil
}
