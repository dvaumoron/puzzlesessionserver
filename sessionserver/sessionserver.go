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
	"sync"
	"time"

	pb "github.com/dvaumoron/puzzlesessionservice"
	"github.com/go-redis/redis/v8"
)

// this key maintains the existence of the session when there is no other data,
// but it is never send to client nor updated by it
const creationTimeName = "sessionCreationTime"

var errInternal = errors.New("internal service error")

// server is used to implement puzzlesessionservice.SessionServer
type server struct {
	pb.UnimplementedSessionServer
	rdb            *redis.Client
	generateMutex  sync.Mutex
	sessionTimeout time.Duration
	retryNumber    int
}

func New(rdb *redis.Client, sessionTimeout time.Duration, retryNumber int) pb.SessionServer {
	return &server{rdb: rdb, sessionTimeout: sessionTimeout, retryNumber: retryNumber}
}

func (s *server) updateWithDefaultTTL(ctx context.Context, id string) {
	err := s.rdb.Expire(ctx, id, s.sessionTimeout)
	if err != nil {
		log.Println("Failed to set TTL :", err)
	}
}

func (s *server) Generate(ctx context.Context, in *pb.SessionInfo) (*pb.SessionId, error) {
	// avoid id clash when generating, but possible bottleneck
	s.generateMutex.Lock()
	defer s.generateMutex.Unlock()
	for i := 0; i < s.retryNumber; i++ {
		id := rand.Uint64()
		idStr := fmt.Sprint(id)
		nb, err := s.rdb.Exists(ctx, idStr).Result()
		if err != nil {
			logError(err)
			return nil, errInternal
		}
		if nb == 0 {
			err := s.rdb.HSet(ctx, idStr, creationTimeName, time.Now().String()).Err()
			if err != nil {
				logError(err)
				return nil, errInternal
			}
			s.updateWithDefaultTTL(ctx, idStr)
			return &pb.SessionId{Id: id}, nil
		}
	}
	return nil, errors.New("generate reached maximum number of retries")
}

func (s *server) GetSessionInfo(ctx context.Context, in *pb.SessionId) (*pb.SessionInfo, error) {
	id := fmt.Sprint(in.Id)
	info, err := s.rdb.HGetAll(ctx, id).Result()
	if err != nil {
		logError(err)
		return nil, errInternal
	}

	s.updateWithDefaultTTL(ctx, id)
	delete(info, creationTimeName)
	return &pb.SessionInfo{Info: info}, nil
}

func (s *server) UpdateSessionInfo(ctx context.Context, in *pb.SessionUpdate) (*pb.Response, error) {
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
	if _, err := pipe.Exec(ctx); err != nil {
		logError(err)
		return nil, errInternal
	}
	s.updateWithDefaultTTL(ctx, id)
	return &pb.Response{Success: true}, nil
}

func logError(err error) {
	log.Println("Failed during Redis call :", err)
}
