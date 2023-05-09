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
	"math/rand"
	"strconv"
	"sync"
	"time"

	pb "github.com/dvaumoron/puzzlesessionservice"
	"github.com/redis/go-redis/v9"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const SessionKey = "puzzleSession"

// this key maintains the existence of the session when there is no other data,
// but it is never send to client nor updated by it
const creationTimeName = "sessionCreationTime"

const redisCallMsg = "Failed during Redis call"

var errInternal = errors.New("internal service error")

// server is used to implement puzzlesessionservice.SessionServer
type server struct {
	pb.UnimplementedSessionServer
	rdb            *redis.Client
	generateMutex  sync.Mutex
	sessionTimeout time.Duration
	retryNumber    int
	updater        func(*redis.Client, context.Context, string, []string, map[string]any) error
	logger         *otelzap.Logger
}

func New(rdb *redis.Client, sessionTimeout time.Duration, retryNumber int, logger *otelzap.Logger, tp trace.TracerProvider, debug bool) pb.SessionServer {
	updater := updateSessionInfoTx
	if debug {
		ctx, initSpan := tp.Tracer(SessionKey).Start(context.Background(), "initialization")
		defer initSpan.End()

		logger.InfoContext(ctx, "Mode debug on")
		updater = updateSessionInfo
	}
	return &server{rdb: rdb, sessionTimeout: sessionTimeout, retryNumber: retryNumber, updater: updater, logger: logger}
}

func (s *server) updateWithDefaultTTL(logger otelzap.LoggerWithCtx, id string) {
	if err := s.rdb.Expire(logger.Context(), id, s.sessionTimeout).Err(); err != nil {
		logger.Info("Failed to set TTL", zap.Error(err))
	}
}

func (s *server) Generate(ctx context.Context, in *pb.SessionInfo) (*pb.SessionId, error) {
	logger := s.logger.Ctx(ctx)
	// avoid id clash when generating, but possible bottleneck
	s.generateMutex.Lock()
	defer s.generateMutex.Unlock()
	for i := 0; i < s.retryNumber; i++ {
		id := rand.Uint64()
		idStr := strconv.FormatUint(id, 10)
		nb, err := s.rdb.Exists(ctx, idStr).Result()
		if err != nil {
			logger.Error(redisCallMsg, zap.Error(err))
			return nil, errInternal
		}
		if nb == 0 {
			err := s.rdb.HSet(ctx, idStr, creationTimeName, time.Now().String()).Err()
			if err != nil {
				logger.Error(redisCallMsg, zap.Error(err))
				return nil, errInternal
			}
			s.updateWithDefaultTTL(logger, idStr)
			return &pb.SessionId{Id: id}, nil
		}
	}
	return nil, errors.New("generate reached maximum number of retries")
}

func (s *server) GetSessionInfo(ctx context.Context, in *pb.SessionId) (*pb.SessionInfo, error) {
	logger := s.logger.Ctx(ctx)
	id := strconv.FormatUint(in.Id, 10)
	info, err := s.rdb.HGetAll(ctx, id).Result()
	if err != nil {
		if err == redis.Nil {
			return &pb.SessionInfo{}, nil
		}

		logger.Error(redisCallMsg, zap.Error(err))
		return nil, errInternal
	}

	s.updateWithDefaultTTL(logger, id)
	delete(info, creationTimeName)
	return &pb.SessionInfo{Info: info}, nil
}

func (s *server) UpdateSessionInfo(ctx context.Context, in *pb.SessionUpdate) (*pb.Response, error) {
	logger := s.logger.Ctx(ctx)
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
	id := strconv.FormatUint(in.Id, 10)
	if err := s.updater(s.rdb, ctx, id, keyToDelete, info); err != nil {
		logger.Error(redisCallMsg, zap.Error(err))
		return nil, errInternal
	}
	s.updateWithDefaultTTL(logger, id)
	return &pb.Response{Success: true}, nil
}

func updateSessionInfoTx(rdb *redis.Client, ctx context.Context, id string, keyToDelete []string, info map[string]any) error {
	haveActions := false
	pipe := rdb.TxPipeline()
	if len(keyToDelete) != 0 {
		haveActions = true
		pipe.HDel(ctx, id, keyToDelete...)
	}
	if len(info) != 0 {
		haveActions = true
		pipe.HSet(ctx, id, info)
	}
	if haveActions {
		_, err := pipe.Exec(ctx)
		return err
	}
	return nil
}

func updateSessionInfo(rdb *redis.Client, ctx context.Context, id string, keyToDelete []string, info map[string]any) error {
	if len(keyToDelete) != 0 {
		if err := rdb.HDel(ctx, id, keyToDelete...).Err(); err != nil {
			return err
		}
	}
	if len(info) != 0 {
		return rdb.HSet(ctx, id, info).Err()
	}
	return nil
}
