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
	_ "embed"
	"os"
	"strconv"
	"strings"
	"time"

	grpcserver "github.com/dvaumoron/puzzlegrpcserver"
	redisclient "github.com/dvaumoron/puzzleredisclient"
	"github.com/dvaumoron/puzzlesessionserver/sessionserver"
	pb "github.com/dvaumoron/puzzlesessionservice"
)

const serviceName = "PuzzleSession"

//go:embed version.txt
var version string

func main() {
	// should start with this, to benefit from the call to godotenv
	s := grpcserver.Make(serviceName, version)

	sessionTimeoutSec, err := strconv.ParseInt(os.Getenv("SESSION_TIMEOUT"), 10, 64)
	if err != nil {
		s.Logger.Fatal("Failed to parse SESSION_TIMEOUT")
	}
	sessionTimeout := time.Duration(sessionTimeoutSec) * time.Second

	retryNumber, err := strconv.Atoi(os.Getenv("RETRY_NUMBER"))
	if err != nil {
		s.Logger.Fatal("Failed to parse RETRY_NUMBER")
	}

	debug := strings.TrimSpace(os.Getenv("DEBUG_MODE")) != ""

	rdb := redisclient.Create(s.Logger)

	pb.RegisterSessionServer(s, sessionserver.New(rdb, sessionTimeout, retryNumber, s.Logger, debug))

	s.Start()
}
