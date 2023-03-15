#!/usr/bin/env bash

go install

buildah from --name puzzlesessionserver-working-container scratch
buildah copy puzzlesessionserver-working-container $HOME/go/bin/puzzlesessionserver /bin/puzzlesessionserver
buildah config --env SERVICE_PORT=50051 puzzlesessionserver-working-container
buildah config --port 50051 puzzlesessionserver-working-container
buildah config --entrypoint '["/bin/puzzlesessionserver"]' puzzlesessionserver-working-container
buildah commit puzzlesessionserver-working-container puzzlesessionserver
buildah rm puzzlesessionserver-working-container
