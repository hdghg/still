#!/bin/bash

mvn -B clean dependency:copy-dependencies package || exit 1
docker build -t hdghg/still . || exit 1
docker push hdghg/still
