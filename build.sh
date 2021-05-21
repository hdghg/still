#!/bin/bash

mvn -B clean dependency:copy-dependencies package || exit 1
docker build -t hdghg/still:0.2 . || exit 1
docker push hdghg/still:0.2
