#!/bin/bash

protoc --python_out=. --mypy_out=. qnet2/network/network.proto
protoc --python_out=. --mypy_out=. qnet2/reduction/reduction.proto
