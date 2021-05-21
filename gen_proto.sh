#!/bin/bash

protoc --python_out=. --mypy_out=. qnet2/network/network.proto
protoc --python_out=. --mypy_out=. qnet2/all_reduce/all_reduce.proto
