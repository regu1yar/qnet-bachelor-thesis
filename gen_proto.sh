#!/bin/bash

protoc --python_out=. --mypy_out=. routing/routing.proto
