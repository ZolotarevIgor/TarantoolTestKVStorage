#!/usr/bin/bash

curl --header "Content-Type: application/json" --request POST --data '{"key":"key","value":{"xyz":"xyz"}}' 37.228.117.134:8080/kv

curl 37.228.117.134:8080/kv/key

curl --header "Content-Type: application/json" --request PUT --data '{"value":{"xyz":"xyz"}}' 37.228.117.134:8080/kv/key

curl --request DELETE 37.228.117.134:8080/kv/key
