#!/bin/bash
#vider la liste des urls
redis-cli -h 178.32.209.39 sdiffstore url url url

# lister les urls dans la liste redis
redis-cli -h 178.32.209.39 smembers url
