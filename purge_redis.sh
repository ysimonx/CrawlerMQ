#!/bin/bash
#vider la liste des urls
redis-cli -h redis.domain.com sdiffstore url url url

# lister les urls dans la liste redis
redis-cli -h redis.domain.com smembers url
