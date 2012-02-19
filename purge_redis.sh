#!/bin/bash
#vider la liste des urls
redis-cli -h redis.domain.com sdiffstore  activemq:urls  activemq:urls  activemq:urls 
redis-cli -h redis.domain.com sdiffstore  activemq:urls:crawled activemq:urls:crawled activemq:urls:crawled 

# lister les urls dans la liste redis
redis-cli -h redis.domain.com smembers activemq:urls
redis-cli -h redis.domain.com smembers activemq:urls:crawled 
