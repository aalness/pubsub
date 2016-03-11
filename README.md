#### Experimenting with Pub/sub

##### AWS ElastiCache+Redis

###### Subscriber command:
`REDIS_HOST_ADDRESS="..redacted.." ./pubsub -n 500000 subscribe`

###### Publisher command:
`REDIS_HOST_ADDRESS="..redacted.." ./pubsub -r 50000 -n 500000 publish`

###### Publishers:
 - 5 total: all 5 processes on `t2.large` 2 cpu instance
 - Each pushed ~1920 messages/sec; ~10% of 1 cpu per process

###### Subscribers:
 - 10 total: 2 proceses per `m4.large` 5x 2 cpu instances
 - Each received ~9440 messages/sec; ~50% of 1 cpu per process

###### Redis node:
 - 1 node: `cache.r3.large` instance    
 - ~75% cpu util; ~575MB memory used

