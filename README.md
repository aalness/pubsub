#### Experimenting with redis pub/sub

I used the code in this repo to perform some analysis of using redis in a situation where there are _many_
topics and relatively few clients (a server cluster.)

##### Setup

Clients run on 5 `m4.large` AWS EC2 instances. The instances have 2 CPUs and 8 GiB of memory each.
Each instance has a publisher client with a pool of 4 threads and a subscriber client with
a pool of 16 threads.

Each subscriber subscribes to 1 million different channels. Each publisher receives every message from every publisher.

Each publisher randomly publishes notificatons across the 1 million channel space.

The redis node is a `cache.r3.large` AWS ElastiCache node. It has 2 CPUs and 13.5 GiB of memory.

I performed several runs to verify results and debug but I'll highlight two simple example runs.

##### Run 1

###### Subscriber command:

`$ REDIS_HOST_ADDRESS="..redacted.." ~/go/bin/pubsub -n 1000000 -p 5 subscribe`

###### Results per client

- `total msgs processed: 49950137, achieved rate: 20051.34 msgs/sec`
- `total msgs processed: 49950137, achieved rate: 20051.24 msgs/sec`
- `total msgs processed: 49950137, achieved rate: 20051.90 msgs/sec`
- `total msgs processed: 49950137, achieved rate: 20051.80 msgs/sec`
- `total msgs processed: 49950137, achieved rate: 20051.20 msgs/sec`

###### Publisher command:

`$ REDIS_HOST_ADDRESS="..redacted.." ~/go/bin/pubsub -r 10000 -n 1000000 publish`

###### Results per client

- `total msgs processed: 9994569, achieved rate: 4018.57 msgs/sec`
- `total msgs processed: 9988446, achieved rate: 4016.03 msgs/sec`
- `total msgs processed: 9968925, achieved rate: 4008.45 msgs/sec`
- `total msgs processed: 10011373, achieved rate: 4025.29 msgs/sec`
- `total msgs processed: 9986820, achieved rate: 4018.85 msgs/sec`

###### CPU and memory usage

Clients CPU:

![clients CPU](https://cloud.githubusercontent.com/assets/4391003/13725542/bcf7a2ac-e859-11e5-9dfd-dea375dab40a.png)

Node CPU:

![node CPU](https://cloud.githubusercontent.com/assets/4391003/13725544/bcf9e828-e859-11e5-8c6f-511412159ec7.png)

Node memory:

![node memory](https://cloud.githubusercontent.com/assets/4391003/13725543/bcf9af3e-e859-11e5-9784-ecce016b9609.png)

##### Run 2

The only practical difference with this run is that I pipelined notifications in batches of 8.

###### Subscriber command:

`$ REDIS_HOST_ADDRESS="..redacted.." ~/go/bin/pubsub -n 1000000 -p 5 subscribe`

###### Results

- `total msgs processed: 75831340, achieved rate: 88071.14 msgs/sec`
- `total msgs processed: 75831340, achieved rate: 88070.69 msgs/sec`
- `total msgs processed: 75831340, achieved rate: 88073.58 msgs/sec`
- `total msgs processed: 75831340, achieved rate: 88073.15 msgs/sec`
- `total msgs processed: 75831340, achieved rate: 88070.50 msgs/sec`

###### Publisher command:

`$ REDIS_HOST_ADDRESS="..redacted.." ~/go/bin/pubsub -r 100000 -n 1000000 -b 8 publish`

###### Results

- `total msgs processed: 14800488, achieved rate: 17273.07 msgs/sec`
- `total msgs processed: 14833464, achieved rate: 17308.82 msgs/sec`
- `total msgs processed: 14652720, achieved rate: 17098.74 msgs/sec`
- `total msgs processed: 17762288, achieved rate: 20726.56 msgs/sec`
- `total msgs processed: 13782376, achieved rate: 16081.49 msgs/sec`

###### CPU and memory usage

Clients CPU:

![clients CPU](https://cloud.githubusercontent.com/assets/4391003/13725548/c7fedf3a-e859-11e5-8db9-42f0218c2f91.png)

Node CPU:

![node CPU](https://cloud.githubusercontent.com/assets/4391003/13725547/c7fcbbf6-e859-11e5-8545-a34646f18c33.png)

Node memory:

![node memory](https://cloud.githubusercontent.com/assets/4391003/13725546/c7fcb610-e859-11e5-9a50-0c2a5294de77.png)



