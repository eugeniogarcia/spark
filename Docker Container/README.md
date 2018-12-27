# docker-compose example

To create a simplistic standalone cluster with [docker-compose](http://docs.docker.com/compose):

    docker-compose up

The SparkUI will be running at `http://${YOUR_DOCKER_HOST}:8080` with one worker listed. To run `pyspark`, exec into a container:

    docker exec -it dockercontainer_master_1 /bin/bash

and then:

    ./bin/pyspark

To run `SparkPi` (this is an example), exec into a container:

    docker exec -it dockercontainer_master_1 /bin/bash
    
and then:

	./bin/run-example SparkPi 10