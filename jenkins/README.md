```shell
docker network create jenkins

```

```shell
docker run --name jenkins-docker --rm \
  --privileged --network jenkins --network-alias docker \
  --env DOCKER_TLS_CERTDIR=/certs \
  --volume jenkins-docker-certs:/certs/client \
  --volume jenkins-data:/var/jenkins_home \
  --publish 2376:2376 \
  docker:dind --storage-driver overlay2
````

```shell
docker build -t myjenkins-blueocean:2.504.2-1 .
```

```shell
docker run --name jenkins-blueocean --restart=on-failure \
  --network jenkins --env DOCKER_HOST=tcp://docker:2376 \
  --env DOCKER_CERT_PATH=/certs/client --env DOCKER_TLS_VERIFY=1 \
  --publish 8090:8080 --publish 50000:50000 \
  --volume jenkins-data:/var/jenkins_home \
  --volume jenkins-docker-certs:/certs/client:ro \
  myjenkins-blueocean:2.504.2-1
```

```shell
ngrok http --url=teal-destined-seemingly.ngrok-free.app localhost:8090
 
```