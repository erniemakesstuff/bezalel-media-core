Bezalel Media Core
Monolith application to handle ingestion of events, orchestrate media processing, syndication workflows, and publishing of media.

Build and run
`go build main.go`
`./main`

Download package imports:
`go get <package name>`


Environment variables to set:
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION

Running in docker
`docker build -t core --build-arg AwsSecretId=$AWS_ACCESS_KEY_ID --build-arg AwsSecretKey=$AWS_SECRET_ACCESS_KEY --build-arg AwsRegion=$AWS_REGION .`
`docker run core -ti`


Stopping running containers; prunning images.
`docker stop $(docker ps -a -q)`
