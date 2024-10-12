Bezalel Media Core
Monolith application to handle ingestion of events, orchestrate media processing, syndication workflows, and publishing of media.
Employs saga-ledger design architecture.
Build and run
`go build main.go`
`./main`

Download package imports:
`go get <package name>`

Run integration tests:
`cd <directory_of_package_you're testing>`
`go test`
Note: ensure your poller and other dependencies are running; purge the queues as needed.

Environment variables to set:
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION

Running in docker
`docker build -t core --build-arg AwsSecretId=$AWS_ACCESS_KEY_ID --build-arg AwsSecretKey=$AWS_SECRET_ACCESS_KEY --build-arg AwsRegion=$AWS_REGION .`
`docker run core`


Stopping running containers; prunning images.
`docker stop $(docker ps -a -q)`

Ensure to prune old images to save space!
https://docs.docker.com/engine/manage-resources/pruning/


## Data Triggers
### Scheduled Crons
- Custom Prompt that requests the LLM to create an article; no prompt-branching from manifest.
## General Notes
If you delete the eventLedgerTable, ensure you re-create the pipe in AWS EventBridge.


## Expanding Content Selection
### Definitions
Source - origination of incomming event such as a news webscraper.
ScriptCategory - describes the distribution format, and niche.
Script - the template, structure, and overall instruction for media.
Channel - YouTube, Medium, Twitter, ...

Relationships:
Source 1:M ScriptCategory
ScriptCategory 1:1 ScriptPrompt

### Steps to add a new channel

### Steps to add a new source

### Steps to add a new 

