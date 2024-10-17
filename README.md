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
ScriptCategory - describes the distribution format, and niche. Tuple `<format>.<niche>`
Script - the template, structure, and overall instruction for media.
Channel - YouTube, Medium, Twitter, ...

Relationships:
Source 1:M ScriptCategory
ScriptCategory 1:1 ScriptPrompt

### Steps to add a new channel
0. Add DistributionChannel in ledger table.
1. Update publisher_driver factory in orchestration/publisher-drivers.
2. Update manifest distribution_format_to_channel file.
3. Update the media-pollers package to support the new DistributionFormat.

### Steps to add a new source
0. Create a new ingestion driver to accept the source payload.
1. Update driver_factory in ingestion package to accept new driver.
2. Assign source to relevant script categories in manifest package: source_to_script...

### Steps to add a new niche
0. Set categoryKeys in manifest package for source_to_script... and script_prompts. Tuple `<format>.<niche>`



### Channel Requirements
Medium - requires IntegrationToken: https://medium.com/me/settings/security


