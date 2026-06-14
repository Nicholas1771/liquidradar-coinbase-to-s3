# liquidradar-coinbase-to-s3
Move live transaction data from coinbase api to s3

# Setup
Replace the below properties in application.properties with your coinbase API keyId and secret

app.coinbase.keyId=YOUR_KEY_ID

app.coinbase.secret=YOU_SECRET

# Start up local Kafka environment with Docker
Download Docker Desktop and run it on local

in docker terminal navigate to docker directory and run below command to bring up kafka

docker compose up -d

# After Kafka is started, start the spring boot app
