# Specify base image 
FROM alpine

# Set the working directory 
WORKDIR /usr/app

# Install Redis in Alpine
RUN apk add --update redis

# Start the redis-server
CMD ["redis-server"]



