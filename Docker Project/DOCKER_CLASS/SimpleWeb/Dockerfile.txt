# Specify base image 
FROM node:18-alpine

# create the working directory 
WORKDIR /usr/app

# Copy package.json into Container 
COPY package.json ./

# Install Dependencies
RUN npm install

#Copy the rest of the files
COPY ./ ./

# Start the web server
CMD ["npm" , "start"]
