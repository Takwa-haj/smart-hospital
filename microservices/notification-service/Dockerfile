FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
COPY ../../proto /proto
CMD ["node", "server.js"]
