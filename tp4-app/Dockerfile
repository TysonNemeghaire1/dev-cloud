FROM node:20-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production

FROM node:20-alpine

WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY src/ ./src/

RUN npm uninstall -g npm

EXPOSE 8080
USER node
CMD ["node", "src/index.js"]
