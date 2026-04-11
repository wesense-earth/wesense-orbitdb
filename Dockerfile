FROM node:22-slim
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
# Bust cache for application code on every CI build
ARG CACHE_BUST=1

COPY src/ ./src/
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
EXPOSE 5200 4001
ENTRYPOINT ["/app/entrypoint.sh"]
