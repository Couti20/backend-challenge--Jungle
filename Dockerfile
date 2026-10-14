FROM oven/bun:1.4.3
WORKDIR /app
COPY package.json bun.lock* ./
RUN bun install
COPY . .
CMD ["sh", "-c", "bun run migrate && bun run start"]
