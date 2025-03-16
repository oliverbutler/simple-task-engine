FROM oven/bun:latest

COPY package.json ./
COPY bun.lock ./
COPY index.ts ./

RUN bun install

EXPOSE 3000

ENTRYPOINT ["bun", "run", "index.ts"]
