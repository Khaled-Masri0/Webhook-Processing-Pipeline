FROM node:24-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY prisma ./prisma
COPY src ./src
COPY scripts ./scripts
COPY prisma.config.ts ./
COPY tsconfig.json ./
COPY tsconfig.test.json ./

RUN npm run prisma:generate
RUN npm run build

EXPOSE 3000

CMD ["node", "dist/api/index.js"]
