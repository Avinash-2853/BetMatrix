FROM node:22-alpine 

WORKDIR /app

# Copy only dependency files first (better caching)
COPY package.json ./


RUN npm install --force 

# Copy rest of project
COPY . .

# Build optimized static files
RUN npm run build

RUN npm install -g serve

EXPOSE 3000

CMD ["serve", "-s", "dist", "-l", "3000"]
