FROM node:14.17-alpine
RUN apk update && apk add --no-cache python3 g++ py-pip make bash
WORKDIR /provider
RUN npm install -g --unsafe-perm serverless
RUN npm install
CMD sls offline start --prefix 'search' --noPrependStageInUrl