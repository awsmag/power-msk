# power-msk

A package to help connect and work with Amazon Managed Streaming for Apache Kafka (MSK). You can run this package locally by disabling ssl and connecting to a kafka docker container.

## Badges

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)

## Environment Variables

The package supports two env variables

`KAFKA_CLIENTID`: Client id of the cluster

`KAFKA_BROKERS`: Comma separated list of brokers

Both the env vars are optional. You can either configure these or can pass them to the function.

## Installation

install the package fron npm

```bash
  npm install @awsmag/power-msk
```

## Usage/Examples

```javascript
import { getKafkaClient } from "@awsmag/power-msk";

async function useWithEnvVarSet() {
  return await getKafkaClient(); // if env variables are set
}

async function useWithoutEnvVarSet() {
  const clientId = "test";
  const brokers = ["127.0.0.1:9092"];
  const ssl = false; // Keep it true when connecting to instance. For local testing and docker container keep it false
  return await getKafkaClient(clientId, brokers, ssl); // if env variables are not set
}

// connecting using AWS IAM
import { getKafkaClient, getAWSIAMAuthMechanism } from "@awsmag/power-msk";

const sasl = getAWSIAMAuthMechanism("eu-west-1");

const clientId = "test";
const brokers = ["127.0.0.1:9092"];
const ssl = true;
return await getKafkaClient(clientId, brokers, ssl, sasl);

```

The package also supports a Koa middleware to attach the client to ctx. This provides a function to sendmassegs to kafka. For creating consumer you should connect and get the client for implementation.

```javascript
import { getKafkaClient, getKafkaClientMw } from "@awsmag/power-msk";
import Koa from "koa";

const server = new Koa();
const clientId = "test";
const brokers = ["127.0.0.1:9092"];
const ssl = false; // Keep it true when connecting to instance. For local testing and docker container keep it false
(async () => {
  await getKafkaClient(clientId, brokers, ssl); // if env variables are not set
  server.use(getKafkaClientMw());

  // rest of your code goes here
})();

// it will be available as `kafkaClient` in ctx. In your handler use it like below.

const kafkaClient = ctx.kafkaClient;
// perform functions using kafkaClient
```

The package is developed and maintained by [S25Digital](https://s25.digital). You can also check our blog [AWSMAG](https://awsmag.com)
