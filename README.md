# pipelinr-clients

Client implementations for interacting with pipelinr.dev.

## Pipelinr: What is it?

Pipelinr is a SaaS platform for streamlining real-time message processing pipelines. It's primary purpose is to remove the need to worry about glue and plumbing, so you can worry about your own business logic.

Features:

- Language-agnostic platform: use one of these clients, or write your own.
- Cloud-agnostic: pipelinr.dev is hosted in Google Cloud Platform, however is cloud-agnostic. It can be turned on in any kubernetes-compatible environment, or even on bare metal/on-prem.
- Multi-protocol: HTTP and gRPC supported. MQTT and others slated for 2022/2023.
- Scalable: managed pipelinr will auto-scale to handle your traffic.
- Message routing: pipelinr handles message routing automatically.
- Message redelivery: no more dropped messages; if a message isn't properly handled by a worker, pipelinr will redeliver the message until it is properly handled.

## How to use

Prerequisites:

- A pipelinr.dev account and API key
- A planned pipeline series of steps or operations - this can be any number of operations with any level of granularity, with the stipulation that the number of steps directly impacts the amount of time it takes to transport and process a message. This set of steps does not need to be constant, and can change with every message

### See each language's README for information on how to work with the packages