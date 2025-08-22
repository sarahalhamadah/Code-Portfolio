[
 `AmazonConnect-AgentDaily`,
 `AmazonConnect-AgentInterval`,
 `AmazonConnect-AgentQueueDaily`,
 `AmazonConnect-AgentQueueInterval`,
 `AmazonConnect-LoginDaily`,
 `AmazonConnect-QueueRoutingDaily`,
 `AmazonConnect-QueueRoutingInterval`
 
]
  .forEach(source => declare({
      schema: "EdwOds",
      name: source
    })
 );