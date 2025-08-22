[
 `AmazonConnect-AgentDaily-FileSnapshot-*`,
 `AmazonConnect-AgentInterval-FileSnapshot-*`,
 `AmazonConnect-AgentQueueDaily-FileSnapshot-*`,
 `AmazonConnect-AgentQueueInterval-FileSnapshot-*`,
 `AmazonConnect-LoginDaily-FileSnapshot-*`,
 `AmazonConnect-QueueRoutingDaily-FileSnapshot-*`,
 `AmazonConnect-QueueRoutingInterval-FileSnapshot-*`
 
]
  .forEach(source => declare({
      schema: "CltUSAAOds",
      name: source
    })
 );