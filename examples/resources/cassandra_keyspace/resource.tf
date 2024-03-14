locals {
  stategy_options = {
    replication_factor = 1
  }
}

resource "cassandra_keyspace" "keyspace" {
  name                 = "some_keyspace_name"
  replication_strategy = "SimpleStrategy"
  strategy_options     = local.strategy_options
}
