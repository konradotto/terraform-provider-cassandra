// Terraform 0.13 and later:

terraform {
  required_providers {
    cassandra = {
      source  = "dactily/cassandra"
      version = "~> 1.2"
    }
  }
}

provider "cassandra" {
  username = "cluster_username"
  password = "cluster_password"
  port     = 9042
  host     = "localhost"
}

resource "cassandra_keyspace" "keyspace" {
  name                 = "some_keyspace_name"
  replication_strategy = "SimpleStrategy"
  strategy_options     = {
    replication_factor = 1
  }
}

// Terraform 0.12 and earlier:

provider "cassandra" {
  username = "cluster_username"
  password = "cluster_password"
  port     = 9042
  host     = "localhost"
}

resource "cassandra_keyspace" "keyspace" {
  name                 = "some_keyspace_name"
  replication_strategy = "SimpleStrategy"
  strategy_options     = {
    replication_factor = 1
  }
}
