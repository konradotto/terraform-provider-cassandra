resource "cassandra_table" "table" {
  name     = "my_table"
  keyspace = "my-keyspace"
  row_keys = ["Name"]

  attribute {
    name = "name"
    type = "S"
  }

  attribute {
    name = "email"
    type = "S"
  }
}
