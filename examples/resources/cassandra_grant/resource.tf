resource "cassandra_grant" "all_access_to_keyspace" {
  privilege     = "all"
  resource_type = "keyspace"
  keyspace_name = "test"
  grantee       = "migration"
}
