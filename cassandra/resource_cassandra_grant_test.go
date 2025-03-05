package cassandra

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

// convertStringMapToInterface converts a map of strings to a map of interfaces.
func convertStringMapToInterface(m map[string]string) map[string]interface{} {
	res := make(map[string]interface{})
	for k, v := range m {
		res[k] = v
	}
	return res
}

// testAccPreCheckNoArgs is a PreCheck function that requires no parameters.
func testAccPreCheckNoArgs() {
	if os.Getenv("CASSANDRA_HOST") == "" {
		panic("CASSANDRA_HOST must be set for acceptance tests")
	}
}

// testAccCassandraGrantConfig returns a Terraform configuration for the cassandra_grant resource.
// The configuration sets the provider "mode" to the provided value.
func testAccCassandraGrantConfig(mode string) string {
	return fmt.Sprintf(`
provider "cassandra" {
  host = "127.0.0.1"
  mode = "%s"
}

resource "cassandra_grant" "test" {
  privilege      = "select"
  grantee        = "test_user"
  resource_type  = "table"
  keyspace_name  = "test_keyspace"
  table_name     = "test_table"
}
`, mode)
}

// testAccCassandraGrantExists checks that the cassandra_grant resource exists.
func testAccCassandraGrantExists(resourceKey string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceKey]
		if !ok {
			return fmt.Errorf("resource not found: %s", resourceKey)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("no ID is set for resource: %s", resourceKey)
		}
		// Convert state attributes to map[string]interface{}
		attrs := convertStringMapToInterface(rs.Primary.Attributes)
		d := schema.TestResourceDataRaw(nil, resourceCassandraGrant().Schema, attrs)
		pc := testAccProvider.Meta().(*ProviderConfig)
		session, err := pc.Cluster.CreateSession()
		if err != nil {
			return err
		}
		defer session.Close()

		exists, err := resourceGrantExists(d, pc)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("grant %s does not exist", rs.Primary.ID)
		}
		return nil
	}
}

// testAccCassandraGrantDestroy verifies that the grant resource is removed.
func testAccCassandraGrantDestroy(s *terraform.State) error {
	pc := testAccProvider.Meta().(*ProviderConfig)
	cluster := pc.Cluster
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "cassandra_grant" {
			continue
		}
		attrs := convertStringMapToInterface(rs.Primary.Attributes)
		d := schema.TestResourceDataRaw(nil, resourceCassandraGrant().Schema, attrs)
		exists, err := resourceGrantExists(d, pc)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("grant %s still exists", rs.Primary.ID)
		}
	}
	return nil
}

// TestAccCassandraGrant_basicCassandra tests the cassandra_grant resource with provider mode "cassandra".
func TestAccCassandraGrant_basicCassandra(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:          testAccPreCheckNoArgs,
		ProviderFactories: testAccProviderFactories,
		CheckDestroy:      testAccCassandraGrantDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccCassandraGrantConfig("cassandra"),
				Check: resource.ComposeTestCheckFunc(
					testAccCassandraGrantExists("cassandra_grant.test"),
					resource.TestCheckResourceAttr("cassandra_grant.test", "privilege", "select"),
					resource.TestCheckResourceAttr("cassandra_grant.test", "grantee", "test_user"),
				),
			},
		},
	})
}

// TestAccCassandraGrant_basicScylla tests the cassandra_grant resource with provider mode "scylla".
func TestAccCassandraGrant_basicScylla(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:          testAccPreCheckNoArgs,
		ProviderFactories: testAccProviderFactories,
		CheckDestroy:      testAccCassandraGrantDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccCassandraGrantConfig("scylla"),
				Check: resource.ComposeTestCheckFunc(
					testAccCassandraGrantExists("cassandra_grant.test"),
					resource.TestCheckResourceAttr("cassandra_grant.test", "privilege", "select"),
					resource.TestCheckResourceAttr("cassandra_grant.test", "grantee", "test_user"),
				),
			},
		},
	})
}
