package cassandra

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccCassandraRole_basic(t *testing.T) {
	name := "user"

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviderFactories,
		CheckDestroy:      testAccCassandraRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccCassandraRoleConfigBasic(name),
				Check: resource.ComposeTestCheckFunc(
					testAccCassandraRoleExists("cassandra_role.user"),
					resource.TestCheckResourceAttr("cassandra_role.user", "name", name),
					resource.TestCheckResourceAttr("cassandra_role.user", "password", "asdf1234"),
				),
			},
		},
	})
}

func TestAccCassandraRole_invalid(t *testing.T) {
	name := "invalid\\\"name"

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviderFactories,
		CheckDestroy:      testAccCassandraRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config:      testAccCassandraRoleConfigBasic(name),
				ExpectError: regexp.MustCompile(".*name must contain between 1 and 256 chars and must not contain single quote.*"),
			},
		},
	})
}

func testAccCassandraRoleConfigBasic(name string) string {
	return fmt.Sprintf(`
resource "cassandra_role" "user" {
    name     = "%s"
    password = "asdf1234"
}
`, name)
}

func testAccCassandraRoleDestroy(s *terraform.State) error {
	pc := testAccProvider.Meta().(*ProviderConfig)
	cluster := pc.Cluster
	session, sessionCreateError := cluster.CreateSession()

	if sessionCreateError != nil {
		return sessionCreateError
	}
	defer session.Close()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "cassandra_role" {
			continue
		}

		name := rs.Primary.Attributes["name"]
		_, _, _, _, err := readRole(session, name, pc.Mode)
		if err != nil {
			return nil
		}
		return fmt.Errorf("role %s still exists", name)
	}
	return nil
}

func testAccCassandraRoleExists(resourceKey string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceKey]
		if !ok {
			return fmt.Errorf("not found: %s", resourceKey)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("no ID is set")
		}
		pc := testAccProvider.Meta().(*ProviderConfig)
		cluster := pc.Cluster
		session, sessionCreateError := cluster.CreateSession()
		if sessionCreateError != nil {
			return sessionCreateError
		}
		defer session.Close()

		_, _, _, _, err := readRole(session, rs.Primary.ID, pc.Mode)
		if err != nil {
			return err
		}
		return nil
	}
}
