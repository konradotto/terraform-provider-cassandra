package cassandra

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceCassandraRole() *schema.Resource {
	return &schema.Resource{
		Description:   "Manage Roles within your cassandra cluster",
		CreateContext: resourceRoleCreate,
		ReadContext:   resourceRoleRead,
		UpdateContext: resourceRoleUpdate,
		DeleteContext: resourceRoleDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				Description:  "Name of role",
				ValidateFunc: validation.StringLenBetween(1, 256),
			},
			"super_user": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Allow role to create and manage other roles",
			},
			"login": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: "Enable login for the role",
			},
			"password": {
				Type:         schema.TypeString,
				Required:     true,
				Sensitive:    true,
				ForceNew:     true,
				ValidateFunc: validation.StringLenBetween(40, 512),
			},
		},
	}
}

func readRole(session *gocql.Session, name string, systemKeyspace string) (string, bool, bool, string, error) {
	tableName := fmt.Sprintf("%s.roles", systemKeyspace)
	query := fmt.Sprintf("SELECT role, can_login, is_superuser, salted_hash FROM %s WHERE role = ?", tableName)
	iter := session.Query(query, name).Iter()
	defer iter.Close()

	var (
		role        string
		canLogin    bool
		isSuperUser bool
		saltedHash  string
	)
	if iter.Scan(&role, &canLogin, &isSuperUser, &saltedHash) {
		return role, canLogin, isSuperUser, saltedHash, nil
	}
	return "", false, false, "", fmt.Errorf("cannot read role with name %s", name)
}

func resourceRoleCreateOrUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}, createRole bool) diag.Diagnostics {
	name := d.Get("name").(string)
	superUser := d.Get("super_user").(bool)
	login := d.Get("login").(bool)
	password := d.Get("password").(string)
	var diags diag.Diagnostics

	providerConfig := meta.(*ProviderConfig)
	cluster := providerConfig.Cluster

	start := time.Now()
	session, err := cluster.CreateSession()
	elapsed := time.Since(start)
	log.Printf("Getting a session took %s", elapsed)
	if err != nil {
		return diag.FromErr(err)
	}
	defer session.Close()

	action := "CREATE"
	if !createRole {
		action = "ALTER"
	}
	query := fmt.Sprintf(`%s ROLE '%s' WITH PASSWORD = '%s' AND LOGIN = %v AND SUPERUSER = %v`,
		action, name, password, login, superUser)
	log.Printf("Executing query: %s", query)
	if err := session.Query(query).Exec(); err != nil {
		return diag.FromErr(err)
	}

	d.SetId(name)
	d.Set("name", name)
	d.Set("super_user", superUser)
	d.Set("login", login)
	d.Set("password", password)

	diags = append(diags, resourceRoleRead(ctx, d, meta)...)
	return diags
}

func resourceRoleCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourceRoleCreateOrUpdate(ctx, d, meta, true)
}

func resourceRoleRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	name := d.Id()
	var diags diag.Diagnostics

	providerConfig := meta.(*ProviderConfig)
	cluster := providerConfig.Cluster

	start := time.Now()
	session, err := cluster.CreateSession()
	elapsed := time.Since(start)
	log.Printf("Getting a session took %s", elapsed)
	if err != nil {
		return diag.FromErr(err)
	}
	defer session.Close()

	_role, login, superUser, _, err := readRole(session, name, providerConfig.SystemKeyspaceName)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("name", _role)
	d.Set("super_user", superUser)
	d.Set("login", login)
	return diags
}

func resourceRoleDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	name := d.Get("name").(string)
	var diags diag.Diagnostics

	providerConfig := meta.(*ProviderConfig)
	cluster := providerConfig.Cluster

	start := time.Now()
	session, err := cluster.CreateSession()
	elapsed := time.Since(start)
	log.Printf("Getting a session took %s", elapsed)
	if err != nil {
		return diag.FromErr(err)
	}
	defer session.Close()

	query := fmt.Sprintf(`DROP ROLE '%s'`, name)
	if err := session.Query(query).Exec(); err != nil {
		return diag.FromErr(err)
	}
	return diags
}

func resourceRoleUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourceRoleCreateOrUpdate(ctx, d, meta, false)
}
