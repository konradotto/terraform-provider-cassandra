package cassandra

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"golang.org/x/crypto/bcrypt"
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
				Description:  "Name of role - must contain between 1 and 256 characters",
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
				Description: "Enables role to be able to login",
			},
			"password": {
				Type:         schema.TypeString,
				Required:     true,
				Description:  "Password for user when using Cassandra internal authentication",
				Sensitive:    true,
				ValidateFunc: validation.StringLenBetween(40, 512),
			},
		},
	}
}

func readRole(session *gocql.Session, name string, systemKeyspace string) (string, bool, bool, string, error) {
	tableName := fmt.Sprintf("%s.roles", systemKeyspace)
	query := fmt.Sprintf("select role, can_login, is_superuser, salted_hash from %s where role = ?", tableName)
	iter := session.Query(query, name).Iter()
	defer iter.Close()

	var (
		role        string
		canLogin    bool
		isSuperUser bool
		saltedHash  string
	)

	for iter.Scan(&role, &canLogin, &isSuperUser, &saltedHash) {
		return role, canLogin, isSuperUser, saltedHash, nil
	}
	return "", false, false, "", fmt.Errorf("cannot read role with name %s", name)
}

func checkPassword(algorithm, storedHash, password string) error {
	if algorithm == "sha-512" {
		hashBytes := sha512.Sum512([]byte(password))
		computedHash := hex.EncodeToString(hashBytes[:])
		if computedHash != storedHash {
			return fmt.Errorf("password hash mismatch")
		}
		return nil
	}
	return bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password))
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
	session, sessionCreateError := cluster.CreateSession()
	elapsed := time.Since(start)
	log.Printf("Getting a session took %s", elapsed)
	if sessionCreateError != nil {
		return diag.FromErr(sessionCreateError)
	}
	defer session.Close()

	err := session.Query(fmt.Sprintf(`%s ROLE '%s' WITH PASSWORD = '%s' AND LOGIN = %v AND SUPERUSER = %v`,
		boolToAction[createRole], name, password, login, superUser)).Exec()
	if err != nil {
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
	password := d.Get("password").(string)
	var diags diag.Diagnostics

	providerConfig := meta.(*ProviderConfig)
	cluster := providerConfig.Cluster

	start := time.Now()
	session, sessionCreateError := cluster.CreateSession()
	elapsed := time.Since(start)
	log.Printf("Getting a session took %s", elapsed)
	if sessionCreateError != nil {
		return diag.FromErr(sessionCreateError)
	}
	defer session.Close()

	_name, login, superUser, saltedHash, readRoleErr := readRole(session, name, providerConfig.SystemKeyspaceName)
	if readRoleErr != nil {
		return diag.FromErr(readRoleErr)
	}

	err := checkPassword(providerConfig.PwEncryptionAlgorithm, saltedHash, password)
	if err == nil {
		d.Set("password", password)
	} else {
		d.Set("password", saltedHash)
	}
	d.SetId(_name)
	d.Set("name", _name)
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
	session, sessionCreateError := cluster.CreateSession()
	elapsed := time.Since(start)
	log.Printf("Getting a session took %s", elapsed)
	if sessionCreateError != nil {
		return diag.FromErr(sessionCreateError)
	}
	defer session.Close()

	err := session.Query(fmt.Sprintf(`DROP ROLE '%s'`, name)).Exec()
	if err != nil {
		return diag.FromErr(err)
	}
	return diags
}

func resourceRoleUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourceRoleCreateOrUpdate(ctx, d, meta, false)
}
