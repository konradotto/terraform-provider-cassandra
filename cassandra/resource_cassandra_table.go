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
	"github.com/kristoiv/gocqltable"
	"github.com/kristoiv/gocqltable/recipes"
)

func resourceCassandraTable() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceTableCreate,
		ReadContext:   resourceTableRead,
		UpdateContext: resourceTableUpdate,
		DeleteContext: resourceTableDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				Description:  "Name of role - must contain between 1 and 256 characters",
				ValidateFunc: validation.StringLenBetween(1, 256),
			},
			"keyspace": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
				ForceNew:    false,
				Description: "Keyspace to create table within",
			},
			"row_keys": &schema.Schema{
				Type:        schema.TypeList,
				Default:     []string{},
				Optional:    false,
				ForceNew:    false,
				Description: "List of Row Keys",
			},
			"range_keys": &schema.Schema{
				Type:        schema.TypeList,
				Default:     []string{},
				Optional:    false,
				ForceNew:    false,
				Description: "List of Range Keys",
			},
		},
	}
}

func readTable(session *gocql.Session, name string) (string, bool, bool, string, error) {

	var (
		role        string
		canLogin    bool
		isSuperUser bool
		saltedHash  string
	)

	iter := session.Query(`select role, can_login, is_superuser, salted_hash from system_auth.roles where role = ?`, name).Iter()

	defer iter.Close()

	log.Printf("read role query returned %d", iter.NumRows())

	for iter.Scan(&role, &canLogin, &isSuperUser, &saltedHash) {
		return role, canLogin, isSuperUser, saltedHash, nil
	}

	return "", false, false, "", fmt.Errorf("cannot read role with name %s", name)
}

type TableRow map[string]interface{}

func resourceTableCreateOrUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}, createTable bool) diag.Diagnostics {
	var err error
	name := d.Get("name").(string)
	keyspace_name := d.Get("keyspace").(string)
	row_keys := d.Get("row_keys").([]string)
	range_keys := d.Get("range_keys").([]string)
	var diags diag.Diagnostics

	cluster := meta.(*gocql.ClusterConfig)
	start := time.Now()
	session, sessionCreateError := cluster.CreateSession()
	elapsed := time.Since(start)

	log.Printf("Getting a session took %s", elapsed)

	if sessionCreateError != nil {
		return diag.FromErr(sessionCreateError)
	}

	defer session.Close()

	// Now we're ready to create our first keyspace. We start by getting a keyspace object
	keyspace := gocqltable.NewKeyspace(keyspace_name)

	resourceTable := struct {
		recipes.CRUD // If you looked at the base example first, notice we replaced this line with the recipe
	}{
		recipes.CRUD{ // Here we didn't replace, but rather wrapped the table object in our recipe, effectively adding more methods to the end API
			keyspace.NewTable(
				name,       // The table name
				row_keys,   // Row keys
				range_keys, // Range keys
				TableRow{},
			),
		},
	}

	if createTable {
		err = resourceTable.Create()
		if err != nil {
			log.Fatalln(err)
		}
	}

	d.SetId(name)
	d.Set("name", name)
	d.Set("keyspace", keyspace_name)
	d.Set("row_keys", row_keys)
	d.Set("range_keys", range_keys)

	diags = append(diags, resourceTableRead(ctx, d, meta)...)

	return diags
}

func resourceTableCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourceRoleCreateOrUpdate(ctx, d, meta, true)
}

func resourceTableRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	name := d.Id()
	keyspace_name := d.Get("keyspace").(string)
	var diags diag.Diagnostics

	cluster := meta.(*gocql.ClusterConfig)
	start := time.Now()
	session, sessionCreateError := cluster.CreateSession()
	elapsed := time.Since(start)

	log.Printf("Getting a session took %s", elapsed)

	if sessionCreateError != nil {
		return diag.FromErr(sessionCreateError)
	}

	defer session.Close()

	keyspace := gocqltable.NewKeyspace(keyspace_name)
	tables, err := keyspace.Tables()
	if err != nil {
		return diag.FromErr(sessionCreateError)
	}

	table_exists := false
	for _, tbl := range tables {
		if tbl == name {
			table_exists = true
			break
		}
	}

	d.SetId(name)
	d.Set("name", name)
	if table_exists {
		d.Set("keyspace", keyspace_name)
	}

	return diags
}

func resourceTableDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	name := d.Get("name").(string)
	keyspace_name := d.Get("keyspace").(string)
	row_keys := d.Get("row_keys").([]string)
	range_keys := d.Get("range_keys").([]string)
	var diags diag.Diagnostics

	cluster := meta.(*gocql.ClusterConfig)
	start := time.Now()
	session, sessionCreateError := cluster.CreateSession()
	elapsed := time.Since(start)

	log.Printf("Getting a session took %s", elapsed)

	if sessionCreateError != nil {
		return diag.FromErr(sessionCreateError)
	}

	defer session.Close()

	keyspace := gocqltable.NewKeyspace(keyspace_name)

	resourceTable := struct {
		recipes.CRUD // If you looked at the base example first, notice we replaced this line with the recipe
	}{
		recipes.CRUD{ // Here we didn't replace, but rather wrapped the table object in our recipe, effectively adding more methods to the end API
			keyspace.NewTable(
				name,       // The table name
				row_keys,   // Row keys
				range_keys, // Range keys
				TableRow{},
			),
		},
	}

	err := resourceTable.Drop()
	if err != nil {
		diag.FromErr(err)
	}

	return diags
}

func resourceTableUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourceTableCreateOrUpdate(ctx, d, meta, false)
}
