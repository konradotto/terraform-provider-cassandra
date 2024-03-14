package cassandra

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/kristoiv/gocqltable"
)

func resourceCassandraTableSpace() *schema.Resource {
	return &schema.Resource{
		Description:   "Create and Delete Tables within Keyspaces",
		CreateContext: resourceTableCreate,
		ReadContext:   resourceTableRead,
		DeleteContext: resourceTableDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				Description:  "Name of table - must contain between 1 and 256 characters",
				ValidateFunc: validation.StringLenBetween(1, 256),
			},
			"keyspace": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Keyspace to create table within",
			},
			"attribute": {
				Type: schema.TypeSet,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"type": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice([]string{"S", "N", "B"}, false),
						},
					},
				},
				Set: func(v interface{}) int {
					var buf bytes.Buffer
					m := v.(map[string]interface{})
					buf.WriteString(fmt.Sprintf("%s-", m["name"].(string)))
					return stringHashcode(buf.String())
				},
				Required:    true,
				ForceNew:    true,
				Description: "List of Row Keys",
			},
			"row_keys": {
				Type:        schema.TypeList,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Optional:    true,
				ForceNew:    true,
				Description: "List of Row Primary Keys",
			},
			"range_keys": {
				Type:        schema.TypeList,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Optional:    true,
				ForceNew:    true,
				Description: "List of Range Keys",
			},
		},
	}
}

func resourceTableCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var err error
	name := d.Get("name").(string)
	keyspace_name := d.Get("keyspace").(string)
	attributes := d.Get("attribute").(*schema.Set)
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

	resourceTable := keyspace.NewTable(
		name,              // The table name
		row_keys,          // Row keys
		range_keys,        // Range keys
		attributes.List(), // Object Schema/Struct to create
	)

	err = resourceTable.Create()
	if err != nil {
		log.Fatalln(err)
	}

	d.SetId(name)
	d.Set("name", name)
	d.Set("keyspace", keyspace_name)
	d.Set("row_keys", row_keys)
	d.Set("range_keys", range_keys)
	d.Set("attributes", attributes)

	diags = append(diags, resourceTableRead(ctx, d, meta)...)

	return diags
}

func resourceTableRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	name := d.Id()
	keyspace_name := d.Get("keyspace").(string)
	attributes := d.Get("attribute").(*schema.Set)
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
	if table_exists {
		d.Set("name", name)
		d.Set("keyspace", keyspace_name)
		d.Set("attributes", attributes)
		d.Set("row_keys", row_keys)
		d.Set("range_keys", range_keys)
	}

	return diags
}

func resourceTableDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	name := d.Get("name").(string)
	keyspace_name := d.Get("keyspace").(string)
	attributes := d.Get("attribute").(*schema.Set)
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

	resourceTable := keyspace.NewTable(
		name,              // The table name
		row_keys,          // Row keys
		range_keys,        // Range keys
		attributes.List(), // Object Schema/Struct to create
	)

	err := resourceTable.Drop()
	if err != nil {
		diag.FromErr(err)
	}

	return diags
}
