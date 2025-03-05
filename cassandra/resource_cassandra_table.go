package cassandra

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

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
				Type:        schema.TypeSet,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Set:         schema.HashString,
				Optional:    true,
				ForceNew:    true,
				Description: "List of Row Primary Keys",
			},
			"range_keys": {
				Type:        schema.TypeSet,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Set:         schema.HashString,
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
	keyspaceName := d.Get("keyspace").(string)
	attributes := d.Get("attribute").(*schema.Set)
	rowKeys := setToArray(d.Get("row_keys"))
	rangeKeys := setToArray(d.Get("range_keys"))
	var diags diag.Diagnostics

	providerConfig := meta.(*ProviderConfig)
	cluster := providerConfig.Cluster

	start := time.Now()
	session, sessionCreateError := cluster.CreateSession()
	gocqltable.SetDefaultSession(session)
	elapsed := time.Since(start)
	log.Printf("Getting a session took %s", elapsed)

	if sessionCreateError != nil {
		return diag.FromErr(sessionCreateError)
	}
	defer session.Close()

	log.Printf("Creating table '%s' in '%s' with obj: %v ", name, keyspaceName, attributes)

	keyspace := gocqltable.NewKeyspace(keyspaceName)
	resourceTable := keyspace.NewTable(
		name,       // The table name
		rowKeys,    // Row keys
		rangeKeys,  // Range keys
		attributes, // Schema/struct to create
	)

	err = resourceTable.Create()
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(name)
	d.Set("name", name)
	d.Set("keyspace", keyspaceName)
	d.Set("row_keys", rowKeys)
	d.Set("range_keys", rangeKeys)
	d.Set("attributes", attributes)

	diags = append(diags, resourceTableRead(ctx, d, meta)...)
	return diags
}

func resourceTableRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	name := d.Get("name").(string)
	keyspaceName := d.Get("keyspace").(string)
	attributes := d.Get("attribute").(*schema.Set)
	rowKeys := setToArray(d.Get("row_keys"))
	rangeKeys := setToArray(d.Get("range_keys"))
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

	keyspaceMetadata, err := session.KeyspaceMetadata(keyspaceName)
	if err != nil {
		return diag.FromErr(err)
	}

	tableExists := false
	for _, tbl := range keyspaceMetadata.Tables {
		if tbl.Name == name {
			log.Printf("Found table '%s' in '%s'", name, keyspaceName)
			tableExists = true
			break
		}
	}

	d.SetId(name)
	if tableExists {
		d.Set("name", name)
		d.Set("keyspace", keyspaceName)
		d.Set("attributes", attributes)
		d.Set("row_keys", rowKeys)
		d.Set("range_keys", rangeKeys)
	}

	return diags
}

func resourceTableDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	name := d.Get("name").(string)
	keyspaceName := d.Get("keyspace").(string)
	attributes := d.Get("attribute").(*schema.Set)
	rowKeys := setToArray(d.Get("row_keys"))
	rangeKeys := setToArray(d.Get("range_keys"))
	var diags diag.Diagnostics

	providerConfig := meta.(*ProviderConfig)
	cluster := providerConfig.Cluster

	start := time.Now()
	session, sessionCreateError := cluster.CreateSession()
	gocqltable.SetDefaultSession(session)
	elapsed := time.Since(start)
	log.Printf("Getting a session took %s", elapsed)

	if sessionCreateError != nil {
		return diag.FromErr(sessionCreateError)
	}
	defer session.Close()

	keyspace := gocqltable.NewKeyspace(keyspaceName)
	log.Printf("Deleting table '%s' with obj: %v ", name, attributes)
	resourceTable := keyspace.NewTable(
		name,       // The table name
		rowKeys,    // Row keys
		rangeKeys,  // Range keys
		attributes, // Schema/struct to create
	)

	err := resourceTable.Drop()
	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
