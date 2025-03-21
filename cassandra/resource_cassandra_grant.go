package cassandra

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"log"
	"regexp"
	"strings"

	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

const (
	deleteGrantRawTemplate = `REVOKE {{ .Privilege }} ON {{.ResourceType}} {{if .Keyspace }}"{{ .Keyspace}}"{{end}}{{if and .Keyspace .Identifier}}.{{end}}{{if .Identifier}}"{{.Identifier}}"{{end}} FROM "{{.Grantee}}"`
	createGrantRawTemplate = `GRANT {{ .Privilege }} ON {{.ResourceType}} {{if .Keyspace }}"{{ .Keyspace}}"{{end}}{{if and .Keyspace .Identifier}}.{{end}}{{if .Identifier}}"{{.Identifier}}"{{end}} TO "{{.Grantee}}"`
)

const templateReadGrant = `SELECT permissions FROM {{.SystemKeyspace}}.role_permissions where resource='data/{{if .Keyspace }}{{ .Keyspace }}{{end}}{{if and .Keyspace .Identifier}}/{{end}}{{if .Identifier}}{{.Identifier}}{{end}}' and role='{{.Grantee}}' ALLOW FILTERING;`

const (
	privilegeAll       = "all"
	privilegeCreate    = "create"
	privilegeAlter     = "alter"
	privilegeDrop      = "drop"
	privilegeSelect    = "select"
	privilegeModify    = "modify"
	privilegeAuthorize = "authorize"
	privilegeDescribe  = "describe"
	privilegeExecute   = "execute"

	resourceAllFunctions           = "all functions"
	resourceAllFunctionsInKeyspace = "all functions in keyspace"
	resourceFunction               = "function"
	resourceAllKeyspaces           = "all keyspaces"
	resourceKeyspace               = "keyspace"
	resourceTable                  = "table"
	resourceAllRoles               = "all roles"
	resourceRole                   = "role"
	resourceRoles                  = "roles"
	resourceMbean                  = "mbean"
	resourceMbeans                 = "mbeans"
	resourceAllMbeans              = "all mbeans"

	identifierFunctionName = "function_name"
	identifierTableName    = "table_name"
	identifierMbeanName    = "mbean_name"
	identifierMbeanPattern = "mbean_pattern"
	identifierRoleName     = "role_name"
	identifierKeyspaceName = "keyspace_name"
	identifierGrantee      = "grantee"
	identifierPrivilege    = "privilege"
	identifierResourceType = "resource_type"
)

var (
	templateDelete, _           = template.New("delete_grant").Parse(deleteGrantRawTemplate)
	templateCreate, _           = template.New("create_grant").Parse(createGrantRawTemplate)
	validIdentifierRegex, _     = regexp.Compile(`^[^"]{1,256}$`)
	validTableNameRegex, _      = regexp.Compile(`^[a-zA-Z0-9][a-zA-Z0-9_]{0,255}`)
	allPrivileges               = []string{privilegeSelect, privilegeCreate, privilegeAlter, privilegeDrop, privilegeModify, privilegeAuthorize, privilegeDescribe, privilegeExecute}
	allResources                = []string{resourceAllFunctions, resourceAllFunctionsInKeyspace, resourceFunction, resourceAllKeyspaces, resourceKeyspace, resourceTable, resourceAllRoles, resourceRole, resourceRoles, resourceMbean, resourceMbeans, resourceAllMbeans}
	privilegeToResourceTypesMap = map[string][]string{
		privilegeAll:       {resourceAllFunctions, resourceAllFunctionsInKeyspace, resourceFunction, resourceAllKeyspaces, resourceKeyspace, resourceTable, resourceAllRoles, resourceRole},
		privilegeCreate:    {resourceAllKeyspaces, resourceKeyspace, resourceAllFunctions, resourceAllFunctionsInKeyspace, resourceAllRoles},
		privilegeAlter:     {resourceAllKeyspaces, resourceKeyspace, resourceTable, resourceAllFunctions, resourceAllFunctionsInKeyspace, resourceFunction, resourceAllRoles, resourceRole},
		privilegeDrop:      {resourceKeyspace, resourceTable, resourceAllFunctions, resourceAllFunctionsInKeyspace, resourceFunction, resourceAllRoles, resourceRole},
		privilegeSelect:    {resourceAllKeyspaces, resourceKeyspace, resourceTable, resourceAllMbeans, resourceMbeans, resourceMbean},
		privilegeModify:    {resourceAllKeyspaces, resourceKeyspace, resourceTable, resourceAllMbeans, resourceMbeans, resourceMbean},
		privilegeAuthorize: {resourceAllKeyspaces, resourceKeyspace, resourceTable, resourceFunction, resourceAllFunctions, resourceAllFunctionsInKeyspace, resourceAllRoles, resourceRoles},
		privilegeDescribe:  {resourceAllRoles, resourceAllMbeans},
		privilegeExecute:   {resourceAllFunctions, resourceAllFunctionsInKeyspace, resourceFunction},
	}
	validResources = map[string]bool{
		resourceAllFunctions:           true,
		resourceAllFunctionsInKeyspace: true,
		resourceFunction:               true,
		resourceAllKeyspaces:           true,
		resourceKeyspace:               true,
		resourceTable:                  true,
		resourceAllRoles:               true,
		resourceRole:                   true,
		resourceRoles:                  true,
		resourceMbean:                  true,
		resourceMbeans:                 true,
		resourceAllMbeans:              true,
	}
	resourcesThatRequireKeyspaceQualifier = []string{resourceAllFunctionsInKeyspace, resourceFunction, resourceKeyspace, resourceTable}
	resourceTypeToIdentifier              = map[string]string{
		resourceFunction: identifierFunctionName,
		resourceMbean:    identifierMbeanName,
		resourceMbeans:   identifierMbeanPattern,
		resourceTable:    identifierTableName,
		resourceRole:     identifierRoleName,
	}
)

type Grant struct {
	Privilege    string
	ResourceType string
	Grantee      string
	Keyspace     string
	Identifier   string
}

func validIdentifier(i interface{}, path cty.Path, identifierName string, regularExpression *regexp.Regexp) diag.Diagnostics {
	identifier := i.(string)
	if identifierName != "" && !regularExpression.MatchString(identifier) {
		return diag.Diagnostics{
			{
				Severity:      diag.Error,
				Summary:       "Not valid value",
				Detail:        fmt.Sprintf("%s is not a valid %s name", identifier, identifierName),
				AttributePath: path,
			},
		}
	}
	return nil
}

func resourceCassandraGrant() *schema.Resource {
	return &schema.Resource{
		Description:   "Manage Grants within your cassandra cluster",
		CreateContext: resourceGrantCreate,
		ReadContext:   resourceGrantRead,
		UpdateContext: resourceGrantUpdate,
		DeleteContext: resourceGrantDelete,
		Schema: map[string]*schema.Schema{
			identifierPrivilege: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: fmt.Sprintf("One of %s", strings.Join(allPrivileges, ", ")),
				ValidateDiagFunc: func(i interface{}, path cty.Path) diag.Diagnostics {
					privilege := i.(string)
					if len(privilegeToResourceTypesMap[privilege]) <= 0 {
						return diag.Diagnostics{
							{
								Severity:      diag.Error,
								Summary:       "Invalid privilege",
								Detail:        fmt.Sprintf("%s not one of %s", privilege, strings.Join(allPrivileges, ", ")),
								AttributePath: path,
							},
						}
					}
					return nil
				},
			},
			identifierGrantee: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				Description:  "role name who we are granting privilege(s) to",
				ValidateFunc: validation.StringLenBetween(1, 256),
			},
			identifierResourceType: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: fmt.Sprintf("Resource type we are granting privilege to. Must be one of %s", strings.Join(allResources, ", ")),
				ValidateDiagFunc: func(i interface{}, path cty.Path) diag.Diagnostics {
					resourceType := i.(string)
					if !validResources[resourceType] {
						return diag.Diagnostics{
							{
								Severity:      diag.Error,
								Summary:       "Not valid resource type",
								Detail:        fmt.Sprintf("%s is not a valid resourceType, must be one of %s", resourceType, strings.Join(allResources, ", ")),
								AttributePath: path,
							},
						}
					}
					return nil
				},
			},
			identifierKeyspaceName: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: fmt.Sprintf("keyspace qualifier to the resource, only applicable for resource %s", strings.Join(resourcesThatRequireKeyspaceQualifier, ", ")),
				ValidateDiagFunc: func(i interface{}, path cty.Path) diag.Diagnostics {
					keyspaceName := i.(string)
					if !keyspaceRegex.MatchString(keyspaceName) {
						return diag.Diagnostics{
							{
								Severity:      diag.Error,
								Summary:       "Not valid keyspace name",
								Detail:        fmt.Sprintf("%s is not a valid keyspace name", keyspaceName),
								AttributePath: path,
							},
						}
					}
					return nil
				},
				ConflictsWith: []string{identifierRoleName, identifierMbeanName, identifierMbeanPattern},
			},
			identifierFunctionName: {
				Type:        schema.TypeString,
				Optional:    true,
				Description: fmt.Sprintf("keyspace qualifier to the resource, only applicable for resource %s", strings.Join(resourcesThatRequireKeyspaceQualifier, ", ")),
				ValidateDiagFunc: func(i interface{}, path cty.Path) diag.Diagnostics {
					return validIdentifier(i, path, "function name", validIdentifierRegex)
				},
				ConflictsWith: []string{identifierTableName, identifierRoleName, identifierMbeanName, identifierMbeanPattern},
			},
			identifierTableName: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: fmt.Sprintf("name of the table, applicable only for resource %s", resourceTable),
				ValidateDiagFunc: func(i interface{}, path cty.Path) diag.Diagnostics {
					return validIdentifier(i, path, "table name", validTableNameRegex)
				},
				ConflictsWith: []string{identifierFunctionName, identifierRoleName, identifierMbeanName, identifierMbeanPattern},
			},
			identifierRoleName: {
				Type:          schema.TypeString,
				Optional:      true,
				ForceNew:      true,
				Description:   fmt.Sprintf("name of the role, applicable only for resource %s", resourceRole),
				ValidateFunc:  validation.StringLenBetween(1, 256),
				ConflictsWith: []string{identifierFunctionName, identifierTableName, identifierMbeanName, identifierMbeanPattern, identifierKeyspaceName},
			},
			identifierMbeanName: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: fmt.Sprintf("name of mbean, only applicable for resource %s", resourceMbean),
				ValidateDiagFunc: func(i interface{}, path cty.Path) diag.Diagnostics {
					return validIdentifier(i, path, "mbean name", validIdentifierRegex)
				},
				ConflictsWith: []string{identifierFunctionName, identifierTableName, identifierRoleName, identifierMbeanPattern, identifierKeyspaceName},
			},
			identifierMbeanPattern: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: fmt.Sprintf("pattern for selecting mbeans, only valid for resource %s", resourceMbeans),
				ValidateDiagFunc: func(i interface{}, path cty.Path) diag.Diagnostics {
					mbeanPatternRaw := i.(string)
					_, err := regexp.Compile(mbeanPatternRaw)
					if err != nil {
						return diag.Diagnostics{
							{
								Severity:      diag.Error,
								Summary:       "Not valid mbean",
								Detail:        fmt.Sprintf("%s is not a valid pattern", mbeanPatternRaw),
								AttributePath: path,
							},
						}
					}
					return nil
				},
				ConflictsWith: []string{identifierFunctionName, identifierTableName, identifierRoleName, identifierMbeanName, identifierKeyspaceName},
			},
		},
	}
}

func parseData(d *schema.ResourceData) (*Grant, error) {
	privilege := d.Get(identifierPrivilege).(string)
	grantee := d.Get(identifierGrantee).(string)
	resourceType := d.Get(identifierResourceType).(string)

	allowedResouceTypesForPrivilege := privilegeToResourceTypesMap[privilege]
	if len(allowedResouceTypesForPrivilege) <= 0 {
		return nil, fmt.Errorf("%s resource not applicable for privilege %s", resourceType, privilege)
	}

	var matchFound = false
	for _, value := range allowedResouceTypesForPrivilege {
		if value == resourceType {
			matchFound = true
		}
	}
	if !matchFound {
		return nil, fmt.Errorf("%s resource not applicable for privilege %s - valid resourceTypes are %s", resourceType, privilege, strings.Join(allowedResouceTypesForPrivilege, ", "))
	}

	var requiresKeyspaceQualifier = false
	for _, _resourceType := range resourcesThatRequireKeyspaceQualifier {
		if resourceType == _resourceType {
			requiresKeyspaceQualifier = true
		}
	}

	var keyspaceName = ""
	if requiresKeyspaceQualifier {
		keyspaceName = d.Get(identifierKeyspaceName).(string)
		if keyspaceName == "" {
			return nil, fmt.Errorf("keyspace name must be set for resourceType %s", resourceType)
		}
	}

	identifierKey := resourceTypeToIdentifier[resourceType]
	var identifier = ""
	if identifierKey != "" {
		identifier = d.Get(identifierKey).(string)
		if identifier == "" {
			return nil, fmt.Errorf("%s needs to be set when resourceType = %s", identifierKey, resourceType)
		}
	}

	return &Grant{privilege, resourceType, grantee, keyspaceName, identifier}, nil
}

func resourceGrantExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	grant, err := parseData(d)
	if err != nil {
		return false, err
	}

	providerConfig := meta.(*ProviderConfig)
	cluster := providerConfig.Cluster

	session, sessionCreationError := cluster.CreateSession()
	if sessionCreationError != nil {
		return false, sessionCreationError
	}
	defer session.Close()

	var buffer bytes.Buffer
	tmpl, err := template.New("read_grant").Parse(templateReadGrant)
	if err != nil {
		return false, err
	}
	data := struct {
		*Grant
		SystemKeyspace string
	}{
		Grant:          grant,
		SystemKeyspace: providerConfig.SystemKeyspaceName,
	}
	if err := tmpl.Execute(&buffer, data); err != nil {
		return false, err
	}
	query := buffer.String()

	iter := session.Query(query).Iter()
	rowCount := iter.NumRows()
	if err := iter.Close(); err != nil {
		return false, err
	}
	return rowCount > 0, nil
}

func resourceGrantCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	grant, err := parseData(d)
	var diags diag.Diagnostics
	if err != nil {
		return diag.FromErr(err)
	}

	providerConfig := meta.(*ProviderConfig)
	cluster := providerConfig.Cluster

	session, sessionCreationError := cluster.CreateSession()
	if sessionCreationError != nil {
		return diag.FromErr(sessionCreationError)
	}
	defer session.Close()

	var buffer bytes.Buffer
	if err := templateCreate.Execute(&buffer, grant); err != nil {
		return diag.FromErr(err)
	}
	query := buffer.String()
	log.Printf("Executing query %v", query)
	if err := session.Query(query).Exec(); err != nil {
		return diag.FromErr(err)
	}
	d.SetId(hash(fmt.Sprintf("%+v", grant)))
	diags = append(diags, resourceGrantRead(ctx, d, meta)...)
	return diags
}

func resourceGrantRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	exists, err := resourceGrantExists(d, meta)
	var diags diag.Diagnostics
	if err != nil {
		return diag.FromErr(err)
	}
	if !exists {
		return diag.Errorf("Grant does not exist")
	}

	grant, err := parseData(d)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set(identifierResourceType, grant.ResourceType)
	d.Set(identifierGrantee, grant.Grantee)
	d.Set(identifierPrivilege, grant.Privilege)
	if grant.Keyspace != "" {
		d.Set(identifierKeyspaceName, grant.Keyspace)
	}
	if grant.Identifier != "" {
		identifierName := resourceTypeToIdentifier[grant.ResourceType]
		d.Set(identifierName, grant.Identifier)
	}
	return diags
}

func resourceGrantDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	grant, err := parseData(d)
	var diags diag.Diagnostics
	if err != nil {
		return diag.FromErr(err)
	}

	var buffer bytes.Buffer
	if err := templateDelete.Execute(&buffer, grant); err != nil {
		return diag.FromErr(err)
	}

	providerConfig := meta.(*ProviderConfig)
	cluster := providerConfig.Cluster
	session, err := cluster.CreateSession()
	if err != nil {
		return diag.FromErr(err)
	}
	defer session.Close()

	query := buffer.String()
	if err := session.Query(query).Exec(); err != nil {
		return diag.FromErr(err)
	}
	return diags
}

func resourceGrantUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return diag.Errorf("Updating of grants is not supported")
}
