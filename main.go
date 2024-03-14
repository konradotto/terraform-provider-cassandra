package main

import (
	"context"
	"flag"
	"log"

	"github.com/davidcollom/terraform-provider-cassandra/cassandra"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)

func main() {
	var debugMode bool
	flag.BoolVar(&debugMode, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	opts := &plugin.ServeOpts{
		ProviderFunc: func() *schema.Provider {
			return cassandra.Provider()
		},
	}

	if debugMode {
		err := plugin.Debug(context.Background(), "hashicorp.com/davidcollom/cassandra", opts)
		if err != nil {
			log.Fatal(err.Error())
		}
		return
	}

	plugin.Serve(opts)

	plugin.Serve(&plugin.ServeOpts{ProviderFunc: cassandra.Provider})
}
