package elastic_helpers

var ForceStringDynamicTemplateDef = `
	"dynamic_templates": [
	{
		"dates": {
		  "match_mapping_type": "date",
		  "mapping": {
			"type": "text"
		  }
		}
	},
    {
		"longs": {
		  "match_mapping_type": "long",
		  "mapping": {
			"type": "text"
		  }
		}
    },
    {
		"doubles": {
		  "match_mapping_type": "double",
		  "mapping": {
			"type": "text"
		  }
		}
    },
    {
		"bools": {
		  "match_mapping_type": "boolean",
		  "mapping": {
			"type": "text"
		  }
		}
    },
    {
		"binaries": {
		  "match_mapping_type": "binary",
		  "mapping": {
			"type": "text"
		  }
		}
	}
]
`
