package attributes_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/agent/component/otelcol"
	"github.com/grafana/agent/component/otelcol/internal/fakeconsumer"
	"github.com/grafana/agent/component/otelcol/processor/attributes"
	"github.com/grafana/agent/pkg/flow/componenttest"
	"github.com/grafana/agent/pkg/river"
	"github.com/grafana/agent/pkg/util"
	"github.com/grafana/dskit/backoff"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// A lot of the TestDecode tests were inspired by tests in the Otel repo:
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.63.0/processor/attributesprocessor/testdata/config.yaml

func Test_Insert(t *testing.T) {
	cfg := `
		action {
			key = "attribute1"
			value = 111111
			action = "insert"
		}
		action {
			key = "string key"
			value = "anotherkey"
			action = "insert"
		}

		output {
			// no-op: will be overridden by test code.
		}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "attribute1", action.Key)
	require.Equal(t, 111111, action.Value)
	require.Equal(t, "insert", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "string key", action.Key)
	require.Equal(t, "anotherkey", action.Value)
	require.Equal(t, "insert", string(action.Action))

	var inputTrace = `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "attribute1",
						"value": { "intValue": "0" }
					}]
				}]
			}]
		}]
	}`

	// We expect:
	// * "attribute1" to stay the same because it already exists
	// * "string key" will be inserted.
	expectedOutputTrace := `{
		"resourceSpans": [{
			"resource": {},
			"scopeSpans": [{
				"scope": {},
				"spans": [{
						"traceId": "",
						"spanId": "",
						"parentSpanId": "",
						"name": "TestSpan",
						"attributes": [{
							"key": "attribute1",
							"value": { "intValue": "0" }
						},
						{
							"key": "string key",
							"value": { "stringValue": "anotherkey" }
						}],
						"status": {}
					}]
				}]
			}]
	}`

	testRunProcessor(t, cfg, inputTrace, expectedOutputTrace)
}

func TestDecode_RegexExtract(t *testing.T) {
	cfg := `
		action {
			key = "http.url"
			pattern = "^(?P<http_protocol>.*):\\/\\/(?P<http_domain>.*)\\/(?P<http_path>.*)(\\?|\\&)(?P<http_query_params>.*)"
			action = "extract"
		}

		output {
			// no-op: will be overridden by test code.
		}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "http.url", action.Key)
	require.Equal(t, "^(?P<http_protocol>.*):\\/\\/(?P<http_domain>.*)\\/(?P<http_path>.*)(\\?|\\&)(?P<http_query_params>.*)", action.RegexPattern)
	require.Equal(t, "extract", string(action.Action))
}

func TestDecode_Update(t *testing.T) {
	cfg := `
		action {
			key = "boo"
			from_attribute = "foo"
			action = "update"
		}
		action {
			key = "db.secret"
			value = "redacted"
			action = "update"
		}

		output {
			// no-op: will be overridden by test code.
		}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "boo", action.Key)
	require.Equal(t, "foo", action.FromAttribute)
	require.Equal(t, "update", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "db.secret", action.Key)
	require.Equal(t, "redacted", action.Value)
	require.Equal(t, "update", string(action.Action))

	var inputTrace = `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "foo",
						"value": { "intValue": "11111" }
					},
					{
						"key": "boo",
						"value": { "intValue": "22222" }
					},
					{
						"key": "db.secret",
						"value": { "stringValue": "top_secret" }
					}]
				}]
			}]
		}]
	}`

	// We expect:
	// * "attribute1" to stay the same because it already exists
	// * "string key" will be inserted.
	expectedOutputTrace := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "foo",
						"value": { "intValue": "11111" }
					},
					{
						"key": "boo",
						"value": { "intValue": "11111" }
					},
					{
						"key": "db.secret",
						"value": { "stringValue": "redacted" }
					}]
				}]
			}]
		}]
	}`

	testRunProcessor(t, cfg, inputTrace, expectedOutputTrace)
}

func Test_Upsert(t *testing.T) {
	cfg := `
		action {
			key = "region"
			value = "planet-earth"
			action = "upsert"
		}
		action {
			key = "new_user_key"
			from_attribute = "user_key"
			action = "upsert"
		}

		output {
			// no-op: will be overridden by test code.
		}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "region", action.Key)
	require.Equal(t, "planet-earth", action.Value)
	require.Equal(t, "upsert", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "new_user_key", action.Key)
	require.Equal(t, "user_key", action.FromAttribute)
	require.Equal(t, "upsert", string(action.Action))

	var inputTrace = `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "user_key",
						"value": { "intValue": "11111" }
					}]
				}]
			}]
		}]
	}`

	// We expect:
	// * "attribute1" to stay the same because it already exists
	// * "string key" will be inserted.
	expectedOutputTrace := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "user_key",
						"value": { "intValue": "11111" }
					},
					{
						"key": "region",
						"value": { "stringValue": "planet-earth" }
					},
					{
						"key": "new_user_key",
						"value": { "intValue": "11111" }
					}]
				}]
			}]
		}]
	}`

	testRunProcessor(t, cfg, inputTrace, expectedOutputTrace)
}

func Test_Delete(t *testing.T) {
	cfg := `
		action {
			key = "credit_card"
			action = "delete"
		}
		action {
			key = "duplicate_key"
			action = "delete"
		}

		output {
			// no-op: will be overridden by test code.
		}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "credit_card", action.Key)
	require.Equal(t, "delete", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "duplicate_key", action.Key)
	require.Equal(t, "delete", string(action.Action))

	var inputTrace = `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "credit_card",
						"value": { "intValue": "11111" }
					},
					{
						"key": "duplicate_key",
						"value": { "intValue": "22222" }
					},
					{
						"key": "db.secret",
						"value": { "stringValue": "top_secret" }
					}]
				}]
			}]
		}]
	}`

	// We expect:
	// * "attribute1" to stay the same because it already exists
	// * "string key" will be inserted.
	expectedOutputTrace := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "db.secret",
						"value": { "stringValue": "top_secret" }
					}]
				}]
			}]
		}]
	}`

	testRunProcessor(t, cfg, inputTrace, expectedOutputTrace)
}

func Test_Hash(t *testing.T) {
	cfg := `
		action {
			key = "user.email"
			action = "hash"
		}

		output {
			// no-op: will be overridden by test code.
		}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "user.email", action.Key)
	require.Equal(t, "hash", string(action.Action))

	var inputTrace = `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "foo",
						"value": { "intValue": "11111" }
					},
					{
						"key": "boo",
						"value": { "intValue": "22222" }
					},
					{
						"key": "user.email",
						"value": { "stringValue": "user@email.com" }
					}]
				}]
			}]
		}]
	}`

	// We expect:
	// * "attribute1" to stay the same because it already exists
	// * "string key" will be inserted.
	expectedOutputTrace := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "foo",
						"value": { "intValue": "11111" }
					},
					{
						"key": "boo",
						"value": { "intValue": "22222" }
					},
					{
						"key": "user.email",
						"value": { "stringValue": "36687c352204c27d9e228a9b34d00c8a1d36a000" }
					}]
				}]
			}]
		}]
	}`

	testRunProcessor(t, cfg, inputTrace, expectedOutputTrace)
}

func Test_Convert(t *testing.T) {
	cfg := `
		action {
			key = "http.status_code"
			converted_type = "int"
			action = "convert"
		}

		output {
			// no-op: will be overridden by test code.
		}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "http.status_code", action.Key)
	require.Equal(t, "int", action.ConvertedType)
	require.Equal(t, "convert", string(action.Action))

	var inputTrace = `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "http.status_code",
						"value": { "stringValue": "500" }
					}]
				}]
			}]
		}]
	}`

	// We expect:
	// * "attribute1" to stay the same because it already exists
	// * "string key" will be inserted.
	expectedOutputTrace := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "http.status_code",
						"value": { "intValue": "500" }
					}]
				}]
			}]
		}]
	}`

	testRunProcessor(t, cfg, inputTrace, expectedOutputTrace)
}

func Test_ExcludeMulti(t *testing.T) {
	cfg := `
	exclude {
		match_type = "strict"
		services = ["svcA", "svcB"]
		attribute {
			key = "env"
			value = "dev"
		}
		attribute {
			key = "test_request"
		}
	}
	action {
		key = "credit_card"
		action = "delete"
	}
	action {
		key = "duplicate_key"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Exclude)

	require.Equal(t, "strict", string(otelObj.Exclude.MatchType))

	svc := &otelObj.Exclude.Services[0]
	require.Equal(t, "svcA", *svc)
	svc = &otelObj.Exclude.Services[1]
	require.Equal(t, "svcB", *svc)

	attr := &otelObj.Exclude.Attributes[0]
	require.Equal(t, "env", attr.Key)
	require.Equal(t, "dev", attr.Value)

	attr = &otelObj.Exclude.Attributes[1]
	require.Equal(t, "test_request", attr.Key)

	action := &otelObj.Actions[0]
	require.Equal(t, "credit_card", action.Key)
	require.Equal(t, "delete", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "duplicate_key", action.Key)
	require.Equal(t, "delete", string(action.Action))

	var inputTrace = `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpan",
					"attributes": [{
						"key": "service.name",
						"value": { "stringValue": "svcA" }
					},
					{
						"key": "env",
						"value": { "stringValue": "dev" }
					},
					{
						"key": "test_request",
						"value": { "stringValue": "req_body" }
					},
					{
						"key": "credit_card",
						"value": { "stringValue": "0000-00000-00000" }
					},
					{
						"key": "duplicate_key",
						"value": { "stringValue": "deuplicateduplicatekey" }
					}]
				},
				{
					"name": "TestSpan",
					"attributes": [{
						"key": "service.name",
						"value": { "stringValue": "svcB" }
					},
					{
						"key": "env",
						"value": { "stringValue": "dev" }
					},
					{
						"key": "test_request",
						"value": { "stringValue": "req_body" }
					},
					{
						"key": "credit_card",
						"value": { "stringValue": "0000-00000-00000" }
					},
					{
						"key": "duplicate_key",
						"value": { "stringValue": "deuplicateduplicatekey" }
					}]
				},
				{
					"name": "TestSpan",
					"attributes": [{
						"key": "service.name",
						"value": { "stringValue": "svcC" }
					},
					{
						"key": "env",
						"value": { "stringValue": "dev" }
					},
					{
						"key": "test_request",
						"value": { "stringValue": "req_body" }
					},
					{
						"key": "credit_card",
						"value": { "stringValue": "0000-00000-00000" }
					},
					{
						"key": "duplicate_key",
						"value": { "stringValue": "deuplicateduplicatekey" }
					}]
				}]
			}]
		}]
	}`

	// We expect:
	// * "attribute1" to stay the same because it already exists
	// * "string key" will be inserted.
	expectedOutputTrace := `{
		"resourceSpans": [{
			"scopeSpans": [{
				"spans": [{
					"name": "TestSpa1",
					"attributes": [{
						"key": "service.name",
						"value": { "stringValue": "svcA" }
					},
					{
						"key": "env",
						"value": { "stringValue": "dev" }
					},
					{
						"key": "test_request",
						"value": { "stringValue": "req_body" }
					}]
				},
				{
					"name": "TestSpan2",
					"attributes": [{
						"key": "service.name",
						"value": { "stringValue": "svcB" }
					},
					{
						"key": "env",
						"value": { "stringValue": "dev" }
					},
					{
						"key": "test_request",
						"value": { "stringValue": "req_body" }
					}]
				},
				{
					"name": "TestSpan3",
					"attributes": [{
						"key": "service.name",
						"value": { "stringValue": "svcC" }
					},
					{
						"key": "env",
						"value": { "stringValue": "dev" }
					},
					{
						"key": "test_request",
						"value": { "stringValue": "req_body" }
					},
					{
						"key": "credit_card",
						"value": { "stringValue": "0000-00000-00000" }
					},
					{
						"key": "duplicate_key",
						"value": { "stringValue": "deuplicateduplicatekey" }
					}]
				}]
			}]
		}]
	}`

	testRunProcessor(t, cfg, inputTrace, expectedOutputTrace)
}

func TestDecode_ExcludeResources(t *testing.T) {
	cfg := `
	exclude {
		match_type = "strict"
		resource {
			key = "host.type"
			value = "n1-standard-1"
		}
	}
	action {
		key = "credit_card"
		action = "delete"
	}
	action {
		key = "duplicate_key"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Exclude)

	res := &otelObj.Exclude.Resources[0]
	require.Equal(t, "strict", string(otelObj.Exclude.MatchType))

	require.Equal(t, "host.type", res.Key)
	require.Equal(t, "n1-standard-1", res.Value)

	action := &otelObj.Actions[0]
	require.Equal(t, "credit_card", action.Key)
	require.Equal(t, "delete", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "duplicate_key", action.Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_ExcludeLibrary(t *testing.T) {
	cfg := `
	exclude {
		match_type = "strict"
		library {
			name = "mongo-java-driver"
			version = "3.8.0"
		}
	}
	action {
		key = "credit_card"
		action = "delete"
	}
	action {
		key = "duplicate_key"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Exclude)
	require.Equal(t, "strict", string(otelObj.Exclude.MatchType))

	lib := &otelObj.Exclude.Libraries[0]
	require.Equal(t, "mongo-java-driver", lib.Name)
	require.Equal(t, "3.8.0", *lib.Version)

	action := &otelObj.Actions[0]
	require.Equal(t, "credit_card", action.Key)
	require.Equal(t, "delete", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "duplicate_key", action.Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_ExcludeLibraryAnyVersion(t *testing.T) {
	cfg := `
	exclude {
		match_type = "strict"
		library {
			name = "mongo-java-driver"
		}
	}
	action {
		key = "credit_card"
		action = "delete"
	}
	action {
		key = "duplicate_key"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Exclude)
	require.Equal(t, "strict", string(otelObj.Exclude.MatchType))

	lib := &otelObj.Exclude.Libraries[0]
	require.Equal(t, "mongo-java-driver", lib.Name)
	require.Nil(t, lib.Version)

	action := &otelObj.Actions[0]
	require.Equal(t, "credit_card", action.Key)
	require.Equal(t, "delete", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "duplicate_key", action.Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_ExcludeLibraryBlankVersion(t *testing.T) {
	cfg := `
	exclude {
		match_type = "strict"
		library {
			name = "mongo-java-driver"
			version = ""
		}
	}
	action {
		key = "credit_card"
		action = "delete"
	}
	action {
		key = "duplicate_key"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Exclude)
	require.Equal(t, "strict", string(otelObj.Exclude.MatchType))

	lib := &otelObj.Exclude.Libraries[0]
	require.Equal(t, "mongo-java-driver", lib.Name)
	require.Equal(t, "", *lib.Version)

	action := &otelObj.Actions[0]
	require.Equal(t, "credit_card", action.Key)
	require.Equal(t, "delete", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "duplicate_key", action.Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_ExcludeServices(t *testing.T) {
	cfg := `
	exclude {
		match_type = "regexp"
		services = ["auth.*", "login.*"]
	}
	action {
		key = "credit_card"
		action = "delete"
	}
	action {
		key = "duplicate_key"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Exclude)
	require.Equal(t, "regexp", string(otelObj.Exclude.MatchType))

	svc := &otelObj.Exclude.Services
	require.Equal(t, "auth.*", (*svc)[0])
	require.Equal(t, "login.*", (*svc)[1])

	action := &otelObj.Actions[0]
	require.Equal(t, "credit_card", action.Key)
	require.Equal(t, "delete", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "duplicate_key", action.Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_SelectiveProcessing(t *testing.T) {
	cfg := `
	include {
		match_type = "strict"
		services = ["svcA", "svcB"]
	}
	exclude {
		match_type = "strict"
		attribute {
			key = "redact_trace"
			value = false
		}
	}
	action {
		key = "credit_card"
		action = "delete"
	}
	action {
		key = "duplicate_key"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Include)
	require.Equal(t, "strict", string(otelObj.Include.MatchType))

	svc := &otelObj.Include.Services
	require.Equal(t, "svcA", (*svc)[0])
	require.Equal(t, "svcB", (*svc)[1])

	require.NotNil(t, otelObj.Exclude)
	require.Equal(t, "strict", string(otelObj.Exclude.MatchType))

	attr := &otelObj.Exclude.Attributes[0]
	require.Equal(t, "redact_trace", attr.Key)
	require.Equal(t, false, attr.Value)

	action := &otelObj.Actions[0]
	require.Equal(t, "credit_card", action.Key)
	require.Equal(t, "delete", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "duplicate_key", action.Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_Complex(t *testing.T) {
	cfg := `
	action {
		key = "operation"
		value = "default"
		action = "insert"
	}
	action {
		key = "svc.operation"
		value = "operation"
		action = "upsert"
	}
	action {
		key = "operation"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "operation", action.Key)
	require.Equal(t, "default", action.Value)
	require.Equal(t, "insert", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "svc.operation", action.Key)
	require.Equal(t, "operation", action.Value)
	require.Equal(t, "upsert", string(action.Action))

	action = &otelObj.Actions[2]
	require.Equal(t, "operation", action.Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_ExampleActions(t *testing.T) {
	cfg := `
	action {
		key = "db.table"
		action = "delete"
	}
	action {
		key = "redacted_span"
		value = true
		action = "upsert"
	}
	action {
		key = "copy_key"
		from_attribute = "key_original"
		action = "update"
	}
	action {
		key = "account_id"
		value = 2245
		action = "insert"
	}
	action {
		key = "account_password"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "db.table", action.Key)
	require.Equal(t, "delete", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "redacted_span", action.Key)
	require.Equal(t, true, action.Value)
	require.Equal(t, "upsert", string(action.Action))

	action = &otelObj.Actions[2]
	require.Equal(t, "copy_key", action.Key)
	require.Equal(t, "key_original", action.FromAttribute)
	require.Equal(t, "update", string(action.Action))

	action = &otelObj.Actions[3]
	require.Equal(t, "account_id", action.Key)
	require.Equal(t, 2245, otelObj.Actions[3].Value)
	require.Equal(t, "insert", string(action.Action))

	action = &otelObj.Actions[4]
	require.Equal(t, "account_password", action.Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_Regexp(t *testing.T) {
	cfg := `
	include {
		match_type = "regexp"
		services = ["auth.*"]
	}
	exclude {
		match_type = "regexp"
		span_names = ["login.*"]
	}
	action {
		key = "password"
		action = "update"
		value = "obfuscated"
	}
	action {
		key = "token"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Include)
	require.Equal(t, "regexp", string(otelObj.Include.MatchType))
	require.Equal(t, "auth.*", otelObj.Include.Services[0])

	require.NotNil(t, otelObj.Exclude)
	require.Equal(t, "regexp", string(otelObj.Exclude.MatchType))
	require.Equal(t, "login.*", otelObj.Exclude.SpanNames[0])

	action := &otelObj.Actions[0]
	require.Equal(t, "password", action.Key)
	require.Equal(t, "obfuscated", action.Value)
	require.Equal(t, "update", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "token", action.Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_Regexp2(t *testing.T) {
	cfg := `
	include {
		match_type = "regexp"
		attribute {
			key = "db.statement"
			value = "SELECT \\* FROM USERS.*"
		}
	}
	action {
		key = "db.statement"
		action = "update"
		value = "SELECT * FROM USERS [obfuscated]"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Include)
	require.Equal(t, "regexp", string(otelObj.Include.MatchType))

	attr := &otelObj.Include.Attributes[0]
	require.Equal(t, "db.statement", attr.Key)
	require.Equal(t, "SELECT \\* FROM USERS.*", attr.Value)

	action := &otelObj.Actions[0]
	require.Equal(t, "db.statement", action.Key)
	require.Equal(t, "SELECT * FROM USERS [obfuscated]", action.Value)
	require.Equal(t, "update", string(action.Action))
}

func TestDecode_LogBodyRegexp(t *testing.T) {
	cfg := `
	include {
		match_type = "regexp"
		log_bodies = ["AUTH.*"]
	}
	action {
		key = "password"
		action = "update"
		value = "obfuscated"
	}
	action {
		key = "token"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Include)
	require.Equal(t, "regexp", string(otelObj.Include.MatchType))

	require.Equal(t, "AUTH.*", otelObj.Include.LogBodies[0])

	action := &otelObj.Actions[0]
	require.Equal(t, "password", action.Key)
	require.Equal(t, "obfuscated", action.Value)
	require.Equal(t, "update", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "token", otelObj.Actions[1].Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_LogSeverityRegexp(t *testing.T) {
	cfg := `
	include {
		match_type = "regexp"
		log_severity_texts = ["debug.*"]
		log_severity {
			min = "DEBUG"
			match_undefined = true
		}
	}
	action {
		key = "password"
		action = "update"
		value = "obfuscated"
	}
	action {
		key = "token"
		action = "delete"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)
	otelObj := (convertedArgs).(*attributesprocessor.Config)

	require.NotNil(t, otelObj.Include)
	require.Equal(t, "regexp", string(otelObj.Include.MatchType))

	require.Equal(t, "debug.*", otelObj.Include.LogSeverityTexts[0])

	require.Equal(t, int32(5), int32(otelObj.Include.LogSeverityNumber.Min))
	require.Equal(t, true, otelObj.Include.LogSeverityNumber.MatchUndefined)

	action := &otelObj.Actions[0]
	require.Equal(t, "password", action.Key)
	require.Equal(t, "obfuscated", action.Value)
	require.Equal(t, "update", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "token", otelObj.Actions[1].Key)
	require.Equal(t, "delete", string(action.Action))
}

func TestDecode_FromContext(t *testing.T) {
	cfg := `
	action {
		key = "origin"
		from_context = "metadata.origin"
		action = "insert"
	}
	action {
		key = "enduser.id"
		from_context = "auth.subject"
		action = "insert"
	}

	output {
		// no-op: will be overridden by test code.
	}
	`
	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(cfg), &args))

	convertedArgs, err := args.Convert()
	require.NoError(t, err)

	otelObj := (convertedArgs).(*attributesprocessor.Config)

	action := &otelObj.Actions[0]
	require.Equal(t, "origin", action.Key)
	require.Equal(t, "metadata.origin", action.FromContext)
	require.Equal(t, "insert", string(action.Action))

	action = &otelObj.Actions[1]
	require.Equal(t, "enduser.id", action.Key)
	require.Equal(t, "auth.subject", action.FromContext)
	require.Equal(t, "insert", string(action.Action))
}

func testRunProcessor(t *testing.T, processorConfig string, inputTraceJson string, expectedTraceOutputJson string) {
	ctx := componenttest.TestContext(t)
	l := util.TestLogger(t)

	ctrl, err := componenttest.NewControllerFromID(l, "otelcol.processor.attributes")
	require.NoError(t, err)

	var args attributes.Arguments
	require.NoError(t, river.Unmarshal([]byte(processorConfig), &args))

	// Override our arguments so traces get forwarded to traceCh.
	traceCh := make(chan ptrace.Traces)
	args.Output = makeTracesOutput(traceCh)

	go func() {
		err := ctrl.Run(ctx, args)
		require.NoError(t, err)
	}()

	require.NoError(t, ctrl.WaitRunning(time.Second), "component never started")
	require.NoError(t, ctrl.WaitExports(time.Second), "component never exported anything")

	inputTrace := createTestTraces(inputTraceJson)

	// Send traces in the background to our processor.
	go func() {
		exports := ctrl.Exports().(otelcol.ConsumerExports)

		bo := backoff.New(ctx, backoff.Config{
			MinBackoff: 10 * time.Millisecond,
			MaxBackoff: 100 * time.Millisecond,
		})
		for bo.Ongoing() {
			err := exports.Input.ConsumeTraces(ctx, inputTrace)
			if err != nil {
				level.Error(l).Log("msg", "failed to send traces", "err", err)
				bo.Wait()
				continue
			}

			return
		}
	}()

	// Wait for our processor to finish and forward data to traceCh.
	select {
	case <-time.After(time.Second):
		require.FailNow(t, "failed waiting for traces")
	case tr := <-traceCh:
		trStr := marshalTraces(tr)
		expStr := marshalTraces(createTestTraces(expectedTraceOutputJson))
		require.JSONEq(t, expStr, trStr)
	}
}

// makeTracesOutput returns ConsumerArguments which will forward traces to the
// provided channel.
func makeTracesOutput(ch chan ptrace.Traces) *otelcol.ConsumerArguments {
	traceConsumer := fakeconsumer.Consumer{
		ConsumeTracesFunc: func(ctx context.Context, t ptrace.Traces) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- t:
				return nil
			}
		},
	}

	return &otelcol.ConsumerArguments{
		Traces: []otelcol.Consumer{&traceConsumer},
	}
}

// traceJson should match format from the protobuf definition:
// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto
func createTestTraces(traceJson string) ptrace.Traces {
	decoder := &ptrace.JSONUnmarshaler{}
	data, err := decoder.UnmarshalTraces([]byte(traceJson))
	if err != nil {
		panic(err)
	}
	return data
}

func marshalTraces(trace ptrace.Traces) string {
	marshaler := &ptrace.JSONMarshaler{}
	data, err := marshaler.MarshalTraces(trace)
	if err != nil {
		panic(err)
	}
	return string(data)
}
