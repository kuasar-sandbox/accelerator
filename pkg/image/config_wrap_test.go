package image

import "testing"

func TestWrapRuntimeConfigJSON(t *testing.T) {
	// OCI shape passes through untouched.
	oci := []byte(`{"architecture":"amd64","os":"linux","config":{"User":"app","Env":["A=1"],"Entrypoint":["/e"]}}`)
	out, err := WrapRuntimeConfigJSON(oci)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(oci) {
		t.Error("OCI input must pass through unchanged")
	}
	rc, err := ExtractRuntimeConfigFromJSON(out)
	if err != nil || rc.User != "app" || rc.Architecture != "amd64" {
		t.Fatalf("OCI projection: %+v, %v", rc, err)
	}

	// Projected shape is re-nested losslessly.
	projected := []byte(`{"Architecture":"arm64","Os":"linux","User":"u","Env":["B=2"],"Cmd":["/c"],"WorkingDir":"/w","StopSignal":"SIGTERM","Labels":{"k":"v"}}`)
	out, err = WrapRuntimeConfigJSON(projected)
	if err != nil {
		t.Fatal(err)
	}
	rc, err = ExtractRuntimeConfigFromJSON(out)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Architecture != "arm64" || rc.User != "u" || rc.WorkingDir != "/w" ||
		rc.StopSignal != "SIGTERM" || rc.Labels["k"] != "v" || len(rc.Cmd) != 1 {
		t.Errorf("projected round-trip: %+v", rc)
	}

	// Empty object → empty config, no error.
	out, err = WrapRuntimeConfigJSON([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if rc, err := ExtractRuntimeConfigFromJSON(out); err != nil || rc.User != "" {
		t.Errorf("empty input: %+v, %v", rc, err)
	}

	// Garbage errors.
	if _, err := WrapRuntimeConfigJSON([]byte(`[1,2]`)); err == nil {
		t.Error("non-object input must error")
	}
}
