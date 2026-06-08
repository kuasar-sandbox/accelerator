package client

import "testing"

func TestNormalizeTarget(t *testing.T) {
	cases := []struct{ in, want string }{
		{"127.0.0.1:7100", "127.0.0.1:7100"},                 // tcp passes through
		{"store:7100", "store:7100"},                         // dns host passes through
		{"/run/sandbox/store.sock", "unix:///run/sandbox/store.sock"}, // bare abs path -> unix:///
		{"unix:///run/x.sock", "unix:///run/x.sock"},         // canonical gRPC form unchanged
		{"unix:/run/x.sock", "unix:/run/x.sock"},             // gRPC short form unchanged
		{"unix-abstract:foo", "unix-abstract:foo"},           // abstract socket scheme unchanged
	}
	for _, c := range cases {
		if got := normalizeTarget(c.in); got != c.want {
			t.Errorf("normalizeTarget(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
