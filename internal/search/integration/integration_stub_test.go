//go:build !integration

package integration

import "testing"

// TestIntegrationStub ensures integration tests are skipped unless the
// integration build tag is provided.
func TestIntegrationStub(t *testing.T) {
	t.Skip("integration tests require integration build tag")
}
