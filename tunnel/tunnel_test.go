package tunnel

import "testing"

func TestTunnel(t *testing.T) {
	tunnel := &Tunnel{
		Host:        "192.168.100.16",
		Port:        22,
		ForwardPort: 3000,
		BindPort:    3000,
		User:        "pi",
	}

	tunnel.Start()
}
