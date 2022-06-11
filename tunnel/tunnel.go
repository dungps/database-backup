package tunnel

import (
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"net"
	"os"
	"os/signal"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Tunnel struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port,omitempty"`
	ForwardPort     int    `yaml:"forward_port,omitempty"`
	BindPort        int    `yaml:"bind_port,omitempty"`
	User            string `yaml:"user,omitempty"`
	Password        string `yaml:"password,omitempty"`
	IdentifyKeyPath string `yaml:"identify_key_path,omitempty"`
	keepAlive       struct {
		Interval int
		CountMax int
	}
}

func (tc *Tunnel) GetHost() string {
	return tc.Host
}

func (tc *Tunnel) GetUser() string {
	if len(tc.User) == 0 {
		return "root"
	}
	return tc.User
}

func (tc *Tunnel) GetPort() int {
	if tc.Port == 0 {
		return 22
	}

	return tc.Port
}

func (tc *Tunnel) GetForwardPort() int {
	if tc.BindPort == 0 {
		return tc.Port
	}

	return tc.ForwardPort
}

func (tc *Tunnel) GetBindPort() int {
	return tc.BindPort
}

func (tc *Tunnel) GetPassword() string {
	return tc.Password
}

func (tc *Tunnel) GetIdentifyKeyPath() string {
	if len(tc.IdentifyKeyPath) == 0 {
		home := os.Getenv("HOME")
		filePath := path.Join(home, ".ssh", "id_rsa")
		if _, err := os.Stat(filePath); err == nil {
			tc.IdentifyKeyPath = filePath
		}
	}

	return tc.IdentifyKeyPath
}

func (tc *Tunnel) GetBindAddr() string {
	return fmt.Sprintf("localhost:%d", tc.GetForwardPort())
}

func (tc *Tunnel) GetDialAddr() string {
	return fmt.Sprintf("localhost:%d", tc.GetBindPort())
}

func (tc *Tunnel) String() string {
	left, mode, right := tc.GetBindAddr(), "->", tc.GetDialAddr()
	return fmt.Sprintf("%s@%s | %s %s %s", tc.GetUser(), tc.GetHost(), left, mode, right)
}

func (tc *Tunnel) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		fmt.Printf("received %v - initiating shutdown\n", <-sigc)
		cancel()
	}()

	var wg sync.WaitGroup
	fmt.Printf("%s starting\n", path.Base(os.Args[0]))
	defer fmt.Printf("%s shutdown\n", path.Base(os.Args[0]))
	wg.Add(1)
	go tc.bindTunnel(ctx, &wg)
	wg.Wait()
}

func (tc *Tunnel) bindTunnel(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		var once sync.Once // Only print errors once per session
		func() {
			auth := make([]ssh.AuthMethod, 0)

			if len(tc.GetPassword()) > 0 {
				auth = append(auth, ssh.Password(tc.GetPassword()))
			} else if len(tc.GetIdentifyKeyPath()) > 0 {
				key, err := os.ReadFile(tc.GetIdentifyKeyPath())
				if err != nil {
					once.Do(func() {
						fmt.Printf("Cannot read identify key from %s\n", tc.GetIdentifyKeyPath())
					})
					return
				}
				signed, err := ssh.ParsePrivateKey(key)
				if err != nil {
					once.Do(func() {
						fmt.Printf(err.Error())
					})
					return
				}
				auth = append(auth, ssh.PublicKeys(signed))
			}

			// Connect to the server host via SSH.
			cl, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", tc.GetHost(), tc.GetPort()), &ssh.ClientConfig{
				User: tc.GetUser(),
				Auth: auth,
				HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
					return nil
				},
				Timeout: 5 * time.Second,
			})
			if err != nil {
				once.Do(func() { fmt.Printf("(%v) SSH dial error: %v\n", tc, err) })
				return
			}
			wg.Add(1)
			go tc.keepAliveMonitor(&once, wg, cl)
			defer cl.Close()

			// Attempt to bind to the inbound socket.
			var ln net.Listener
			ln, err = net.Listen("tcp", tc.GetBindAddr())
			if err != nil {
				once.Do(func() { fmt.Printf("(%v) bind error: %v\n", tc, err) })
				return
			}

			// The socket is binded. Make sure we close it eventually.
			bindCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() {
				_ = cl.Wait()
				cancel()
			}()
			go func() {
				<-bindCtx.Done()
				once.Do(func() {}) // Suppress future errors
				_ = ln.Close()
			}()

			fmt.Printf("(%v) binded tunnel\n", tc)
			defer fmt.Printf("(%v) collapsed tunnel\n", tc)

			// Accept all incoming connections.
			for {
				cn1, err := ln.Accept()
				if err != nil {
					once.Do(func() { fmt.Printf("(%v) accept error: %v\n", tc, err) })
					return
				}
				wg.Add(1)
				go tc.dialTunnel(bindCtx, wg, cl, cn1)
			}
		}()

		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
			fmt.Printf("(%v) retrying...\n", tc)
		}
	}
}

func (tc *Tunnel) dialTunnel(ctx context.Context, wg *sync.WaitGroup, client *ssh.Client, cn1 net.Conn) {
	defer wg.Done()

	// The inbound connection is established. Make sure we close it eventually.
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = cn1.Close()
	}()

	// Establish the outbound connection.
	var cn2 net.Conn
	var err error
	cn2, err = client.Dial("tcp", tc.GetDialAddr())
	if err != nil {
		fmt.Printf("(%v) dial error: %v", tc, err)
		return
	}

	go func() {
		<-connCtx.Done()
		_ = cn2.Close()
	}()

	fmt.Printf("(%v) connection established", tc)
	defer fmt.Printf("(%v) connection closed", tc)

	// Copy bytes from one connection to the other until one side closes.
	var once sync.Once
	var wg2 sync.WaitGroup
	wg2.Add(2)
	go func() {
		defer wg2.Done()
		defer cancel()
		if _, err := io.Copy(cn1, cn2); err != nil {
			once.Do(func() { fmt.Printf("(%v) connection error: %v", tc, err) })
		}
		once.Do(func() {}) // Suppress future errors
	}()
	go func() {
		defer wg2.Done()
		defer cancel()
		if _, err := io.Copy(cn2, cn1); err != nil {
			once.Do(func() { fmt.Printf("(%v) connection error: %v", tc, err) })
		}
		once.Do(func() {}) // Suppress future errors
	}()
	wg2.Wait()
}

func (tc *Tunnel) keepAliveMonitor(once *sync.Once, wg *sync.WaitGroup, client *ssh.Client) {
	defer wg.Done()
	if tc.keepAlive.Interval == 0 || tc.keepAlive.CountMax == 0 {
		return
	}

	// Detect when the SSH connection is closed.
	wait := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wait <- client.Wait()
	}()

	// Repeatedly check if the remote server is still alive.
	var aliveCount int32
	ticker := time.NewTicker(time.Duration(tc.keepAlive.Interval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-wait:
			if err != nil && err != io.EOF {
				once.Do(func() { fmt.Printf("(%v) SSH error: %v", tc, err) })
			}
			return
		case <-ticker.C:
			if n := atomic.AddInt32(&aliveCount, 1); n > int32(tc.keepAlive.CountMax) {
				once.Do(func() { fmt.Printf("(%v) SSH keep-alive termination", tc) })
				_ = client.Close()
				return
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := client.SendRequest("keepalive@openssh.com", true, nil)
			if err == nil {
				atomic.StoreInt32(&aliveCount, 0)
			}
		}()
	}
}
