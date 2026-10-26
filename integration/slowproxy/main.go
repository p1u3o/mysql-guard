// slowproxy: drill-only TCP proxy for MySQL. Forwards everything; for the first
// client COM_QUERY containing -match it either delays forwarding by -delay
// (mode=delay) or forwards it and then drops the client connection (mode=drop).
// Later matches pass through. Requires a non-TLS client connection.
package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"
)

func main() {
	listen := flag.String("listen", ":3399", "listen address")
	target := flag.String("target", "", "upstream host:port")
	match := flag.String("match", "START REPLICA", "query substring to act on")
	mode := flag.String("mode", "delay", "delay or drop")
	delay := flag.Duration("delay", 5*time.Second, "delay for mode=delay")
	flag.Parse()
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	var fired atomic.Bool
	log.Printf("slowproxy %s -> %s mode=%s delay=%s match=%q", *listen, *target, *mode, *delay, *match)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handle(c, *target, []byte(*match), *mode, *delay, &fired)
	}
}

func handle(c net.Conn, target string, match []byte, mode string, delay time.Duration, fired *atomic.Bool) {
	defer c.Close()
	u, err := net.Dial("tcp", target)
	if err != nil {
		log.Printf("dial: %v", err)
		return
	}
	defer u.Close()
	go func() { _, _ = io.Copy(c, u); c.Close() }()
	hdr := make([]byte, 4)
	for {
		if _, err := io.ReadFull(c, hdr); err != nil {
			return
		}
		n := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
		body := make([]byte, n)
		if _, err := io.ReadFull(c, body); err != nil {
			return
		}
		hit := hdr[3] == 0 && n > 0 && body[0] == 0x03 && bytes.Contains(bytes.ToUpper(body[1:]), match) && fired.CompareAndSwap(false, true)
		if hit && mode == "delay" {
			log.Printf("DELAYING %q by %s", body[1:], delay)
			time.Sleep(delay)
		}
		if hit && mode == "drop" {
			// Reset the client before the server's reply can be relayed, so the
			// statement executes upstream but the client sees a dropped connection.
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
			}
			c.Close()
			_, _ = u.Write(append(hdr, body...))
			log.Printf("FORWARDED then DROPPING client after %q", body[1:])
			time.Sleep(2 * time.Second) // let upstream execute before closing it
			return
		}
		if _, err := u.Write(append(hdr, body...)); err != nil {
			return
		}
	}
}
