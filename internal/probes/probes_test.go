package probes

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
)

func TestTCPCheckFailure(t *testing.T) {
	// Bind then close to get a free port that refuses connections.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ok, dur := TCPCheck(context.Background(), addr, 200*time.Millisecond)
	if ok {
		t.Fatal("expected failure")
	}
	if dur <= 0 {
		t.Fatal("duration")
	}
}

func TestTCPCheckSuccess(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.Close()
		}
	}()
	ok, _ := TCPCheck(context.Background(), ln.Addr().String(), time.Second)
	if !ok {
		t.Fatal("expected success")
	}
}

func TestRunnerTCPOnly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	r := NewRunner(1)
	tgt := config.ResolvedTarget{
		ID: "g/t1", Name: "t1", AMQPAddress: ln.Addr().String(),
		Labels: map[string]string{"instance": "t1", "job": "j"},
		Probes: config.ProbesConfig{
			AMQPTCP: config.ProbeAMQPTCP{
				Enabled:  true,
				Timeout:  config.Duration(time.Second),
				Interval: config.Duration(time.Hour),
			},
		},
	}
	res := r.Run(context.Background(), tgt)
	if !res.TCPOK {
		t.Fatal("tcp")
	}
	// Second run within interval should reuse cache (listener can be closed).
	_ = ln.Close()
	res2 := r.Run(context.Background(), tgt)
	if !res2.TCPOK {
		t.Fatal("expected cached tcp success after listener close")
	}
	found := false
	for _, f := range res.Families {
		if f.Name == "rabbitmq_amqp_tcp_success" && f.Samples[0].Value == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("metric missing")
	}
}
