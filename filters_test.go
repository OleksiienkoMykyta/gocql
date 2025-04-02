/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilter_WhiteList(t *testing.T) {
	f := WhiteListHostFilter("127.0.0.1", "127.0.0.2")
	tests := [...]struct {
		addr   net.IP
		accept bool
	}{
		{net.ParseIP("127.0.0.1"), true},
		{net.ParseIP("127.0.0.2"), true},
		{net.ParseIP("127.0.0.3"), false},
	}

	for i, test := range tests {
		if f.Accept(&HostInfo{connectAddress: test.addr}) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}

func TestFilter_AllowAll(t *testing.T) {
	f := AcceptAllFilter()
	tests := [...]struct {
		addr   net.IP
		accept bool
	}{
		{net.ParseIP("127.0.0.1"), true},
		{net.ParseIP("127.0.0.2"), true},
		{net.ParseIP("127.0.0.3"), true},
	}

	for i, test := range tests {
		if f.Accept(&HostInfo{connectAddress: test.addr}) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}

func TestFilter_DenyAll(t *testing.T) {
	f := DenyAllFilter()
	tests := [...]struct {
		addr   net.IP
		accept bool
	}{
		{net.ParseIP("127.0.0.1"), false},
		{net.ParseIP("127.0.0.2"), false},
		{net.ParseIP("127.0.0.3"), false},
	}

	for i, test := range tests {
		if f.Accept(&HostInfo{connectAddress: test.addr}) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}

func TestFilter_DataCenter(t *testing.T) {
	f := DataCenterHostFilter("dc1")
	fDeprecated := DataCentreHostFilter("dc1")

	tests := [...]struct {
		dc     string
		accept bool
	}{
		{"dc1", true},
		{"dc2", false},
	}

	for i, test := range tests {
		if f.Accept(&HostInfo{dataCenter: test.dc}) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}

		if f.Accept(&HostInfo{dataCenter: test.dc}) != fDeprecated.Accept(&HostInfo{dataCenter: test.dc}) {
			t.Errorf("%d: DataCenterHostFilter and DataCentreHostFilter should be the same", i)
		}
	}
}

// mockHost is a custom implementation of the Host interface for testing.
type mockHost struct {
	peer           net.IP
	connectAddress net.IP
	dataCenter     string
	version        CassVersion
}

func (m *mockHost) Peer() net.IP             { return m.peer }
func (m *mockHost) ConnectAddress() net.IP   { return m.connectAddress }
func (m *mockHost) BroadcastAddress() net.IP { return nil }
func (m *mockHost) ListenAddress() net.IP    { return nil }
func (m *mockHost) RPCAddress() net.IP       { return nil }
func (m *mockHost) PreferredIP() net.IP      { return nil }
func (m *mockHost) DataCenter() string       { return m.dataCenter }
func (m *mockHost) Rack() string             { return "" }
func (m *mockHost) HostID() string           { return "" }
func (m *mockHost) WorkLoad() string         { return "" }
func (m *mockHost) Graph() bool              { return false }
func (m *mockHost) DSEVersion() string       { return "" }
func (m *mockHost) Partitioner() string      { return "" }
func (m *mockHost) ClusterName() string      { return "" }
func (m *mockHost) Version() CassVersion     { return m.version }
func (m *mockHost) Tokens() []string         { return nil }
func (m *mockHost) Port() int                { return 9042 }
func (m *mockHost) IsUp() bool               { return true }
func (m *mockHost) String() string           { return "mockHost" }

// mockCassVersion is a fake CassVersion implementation for testing.
type mockCassVersion struct {
	versionString string
}

func (m *mockCassVersion) Set(v string) error                            { m.versionString = v; return nil }
func (m *mockCassVersion) UnmarshalCQL(info TypeInfo, data []byte) error { return nil }
func (m *mockCassVersion) AtLeast(major, minor, patch int) bool          { return true }
func (m *mockCassVersion) String() string                                { return m.versionString }

// Test custom Host implementation
func TestHostImplementation(t *testing.T) {
	mockVersion := &mockCassVersion{versionString: "3.11.4"}
	mockHost := &mockHost{
		peer:           net.ParseIP("192.168.1.1"),
		connectAddress: net.ParseIP("10.0.0.1"),
		dataCenter:     "datacenter1",
		version:        mockVersion,
	}

	assert.Equal(t, "datacenter1", mockHost.DataCenter(), "DataCenter() should return the correct value")
	assert.Equal(t, net.ParseIP("10.0.0.1"), mockHost.ConnectAddress(), "ConnectAddress() should return the correct IP")
	assert.Equal(t, "3.11.4", mockHost.Version().String(), "Version() should return the correct Cassandra version")
	assert.True(t, mockHost.IsUp(), "IsUp() should return true")
	assert.Equal(t, "mockHost", mockHost.String(), "String() should return 'mockHost'")
}

// Test CassVersion interface implementation
func TestHostCassVersion(t *testing.T) {
	mockVersion := &mockCassVersion{versionString: "4.0.0"}

	// Test setting version
	err := mockVersion.Set("4.0.1")
	assert.NoError(t, err, "Set() should not return an error")
	assert.Equal(t, "4.0.1", mockVersion.String(), "String() should return the updated version")

	// Test AtLeast method
	assert.True(t, mockVersion.AtLeast(4, 0, 0), "AtLeast() should return true for matching version")
}
