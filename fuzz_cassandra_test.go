//go:build fuzz
// +build fuzz

package gocql

import (
	"bytes"
	"fmt"
	"testing"
)

// FuzzMarshalFloat64Ptr aimed to repeatedly test float64 marshaling with generated inputs based on seed corpus.
func FuzzMarshalFloat64Ptr(f *testing.F) {
	f.Add(float64(7500), float64(7500.00))

	f.Fuzz(func(t *testing.T, num, numWithPoints float64) {
		session := createSession(t)

		defer session.Close()

		if err := createTable(session, "CREATE TABLE IF NOT EXISTS gocql_test.float_test (id double, test double, primary key (id))"); err != nil {
			t.Fatal("create table:", err)
		}

		if err := session.Query(`TRUNCATE TABLE gocql_test.float_test`).Exec(); err != nil {
			t.Fatal("truncate table:", err)
		}

		if err := session.Query(`INSERT INTO float_test (id,test) VALUES (?,?)`, numWithPoints, &num).Exec(); err != nil {
			t.Fatal("insert float64:", err)
		}
	})
}

func FuzzTracing(f *testing.F) {
	f.Add(42)

	f.Fuzz(func(t *testing.T, id int) {
		session := createSession(t)
		defer session.Close()

		if err := createTable(session, `CREATE TABLE gocql_test.trace (id int primary key)`); err != nil {
			t.Fatal("create:", err)
		}

		buf := &bytes.Buffer{}
		trace := &traceWriter{session: session, w: buf}
		if err := session.Query(`INSERT INTO trace (id) VALUES (?)`, id).Trace(trace).Exec(); err != nil {
			t.Fatal("insert:", err)
		} else if buf.Len() == 0 {
			t.Fatal("insert: failed to obtain any tracing")
		}
		trace.mu.Lock()
		buf.Reset()
		trace.mu.Unlock()

		var value int
		if err := session.Query(`SELECT id FROM trace WHERE id = ?`, id).Trace(trace).Scan(&value); err != nil {
			t.Fatal("select:", err)
		} else if value != id {
			t.Fatalf("value: expected %d, got %d", id, value)
		} else if buf.Len() == 0 {
			t.Fatal("select: failed to obtain any tracing")
		}

		// also works from session tracer
		session.SetTrace(trace)
		trace.mu.Lock()
		buf.Reset()
		trace.mu.Unlock()
		if err := session.Query(`SELECT id FROM trace WHERE id = ?`, id).Scan(&value); err != nil {
			t.Fatal("select:", err)
		}
		if buf.Len() == 0 {
			t.Fatal("select: failed to obtain any tracing")
		}
	})
}
func FuzzDurationType(f *testing.F) {
	f.Add(int32(1), int32(500), int32(0x7FFFFFFF), int64(0x7FFFFFFFFFFFFFFF))

	f.Fuzz(func(t *testing.T, id, month, day int32, nanoseconds int64) {

		session := createSession(t)
		defer session.Close()

		fmt.Println("\nLOL")
		fmt.Printf("\nid:%v.\nmonth:%v.\nday:%v.\nnanoseconds:%v\n", id, month, day, nanoseconds)
		if session.cfg.ProtoVersion < 4 {
			t.Skip("Duration type is not supported. Please use protocol version >= 4 and cassandra version >= 3.11")
		}

		if err := createTable(session, `CREATE TABLE gocql_test.duration_table (
		k int primary key, v duration
	)`); err != nil {
			t.Fatal("create:", err)
		}

		duration := Duration{
			Months:      month,
			Days:        day,
			Nanoseconds: nanoseconds,
		}

		if err := session.Query(`INSERT INTO gocql_test.duration_table (k, v) VALUES (?, ?)`, id, duration).Exec(); err != nil {
			t.Fatal(err)
		}

		var scanedID int
		var selectedDuration Duration
		if err := session.Query(`SELECT k, v FROM gocql_test.duration_table`).Scan(&scanedID, &selectedDuration); err != nil {
			t.Fatal(err)
		}
		if selectedDuration.Months != duration.Months || selectedDuration.Days != duration.Days || selectedDuration.Nanoseconds != duration.Nanoseconds {
			t.Fatalf("Unexpeted value returned, expected=%v, received=%v", duration, selectedDuration)
		}
	})
}
