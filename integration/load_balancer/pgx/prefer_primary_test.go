package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func skipUnlessPreferPrimary(t *testing.T) {
	if os.Getenv("PGDOG_READ_WRITE_SPLIT") != "prefer_primary" {
		t.Skip("skipping: PGDOG_READ_WRITE_SPLIT != prefer_primary")
	}
}

func TestPreferPrimaryReadsGoToPrimary(t *testing.T) {
	skipUnlessPreferPrimary(t)

	pool := GetPool()
	defer pool.Close()

	ResetStats()

	_, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_pp_reads (
		id BIGINT,
		data VARCHAR
	)`)
	assert.NoError(t, err)
	defer pool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_pp_reads")

	time.Sleep(2 * time.Second)

	for i := range 50 {
		_, err = pool.Exec(context.Background(), "SELECT $1::bigint FROM lb_pp_reads LIMIT 1", int64(i))
		assert.NoError(t, err)
	}

	primaryCalls := LoadStatsForPrimary("lb_pp_reads")
	assert.True(t, primaryCalls.Calls >= 50, "expected >= 50 calls on primary, got %d", primaryCalls.Calls)

	replicaCalls := LoadStatsForReplicas("lb_pp_reads")
	for _, rc := range replicaCalls {
		assert.Equal(t, int64(0), rc.Calls, "expected 0 read calls on replica")
	}
}

func TestPreferPrimaryWritesGoToPrimary(t *testing.T) {
	skipUnlessPreferPrimary(t)

	pool := GetPool()
	defer pool.Close()

	ResetStats()

	_, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_pp_writes (
		id BIGINT,
		email VARCHAR,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)
	assert.NoError(t, err)
	defer pool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_pp_writes")

	done := make(chan int)

	for i := range 50 {
		go func(id int) {
			_, err := pool.Exec(context.Background(), "INSERT INTO lb_pp_writes (id, email) VALUES ($1, $2)", id, fmt.Sprintf("test-%d@test.com", id))
			assert.NoError(t, err)
			done <- 1
		}(i)

		go func(id int) {
			_, err := pool.Exec(context.Background(), "UPDATE lb_pp_writes SET email = $2 WHERE id = $1", id, fmt.Sprintf("updated-%d@test.com", id))
			assert.NoError(t, err)
			done <- 1
		}(i)

		go func(id int) {
			_, err := pool.Exec(context.Background(), "DELETE FROM lb_pp_writes WHERE id = $1", id)
			assert.NoError(t, err)
			done <- 1
		}(i)
	}

	for range 50 * 3 {
		<-done
	}

	calls := LoadStatsForPrimary("INSERT INTO lb_pp_writes")
	assert.Equal(t, int64(50), calls.Calls)

	calls = LoadStatsForPrimary("UPDATE lb_pp_writes")
	assert.Equal(t, int64(50), calls.Calls)

	calls = LoadStatsForPrimary("DELETE FROM lb_pp_writes")
	assert.Equal(t, int64(50), calls.Calls)
}

func TestPreferPrimaryCommentReplicaOverride(t *testing.T) {
	skipUnlessPreferPrimary(t)

	pool := GetPool()
	defer pool.Close()

	_, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_pp_comment (
		id BIGINT,
		data VARCHAR
	)`)
	assert.NoError(t, err)
	defer pool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_pp_comment")

	time.Sleep(2 * time.Second)
	ResetStats()

	for i := range 50 {
		_, err = pool.Exec(context.Background(), "/* pgdog_role: prefer-replica */ SELECT $1::bigint FROM lb_pp_comment LIMIT 1", int64(i))
		assert.NoError(t, err)
	}

	replicaCalls := LoadStatsForReplicas("lb_pp_comment")
	totalReplicaCalls := int64(0)
	for _, rc := range replicaCalls {
		totalReplicaCalls += rc.Calls
	}
	assert.Equal(t, int64(50), totalReplicaCalls, "expected 50 total calls on replicas")

	primaryCalls := LoadStatsForPrimary("lb_pp_comment")
	assert.Equal(t, int64(0), primaryCalls.Calls, "expected 0 read calls on primary after comment override")
}

func TestPreferPrimaryConnectionParamReplica(t *testing.T) {
	skipUnlessPreferPrimary(t)

	conn, err := pgx.Connect(context.Background(), "postgres://postgres:postgres@127.0.0.1:6432/postgres?sslmode=disable&options=-c%20pgdog.role%3Dprefer-replica")
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_pp_connparam (
		id BIGINT,
		data VARCHAR
	)`)
	assert.NoError(t, err)

	otherPool := GetPool()
	defer otherPool.Close()
	defer otherPool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_pp_connparam")

	time.Sleep(2 * time.Second)
	ResetStats()

	for i := range 50 {
		_, err = conn.Exec(context.Background(), "SELECT $1::bigint FROM lb_pp_connparam LIMIT 1", int64(i))
		assert.NoError(t, err)
	}

	replicaCalls := LoadStatsForReplicas("lb_pp_connparam")
	totalReplicaCalls := int64(0)
	for _, rc := range replicaCalls {
		totalReplicaCalls += rc.Calls
	}
	assert.Equal(t, int64(50), totalReplicaCalls, "expected 50 total calls on replicas via connection param")
}

func TestPreferPrimaryResetReverts(t *testing.T) {
	skipUnlessPreferPrimary(t)

	conn, err := pgx.Connect(context.Background(), "postgres://postgres:postgres@127.0.0.1:6432/postgres?sslmode=disable&options=-c%20pgdog.role%3Dprefer-replica")
	assert.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS lb_pp_reset (
		id BIGINT,
		data VARCHAR
	)`)
	assert.NoError(t, err)

	otherPool := GetPool()
	defer otherPool.Close()
	defer otherPool.Exec(context.Background(), "DROP TABLE IF EXISTS lb_pp_reset")

	time.Sleep(2 * time.Second)
	ResetStats()

	for i := range 25 {
		_, err = conn.Exec(context.Background(), "SELECT $1::bigint FROM lb_pp_reset LIMIT 1", int64(i))
		assert.NoError(t, err)
	}

	replicaCalls := LoadStatsForReplicas("lb_pp_reset")
	totalReplicaCalls := int64(0)
	for _, rc := range replicaCalls {
		totalReplicaCalls += rc.Calls
	}
	assert.Equal(t, int64(25), totalReplicaCalls, "expected 25 calls on replicas before RESET")

	_, err = conn.Exec(context.Background(), "RESET pgdog.role")
	assert.NoError(t, err)

	ResetStats()

	for i := range 25 {
		_, err = conn.Exec(context.Background(), "SELECT $1::bigint FROM lb_pp_reset LIMIT 1", int64(i))
		assert.NoError(t, err)
	}

	primaryCalls := LoadStatsForPrimary("lb_pp_reset")
	assert.True(t, primaryCalls.Calls >= 25, "expected >= 25 calls on primary after RESET, got %d", primaryCalls.Calls)
}
