package pgscheduler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tzahifadida/pgln"
	"log/slog"
	"os"
	"testing"
	"time"
)

func setupTestSchedulerWithPGLN(t *testing.T, schema string) (*Scheduler, *pgln.PGListenNotify, clockwork.FakeClock) {
	testLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create PGLN instance
	builder := pgln.NewPGListenNotifyBuilder().
		SetContext(context.Background()).
		SetDB(testDBSQL).
		SetReconnectInterval(time.Second)

	pglnInstance, err := builder.Build()
	require.NoError(t, err)
	err = pglnInstance.Start()
	require.NoError(t, err)

	config := SchedulerConfig{
		DB:                                   testDBSQL,
		DBDriverName:                         "postgres",
		MaxRunningJobs:                       10,
		JobCheckInterval:                     100 * time.Second,
		HeartbeatInterval:                    time.Second,
		NoHeartbeatTimeout:                   3 * time.Second,
		OrphanedJobTimeout:                   14 * 24 * time.Hour,
		CreateSchema:                         true,
		RunImmediately:                       false,
		TablePrefix:                          "test_",
		ShutdownTimeout:                      5 * time.Second,
		Logger:                               testLogger,
		FailedAndCompletedJobCleanupInterval: time.Hour,
		CancelCheckPeriod:                    100 * time.Second,
		Schema:                               schema,
		Ctx:                                  context.Background(),
		PGLNInstance:                         pglnInstance,
	}

	scheduler, err := NewScheduler(config)
	require.NoError(t, err)
	require.NotNil(t, scheduler)
	testDB = scheduler.db

	err = scheduler.Init()
	require.NoError(t, err)

	err = scheduler.Start()
	require.NoError(t, err)

	return scheduler, pglnInstance, clockwork.NewFakeClock()
}

func cleanupTestDatabaseWithPGLN(t *testing.T, scheduler *Scheduler, pgln *pgln.PGListenNotify) {
	scheduler.Shutdown()
	pgln.Shutdown()

	query := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, scheduler.config.Schema, scheduler.config.TablePrefix+scheduledJobsTableName)
	_, err := testDB.Exec(query)
	require.NoError(t, err)

	if scheduler.config.Schema != "public" {
		query = fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, scheduler.config.Schema)
		_, err = testDB.Exec(query)
		require.NoError(t, err)
	}
}

func TestPGLNJobReadyNotification(t *testing.T) {
	scheduler, pglnInstance, _ := setupTestSchedulerWithPGLN(t, "public")
	defer cleanupTestDatabaseWithPGLN(t, scheduler, pglnInstance)

	jobProcessed := make(chan struct{})
	job := &TestJob{
		name:       "test_job",
		key:        "key1",
		maxRetries: 0,
		runFunc: func(ctx context.Context) error {
			select {
			case jobProcessed <- struct{}{}:
			default:
			}
			return nil
		},
	}

	err := scheduler.RegisterJob(job)
	require.NoError(t, err)

	// Schedule the job and wait for notification to trigger processing
	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	// Wait for job processing or timeout
	select {
	case <-jobProcessed:
		time.Sleep(2 * time.Second)
		// Job was processed via notification
	case <-time.After(5 * time.Second):
		t.Fatal("Job was not processed after notification")
	}

	// Verify job status
	var record JobRecord
	query := fmt.Sprintf(`SELECT * FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	err = testDB.Get(&record, query, job.Name(), job.Key())
	require.NoError(t, err)
	assert.False(t, record.Picked)
	assert.Equal(t, StatusCompleted, record.Status)
}

func TestPGLNCancelNotification(t *testing.T) {
	scheduler, pglnInstance, _ := setupTestSchedulerWithPGLN(t, "public")
	defer cleanupTestDatabaseWithPGLN(t, scheduler, pglnInstance)

	jobStarted := make(chan struct{})
	jobCancelled := make(chan struct{})

	longRunningJob := &TestJob{
		name:       "long_job",
		key:        "key1",
		maxRetries: 0,
		runFunc: func(ctx context.Context) error {
			close(jobStarted)
			select {
			case <-ctx.Done():
				close(jobCancelled)
				return ctx.Err()
			case <-time.After(10 * time.Second):
				return nil
			}
		},
	}

	err := scheduler.RegisterJob(longRunningJob)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(longRunningJob)
	require.NoError(t, err)

	// Wait for job to start
	select {
	case <-jobStarted:
		// Job started
	case <-time.After(5 * time.Second):
		t.Fatal("Job did not start")
	}

	// Cancel the job
	err = scheduler.CancelJob(longRunningJob.Name(), longRunningJob.Key())
	require.NoError(t, err)

	// Wait for cancellation
	select {
	case <-jobCancelled:
		time.Sleep(2 * time.Second)
		// Job was cancelled via notification
	case <-time.After(5 * time.Second):
		t.Fatal("Job was not cancelled after notification")
	}

	// Verify job status
	var record JobRecord
	query := fmt.Sprintf(`SELECT * FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	err = testDB.Get(&record, query, longRunningJob.Name(), longRunningJob.Key())
	require.NoError(t, err)
	assert.False(t, record.Picked)
	assert.Equal(t, StatusFailed, record.Status)
	assert.False(t, record.CancelRequested)
}
func dropAllConnections(connectionString string) error {
	ctx := context.Background()
	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE pid <> pg_backend_pid()
		AND datname = current_database()
	`)
	if err != nil {
		return fmt.Errorf("failed to terminate connections: %w", err)
	}

	return nil
}
func TestPGLNOutOfSync(t *testing.T) {
	scheduler, pglnInstance, _ := setupTestSchedulerWithPGLN(t, "public")
	defer cleanupTestDatabaseWithPGLN(t, scheduler, pglnInstance)

	jobProcessed := make(chan struct{}, 1)
	job := &TestJob{
		name:       "test_job",
		key:        "key1",
		maxRetries: 0,
		runFunc: func(ctx context.Context) error {
			select {
			case jobProcessed <- struct{}{}:
				time.Sleep(2 * time.Second)
			default:
			}
			return nil
		},
	}

	err := scheduler.RegisterJob(job)
	require.NoError(t, err)

	// Schedule job but simulate it being added while PGLN was disconnected
	dropAllConnections(dsn)
	time.Sleep(2 * time.Second)
	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	// Wait for job to be processed during out-of-sync recovery
	select {
	case <-jobProcessed:
		time.Sleep(4 * time.Second)
		// Job was processed during out-of-sync
	case <-time.After(5 * time.Second):
		t.Fatal("Job was not processed during out-of-sync recovery")
	}

	// Verify job status
	var record JobRecord
	query := fmt.Sprintf(`SELECT * FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	err = testDB.Get(&record, query, job.Name(), job.Key())
	require.NoError(t, err)
	assert.False(t, record.Picked)
	assert.Equal(t, StatusCompleted, record.Status)
}

func TestStatusChangeCallback(t *testing.T) {
	// Create channels to track status changes and their order
	statusChanges := make(chan struct {
		name       string
		key        string
		prevStatus Status
		newStatus  Status
	}, 10)

	jobProcessed := make(chan struct{})

	testLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create PGLN instance
	builder := pgln.NewPGListenNotifyBuilder().
		SetContext(context.Background()).
		SetDB(testDBSQL).
		SetReconnectInterval(time.Second)

	pglnInstance, err := builder.Build()
	require.NoError(t, err)
	err = pglnInstance.Start()
	require.NoError(t, err)

	config := SchedulerConfig{
		DB:                                   testDBSQL,
		DBDriverName:                         "postgres",
		MaxRunningJobs:                       10,
		JobCheckInterval:                     100 * time.Second,
		HeartbeatInterval:                    time.Second,
		NoHeartbeatTimeout:                   3 * time.Second,
		OrphanedJobTimeout:                   14 * 24 * time.Hour,
		CreateSchema:                         true,
		RunImmediately:                       false,
		TablePrefix:                          "test_",
		ShutdownTimeout:                      5 * time.Second,
		Logger:                               testLogger,
		FailedAndCompletedJobCleanupInterval: time.Hour,
		CancelCheckPeriod:                    100 * time.Second,
		Schema:                               "public",
		Ctx:                                  context.Background(),
		PGLNInstance:                         pglnInstance,
		JobStatusChangeCallback: func(name, key string, prevStatus, newStatus Status) {
			statusChanges <- struct {
				name       string
				key        string
				prevStatus Status
				newStatus  Status
			}{name, key, prevStatus, newStatus}
		},
	}

	scheduler, err := NewScheduler(config)
	require.NoError(t, err)
	require.NotNil(t, scheduler)
	defer cleanupTestDatabaseWithPGLN(t, scheduler, pglnInstance)

	err = scheduler.Init()
	require.NoError(t, err)

	err = scheduler.Start()
	require.NoError(t, err)

	// Create a test job that will succeed
	job := &TestJob{
		name:       "test_job",
		key:        "key1",
		maxRetries: 0,
		runFunc: func(ctx context.Context) error {
			close(jobProcessed)
			return nil
		},
	}

	err = scheduler.RegisterJob(job)
	require.NoError(t, err)

	// Schedule the job and collect status changes
	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	// We expect these status transitions:
	// 1. "" -> "pending" (from scheduling)
	// 2. "pending" -> "running" (from job start)
	// 3. "running" -> "pending" (from job completion)

	expectedTransitions := []struct {
		prevStatus Status
		newStatus  Status
	}{
		{"", StatusPending},
		{StatusPending, StatusRunning},
		{StatusRunning, StatusCompleted},
	}

	// Wait for job to complete
	select {
	case <-jobProcessed:
	case <-time.After(5 * time.Second):
		t.Fatal("Job did not complete in time")
	}

	// Give time for final status change to process
	time.Sleep(time.Second)

	// Verify all status transitions
	for i, expected := range expectedTransitions {
		select {
		case change := <-statusChanges:
			assert.Equal(t, job.Name(), change.name, "Unexpected job name in transition %d", i)
			assert.Equal(t, job.Key(), change.key, "Unexpected job key in transition %d", i)
			assert.Equal(t, expected.prevStatus, change.prevStatus, "Unexpected previous status in transition %d", i)
			assert.Equal(t, expected.newStatus, change.newStatus, "Unexpected new status in transition %d", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not receive status change notification %d", i)
		}
	}

	// Verify no additional status changes occurred
	select {
	case change := <-statusChanges:
		t.Fatalf("Unexpected additional status change: %v -> %v", change.prevStatus, change.newStatus)
	case <-time.After(time.Second):
		// No additional changes, as expected
	}
}
