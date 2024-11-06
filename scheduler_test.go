package pgscheduler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jonboulle/clockwork"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testDB *sqlx.DB
var testDBSQL *sql.DB
var dsn string

func TestMain(m *testing.M) {
	ctx := context.Background()

	fmt.Println("Starting PostgreSQL container...")
	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:13",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       "testdb",
				"POSTGRES_USER":     "testuser",
				"POSTGRES_PASSWORD": "testpass",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		fmt.Printf("Failed to start PostgreSQL container: %s\n", err)
		os.Exit(1)
	}
	defer postgresContainer.Terminate(ctx)

	host, err := postgresContainer.Host(ctx)
	if err != nil {
		fmt.Printf("Failed to get container host: %s\n", err)
		os.Exit(1)
	}
	port, err := postgresContainer.MappedPort(ctx, "5432")
	if err != nil {
		fmt.Printf("Failed to get container port: %s\n", err)
		os.Exit(1)
	}

	dsn = fmt.Sprintf("host=%s port=%s user=testuser password=testpass dbname=testdb sslmode=disable", host, port.Port())

	// Try to connect multiple times
	for i := 0; i < 5; i++ {
		testDBSQL, err = sql.Open("pgx", dsn)
		if err == nil {
			err = testDBSQL.Ping()
			if err == nil {
				break
			}
		}
		fmt.Printf("Failed to connect to database (attempt %d): %s\n", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		fmt.Printf("Failed to connect to database after all attempts: %s\n", err)
		os.Exit(1)
	}

	if testDBSQL == nil {
		fmt.Println("Failed to connect to database")
		os.Exit(1)
	}

	fmt.Println("Successfully connected to database")
	defer testDBSQL.Close()

	code := m.Run()
	os.Exit(code)
}

// TestJob implementation
type TestJob struct {
	name       string
	key        string
	calculator NextRunCalculator
	params     interface{}
	maxRetries int
	runFunc    func(context.Context) error
}

func NewTestRecurringJob(name string, cronSchedule string) (*TestJob, error) {
	calculator, err := NewCronSchedule(cronSchedule)
	if err != nil {
		return nil, err
	}
	return &TestJob{
		name:       name,
		key:        "test-key",
		calculator: calculator,
		maxRetries: 0,
	}, nil
}

func NewTestRecurringJobWithKey(name string, key string, cronSchedule string) (*TestJob, error) {
	calculator, err := NewCronSchedule(cronSchedule)
	if err != nil {
		return nil, err
	}
	return &TestJob{
		name:       name,
		key:        key,
		calculator: calculator,
		maxRetries: 0,
	}, nil
}

func NewTestOneTimeJob(name string, key string, params interface{}, maxRetries int) (*TestJob, error) {
	if maxRetries < 0 {
		return nil, fmt.Errorf("retries must be non-negative")
	}
	return &TestJob{
		name:       name,
		key:        key,
		params:     params,
		maxRetries: maxRetries,
	}, nil
}

func (j *TestJob) Name() string                         { return j.name }
func (j *TestJob) Key() string                          { return j.key }
func (j *TestJob) Parameters() interface{}              { return j.params }
func (j *TestJob) MaxRetries() int                      { return j.maxRetries }
func (j *TestJob) NextRunCalculator() NextRunCalculator { return j.calculator }
func (j *TestJob) Run(ctx context.Context) error {
	if j.runFunc != nil {
		return j.runFunc(ctx)
	}

	value := ctx.Value("clockwork")
	if value != nil {
		clock := value.(clockwork.FakeClock)
		clock.Advance(1 * time.Second)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		time.Sleep(1 * time.Second)
		return nil
	}
}
func setupTestScheduler(t *testing.T, schema string) (*Scheduler, clockwork.FakeClock) {
	testLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	fakeClock := clockwork.NewFakeClock()
	ctx := context.WithValue(context.Background(), "clockwork", fakeClock)
	config := SchedulerConfig{
		DB:                                   testDBSQL,
		DBDriverName:                         "postgres",
		MaxRunningJobs:                       10,
		JobCheckInterval:                     time.Second,
		HeartbeatInterval:                    time.Second,
		NoHeartbeatTimeout:                   3 * time.Second,
		OrphanedJobTimeout:                   14 * 24 * time.Hour,
		CreateSchema:                         true,
		RunImmediately:                       true,
		TablePrefix:                          "test_",
		ShutdownTimeout:                      5 * time.Second,
		Logger:                               testLogger,
		FailedAndCompletedJobCleanupInterval: time.Hour,
		CancelCheckPeriod:                    time.Second,
		Schema:                               schema,
		clock:                                fakeClock,
		Ctx:                                  ctx,
	}

	scheduler, err := NewScheduler(config)
	require.NoError(t, err)
	require.NotNil(t, scheduler)
	testDB = scheduler.db

	err = scheduler.Init()
	require.NoError(t, err)

	err = scheduler.Start()
	require.NoError(t, err)

	return scheduler, fakeClock
}

func cleanupTestDatabase(t *testing.T, scheduler *Scheduler) {
	scheduler.Shutdown()
	query := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, scheduler.config.Schema, scheduler.config.TablePrefix+scheduledJobsTableName)
	_, err := testDB.Exec(query)
	require.NoError(t, err)

	if scheduler.config.Schema != "public" {
		query = fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, scheduler.config.Schema)
		_, err = testDB.Exec(query)
		require.NoError(t, err)
	}
}

func TestNewScheduler(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	assert.Equal(t, `"public"."test_scheduled_jobs"`, scheduler.tableName)
}

func TestNewSchedulerWithCustomSchema(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "custom_schema")
	defer cleanupTestDatabase(t, scheduler)

	assert.Equal(t, `"custom_schema"."test_scheduled_jobs"`, scheduler.tableName)

	var schemaExists bool
	query := `SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`
	err := testDB.Get(&schemaExists, query, "custom_schema")
	require.NoError(t, err)
	assert.True(t, schemaExists, "Custom schema should exist")
}

func TestRegisterAndScheduleJob(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	job, err := NewTestRecurringJobWithKey("test_job", "key1", "* * * * *")
	require.NoError(t, err)

	// Try scheduling without registration first
	err = scheduler.ScheduleJob(job)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be registered before scheduling")

	// Register the job
	err = scheduler.RegisterJob(job)
	assert.NoError(t, err)

	// Schedule the job
	err = scheduler.ScheduleJob(job)
	assert.NoError(t, err)

	// Verify job was added to the database
	var record JobRecord
	query := fmt.Sprintf(`SELECT * FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	err = testDB.Get(&record, query, job.Name(), job.Key())
	assert.NoError(t, err)
	assert.Equal(t, job.Name(), record.Name)
	assert.Equal(t, job.Key(), record.Key)
	assert.Equal(t, epochStart, record.Heartbeat.Time.UTC())
	assert.False(t, record.Picked)
}

func TestScheduleDifferentJobKeys(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	job1, err := NewTestRecurringJobWithKey("test_job", "key1", "* * * * *")
	require.NoError(t, err)

	job2, err := NewTestRecurringJobWithKey("test_job", "key2", "* * * * *")
	require.NoError(t, err)

	// Register the job
	err = scheduler.RegisterJob(job1)
	assert.NoError(t, err)

	// Schedule both jobs
	err = scheduler.ScheduleJob(job1)
	assert.NoError(t, err)

	err = scheduler.ScheduleJob(job2)
	assert.NoError(t, err)

	// Verify both jobs were added to the database
	var count int
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE name = $1`, scheduler.tableName)
	err = testDB.Get(&count, query, job1.Name())
	assert.NoError(t, err)
	assert.Equal(t, 2, count, "Should have two jobs with different keys")
}

func TestRegisterDuplicateJob(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	job, err := NewTestRecurringJobWithKey("test_job", "key1", "* * * * *")
	require.NoError(t, err)

	// Register the job first time
	err = scheduler.RegisterJob(job)
	assert.NoError(t, err)

	// Try to register the same job again
	err = scheduler.RegisterJob(job)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestJobExecution(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	job, err := NewTestRecurringJobWithKey("test_job", "key1", "* * * * *")
	require.NoError(t, err)

	// Register and schedule the job
	err = scheduler.RegisterJob(job)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	// Advance clock to trigger job execution
	fakeClock.Advance(time.Minute)
	time.Sleep(2 * time.Second) // Allow for job processing

	// Verify job execution
	var record JobRecord
	query := fmt.Sprintf(`SELECT * FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	err = testDB.Get(&record, query, job.Name(), job.Key())
	require.NoError(t, err)

	assert.False(t, record.Picked)
	assert.True(t, record.LastSuccess.Valid)
	assert.True(t, record.ExecutionTime.Valid)
	assert.Greater(t, record.ExecutionTime.Int64, int64(0))
	assert.Equal(t, StatusPending, record.Status)
}
func TestRetryLogic(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Create a job with retry
	maxRetries := 2
	failingJob := &TestJob{
		name:       "retry_job",
		key:        "key1",
		maxRetries: maxRetries,
		calculator: nil,
		runFunc: func(ctx context.Context) error {
			return fmt.Errorf("simulated failure")
		},
	}

	// Register and schedule the job
	err := scheduler.RegisterJob(failingJob)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(failingJob)
	require.NoError(t, err)

	// Run and wait for all retries to complete
	fakeClock.Advance(time.Minute)

	// Wait for the job to reach final failed state with retries exhausted
	var record JobRecord
	query := fmt.Sprintf(`SELECT * FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)

	// Poll until the job reaches its final state or timeout
	deadline := time.Now().Add(10 * time.Second)
	var finalState bool
	for time.Now().Before(deadline) {
		err = testDB.Get(&record, query, failingJob.Name(), failingJob.Key())
		require.NoError(t, err)

		if record.Status == StatusFailed && record.Retries == 0 {
			finalState = true
			break
		}
		time.Sleep(100 * time.Millisecond)
		fakeClock.Advance(scheduler.config.JobCheckInterval)
	}

	// Verify the job eventually failed
	assert.True(t, finalState, "Job should have reached failed state with retries exhausted")
	assert.Equal(t, StatusFailed, record.Status)
	assert.Equal(t, 0, record.Retries)
	assert.True(t, record.LastFailure.Valid)
	assert.Greater(t, record.ConsecutiveFailures, 0)
	assert.False(t, record.Picked)
}

func TestJobCancellation(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	longRunningJob := &TestJob{
		name:       "long_job",
		key:        "key1",
		maxRetries: 0,
		runFunc: func(ctx context.Context) error {
			timer := time.NewTimer(15 * time.Second)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}

	// Register and schedule the job
	err := scheduler.RegisterJob(longRunningJob)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(longRunningJob)
	require.NoError(t, err)

	// Start the job
	fakeClock.Advance(time.Minute)
	time.Sleep(2 * time.Second) // Allow job to start
	fakeClock.Advance(time.Minute)
	time.Sleep(2 * time.Second) // Allow job to start
	fakeClock.Advance(time.Minute)
	time.Sleep(2 * time.Second) // Allow job to start

	// Verify job is running
	var record JobRecord
	query := fmt.Sprintf(`SELECT * FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	err = testDB.Get(&record, query, longRunningJob.Name(), longRunningJob.Key())
	require.NoError(t, err)
	assert.Equal(t, StatusRunning, record.Status)

	// error already running
	err = scheduler.ScheduleJob(longRunningJob)
	require.Equal(t, err, ErrJobRunning)

	// Cancel the job
	err = scheduler.CancelJob(longRunningJob.Name(), longRunningJob.Key())
	require.NoError(t, err)

	// Wait for cancellation to take effect
	time.Sleep(2 * time.Second)

	// Verify job was cancelled
	err = testDB.Get(&record, query, longRunningJob.Name(), longRunningJob.Key())
	require.NoError(t, err)
	assert.Equal(t, StatusFailed, record.Status)
	assert.False(t, record.Picked)
	assert.True(t, record.NextRun == nil)
	assert.False(t, record.CancelRequested)
}

func TestConcurrentJobProcessing(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Create multiple jobs
	jobCount := 5
	for i := 0; i < jobCount; i++ {
		job, err := NewTestRecurringJobWithKey(
			fmt.Sprintf("job_%d", i),
			fmt.Sprintf("key_%d", i),
			"* * * * *",
		)
		require.NoError(t, err)

		err = scheduler.RegisterJob(job)
		require.NoError(t, err)
		err = scheduler.ScheduleJob(job)
		require.NoError(t, err)
	}

	// Trigger job execution
	fakeClock.Advance(time.Minute)
	time.Sleep(3 * time.Second) // Allow for concurrent processing

	// Verify all jobs were executed
	query := fmt.Sprintf(`
        SELECT COUNT(*) 
        FROM %s 
        WHERE last_success IS NOT NULL`, scheduler.tableName)

	var completedCount int
	err := testDB.Get(&completedCount, query)
	require.NoError(t, err)
	assert.Equal(t, jobCount, completedCount)

	// Verify running jobs count is back to zero
	assert.Equal(t, int64(0), scheduler.getRunningJobsCount())
}

func TestCancelNonExistentJob(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	err := scheduler.CancelJob("non_existent", "key1")
	assert.Error(t, err)
	assert.Equal(t, ErrJobNotFound, err)
}

func TestCancelRunningJobFromDifferentNode(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	longRunningJob := &TestJob{
		name:       "long_job",
		key:        "key1",
		maxRetries: 0,
		runFunc: func(ctx context.Context) error {
			timer := time.NewTimer(15 * time.Second)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}

	// Register and schedule the job
	err := scheduler.RegisterJob(longRunningJob)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(longRunningJob)
	require.NoError(t, err)

	// Start the job
	fakeClock.Advance(time.Minute)
	time.Sleep(2 * time.Second)
	fakeClock.Advance(time.Minute)
	time.Sleep(2 * time.Second)

	// Manually update the job to appear as if running on a different node
	differentNodeID := uuid.New()
	query := fmt.Sprintf(`
        UPDATE %s 
        SET picked_by = $1 
        WHERE name = $2 AND key = $3`,
		scheduler.tableName)
	_, err = testDB.Exec(query, differentNodeID, longRunningJob.Name(), longRunningJob.Key())
	require.NoError(t, err)

	// Try to cancel the job
	err = scheduler.CancelJob(longRunningJob.Name(), longRunningJob.Key())
	require.NoError(t, err)

	fakeClock.Advance(time.Minute)
	time.Sleep(2 * time.Second)
	fakeClock.Advance(time.Minute)
	time.Sleep(2 * time.Second)

	// Verify cancel_requested was done
	var record JobRecord
	query = fmt.Sprintf(`SELECT * FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	err = testDB.Get(&record, query, longRunningJob.Name(), longRunningJob.Key())
	require.NoError(t, err)
	assert.Equal(t, StatusFailed, record.Status)
	assert.False(t, record.CancelRequested)
	assert.False(t, record.Picked)
}
func TestHeartbeatMonitoring(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	job, err := NewTestRecurringJobWithKey("test_job", "key1", "* * * * *")
	require.NoError(t, err)

	// Register and schedule the job
	err = scheduler.RegisterJob(job)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	query := fmt.Sprintf(`SELECT heartbeat FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	var heartbeat time.Time
	err = testDB.Get(&heartbeat, query, job.Name(), job.Key())
	require.NoError(t, err)
	assert.Equal(t, epochStart, heartbeat.UTC())

	// Start the job
	fakeClock.Advance(time.Minute)
	time.Sleep(2 * time.Second) // Allow job to start and update heartbeat

	err = testDB.Get(&heartbeat, query, job.Name(), job.Key())
	require.NoError(t, err)
	assert.True(t, heartbeat.After(epochStart))
}

func TestHeartbeatTimeout(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	job, err := NewTestRecurringJobWithKey("test_job", "key1", "* * * * *")
	require.NoError(t, err)

	err = scheduler.RegisterJob(job)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	// Manually mark the job as running with a stale heartbeat
	query := fmt.Sprintf(`
        UPDATE %s 
        SET picked = true, 
            picked_by = $1, 
            status = $2,
            heartbeat = $3 
        WHERE name = $4 AND key = $5`,
		scheduler.tableName)

	_, err = testDB.Exec(query,
		scheduler.nodeID,
		StatusRunning,
		fakeClock.Now().UTC(),
		job.Name(),
		job.Key())
	require.NoError(t, err)

	// Advance clock beyond heartbeat timeout
	fakeClock.Advance(scheduler.config.NoHeartbeatTimeout + time.Second)
	time.Sleep(2 * time.Second) // Allow monitor to process

	// Verify job was reset
	var record JobRecord
	query = fmt.Sprintf(`SELECT * FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	err = testDB.Get(&record, query, job.Name(), job.Key())
	require.NoError(t, err)

	assert.False(t, record.Picked)
	assert.Equal(t, StatusPending, record.Status)
}

func TestFailedAndCompletedJobCleanup(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Create and schedule a one-time job
	job, err := NewTestOneTimeJob("cleanup_job", "key1", nil, 0)
	require.NoError(t, err)

	err = scheduler.RegisterJob(job)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	fakeClock.Advance(time.Minute)
	time.Sleep(3 * time.Second)

	// Mark the job as failed
	query := fmt.Sprintf(`
        UPDATE %s 
        SET status = $1,
            last_run = $2,
            next_run = NULL
        WHERE name = $3 AND key = $4`,
		scheduler.tableName)

	_, err = testDB.Exec(query,
		StatusFailed,
		fakeClock.Now().UTC(),
		job.Name(),
		job.Key())
	require.NoError(t, err)

	fakeClock.Advance(scheduler.config.FailedAndCompletedJobCleanupInterval + time.Second)
	time.Sleep(3 * time.Second) // Allow cleanup to process

	// Verify job was cleaned up
	var count int
	query = fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE name = $1 AND key = $2`, scheduler.tableName)
	err = testDB.Get(&count, query, job.Name(), job.Key())
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestOrphanedJobCleanup(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Create an unregistered job directly in the database
	query := fmt.Sprintf(`
        INSERT INTO %s (name, key, heartbeat, picked, status, next_run)
        VALUES ($1, $2, $3, false, $4, $5)`,
		scheduler.tableName)

	_, err := testDB.Exec(query,
		"orphaned_job",
		"orphaned_key",
		epochStart,
		StatusPending,
		fakeClock.Now().UTC())
	require.NoError(t, err)

	// Create and register a valid job
	job, err := NewTestRecurringJobWithKey("valid_job", "key1", "* * * * *")
	require.NoError(t, err)
	err = scheduler.RegisterJob(job)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	// Advance clock beyond orphaned timeout
	fakeClock.Advance(scheduler.config.OrphanedJobTimeout + time.Second)
	time.Sleep(2 * time.Second) // Allow cleanup to process

	// Verify orphaned job was cleaned up but valid job remains
	var count int
	query = fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE name = $1`, scheduler.tableName)

	err = testDB.Get(&count, query, "orphaned_job")
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Orphaned job should be cleaned up")

	err = testDB.Get(&count, query, "valid_job")
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Valid job should remain")
}

func TestSchedulerShutdown(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")

	// Create and register multiple jobs
	for i := 0; i < 3; i++ {
		job, err := NewTestRecurringJobWithKey(
			fmt.Sprintf("job_%d", i),
			fmt.Sprintf("key_%d", i),
			"* * * * *",
		)
		require.NoError(t, err)
		err = scheduler.RegisterJob(job)
		require.NoError(t, err)
		err = scheduler.ScheduleJob(job)
		require.NoError(t, err)
	}

	// Start jobs
	fakeClock.Advance(time.Minute)
	time.Sleep(1 * time.Second) // Allow jobs to start

	// Initiate shutdown
	done := make(chan struct{})
	go func() {
		cleanupTestDatabase(t, scheduler)
		close(done)
	}()

	// Wait for shutdown or timeout
	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Scheduler shutdown timed out")
	}

	// Verify all jobs are stopped
	assert.Equal(t, int64(0), scheduler.getRunningJobsCount())

	// Verify job registry is empty
	scheduler.jobRegistryRWLock.RLock()
	count := len(scheduler.jobRegistry)
	scheduler.jobRegistryRWLock.RUnlock()
	assert.Equal(t, 0, count)
}

func TestSchedulerMaxRunningJobs(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	scheduler.config.MaxRunningJobs = 2 // Set low limit for testing
	defer cleanupTestDatabase(t, scheduler)

	// Create more jobs than the max limit
	for i := 0; i < 4; i++ {
		job := &TestJob{
			name:       fmt.Sprintf("job_%d", i),
			key:        fmt.Sprintf("key_%d", i),
			maxRetries: 0,
			runFunc: func(ctx context.Context) error {
				time.Sleep(2 * time.Second) // Make jobs run long enough to test concurrency
				return nil
			},
		}

		err := scheduler.RegisterJob(job)
		require.NoError(t, err)
		err = scheduler.ScheduleJob(job)
		require.NoError(t, err)
	}

	// Trigger job execution
	fakeClock.Advance(time.Minute)
	time.Sleep(1 * time.Second) // Allow initial jobs to start

	// Verify only MaxRunningJobs are running
	assert.Equal(t, int64(2), scheduler.getRunningJobsCount())

	// Wait for first batch to complete and verify second batch starts
	fakeClock.Advance(time.Minute)
	time.Sleep(3 * time.Second)
	assert.LessOrEqual(t, scheduler.getRunningJobsCount(), int64(2))

	// Wait for all jobs to complete
	fakeClock.Advance(time.Minute)
	time.Sleep(3 * time.Second)
	assert.Equal(t, int64(0), scheduler.getRunningJobsCount())

	// Verify all jobs eventually completed
	var completedCount int
	query := fmt.Sprintf(`
        SELECT COUNT(*) 
        FROM %s 
        WHERE last_success IS NOT NULL`,
		scheduler.tableName)

	err := testDB.Get(&completedCount, query)
	require.NoError(t, err)
	assert.Equal(t, 4, completedCount)
}
