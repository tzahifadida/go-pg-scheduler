package pgscheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jonboulle/clockwork"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testDB *sqlx.DB
var testDBSQL *sql.DB

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

	fmt.Println("Container started, getting host and port...")
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

	fmt.Printf("Container is running on %s:%s\n", host, port.Port())

	dsn := fmt.Sprintf("host=%s port=%s user=testuser password=testpass dbname=testdb sslmode=disable", host, port.Port())
	fmt.Printf("Connecting to database with DSN: %s\n", dsn)

	// Try to connect multiple times
	for i := 0; i < 5; i++ {
		testDBSQL, err = sql.Open("postgres", dsn)
		if err == nil {
			err = testDBSQL.Ping()
			if err == nil {
				break
			}
		}
		fmt.Printf("Failed to connect to database (attempt %d): %s\n", i+1, err)
		time.Sleep(2 * time.Second)
	}
	err = testDBSQL.Ping()
	if err != nil {
		fmt.Println("Failed to ping the database")
		os.Exit(1)
	}

	if testDBSQL == nil {
		fmt.Println("Failed to connect to database after multiple attempts")
		os.Exit(1)
	}

	fmt.Println("Successfully connected to database")
	defer testDBSQL.Close()

	code := m.Run()
	os.Exit(code)
}

func setupTestScheduler(t *testing.T, schema string) (*Scheduler, clockwork.FakeClock) {
	testLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	fakeClock := clockwork.NewFakeClock()
	ctx := context.WithValue(context.Background(), "clockwork", fakeClock)
	config := SchedulerConfig{
		DB:                                   testDBSQL,
		DBDriverName:                         "postgres",
		MaxConcurrentOneTimeJobs:             5,
		MaxConcurrentRecurringJobs:           5,
		OrphanedJobTimeout:                   14 * 24 * time.Hour,
		JobCheckInterval:                     time.Second,
		HeartbeatInterval:                    time.Second,
		NoHeartbeatTimeout:                   3 * time.Second,
		RunImmediately:                       true,
		CreateSchema:                         true,
		TablePrefix:                          "test_",
		ShutdownTimeout:                      5 * time.Second,
		Logger:                               testLogger,
		FailedAndCompletedOneTimeJobInterval: time.Hour,
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

	scheduler.Start()
	require.NoError(t, err)

	return scheduler, fakeClock
}

func cleanupTestDatabase(t *testing.T, scheduler *Scheduler) {
	scheduler.Shutdown()
	query := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	_, err := testDB.Exec(query)
	require.NoError(t, err)

	if scheduler.config.Schema != "public" {
		query = fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, scheduler.config.Schema)
		_, err = testDB.Exec(query)
		require.NoError(t, err)
	}
}

type TestJob struct {
	JobConfig
}

func (j *TestJob) Init(ctx context.Context) error { return nil }
func (j *TestJob) Run(ctx context.Context) error {
	value := ctx.Value("clockwork")
	if value != nil {
		clock := value.(clockwork.FakeClock)
		clock.Advance(1 * time.Second)
	}
	time.Sleep(1 * time.Second)
	return nil
}
func (j *TestJob) Shutdown(ctx context.Context) error { return nil }

func TestNewScheduler(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	assert.Equal(t, `"public"."test_scheduled_jobs"`, scheduler.tableName)
}

func TestNewSchedulerWithCustomSchema(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "custom_schema")
	defer cleanupTestDatabase(t, scheduler)

	assert.Equal(t, `"custom_schema"."test_scheduled_jobs"`, scheduler.tableName)

	// Verify that the custom schema was created
	var schemaExists bool
	query := `SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`
	err := testDB.Get(&schemaExists, query, "custom_schema")
	require.NoError(t, err)
	assert.True(t, schemaExists, "Custom schema should exist")
}

func TestScheduleJob(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	jobConfig, err := NewRecurringJobConfig("test_job", "* * * * *")
	require.NoError(t, err)

	job := &TestJob{JobConfig: *jobConfig}

	err = scheduler.ScheduleJob(job)
	assert.NoError(t, err)

	// Verify job was added to the database
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s" WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var count int
	err = testDB.Get(&count, query, job.Name())
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestScheduleJobWithCustomSchema(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "custom_schema")
	defer cleanupTestDatabase(t, scheduler)

	jobConfig, err := NewRecurringJobConfig("test_job", "* * * * *")
	require.NoError(t, err)

	job := &TestJob{JobConfig: *jobConfig}

	err = scheduler.ScheduleJob(job)
	assert.NoError(t, err)

	// Verify job was added to the database in the custom schema
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s" WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var count int
	err = testDB.Get(&count, query, job.Name())
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestAcquireLockAndRun(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	job1Config, err := NewRecurringJobConfig("test_job_1", "* * * * *")
	require.NoError(t, err)
	job1 := &TestJob{JobConfig: *job1Config}

	job2Config, err := NewRecurringJobConfig("test_job_2", "* * * * *")
	require.NoError(t, err)
	job2 := &TestJob{JobConfig: *job2Config}

	err = scheduler.ScheduleJob(job1)
	require.NoError(t, err)

	err = scheduler.ScheduleJob(job2)
	require.NoError(t, err)

	// Register the jobs
	jobRegistry[job1.Name()] = job1
	jobRegistry[job2.Name()] = job2

	// Advance clock to trigger jobs
	fakeClock.Advance(time.Minute)

	// Run acquireLockAndRun
	err = scheduler.acquireLockAndRun(JobTypeRecurring, 2)
	assert.NoError(t, err)
	// Verify jobs were picked
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s" WHERE "picked" = true OR last_success is not null`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var pickedCount int
	err = testDB.Get(&pickedCount, query)
	assert.NoError(t, err)
	assert.Equal(t, 2, pickedCount)
}

func TestCheckAndResetTimedOutJobs(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Insert a job that has timed out
	query := fmt.Sprintf(`
		INSERT INTO "%s"."%s" ("key", "name", "picked", "picked_by", "heartbeat", "job_type", "retries", "status", "next_run")
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	_, err := testDB.Exec(query, "test_job_key", "test_job", true, scheduler.nodeID, fakeClock.Now().UTC().Add(-5*time.Minute), JobTypeOneTime, 2, statusRunning, fakeClock.Now().UTC())
	require.NoError(t, err)

	// Advance clock to trigger timeout
	fakeClock.Advance(scheduler.config.NoHeartbeatTimeout + time.Second)

	err = scheduler.checkAndResetTimedOutJobs()
	assert.NoError(t, err)

	// Verify job was reset
	query = fmt.Sprintf(`SELECT "picked", "status", "retries" FROM "%s"."%s" WHERE "key" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var job jobRecord
	err = testDB.Get(&job, query, "test_job_key")
	assert.NoError(t, err)
	assert.False(t, job.Picked)
	assert.Equal(t, statusPending, job.Status)
	assert.Equal(t, 1, job.Retries)
}

func TestCalculateNextRun(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Set the fake clock to a specific time
	fakeNow := time.Date(2023, 5, 1, 10, 0, 0, 0, time.UTC)
	scheduler.clock = clockwork.NewFakeClockAt(fakeNow)

	// Test for immediate next minute
	nextRun, err := scheduler.calculateNextRun("* * * * *")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 5, 1, 10, 1, 0, 0, time.UTC), nextRun)

	// Test for next 5 minutes
	nextRun, err = scheduler.calculateNextRun("*/5 * * * *")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 5, 1, 10, 5, 0, 0, time.UTC), nextRun)

	// Test for specific time today
	nextRun, err = scheduler.calculateNextRun("30 15 * * *")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 5, 1, 15, 30, 0, 0, time.UTC), nextRun)

	// Test for specific time tomorrow
	nextRun, err = scheduler.calculateNextRun("30 9 * * *")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2023, 5, 2, 9, 30, 0, 0, time.UTC), nextRun)

	_, err = scheduler.calculateNextRun("invalid cron")
	assert.Error(t, err)
}

func TestNewRecurringJobConfig(t *testing.T) {
	config, err := NewRecurringJobConfig("test_job", "*/5 * * * *")
	assert.NoError(t, err)
	assert.Equal(t, "test_job", config.Name())
	assert.Equal(t, "*/5 * * * *", config.CronSchedule())
	assert.Equal(t, JobTypeRecurring, config.JobType())
}

func TestNewOneTimeJobConfig(t *testing.T) {
	params := map[string]string{"key": "value"}
	config, err := NewOneTimeJobConfig("test_job", params, 5)
	assert.NoError(t, err)
	assert.Equal(t, "test_job", config.Name())
	assert.Empty(t, config.CronSchedule())
	assert.Equal(t, JobTypeOneTime, config.JobType())
	assert.Equal(t, params, config.Parameters())
	assert.Equal(t, 5, config.Retries())

	_, err = NewOneTimeJobConfig("test_job", params, -1)
	assert.Error(t, err)
}

func TestGetJobKey(t *testing.T) {
	recurringJobConfig, err := NewRecurringJobConfig("recurring_job", "* * * * *")
	require.NoError(t, err)
	recurringJob := &TestJob{JobConfig: *recurringJobConfig}

	key, err := getJobKey(recurringJob)
	assert.NoError(t, err)
	assert.Equal(t, "recurring_job", key)

	oneTimeJobConfig, err := NewOneTimeJobConfig("one_time_job", nil, 3)
	require.NoError(t, err)
	oneTimeJob := &TestJob{JobConfig: *oneTimeJobConfig}

	key, err = getJobKey(oneTimeJob)
	assert.NoError(t, err)
	assert.Contains(t, key, "one_time_job#")
	assert.Len(t, key, len("one_time_job")+1+36) // name + # + UUID
}

func TestRunJob(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	jobConfig, err := NewRecurringJobConfig("test_run_job", "* * * * *")
	require.NoError(t, err)
	job := &TestJob{JobConfig: *jobConfig}

	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	query := fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var jobRecord jobRecord
	err = testDB.Get(&jobRecord, query, job.Name())
	require.NoError(t, err)

	scheduler.runJob(jobRecord.Key, job, jobRecord.Parameters, jobRecord.Retries)
	fakeClock.Advance(10 * time.Second)
	time.Sleep(3 * time.Second)
	// Verify job status after run
	err = testDB.Get(&jobRecord, query, job.Name())
	assert.NoError(t, err)
	assert.Equal(t, statusPending, jobRecord.Status)
	assert.False(t, jobRecord.Picked)
	assert.True(t, jobRecord.NextRun.After(fakeClock.Now().UTC()))
	assert.True(t, jobRecord.ExecutionTime.Valid)
	assert.Greater(t, jobRecord.ExecutionTime.Int64, int64(0))
	assert.True(t, jobRecord.LastSuccess.Valid)
}

func TestMarkJobFailed(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Test recurring job
	recurringJobConfig, err := NewRecurringJobConfig("test_recurring_job", "* * * * *")
	require.NoError(t, err)
	recurringJob := &TestJob{JobConfig: *recurringJobConfig}

	err = scheduler.ScheduleJob(recurringJob)
	require.NoError(t, err)

	query := fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var jobRecord jobRecord
	err = testDB.Get(&jobRecord, query, recurringJob.Name())
	require.NoError(t, err)

	scheduler.markJobFailed(jobRecord.Key, recurringJob, 0, time.Second)

	err = testDB.Get(&jobRecord, query, recurringJob.Name())
	assert.NoError(t, err)
	assert.Equal(t, statusFailed, jobRecord.Status)
	assert.True(t, jobRecord.ExecutionTime.Valid)
	assert.Equal(t, int64(1000), jobRecord.ExecutionTime.Int64) // 1 second in milliseconds
	assert.True(t, jobRecord.LastFailure.Valid)
	assert.Equal(t,
		fakeClock.Now().UTC().Truncate(time.Millisecond),
		jobRecord.LastFailure.Time.UTC().Truncate(time.Millisecond))
	assert.Equal(t, 1, jobRecord.ConsecutiveFailures)

	// Test one-time job with retries
	oneTimeJobConfig, err := NewOneTimeJobConfig("test_one_time_job", nil, 2)
	require.NoError(t, err)
	oneTimeJob := &TestJob{JobConfig: *oneTimeJobConfig}

	err = scheduler.ScheduleJob(oneTimeJob)
	require.NoError(t, err)

	err = testDB.Get(&jobRecord, query, oneTimeJob.Name())
	require.NoError(t, err)

	scheduler.markJobFailed(jobRecord.Key, oneTimeJob, jobRecord.Retries, time.Second)

	err = testDB.Get(&jobRecord, query, oneTimeJob.Name())
	assert.NoError(t, err)
	assert.Equal(t, statusPending, jobRecord.Status)
	assert.Equal(t, 1, jobRecord.Retries)
	assert.True(t, jobRecord.ExecutionTime.Valid)
	assert.Equal(t, int64(1000), jobRecord.ExecutionTime.Int64)
	assert.True(t, jobRecord.LastFailure.Valid)
	assert.Equal(t,
		fakeClock.Now().UTC().Truncate(time.Millisecond),
		jobRecord.LastFailure.Time.UTC().Truncate(time.Millisecond))
	assert.Equal(t, 1, jobRecord.ConsecutiveFailures)
}

func TestRemoveJob(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	jobConfig, err := NewOneTimeJobConfig("test_remove_job", nil, 0)
	require.NoError(t, err)
	job := &TestJob{JobConfig: *jobConfig}

	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	query := fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var jobRecord jobRecord
	err = testDB.Get(&jobRecord, query, job.Name())
	require.NoError(t, err)

	err = scheduler.removeJob(jobRecord.Key)
	assert.NoError(t, err)

	query = fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s" WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var count int
	err = testDB.Get(&count, query, job.Name())
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestUpdateHeartbeat(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	jobConfig, err := NewRecurringJobConfig("test_heartbeat_job", "* * * * *")
	require.NoError(t, err)
	job := &TestJob{JobConfig: *jobConfig}

	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)
	fakeClock.Advance(2 * time.Minute)

	_, err = testDB.Exec(fmt.Sprintf(`UPDATE "%s"."%s" SET "picked_by" = $2 WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs"), job.Name(), scheduler.nodeID)
	require.NoError(t, err)

	query := fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var jobRecord jobRecord
	err = testDB.Get(&jobRecord, query, job.Name())
	require.NoError(t, err)

	stop := make(chan struct{})
	go scheduler.updateHeartbeat(jobRecord.Key, stop)
	time.Sleep(2 * time.Second) //wait for the ticker to register.

	// Simulate time passing
	fakeClock.Advance(1 * time.Second)
	time.Sleep(time.Second) //yield just in case.
	close(stop)

	err = testDB.Get(&jobRecord, query, job.Name())
	assert.NoError(t, err)
	assert.True(t, jobRecord.Heartbeat.Valid)
	assert.True(t, jobRecord.Heartbeat.Time.After(fakeClock.Now().UTC().Add(-3*time.Second)))
}

func TestShutdown(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	// Verify job registry is empty
	defer func() {
		jobRegistryRWLock.RLock()
		assert.Len(t, jobRegistry, 0)
		jobRegistryRWLock.RUnlock()
	}()
	defer cleanupTestDatabase(t, scheduler) //shutdown is in cleanup.

	// Schedule a job to ensure there's something in the registry
	jobConfig, err := NewRecurringJobConfig("test_shutdown_job", "* * * * *")
	require.NoError(t, err)
	job := &TestJob{JobConfig: *jobConfig}
	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	// Verify job is in the registry
	jobRegistryRWLock.RLock()
	assert.Len(t, jobRegistry, 1)
	jobRegistryRWLock.RUnlock()
}

func TestFailedOneTimeJobCleaner(t *testing.T) {
	scheduler, _ := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Add a failed one-time job
	jobConfig, err := NewOneTimeJobConfig("test_failed_job", nil, 0)
	require.NoError(t, err)
	job := &TestJob{JobConfig: *jobConfig}

	err = scheduler.ScheduleJob(job)
	require.NoError(t, err)

	// Mark the job as failed
	query := fmt.Sprintf(`UPDATE "%s"."%s" SET "status" = $1 WHERE "name" = $2`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	_, err = testDB.Exec(query, statusFailed, job.Name())
	require.NoError(t, err)

	// Run the cleaner
	err = scheduler.cleanFailedAndCompletedOneTimeJobs()
	require.NoError(t, err)

	// Verify the job was deleted
	query = fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s" WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var count int
	err = testDB.Get(&count, query, job.Name())
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestProcessJobs(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Add a mix of recurring and one-time jobs
	recurringJob, err := NewRecurringJobConfig("test_recurring_job", "* * * * *")
	require.NoError(t, err)
	err = scheduler.ScheduleJob(&TestJob{JobConfig: *recurringJob})
	require.NoError(t, err)

	oneTimeJob, err := NewOneTimeJobConfig("test_one_time_job", nil, 1)
	require.NoError(t, err)
	err = scheduler.ScheduleJob(&TestJob{JobConfig: *oneTimeJob})
	require.NoError(t, err)

	// Register the jobs
	jobRegistry[recurringJob.Name()] = &TestJob{JobConfig: *recurringJob}
	jobRegistry[oneTimeJob.Name()] = &TestJob{JobConfig: *oneTimeJob}

	// Advance clock to trigger jobs
	fakeClock.Advance(time.Minute)

	// Process jobs
	err = scheduler.processJobs()
	assert.NoError(t, err)

	// Verify jobs were processed
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s" WHERE "picked" = true or last_success is not null`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	var pickedCount int
	err = testDB.Get(&pickedCount, query)
	assert.NoError(t, err)
	assert.Equal(t, 2, pickedCount)
}

func TestOrphanedJobMonitor(t *testing.T) {
	scheduler, fakeClock := setupTestScheduler(t, "public")
	defer cleanupTestDatabase(t, scheduler)

	// Override the OrphanedJobTimeout for testing
	scheduler.config.OrphanedJobTimeout = 1 * time.Hour

	// Create an orphaned job (not in the job registry)
	orphanedJobKey := "orphaned_job"
	query := fmt.Sprintf(`
                INSERT INTO "%s"."%s" ("key", "name", "picked", "heartbeat", "job_type", "status", "next_run")
                VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	_, err := testDB.Exec(query, orphanedJobKey, "orphaned_job", false, fakeClock.Now().UTC().Add(-2*time.Hour), JobTypeRecurring, statusPending, fakeClock.Now().UTC())
	require.NoError(t, err)

	// Create a non-orphaned job (in the job registry)
	nonOrphanedJobConfig, err := NewRecurringJobConfig("non_orphaned_job", "* * * * *")
	require.NoError(t, err)
	nonOrphanedJob := &TestJob{JobConfig: *nonOrphanedJobConfig}
	err = scheduler.ScheduleJob(nonOrphanedJob)
	require.NoError(t, err)

	// Register the non-orphaned job
	jobRegistry[nonOrphanedJob.Name()] = nonOrphanedJob

	// Advance clock to trigger orphaned job cleanup
	fakeClock.Advance(2 * time.Hour)

	// Run the orphaned job cleaner
	err = scheduler.cleanOrphanedJobs()
	assert.NoError(t, err)

	// Verify the orphaned job was deleted
	var orphanedJobCount int
	query = fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s" WHERE "key" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	err = testDB.Get(&orphanedJobCount, query, orphanedJobKey)
	assert.NoError(t, err)
	assert.Equal(t, 0, orphanedJobCount, "Orphaned job should have been deleted")

	// Verify the non-orphaned job still exists
	var nonOrphanedJobCount int
	query = fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s" WHERE "name" = $1`, scheduler.config.Schema, scheduler.config.TablePrefix+"scheduled_jobs")
	err = testDB.Get(&nonOrphanedJobCount, query, nonOrphanedJob.Name())
	assert.NoError(t, err)
	assert.Equal(t, 1, nonOrphanedJobCount, "Non-orphaned job should still exist")
}
