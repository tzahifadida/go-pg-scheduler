// Package pgscheduler provides a PostgreSQL-based job scheduler for Go applications.
// It allows for scheduling and managing both recurring and one-time jobs with
// configurable concurrency, timeouts, and retry mechanisms.
package pgscheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/jonboulle/clockwork"
	"github.com/robfig/cron/v3"
)

var jobRegistry = make(map[string]Job)
var jobRegistryRWLock sync.RWMutex

// JobType represents the type of a job (Recurring or OneTime).
type JobType int

// ClusterSchedulerParametersKey is the context key for storing job parameters.
const ClusterSchedulerParametersKey = "ClusterSchedulerParametersKey"

const (
	JobTypeRecurring JobType = iota
	JobTypeOneTime

	jobTypeLast // This should always be the last constant
)

type status string

const (
	statusPending   status = "pending"
	statusRunning   status = "running"
	statusCompleted status = "completed"
	statusFailed    status = "failed"
)

// JobRunnerI defines the interface for job execution.
type JobRunnerI interface {
	// Run executes the job with the given context.
	// It returns an error if the job execution fails.
	Run(ctx context.Context) error
}

// JobConfigI defines the interface for job configuration.
type JobConfigI interface {
	// Name returns the name of the job.
	Name() string

	// CronSchedule returns the cron schedule string for recurring jobs.
	// For one-time jobs, this will be an empty string.
	CronSchedule() string

	// JobType returns the type of the job (Recurring or OneTime).
	JobType() JobType

	// Parameters returns the parameters associated with the job.
	Parameters() interface{}

	// Retries returns the number of retries allowed for the job (relevant to one time jobs).
	Retries() int
}

// Job is an interface that combines JobConfigI and JobRunnerI.
type Job interface {
	JobConfigI
	JobRunnerI
}

// JobConfig represents the configuration for a job.
type JobConfig struct {
	name         string
	cronSchedule string
	jobType      JobType
	parameters   interface{}
	retries      int
}

func (jc *JobConfig) Name() string            { return jc.name }
func (jc *JobConfig) CronSchedule() string    { return jc.cronSchedule }
func (jc *JobConfig) JobType() JobType        { return jc.jobType }
func (jc *JobConfig) Parameters() interface{} { return jc.parameters }
func (jc *JobConfig) Retries() int            { return jc.retries }

// NewRecurringJobConfig creates a new JobConfig for a recurring job.
//
// Parameters:
//   - name: A string representing the name of the job. Essentially an identifier for the Job code.
//   - cronSchedule: A string representing the cron schedule for the job.
//
// Returns:
//   - *JobConfig: A pointer to the created JobConfig.
//   - error: An error if the cron schedule is invalid or the name is empty.
func NewRecurringJobConfig(name string, cronSchedule string) (*JobConfig, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	_, err := parser.Parse(cronSchedule)
	if err != nil {
		return nil, fmt.Errorf("invalid cron schedule: %w", err)
	}
	if len(name) == 0 {
		return nil, fmt.Errorf("name cannot be empty")
	}

	return &JobConfig{
		name:         name,
		cronSchedule: cronSchedule,
		jobType:      JobTypeRecurring,
		parameters:   nil,
		retries:      0,
	}, nil
}

// NewOneTimeJobConfig creates a new JobConfig for a one-time job.
//
// Parameters:
//   - name: A string representing the name of the job.
//   - parameters: An interface{} containing any parameters needed for the job.
//   - retries: An int representing the number of retries allowed for the job.
//
// Returns:
//   - *JobConfig: A pointer to the created JobConfig.
//   - error: An error if the retries are negative or the name is empty.
func NewOneTimeJobConfig(name string, parameters interface{}, retries int) (*JobConfig, error) {
	if retries < 0 {
		return nil, fmt.Errorf("retries must be non-negative")
	}
	if len(name) == 0 {
		return nil, fmt.Errorf("name cannot be empty")
	}
	return &JobConfig{
		name:         name,
		cronSchedule: "",
		jobType:      JobTypeOneTime,
		parameters:   parameters,
		retries:      retries,
	}, nil
}

func getJobKey(j Job) (string, error) {
	if j.JobType() == JobTypeOneTime {
		random, err := uuid.NewRandom()
		if err != nil {
			return "", fmt.Errorf("failed to generate job key: %w", err)
		}
		return fmt.Sprintf("%s#%s", j.Name(), random.String()), nil
	}
	return j.Name(), nil
}

// Logger defines the interface for logging operations.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// SchedulerConfig represents the configuration for the Scheduler.
type SchedulerConfig struct {
	Ctx                                  context.Context
	DB                                   *sql.DB
	DBDriverName                         string
	MaxConcurrentOneTimeJobs             int
	MaxConcurrentRecurringJobs           int
	JobCheckInterval                     time.Duration
	OrphanedJobTimeout                   time.Duration
	HeartbeatInterval                    time.Duration
	NoHeartbeatTimeout                   time.Duration
	CreateSchema                         bool
	Logger                               Logger
	RunImmediately                       bool
	TablePrefix                          string
	ShutdownTimeout                      time.Duration
	FailedAndCompletedOneTimeJobInterval time.Duration
	clock                                clockwork.Clock
}

// Scheduler manages the execution of jobs.
type Scheduler struct {
	initialized  bool
	started      bool
	config       SchedulerConfig
	db           *sqlx.DB
	nodeID       uuid.UUID
	shutdownChan chan struct{}
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	tableName    string
	runningJobs  sync.Map
	clock        clockwork.Clock
}

type jobRecord struct {
	Key                 string          `db:"key"`
	Name                string          `db:"name"`
	LastRun             sql.NullTime    `db:"last_run"`
	Picked              bool            `db:"picked"`
	PickedBy            uuid.NullUUID   `db:"picked_by"`
	Heartbeat           sql.NullTime    `db:"heartbeat"`
	NextRun             time.Time       `db:"next_run"`
	JobType             JobType         `db:"job_type"`
	Parameters          json.RawMessage `db:"parameters"`
	Status              status          `db:"status"`
	Retries             int             `db:"retries"`
	ExecutionTime       sql.NullInt64   `db:"execution_time"`
	LastSuccess         sql.NullTime    `db:"last_success"`
	LastFailure         sql.NullTime    `db:"last_failure"`
	ConsecutiveFailures int             `db:"consecutive_failures"`
}

// NewScheduler creates a new Scheduler instance with the given configuration.
//
// Parameters:
//   - config: A SchedulerConfig struct containing the configuration for the scheduler.
//
// Returns:
//   - *Scheduler: A pointer to the created Scheduler.
//   - error: An error if the configuration is invalid or initialization fails.
func NewScheduler(config SchedulerConfig) (*Scheduler, error) {
	if config.MaxConcurrentRecurringJobs < 0 {
		return nil, fmt.Errorf("MaxConcurrentRecurringJobs must be at least 1")
	}

	if config.MaxConcurrentOneTimeJobs < 0 {
		return nil, fmt.Errorf("MaxConcurrentOneTimeJobs must be at least 1")
	}

	if config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	if config.DBDriverName == "" {
		return nil, fmt.Errorf("database driver name is required")
	}

	// Wrap the *sql.DB with sqlx
	sqlxDB := sqlx.NewDb(config.DB, config.DBDriverName)

	if config.MaxConcurrentOneTimeJobs == 0 {
		config.MaxConcurrentOneTimeJobs = 2
	}
	if config.MaxConcurrentRecurringJobs == 0 {
		config.MaxConcurrentRecurringJobs = 2
	}

	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	if config.clock == nil {
		config.clock = clockwork.NewRealClock()
	}

	if config.JobCheckInterval == 0 {
		config.JobCheckInterval = 1 * time.Minute
	}

	if config.OrphanedJobTimeout == 0 {
		config.OrphanedJobTimeout = 14 * 24 * time.Hour
	}

	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 30 * time.Second
	}

	if config.NoHeartbeatTimeout == 0 {
		config.NoHeartbeatTimeout = 5 * time.Minute
	}

	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = 30 * time.Second
	}

	if config.FailedAndCompletedOneTimeJobInterval == 0 {
		config.FailedAndCompletedOneTimeJobInterval = 1 * time.Hour
	}

	nodeID := uuid.New()
	ctx := context.Background()
	if config.Ctx != nil {
		ctx = config.Ctx
	}
	ctx, cancel := context.WithCancel(ctx)

	return &Scheduler{
		config:       config,
		db:           sqlxDB,
		nodeID:       nodeID,
		shutdownChan: make(chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
		tableName:    config.TablePrefix + "scheduled_jobs",
		runningJobs:  sync.Map{},
		clock:        config.clock,
	}, nil
}

func (s *Scheduler) createSchemaIfMissing() error {
	createTable := "CREATE TABLE IF NOT EXISTS"
	schema := fmt.Sprintf(createTable+` %s (
        "key" TEXT PRIMARY KEY,
        "name" TEXT,
        "last_run" TIMESTAMP,
        "picked" BOOLEAN,
        "picked_by" UUID,
        "heartbeat" TIMESTAMP,
        "next_run" TIMESTAMP,
        "job_type" SMALLINT DEFAULT 0,
        "parameters" JSONB,
        "status" TEXT DEFAULT '%s',
        "retries" INT DEFAULT 0,
        "execution_time" INT DEFAULT 0,
        "last_success" TIMESTAMP,
        "last_failure" TIMESTAMP,
        "consecutive_failures" INT DEFAULT 0
    );

    CREATE INDEX IF NOT EXISTS idx_%s_job_acquisition ON %s("job_type", "picked", "next_run");
    CREATE INDEX IF NOT EXISTS idx_%s_heartbeat_monitor ON %s("picked", "heartbeat");
    CREATE INDEX IF NOT EXISTS idx_%s_cleanup ON %s("last_run");
    `, s.tableName, statusPending, s.tableName, s.tableName, s.tableName, s.tableName, s.tableName, s.tableName)

	_, err := s.config.DB.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

// Init initializes the Scheduler, creating the necessary database schema if required.
//
// Returns:
//   - error: An error if initialization fails.
func (s *Scheduler) Init() error {
	if s.initialized {
		return fmt.Errorf("scheduler is already initialized")
	}
	s.initialized = true
	if s.config.CreateSchema {
		if err := s.createSchemaIfMissing(); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	return nil
}

// Start begins the job processing routines of the Scheduler.
//
// Returns:
//   - error: An error if starting the scheduler fails.
func (s *Scheduler) Start() error {
	if !s.initialized {
		return fmt.Errorf("scheduler was not initialized")
	}
	s.started = true

	s.wg.Add(4)
	go s.startJobProcessing()
	go s.startHeartbeatMonitorForOneTimeJobs()
	go s.startFailedAndCompletedOneTimeJobCleaner()
	go s.startOrphanedJobMonitor()

	return nil
}

// Shutdown gracefully stops the Scheduler and its running jobs.
func (s *Scheduler) Shutdown() {
	if !s.started {
		s.config.Logger.Error("cannot shutdown scheduler, scheduler was not started")
		return
	}
	close(s.shutdownChan)
	s.cancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.config.Logger.Info("all jobs completed gracefully")
	case <-s.clock.After(s.config.ShutdownTimeout):
		s.config.Logger.Error("shutdown timeout exceeded, some jobs may not have completed")
	}
	jobRegistryRWLock.Lock()
	defer jobRegistryRWLock.Unlock()
	for k := range jobRegistry {
		delete(jobRegistry, k)
	}
}

// ScheduleJob adds a new job to the Scheduler.
//
// Parameters:
//   - job: A Job interface representing the job to be scheduled.
//
// Returns:
//   - error: An error if scheduling the job fails.
func (s *Scheduler) ScheduleJob(job Job) error {
	if !s.initialized {
		return fmt.Errorf("scheduler was not initialized")
	}
	key, err := getJobKey(job)
	if err != nil {
		return fmt.Errorf("failed to get job key: %w", err)
	}

	nextRun, err := s.calculateNextRun(job.CronSchedule())
	if err != nil {
		return fmt.Errorf("failed to calculate next run cron schedule: %w", err)
	}

	parameters, err := json.Marshal(job.Parameters())
	if err != nil {
		return fmt.Errorf("failed to marshal job parameters: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s ("key", "name", "last_run", "picked", "picked_by", "heartbeat", "next_run", "job_type", "parameters", "status", "retries")
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT ("key") DO NOTHING`, s.tableName)

	_, err = s.config.DB.Exec(query,
		key, job.Name(), nil, false, uuid.Nil, nil, nextRun, job.JobType(), parameters, statusPending, job.Retries())
	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	jobRegistryRWLock.Lock()
	defer jobRegistryRWLock.Unlock()
	if _, ok := jobRegistry[job.Name()]; ok {
		if job.JobType() == JobTypeRecurring {
			return fmt.Errorf("recurring job already exists in job registry: %s", key)
		}
		// For one-time jobs, we simply don't re-add them to the registry
		return nil
	}
	jobRegistry[job.Name()] = job

	return nil
}

func (s *Scheduler) calculateNextRun(cronSchedule string) (time.Time, error) {
	if cronSchedule == "" {
		return s.clock.Now().UTC(), nil
	}
	cronParser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := cronParser.Parse(cronSchedule)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse cron schedule: %w", err)
	}
	now := s.clock.Now().UTC()
	next := schedule.Next(now)
	return next, nil
}

func (s *Scheduler) startJobProcessing() {
	defer s.wg.Done()
	ticker := s.clock.NewTicker(s.config.JobCheckInterval)
	defer ticker.Stop()

	if s.config.RunImmediately {
		jobRegistryRWLock.RLock()
		jobNames := make([]string, 0, len(jobRegistry))
		for name := range jobRegistry {
			jobNames = append(jobNames, name)
		}
		jobRegistryRWLock.RUnlock()
		if len(jobNames) > 0 {
			if err := s.processJobs(); err != nil {
				s.config.Logger.Error("error processing jobs", "error", err)
			}
		}
	}

	for {
		select {
		case <-ticker.Chan():
			if err := s.processJobs(); err != nil {
				s.config.Logger.Error("error processing jobs", "error", err)
			}
		case <-s.shutdownChan:
			return
		}
	}
}

func (s *Scheduler) startOrphanedJobMonitor() {
	defer s.wg.Done()
	ticker := s.clock.NewTicker(s.config.OrphanedJobTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			if err := s.cleanOrphanedJobs(); err != nil {
				s.config.Logger.Error("error cleaning orphaned jobs", "error", err)
			}
		case <-s.shutdownChan:
			return
		}
	}
}

func (s *Scheduler) cleanOrphanedJobs() error {
	jobRegistryRWLock.RLock()
	jobNames := make([]string, 0, len(jobRegistry))
	for name := range jobRegistry {
		jobNames = append(jobNames, name)
	}
	jobRegistryRWLock.RUnlock()

	query := fmt.Sprintf(`
        DELETE FROM %s
        WHERE "name" NOT IN (?)
        AND "heartbeat" < ?
    `, s.tableName)

	query, args, err := sqlx.In(query, jobNames, s.clock.Now().UTC().Add(-s.config.OrphanedJobTimeout))
	if err != nil {
		return fmt.Errorf("failed to expand IN clause: %w", err)
	}

	query = s.db.Rebind(query)

	result, err := s.config.DB.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to clean orphaned jobs: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		s.config.Logger.Error("failed to get rows affected", "error", err)
	} else if rowsAffected > 0 {
		s.config.Logger.Info("cleaned orphaned jobs", "count", rowsAffected)
	}

	return nil
}

func (s *Scheduler) startHeartbeatMonitorForOneTimeJobs() {
	defer s.wg.Done()
	ticker := s.clock.NewTicker(s.config.NoHeartbeatTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			if err := s.checkAndResetTimedOutJobs(); err != nil {
				s.config.Logger.Error("error checking and resetting timed out jobs", "error", err)
			}
		case <-s.shutdownChan:
			return
		}
	}
}

func (s *Scheduler) startFailedAndCompletedOneTimeJobCleaner() {
	defer s.wg.Done()
	ticker := s.clock.NewTicker(s.config.FailedAndCompletedOneTimeJobInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			if err := s.cleanFailedAndCompletedOneTimeJobs(); err != nil {
				s.config.Logger.Error("error cleaning failed one-time jobs", "error", err)
			}
		case <-s.shutdownChan:
			return
		}
	}
}

func (s *Scheduler) cleanFailedAndCompletedOneTimeJobs() error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE "job_type" = $1 AND ("status" = $2 OR "status" = $3)`, s.tableName)
	_, err := s.config.DB.Exec(query, JobTypeOneTime, statusFailed, statusCompleted)
	if err != nil {
		return fmt.Errorf("failed to clean failed one-time jobs: %w", err)
	}
	return nil
}

func (s *Scheduler) processJobs() error {
	var wg sync.WaitGroup
	var errRecurring error
	var errOneTime error
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.acquireLockAndRun(JobTypeRecurring, s.config.MaxConcurrentRecurringJobs); err != nil {
			errRecurring = fmt.Errorf("error acquiring lock and running jobs of type %d: %w", JobTypeRecurring, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.acquireLockAndRun(JobTypeOneTime, s.config.MaxConcurrentOneTimeJobs); err != nil {
			errOneTime = fmt.Errorf("error acquiring lock and running jobs of type %d: %w", JobTypeOneTime, err)
		}
	}()
	wg.Wait()
	if errRecurring != nil && errOneTime == nil {
		return errRecurring
	} else if errRecurring == nil && errOneTime != nil {
		return errOneTime
	} else if errRecurring != nil && errOneTime != nil {
		return errors.Join(errRecurring, errOneTime)
	}
	return nil
}

func (s *Scheduler) acquireLockAndRun(jobType JobType, quota int) error {
	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	jobRegistryRWLock.RLock()
	jobNames := make([]string, 0, len(jobRegistry))
	for name := range jobRegistry {
		jobNames = append(jobNames, name)
	}
	jobRegistryRWLock.RUnlock()

	query := fmt.Sprintf(`SELECT "key", "name", "last_run", "picked", "picked_by", "heartbeat", "next_run", "job_type", "parameters", "status", "retries", "execution_time", "last_success", "last_failure", "consecutive_failures"
		FROM %s 
		WHERE "job_type" = ?
		  AND "name" IN (?)
		  AND "picked" = false 
		  AND "next_run" <= ?
		ORDER BY "next_run" 
		LIMIT ?
		FOR UPDATE`, s.tableName)

	query, args, err := sqlx.In(query, jobType, jobNames, s.clock.Now().UTC(), quota)
	if err != nil {
		return fmt.Errorf("failed to expand IN clause: %w", err)
	}

	query = tx.Rebind(query)

	var jobRecords []jobRecord
	err = tx.Select(&jobRecords, query, args...)
	if err != nil {
		return fmt.Errorf("failed to select jobs: %w", err)
	}

	for _, job := range jobRecords {
		updateQuery := fmt.Sprintf(`UPDATE %s SET "picked" = ?, "picked_by" = ?, "heartbeat" = ?, "status" = ? WHERE "key" = ?`, s.tableName)
		updateQuery = tx.Rebind(updateQuery)
		_, err := tx.Exec(updateQuery, true, s.nodeID, s.clock.Now().UTC(), statusRunning, job.Key)
		if err != nil {
			return fmt.Errorf("failed to pick job: %w", err)
		}
		s.runningJobs.Store(job.Key, true)
		go s.runJob(job.Key, jobRegistry[job.Name], job.Parameters, job.Retries)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *Scheduler) recoverLogAndFail(msg string, jobKey string, job Job, retries int, startTime time.Time) {
	if p := recover(); p != nil {
		if err, ok := p.(error); ok {
			s.config.Logger.Error(msg+": %w, stack: %s", err, string(debug.Stack()))
		} else {
			s.config.Logger.Error(msg+": %v, stack: %s", p, string(debug.Stack()))
		}
		s.markJobFailed(jobKey, job, retries, s.clock.Now().UTC().Sub(startTime))
	}
}

func (s *Scheduler) runJob(jobKey string, job Job, parameters json.RawMessage, retries int) {
	defer s.runningJobs.Delete(jobKey)
	startTime := s.clock.Now().UTC()
	defer s.recoverLogAndFail(fmt.Sprintf("failed to run job - %s", jobKey), jobKey, job, retries, startTime)

	s.config.Logger.Debug("running job", "job", jobKey, "time", startTime)
	ctx := s.ctx

	stopHeartbeat := make(chan struct{})
	s.wg.Add(1)
	defer close(stopHeartbeat)
	defer s.wg.Done()
	go s.updateHeartbeat(jobKey, stopHeartbeat)

	var err error
	if job.JobType() == JobTypeOneTime {
		var params interface{}
		err = json.Unmarshal(parameters, &params)
		if err != nil {
			s.config.Logger.Error("error unmarshalling job parameters", "job", jobKey, "error", err)
			s.markJobFailed(jobKey, job, retries, s.clock.Now().UTC().Sub(startTime))
			return
		}
		ctx = context.WithValue(s.ctx, ClusterSchedulerParametersKey, params)
	}

	err = job.Run(ctx)
	if err != nil {
		s.config.Logger.Error("error running job", "job", jobKey, "error", err)
		s.markJobFailed(jobKey, job, retries, s.clock.Now().UTC().Sub(startTime))
		return
	}

	s.markJobSucceeded(jobKey, job, s.clock.Now().UTC().Sub(startTime))
}

func (s *Scheduler) markJobFailed(jobKey string, job Job, retries int, executionTime time.Duration) {
	if job.JobType() == JobTypeOneTime && retries > 0 {
		nextRun := s.clock.Now().UTC().Add(s.config.JobCheckInterval)
		query := fmt.Sprintf(`
			UPDATE %s 
			SET "picked" = false, "status" = $1, "next_run" = $2, "retries" = $3, 
				"execution_time" = $4, "last_failure" = $5, 
				"consecutive_failures" = "consecutive_failures" + 1
			WHERE "key" = $6`, s.tableName)
		_, err := s.config.DB.Exec(query, statusPending, nextRun, retries-1, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
		if err != nil {
			s.config.Logger.Error("failed to mark one-time job for retry", "job", jobKey, "error", err)
		}
	} else if job.JobType() == JobTypeOneTime {
		// For one-time jobs with no retries left, set next_run to max time
		maxTime := time.Unix(1<<63-62135596801, 999999999)
		query := fmt.Sprintf(`
			UPDATE %s 
			SET "picked" = false, "status" = $1, "next_run" = $2, "execution_time" = $3, 
				"last_failure" = $4, "consecutive_failures" = "consecutive_failures" + 1
			WHERE "key" = $5`, s.tableName)
		_, err := s.config.DB.Exec(query, statusFailed, maxTime, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
		if err != nil {
			s.config.Logger.Error("failed to mark one-time job as failed", "job", jobKey, "error", err)
		}
	} else {
		nextRun, err := s.calculateNextRun(job.CronSchedule())
		if err != nil {
			s.config.Logger.Error("failed to calculate next run", "job", jobKey, "error", err)
			return
		}
		query := fmt.Sprintf(`
			UPDATE %s 
			SET "picked" = false, "picked_by" = $1, "next_run" = $2, "status" = $3, "execution_time" = $4, 
				"last_failure" = $5, "consecutive_failures" = "consecutive_failures" + 1
			WHERE "key" = $6`, s.tableName)
		_, err = s.config.DB.Exec(query, uuid.Nil, nextRun, statusFailed, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
		if err != nil {
			s.config.Logger.Error("failed to mark job as failed", "job", jobKey, "error", err)
		}
	}
}

func (s *Scheduler) markJobSucceeded(jobKey string, job Job, executionTime time.Duration) {
	if job.JobType() == JobTypeOneTime {
		query := fmt.Sprintf(`
			UPDATE %s 
			SET "status" = $1, "execution_time" = $2, 
				"last_success" = $3, "consecutive_failures" = 0
			WHERE "key" = $4`, s.tableName)
		_, err := s.config.DB.Exec(query, statusCompleted, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
		if err != nil {
			s.config.Logger.Error("failed to mark one-time job as completed", "job", jobKey, "error", err)
		}
	} else {
		nextRun, err := s.calculateNextRun(job.CronSchedule())
		if err != nil {
			s.config.Logger.Error("failed to calculate next run", "job", jobKey, "error", err)
			return
		}
		query := fmt.Sprintf(`
			UPDATE %s 
			SET "status" = $1, "last_run" = $2, "picked" = false, 
				"picked_by" = $3, "next_run" = $4, "execution_time" = $5, 
				"last_success" = $6, "consecutive_failures" = 0
			WHERE "key" = $7`, s.tableName)
		_, err = s.config.DB.Exec(query, statusPending, s.clock.Now().UTC(), uuid.Nil, nextRun, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
		if err != nil {
			s.config.Logger.Error("failed to update recurring job", "job", jobKey, "error", err)
		}
	}
}

func (s *Scheduler) checkAndResetTimedOutJobs() error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET "picked" = false, "picked_by" = $1, "next_run" = $2, "status" = $3, 
			"retries" = CASE WHEN "job_type" = $4 THEN "retries" - 1 ELSE "retries" END
		WHERE "picked" = true AND "heartbeat" < $5 AND ("job_type" = $4 AND "retries" > 0)
		RETURNING "key", "name", "job_type", "retries"`, s.tableName)

	var resetJobs []struct {
		Key     string
		Name    string
		JobType JobType `db:"job_type"`
		Retries int
	}
	err := s.db.Select(&resetJobs, query, uuid.Nil, s.clock.Now().UTC(), statusPending, JobTypeOneTime, s.clock.Now().UTC().Add(-s.config.NoHeartbeatTimeout))
	if err != nil {
		return fmt.Errorf("failed to reset timed-out jobs: %w", err)
	}

	for _, job := range resetJobs {
		s.config.Logger.Debug("reset timed-out job", "job", job.Name, "key", job.Key, "type", job.JobType, "retries_left", job.Retries)
		if job.JobType == JobTypeOneTime && job.Retries <= 0 {
			if err := s.removeJob(job.Key); err != nil {
				s.config.Logger.Error("failed to remove exhausted one-time job", "job", job.Name, "key", job.Key, "error", err)
			}
		}
	}

	return nil
}

func (s *Scheduler) removeJob(jobKey string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE "key" = $1`, s.tableName)
	_, err := s.config.DB.Exec(query, jobKey)
	if err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}
	return nil
}

func (s *Scheduler) updateHeartbeat(jobKey string, stop chan struct{}) {
	ticker := s.clock.NewTicker(s.config.HeartbeatInterval)
	defer ticker.Stop()
	query := fmt.Sprintf(`UPDATE %s SET "heartbeat" = $1 WHERE "key" = $2 AND "picked_by" = $3`, s.tableName)
	for {
		select {
		case <-ticker.Chan():
			_, err := s.config.DB.Exec(query, s.clock.Now().UTC(), jobKey, s.nodeID)
			if err != nil {
				s.config.Logger.Error("failed to update heartbeat", "job", jobKey, "error", err)
			}
		case <-stop:
			return
		}
	}
}
