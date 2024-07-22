package pgscheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/jonboulle/clockwork"
	"github.com/robfig/cron/v3"
)

var jobRegistry = make(map[string]Job)
var jobRegistryRWLock sync.RWMutex

type JobType int

const ClusterSchedulerParametersKey = "ClusterSchedulerParametersKey"

const (
	JobTypeRecurring JobType = iota
	JobTypeOneTime

	jobTypeLast // This should always be the last constant
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

type JobLifecycle interface {
	Init(ctx context.Context) error
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type JobConfigI interface {
	Name() string
	CronSchedule() string
	JobType() JobType
	Parameters() interface{}
	Retries() int
}

type Job interface {
	JobConfigI
	JobLifecycle
}

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

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type SchedulerConfig struct {
	Ctx                                  context.Context
	DB                                   *sql.DB
	DBDriverName                         string
	MaxConcurrentJobs                    int
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

type Scheduler struct {
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
	Status              Status          `db:"status"`
	Retries             int             `db:"retries"`
	ExecutionTime       sql.NullInt64   `db:"execution_time"`
	LastSuccess         sql.NullTime    `db:"last_success"`
	LastFailure         sql.NullTime    `db:"last_failure"`
	ConsecutiveFailures int             `db:"consecutive_failures"`
}

func NewScheduler(config SchedulerConfig) (*Scheduler, error) {
	jobTypeCount := int(jobTypeLast)

	if config.MaxConcurrentJobs < jobTypeCount {
		return nil, fmt.Errorf("MaxConcurrentJobs must be at least %d (number of job types)", jobTypeCount)
	}

	if config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	if config.DBDriverName == "" {
		return nil, fmt.Errorf("database driver name is required")
	}

	// Wrap the *sql.DB with sqlx
	sqlxDB := sqlx.NewDb(config.DB, config.DBDriverName)

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
    `, s.tableName, StatusPending, s.tableName, s.tableName, s.tableName, s.tableName, s.tableName, s.tableName)

	_, err := s.config.DB.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

func (s *Scheduler) Init() error {
	if s.config.CreateSchema {
		if err := s.createSchemaIfMissing(); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	s.wg.Add(4)
	go s.startJobProcessing()
	go s.startHeartbeatMonitorForOneTimeJobs()
	go s.startFailedAndCompletedOneTimeJobCleaner()
	go s.startOrphanedJobMonitor()

	return nil
}

func (s *Scheduler) Shutdown() {
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

func (s *Scheduler) ScheduleJob(job Job) error {
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
		key, job.Name(), nil, false, uuid.Nil, nil, nextRun, job.JobType(), parameters, StatusPending, job.Retries())
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
		if err := s.processJobs(); err != nil {
			s.config.Logger.Error("error processing jobs", "error", err)
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
	} else {
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
	_, err := s.config.DB.Exec(query, JobTypeOneTime, StatusFailed, StatusCompleted)
	if err != nil {
		return fmt.Errorf("failed to clean failed one-time jobs: %w", err)
	}
	return nil
}

func (s *Scheduler) processJobs() error {
	quotaPerType := s.config.MaxConcurrentJobs / int(jobTypeLast)
	for jobType := JobType(0); jobType < jobTypeLast; jobType++ {
		if err := s.acquireLockAndRun(jobType, quotaPerType); err != nil {
			return fmt.Errorf("error acquiring lock and running jobs of type %d: %w", jobType, err)
		}
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
		_, err := tx.Exec(updateQuery, true, s.nodeID, s.clock.Now().UTC(), StatusRunning, job.Key)
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

func (s *Scheduler) runJob(jobKey string, job Job, parameters json.RawMessage, retries int) {
	defer s.runningJobs.Delete(jobKey)

	startTime := s.clock.Now().UTC()
	s.config.Logger.Info("running job", "job", jobKey, "time", startTime)
	ctx := s.ctx

	err := job.Init(ctx)
	if err != nil {
		s.config.Logger.Error("error initializing job", "job", jobKey, "error", err)
		s.markJobFailed(jobKey, job.JobType(), retries, s.clock.Now().UTC().Sub(startTime))
		return
	}

	stopHeartbeat := make(chan struct{})
	s.wg.Add(1)
	defer close(stopHeartbeat)
	defer s.wg.Done()
	go s.updateHeartbeat(jobKey, stopHeartbeat)

	if job.JobType() == JobTypeOneTime {
		var params interface{}
		err = json.Unmarshal(parameters, &params)
		if err != nil {
			s.config.Logger.Error("error unmarshaling job parameters", "job", jobKey, "error", err)
			s.markJobFailed(jobKey, job.JobType(), retries, s.clock.Now().UTC().Sub(startTime))
			return
		}
		ctx = context.WithValue(s.ctx, ClusterSchedulerParametersKey, params)
	}

	err = job.Run(ctx)
	if err != nil {
		s.config.Logger.Error("error running job", "job", jobKey, "error", err)
		s.markJobFailed(jobKey, job.JobType(), retries, s.clock.Now().UTC().Sub(startTime))
		return
	}

	err = job.Shutdown(ctx)
	if err != nil {
		s.config.Logger.Error("error shutting down job", "job", jobKey, "error", err)
		s.markJobFailed(jobKey, job.JobType(), retries, s.clock.Now().UTC().Sub(startTime))
		return
	}

	s.markJobSucceeded(jobKey, job, s.clock.Now().UTC().Sub(startTime))
}

func (s *Scheduler) markJobFailed(jobKey string, jobType JobType, retries int, executionTime time.Duration) {
	if jobType == JobTypeOneTime && retries > 0 {
		nextRun := s.clock.Now().UTC().Add(s.config.JobCheckInterval)
		query := fmt.Sprintf(`
			UPDATE %s 
			SET "status" = $1, "next_run" = $2, "retries" = $3, 
				"execution_time" = $4, "last_failure" = $5, 
				"consecutive_failures" = "consecutive_failures" + 1
			WHERE "key" = $6`, s.tableName)
		_, err := s.config.DB.Exec(query, StatusPending, nextRun, retries-1, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
		if err != nil {
			s.config.Logger.Error("failed to mark one-time job for retry", "job", jobKey, "error", err)
		}
	} else if jobType == JobTypeOneTime {
		// For one-time jobs with no retries left, set next_run to max time
		maxTime := time.Unix(1<<63-62135596801, 999999999)
		query := fmt.Sprintf(`
			UPDATE %s 
			SET "status" = $1, "next_run" = $2, "execution_time" = $3, 
				"last_failure" = $4, "consecutive_failures" = "consecutive_failures" + 1
			WHERE "key" = $5`, s.tableName)
		_, err := s.config.DB.Exec(query, StatusFailed, maxTime, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
		if err != nil {
			s.config.Logger.Error("failed to mark one-time job as failed", "job", jobKey, "error", err)
		}
	} else {
		query := fmt.Sprintf(`
			UPDATE %s 
			SET "status" = $1, "execution_time" = $2, 
				"last_failure" = $3, "consecutive_failures" = "consecutive_failures" + 1
			WHERE "key" = $4`, s.tableName)
		_, err := s.config.DB.Exec(query, StatusFailed, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
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
		_, err := s.config.DB.Exec(query, StatusCompleted, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
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
		_, err = s.config.DB.Exec(query, StatusPending, s.clock.Now().UTC(), uuid.Nil, nextRun, sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true}, sql.NullTime{Time: s.clock.Now().UTC(), Valid: true}, jobKey)
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
	err := s.db.Select(&resetJobs, query, uuid.Nil, s.clock.Now().UTC(), StatusPending, JobTypeOneTime, s.clock.Now().UTC().Add(-s.config.NoHeartbeatTimeout))
	if err != nil {
		return fmt.Errorf("failed to reset timed-out jobs: %w", err)
	}

	for _, job := range resetJobs {
		s.config.Logger.Info("reset timed-out job", "job", job.Name, "key", job.Key, "type", job.JobType, "retries_left", job.Retries)
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
