package pgscheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tzahifadida/pgln"
	"log/slog"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/jonboulle/clockwork"
	"github.com/robfig/cron/v3"
)

var epochStart = time.Unix(0, 0).UTC()

const (
	ClusterSchedulerParametersKey = "ClusterSchedulerParametersKey"
	scheduledJobsTableName        = "scheduled_jobs"
	jobKeySeparator               = "@@"
	notificationTypeStatusChange  = "status_change"
	notificationTypeCancel        = "cancel"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

// Custom errors
var (
	ErrJobNotFound   = errors.New("job not found")
	ErrJobRunning    = errors.New("job is currently running")
	ErrJobNotRunning = errors.New("job exists but is not currently running")
)

// NextRunCalculator defines how to calculate the next run time
type NextRunCalculator interface {
	NextRun(now time.Time) (time.Time, error)
}

// CronSchedule implements NextRunCalculator using cron expressions
type CronSchedule struct {
	expression string
	parser     cron.Parser
}

func NewCronSchedule(expression string) (*CronSchedule, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	if _, err := parser.Parse(expression); err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}
	return &CronSchedule{
		expression: expression,
		parser:     parser,
	}, nil
}

func (c *CronSchedule) NextRun(now time.Time) (time.Time, error) {
	schedule, err := c.parser.Parse(c.expression)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse cron schedule: %w", err)
	}
	return schedule.Next(now), nil
}

// FixedInterval implements NextRunCalculator using fixed time intervals
type FixedInterval struct {
	interval time.Duration
}

func NewFixedInterval(interval time.Duration) *FixedInterval {
	return &FixedInterval{interval: interval}
}

func (f *FixedInterval) NextRun(now time.Time) (time.Time, error) {
	return now.Add(f.interval), nil
}

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type statusChangeNotification struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	Key        string `json:"key"`
	Status     Status `json:"status"`
	PrevStatus Status `json:"prev_status"`
}

// Job defines the interface for a schedulable job
type Job interface {
	Name() string
	Key() string
	Run(ctx context.Context) error
	NextRunCalculator() NextRunCalculator
	Parameters() interface{}
	MaxRetries() int
}
type JobRecord struct {
	Name                string          `db:"name"`
	Key                 string          `db:"key"`
	LastRun             sql.NullTime    `db:"last_run"`
	Picked              bool            `db:"picked"`
	PickedBy            uuid.NullUUID   `db:"picked_by"`
	Heartbeat           sql.NullTime    `db:"heartbeat"`
	NextRun             *time.Time      `db:"next_run"`
	Parameters          json.RawMessage `db:"parameters"`
	Status              Status          `db:"status"`
	Retries             int             `db:"retries"`
	ExecutionTime       sql.NullInt64   `db:"execution_time"`
	LastSuccess         sql.NullTime    `db:"last_success"`
	LastFailure         sql.NullTime    `db:"last_failure"`
	ConsecutiveFailures int             `db:"consecutive_failures"`
	CancelRequested     bool            `db:"cancel_requested"`
}

// SchedulerConfig represents the configuration for the Scheduler
type SchedulerConfig struct {
	Ctx                                  context.Context
	DB                                   *sql.DB
	DBDriverName                         string
	MaxRunningJobs                       int
	JobCheckInterval                     time.Duration
	OrphanedJobTimeout                   time.Duration
	HeartbeatInterval                    time.Duration
	NoHeartbeatTimeout                   time.Duration
	CreateSchema                         bool
	Logger                               Logger
	RunImmediately                       bool
	TablePrefix                          string
	ShutdownTimeout                      time.Duration
	FailedAndCompletedJobCleanupInterval time.Duration
	CancelCheckPeriod                    time.Duration
	NotificationDebounceInterval         time.Duration
	JobStatusChangeCallback              func(name, key string, prevStatus, newStatus Status)
	Schema                               string
	clock                                clockwork.Clock
	PGLNInstance                         *pgln.PGListenNotify
}

type Scheduler struct {
	initialized          bool
	started              bool
	config               SchedulerConfig
	db                   *sqlx.DB
	nodeID               uuid.UUID
	shutdownChan         chan struct{}
	wg                   sync.WaitGroup
	ctx                  context.Context
	cancel               context.CancelFunc
	tableName            string
	runningJobsCount     int64
	runningJobsMutex     sync.RWMutex
	cancelFuncs          sync.Map
	jobRegistry          map[string]Job
	jobRegistryRWLock    sync.RWMutex
	jobNames             []string
	notificationChan     string
	statusChangeCallback func(name, key string, prevStatus, newStatus Status)
	clock                clockwork.Clock
	lastProcessTime      time.Time
	processMutex         sync.Mutex
	hasEventsSinceCheck  bool
	processWakeupCh      chan struct{}
	debounceTimer        clockwork.Timer
	debounceTimerRunning bool
}
type cancelTracker struct {
	sync.RWMutex
	cancelled map[string]bool
}

func getCancelKey(name, key string) string {
	return name + jobKeySeparator + key
}

func (s *Scheduler) prepareStatusChangeNotification(name string, key string, prevStatus, newStatus Status) (*pgln.NotifyQueryResult, error) {
	if s.config.PGLNInstance == nil {
		return nil, nil
	}

	notification := statusChangeNotification{
		Type:       notificationTypeStatusChange,
		Name:       name,
		Key:        key,
		Status:     newStatus,
		PrevStatus: prevStatus,
	}

	payload, err := json.Marshal(notification)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal notification: %w", err)
	}
	query := s.config.PGLNInstance.NotifyQuery(s.notificationChan, string(payload))
	return &query, nil
}
func NewScheduler(config SchedulerConfig) (*Scheduler, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	if config.DBDriverName == "" {
		return nil, fmt.Errorf("database driver name is required")
	}

	if config.MaxRunningJobs <= 0 {
		config.MaxRunningJobs = 10
	}

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

	if config.FailedAndCompletedJobCleanupInterval == 0 {
		config.FailedAndCompletedJobCleanupInterval = 10 * time.Minute
	}

	if config.CancelCheckPeriod == 0 {
		config.CancelCheckPeriod = 30 * time.Second
	}

	if config.NotificationDebounceInterval == 0 {
		config.NotificationDebounceInterval = 1 * time.Second
	}

	if config.Schema == "" {
		config.Schema = "public"
	}

	nodeID := uuid.New()
	ctx := context.Background()
	if config.Ctx != nil {
		ctx = config.Ctx
	}
	ctx, cancel := context.WithCancel(ctx)

	prefix := config.TablePrefix
	if prefix == "" {
		prefix = "scheduler"
	}

	s := &Scheduler{
		config:               config,
		db:                   sqlxDB,
		nodeID:               nodeID,
		shutdownChan:         make(chan struct{}),
		ctx:                  ctx,
		cancel:               cancel,
		tableName:            fmt.Sprintf(`"%s"."%s"`, config.Schema, prefix+scheduledJobsTableName),
		cancelFuncs:          sync.Map{},
		jobRegistry:          make(map[string]Job),
		notificationChan:     fmt.Sprintf("%s_job_notifications", prefix),
		processWakeupCh:      make(chan struct{}, 1),
		debounceTimer:        config.clock.NewTimer(config.NotificationDebounceInterval),
		debounceTimerRunning: false,
		statusChangeCallback: config.JobStatusChangeCallback,
		clock:                config.clock,
	}

	// Stop timer initially since we don't have events yet
	if !s.debounceTimer.Stop() {
		select {
		case <-s.debounceTimer.Chan():
		default:
		}
	}

	if config.PGLNInstance != nil {
		if err := s.setupPGLN(); err != nil {
			return nil, fmt.Errorf("failed to setup PGLN: %w", err)
		}
	}

	return s, nil
}

func (s *Scheduler) setupPGLN() error {
	return s.config.PGLNInstance.ListenAndWaitForListening(s.notificationChan, pgln.ListenOptions{
		NotificationCallback: func(channel string, payload string) {
			var notification statusChangeNotification
			if err := json.Unmarshal([]byte(payload), &notification); err != nil {
				s.config.Logger.Error("failed to unmarshal notification", "error", err)
				return
			}

			switch notification.Type {
			case notificationTypeCancel:
				go s.checkForCancellations()
			case notificationTypeStatusChange:
				// Call the callback synchronously
				if s.statusChangeCallback != nil {
					s.statusChangeCallback(notification.Name, notification.Key,
						notification.PrevStatus, notification.Status)
				}

				// Only the job processing check should be async
				if notification.Status == StatusPending ||
					notification.Status == StatusFailed ||
					notification.Status == StatusCompleted {
					s.checkForNewJobs()
				}
			}
		},
		ErrorCallback: func(channel string, err error) {
			s.config.Logger.Error("notification error", "error", err)
		},
	})
}
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

func (s *Scheduler) createSchemaIfMissing() error {
	createSchema := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s";`, s.config.Schema)
	_, err := s.config.DB.Exec(createSchema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	tableNameNoQuotes := s.config.TablePrefix + scheduledJobsTableName
	createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
        "name" TEXT,
        "key" TEXT NOT NULL,
        "last_run" TIMESTAMP,
        "picked" BOOLEAN,
        "picked_by" UUID,
        "heartbeat" TIMESTAMP NOT NULL,
        "next_run" TIMESTAMP,
        "parameters" JSONB,
        "status" TEXT DEFAULT '%s',
        "retries" INT DEFAULT 0,
        "execution_time" INT DEFAULT 0,
        "last_success" TIMESTAMP,
        "last_failure" TIMESTAMP,
        "consecutive_failures" INT DEFAULT 0,
        "cancel_requested" BOOLEAN DEFAULT false,
        PRIMARY KEY ("name", "key")
    );

    CREATE INDEX IF NOT EXISTS "idx_%s_job_acquisition" ON %s("picked", "next_run");
    CREATE INDEX IF NOT EXISTS "idx_%s_heartbeat_monitor" ON %s("picked", "heartbeat");
    CREATE INDEX IF NOT EXISTS "idx_%s_cleanup" ON %s("last_run");
    CREATE INDEX IF NOT EXISTS "idx_%s_cancel_monitor" ON %s("cancel_requested", "picked");
    `, s.tableName, StatusPending, tableNameNoQuotes, s.tableName, tableNameNoQuotes, s.tableName,
		tableNameNoQuotes, s.tableName, tableNameNoQuotes, s.tableName)

	_, err = s.config.DB.Exec(createTable)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

func (s *Scheduler) RegisterJob(job Job) error {
	if job.Name() == "" {
		return fmt.Errorf("job name cannot be empty")
	}

	if strings.Contains(job.Name(), jobKeySeparator) {
		return fmt.Errorf("job name cannot contain %q", jobKeySeparator)
	}

	s.jobRegistryRWLock.Lock()
	defer s.jobRegistryRWLock.Unlock()

	if _, exists := s.jobRegistry[job.Name()]; exists {
		return fmt.Errorf("job %s is already registered", job.Name())
	}

	s.jobRegistry[job.Name()] = job
	s.jobNames = append(s.jobNames, job.Name())
	return nil
}

func (s *Scheduler) GetJob(name, key string) (*JobRecord, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE "name" = $1 AND "key" = $2`, s.tableName)
	var job JobRecord
	err := s.db.Get(&job, query, name, key)
	if err == sql.ErrNoRows {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	return &job, nil
}

func (s *Scheduler) incrementRunningJobs() {
	s.runningJobsMutex.Lock()
	s.runningJobsCount++
	s.runningJobsMutex.Unlock()
}

func (s *Scheduler) decrementRunningJobs() {
	s.runningJobsMutex.Lock()
	s.runningJobsCount--
	s.runningJobsMutex.Unlock()
}

func (s *Scheduler) getRunningJobsCount() int64 {
	s.runningJobsMutex.RLock()
	defer s.runningJobsMutex.RUnlock()
	return s.runningJobsCount
}

func (s *Scheduler) Start() error {
	if !s.initialized {
		return fmt.Errorf("scheduler was not initialized")
	}

	s.wg.Add(5)
	go s.startJobProcessing()
	go s.startHeartbeatMonitor()
	go s.startFailedAndCompletedJobCleaner()
	go s.startOrphanedJobMonitor()
	go s.startCancelMonitor()

	s.started = true
	return nil
}

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

	s.jobRegistryRWLock.Lock()
	defer s.jobRegistryRWLock.Unlock()
	for k := range s.jobRegistry {
		delete(s.jobRegistry, k)
	}
	s.jobNames = nil
}
func (s *Scheduler) ScheduleJob(job Job) error {
	if !s.initialized {
		return fmt.Errorf("scheduler was not initialized")
	}

	// Check if job is registered
	s.jobRegistryRWLock.RLock()
	registeredJob, exists := s.jobRegistry[job.Name()]
	s.jobRegistryRWLock.RUnlock()
	if !exists {
		return fmt.Errorf("job %s must be registered before scheduling", job.Name())
	}

	// Ensure job implementations match
	if reflect.TypeOf(registeredJob) != reflect.TypeOf(job) {
		return fmt.Errorf("job implementation does not match registered job")
	}

	// Start transaction
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var nextRun time.Time
	calculator := job.NextRunCalculator()
	if calculator == nil {
		nextRun = s.clock.Now().UTC()
	} else {
		nextRun, err = calculator.NextRun(s.clock.Now().UTC())
		if err != nil {
			return fmt.Errorf("failed to calculate next run: %w", err)
		}
	}

	parameters, err := json.Marshal(job.Parameters())
	if err != nil {
		return fmt.Errorf("failed to marshal job parameters: %w", err)
	}

	query := fmt.Sprintf(`
        INSERT INTO %s ("name", "key", "picked", "picked_by", "next_run", "parameters", "status", "retries", "heartbeat")
        VALUES ($1, $2, false, $3, $4, $5, $6, $7, $8)
        ON CONFLICT ("name", "key") 
        DO UPDATE SET 
            next_run = $4,
            parameters = $5,
            status = $6,    
            retries = $7            
        WHERE NOT %s.picked
        RETURNING name`,
		s.tableName, s.tableName)

	var returnedName string
	err = tx.QueryRow(query,
		job.Name(),
		job.Key(),
		uuid.Nil,
		nextRun,
		parameters,
		StatusPending,
		job.MaxRetries(),
		epochStart).Scan(&returnedName)

	if err == sql.ErrNoRows {
		return ErrJobRunning
	}
	if err != nil {
		return fmt.Errorf("failed to schedule job: %w", err)
	}
	// Prepare status change notification for scheduled job
	notifyQuery, err := s.prepareStatusChangeNotification(job.Name(), job.Key(), "", StatusPending)
	if err != nil {
		return fmt.Errorf("failed to prepare status change notification: %w", err)
	}

	if notifyQuery != nil {
		_, err = tx.ExecContext(s.ctx, notifyQuery.Query, notifyQuery.Params...)
		if err != nil {
			return fmt.Errorf("failed to execute notification: %w", err)
		}
	}

	return tx.Commit()
}

func (s *Scheduler) startJobProcessing() {
	defer s.wg.Done()

	regularTicker := s.clock.NewTicker(s.config.JobCheckInterval)
	defer regularTicker.Stop()
	defer s.debounceTimer.Stop()

	if s.config.RunImmediately {
		if err := s.processJobs(); err != nil {
			s.config.Logger.Error("error processing jobs", "error", err)
		}
	}

	for {
		select {
		case <-s.shutdownChan:
			return

		case <-regularTicker.Chan():
			// Regular interval check for missed notifications
			if err := s.processJobs(); err != nil {
				s.config.Logger.Error("error processing jobs", "error", err)
			}

		case <-s.processWakeupCh:
			// Received wake-up signal, ensure timer is running
			s.processMutex.Lock()
			if !s.debounceTimerRunning {
				s.debounceTimer.Reset(s.config.NotificationDebounceInterval)
				s.debounceTimerRunning = true
			}
			s.processMutex.Unlock()

		case <-s.debounceTimer.Chan():
			s.processMutex.Lock()
			hasEvents := s.hasEventsSinceCheck
			s.hasEventsSinceCheck = false

			// Stop timer if no new events
			if !hasEvents {
				s.debounceTimerRunning = false
				s.processMutex.Unlock()
				continue
			}

			// Reset timer if we had events
			s.debounceTimer.Reset(s.config.NotificationDebounceInterval)
			s.processMutex.Unlock()

			// Process jobs since we had events
			if err := s.processJobs(); err != nil {
				s.config.Logger.Error("error processing jobs", "error", err)
			}
		}
	}
}

func (s *Scheduler) checkForNewJobs() {
	s.processMutex.Lock()
	defer s.processMutex.Unlock()

	now := s.clock.Now()

	// If we're still within debounce window, just mark that we have events
	if now.Sub(s.lastProcessTime) < s.config.NotificationDebounceInterval {
		s.hasEventsSinceCheck = true
		return
	}

	s.lastProcessTime = now
	s.hasEventsSinceCheck = true

	// Start timer if it's not running
	if !s.debounceTimerRunning {
		s.debounceTimer.Reset(s.config.NotificationDebounceInterval)
		s.debounceTimerRunning = true
	}

	// Wake up the monitor
	select {
	case s.processWakeupCh <- struct{}{}:
	default:
		// Channel already has a wake-up signal
	}
}
func (s *Scheduler) processJobs() error {
	runningCount := s.getRunningJobsCount()
	if runningCount >= int64(s.config.MaxRunningJobs) {
		return nil
	}

	available := s.config.MaxRunningJobs - int(runningCount)
	if available <= 0 {
		return nil
	}

	if err := s.acquireAndRunJobs(available); err != nil {
		return fmt.Errorf("error acquiring and running jobs: %w", err)
	}

	return nil
}

func (s *Scheduler) acquireAndRunJobs(quota int) error {
	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	s.jobRegistryRWLock.RLock()
	jobNames := s.jobNames
	s.jobRegistryRWLock.RUnlock()

	query := fmt.Sprintf(`
        WITH available_jobs AS (
            SELECT name, key, parameters, retries
            FROM %s
            WHERE name IN (?)
            AND picked = false 
            AND next_run <= ?
            AND status = ?
            ORDER BY next_run 
            LIMIT ?
            FOR UPDATE SKIP LOCKED
        )
        UPDATE %s j
        SET 
            picked = true,
            picked_by = ?,
            heartbeat = ?,
            status = ?
        FROM available_jobs
        WHERE j.name = available_jobs.name
        AND j.key = available_jobs.key
        RETURNING j.name, j.key, j.parameters, j.retries`,
		s.tableName, s.tableName)

	query, args, err := sqlx.In(query, jobNames,
		s.clock.Now().UTC(),
		StatusPending,
		quota,
		s.nodeID,
		s.clock.Now().UTC(),
		StatusRunning)
	if err != nil {
		return fmt.Errorf("failed to expand IN clause: %w", err)
	}
	query = tx.Rebind(query)

	rows, err := tx.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to acquire jobs: %w", err)
	}
	defer rows.Close()

	jobsToRun := make([]struct {
		name       string
		key        string
		parameters json.RawMessage
		retries    int
	}, 0, quota)

	for rows.Next() {
		var job struct {
			name       string
			key        string
			parameters json.RawMessage
			retries    int
		}
		if err := rows.Scan(&job.name, &job.key, &job.parameters, &job.retries); err != nil {
			return fmt.Errorf("failed to scan job row: %w", err)
		}
		jobsToRun = append(jobsToRun, job)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Start the jobs after committing the transaction
	for _, jobToRun := range jobsToRun {
		s.jobRegistryRWLock.RLock()
		job := s.jobRegistry[jobToRun.name]
		s.jobRegistryRWLock.RUnlock()

		s.incrementRunningJobs()
		go s.runJob(jobToRun.name, jobToRun.key, job, jobToRun.parameters, jobToRun.retries)

		notifyQuery, err := s.prepareStatusChangeNotification(jobToRun.name, jobToRun.key,
			StatusPending, StatusRunning)
		if err != nil {
			return fmt.Errorf("failed to prepare status change notification: %w", err)
		}
		if notifyQuery != nil {
			_, err = s.db.ExecContext(s.ctx, notifyQuery.Query, notifyQuery.Params...)
			if err != nil {
				return fmt.Errorf("failed to execute notification: %w", err)
			}
		}
	}

	return nil
}

func (s *Scheduler) runJob(name string, key string, job Job, parameters json.RawMessage, retries int) {
	defer s.decrementRunningJobs()

	startTime := s.clock.Now().UTC()

	// Create cancellable context
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// Store cancel function in a map for external cancellation
	cancelKey := getCancelKey(name, key)
	s.cancelFuncs.Store(cancelKey, cancel)
	defer s.cancelFuncs.Delete(cancelKey)

	defer func() {
		if p := recover(); p != nil {
			if err, ok := p.(error); ok {
				s.config.Logger.Error("job panic", "job", name, "key", key, "error", err, "stack", string(debug.Stack()))
			} else {
				s.config.Logger.Error("job panic", "job", name, "key", key, "panic", p, "stack", string(debug.Stack()))
			}
			s.markJobFailed(name, key, job, s.clock.Since(startTime))
		}
	}()

	s.config.Logger.Debug("running job", "job", name, "key", key, "time", startTime)

	// Start heartbeat
	stopHeartbeat := make(chan struct{})
	s.wg.Add(1)
	defer close(stopHeartbeat)
	defer s.wg.Done()
	go s.updateHeartbeat(name, key, stopHeartbeat)

	// Setup parameters if any
	if len(parameters) > 0 {
		var params interface{}
		if err := json.Unmarshal(parameters, &params); err != nil {
			s.config.Logger.Error("error unmarshalling job parameters", "job", name, "key", key, "error", err)
			s.markJobFailed(name, key, job, s.clock.Since(startTime))
			return
		}
		ctx = context.WithValue(ctx, ClusterSchedulerParametersKey, params)
	}

	// Run the job
	if err := job.Run(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			s.markJobCancelled(name, key)
		} else {
			s.config.Logger.Error("job failed", "job", name, "key", key, "error", err)
			s.markJobFailed(name, key, job, s.clock.Since(startTime))
		}
		return
	}

	s.markJobSucceeded(name, key, job, s.clock.Since(startTime))
}

func (s *Scheduler) markJobFailed(name, key string, job Job, executionTime time.Duration) {
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		s.config.Logger.Error("failed to begin transaction", "error", err)
		return
	}
	defer tx.Rollback()

	parameters, err := json.Marshal(job.Parameters())
	if err != nil {
		s.config.Logger.Error("failed to marshal job parameters", "job", name, "error", err)
		parameters = nil
	}

	// Get current retries from database
	var currentRetries int
	query := fmt.Sprintf(`
        SELECT retries 
        FROM %s 
        WHERE name = $1 AND key = $2`, s.tableName)
	err = tx.QueryRow(query, name, key).Scan(&currentRetries)
	if err != nil {
		s.config.Logger.Error("failed to get current retries", "job", name, "key", key, "error", err)
		return
	}

	nowUTC := s.clock.Now().UTC()
	var nextRun *time.Time
	var status Status
	var newRetries int

	if currentRetries > 0 {
		// Still have retries left
		next := nowUTC.Add(s.config.JobCheckInterval) // Retry after interval
		nextRun = &next
		status = StatusPending
		newRetries = currentRetries - 1
	} else if calculator := job.NextRunCalculator(); calculator != nil {
		// No retries left but job is recurring
		if next, err := calculator.NextRun(nowUTC); err == nil {
			nextRun = &next
			status = StatusPending
			newRetries = job.MaxRetries() // Reset retries for next run
		} else {
			s.config.Logger.Error("failed to calculate next run", "job", name, "error", err)
			return
		}
	} else {
		// No retries left and not recurring
		status = StatusFailed
		newRetries = 0
	}
	query = fmt.Sprintf(`
        UPDATE %s 
        SET picked = false, 
            picked_by = $1,
            next_run = $2,
            status = $3,
            execution_time = $4,
            last_failure = $5,
            consecutive_failures = consecutive_failures + 1,
            parameters = $6,
            retries = $7
        WHERE name = $8 AND key = $9`, s.tableName)

	_, err = tx.Exec(query,
		uuid.Nil,
		nextRun,
		status,
		sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true},
		sql.NullTime{Time: nowUTC, Valid: true},
		parameters,
		newRetries,
		name,
		key)

	if err != nil {
		s.config.Logger.Error("failed to mark job as failed", "job", name, "error", err)
		return
	}

	// Prepare and execute status change notification
	notifyQuery, err := s.prepareStatusChangeNotification(name, key, StatusRunning, status)
	if err != nil {
		s.config.Logger.Error("failed to prepare status change notification", "error", err)
		return
	}

	if notifyQuery != nil {
		_, err = tx.ExecContext(s.ctx, notifyQuery.Query, notifyQuery.Params...)
		if err != nil {
			s.config.Logger.Error("failed to execute notification", "error", err)
			return
		}
	}

	if err = tx.Commit(); err != nil {
		s.config.Logger.Error("failed to commit transaction", "error", err)
	}
}

func (s *Scheduler) markJobSucceeded(name, key string, job Job, executionTime time.Duration) {
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		s.config.Logger.Error("failed to begin transaction", "error", err)
		return
	}
	defer tx.Rollback()

	parameters, err := json.Marshal(job.Parameters())
	if err != nil {
		s.config.Logger.Error("failed to marshal job parameters", "job", name, "error", err)
		parameters = nil
	}

	nowUTC := s.clock.Now().UTC()
	var nextRun *time.Time

	if calculator := job.NextRunCalculator(); calculator != nil {
		if next, err := calculator.NextRun(nowUTC); err == nil {
			nextRun = &next
		} else {
			s.config.Logger.Error("failed to calculate next run", "job", name, "error", err)
			return
		}
	}
	newStatus := StatusCompleted
	if nextRun != nil {
		newStatus = StatusPending
	}
	query := fmt.Sprintf(`
        UPDATE %s 
        SET status = $1,
            last_run = $2,
            picked = false,
            picked_by = $3,
            next_run = $4,
            execution_time = $5,
            last_success = $6,
            consecutive_failures = 0,
            parameters = $7,
            cancel_requested = false,
            retries = $8
        WHERE name = $9 AND key = $10`, s.tableName)

	_, err = tx.Exec(query,
		newStatus,
		nowUTC,
		uuid.Nil,
		nextRun,
		sql.NullInt64{Int64: executionTime.Milliseconds(), Valid: true},
		sql.NullTime{Time: nowUTC, Valid: true},
		parameters,
		job.MaxRetries(), // Reset retries on success for recurring jobs
		name,
		key)

	if err != nil {
		s.config.Logger.Error("failed to update job status", "job", name, "error", err)
		return
	}

	// Prepare and execute status change notification
	notifyQuery, err := s.prepareStatusChangeNotification(name, key, StatusRunning, newStatus)
	if err != nil {
		s.config.Logger.Error("failed to prepare status change notification", "error", err)
		return
	}

	if notifyQuery != nil {
		_, err = tx.ExecContext(s.ctx, notifyQuery.Query, notifyQuery.Params...)
		if err != nil {
			s.config.Logger.Error("failed to execute notification", "error", err)
			return
		}
	}

	if err = tx.Commit(); err != nil {
		s.config.Logger.Error("failed to commit transaction", "error", err)
	}
}
func (s *Scheduler) markJobCancelled(name, key string) {
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		s.config.Logger.Error("failed to begin transaction", "error", err)
		return
	}
	defer tx.Rollback()

	nowUTC := s.clock.Now().UTC()
	query := fmt.Sprintf(`
        UPDATE %s 
        SET status = $1,
            last_run = $2,
            picked = false,
            picked_by = $3,
            next_run = NULL,
            cancel_requested = false
        WHERE name = $4 AND key = $5`, s.tableName)

	_, err = tx.Exec(query,
		StatusFailed,
		nowUTC,
		uuid.Nil,
		name,
		key)

	if err != nil {
		s.config.Logger.Error("failed to mark job as cancelled", "job", name, "key", key, "error", err)
		return
	}

	// Prepare and execute status change notification
	notifyQuery, err := s.prepareStatusChangeNotification(name, key, StatusRunning, StatusFailed)
	if err != nil {
		s.config.Logger.Error("failed to prepare status change notification", "error", err)
		return
	}

	if notifyQuery != nil {
		_, err = tx.ExecContext(s.ctx, notifyQuery.Query, notifyQuery.Params...)
		if err != nil {
			s.config.Logger.Error("failed to execute notification", "error", err)
			return
		}
	}

	if err = tx.Commit(); err != nil {
		s.config.Logger.Error("failed to commit transaction", "error", err)
	}
}

func (s *Scheduler) CancelJob(name, key string) error {
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// First check if the job exists
	var exists bool
	existsQuery := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE name = $1 AND key = $2)`, s.tableName)
	err = tx.QueryRow(existsQuery, name, key).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check job existence: %w", err)
	}
	if !exists {
		return ErrJobNotFound
	}

	// Try to mark the job for cancellation if it's running
	query := fmt.Sprintf(`
        UPDATE %s
        SET cancel_requested = true
        WHERE name = $1 AND key = $2 AND picked = true
        RETURNING name`, s.tableName)

	var returnedName string
	err = tx.QueryRow(query, name, key).Scan(&returnedName)
	if err == sql.ErrNoRows {
		return ErrJobNotRunning
	}
	if err != nil {
		return fmt.Errorf("failed to request job cancellation: %w", err)
	}

	// Prepare cancel notification
	notification := statusChangeNotification{
		Type: notificationTypeCancel,
		Name: name,
		Key:  key,
	}
	payload, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal cancel notification: %w", err)
	}

	if s.config.PGLNInstance != nil {
		notifyQuery := s.config.PGLNInstance.NotifyQuery(s.notificationChan, string(payload))
		_, err = tx.ExecContext(s.ctx, notifyQuery.Query, notifyQuery.Params...)
		if err != nil {
			return fmt.Errorf("failed to execute notification: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Check if the job is running on this node and cancel it
	cancelKey := getCancelKey(name, key)
	if cancel, ok := s.cancelFuncs.Load(cancelKey); ok {
		cancel.(context.CancelFunc)()
	}

	return nil
}
func (s *Scheduler) startCancelMonitor() {
	defer s.wg.Done()
	ticker := s.clock.NewTicker(s.config.CancelCheckPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			s.checkForCancellations()
		case <-s.shutdownChan:
			return
		}
	}
}

func (s *Scheduler) checkForCancellations() {
	query := fmt.Sprintf(`
        SELECT name, key 
        FROM %s 
        WHERE picked = true 
        AND status = $1
        AND cancel_requested = true
        AND picked_by = $2`, s.tableName)

	var jobs []struct {
		Name string
		Key  string
	}
	err := s.db.Select(&jobs, query, StatusRunning, s.nodeID)
	if err != nil {
		s.config.Logger.Error("failed to check for cancelled jobs", "error", err)
		return
	}

	for _, job := range jobs {
		cancelKey := getCancelKey(job.Name, job.Key)
		if cancel, ok := s.cancelFuncs.Load(cancelKey); ok {
			cancel.(context.CancelFunc)()
		}
	}
}

func (s *Scheduler) updateHeartbeat(name, key string, stop chan struct{}) {
	ticker := s.clock.NewTicker(s.config.HeartbeatInterval)
	defer ticker.Stop()

	query := fmt.Sprintf(`
        UPDATE %s 
        SET heartbeat = $1 
        WHERE name = $2 
        AND key = $3
        AND picked_by = $4`, s.tableName)

	for {
		select {
		case <-ticker.Chan():
			_, err := s.db.Exec(query,
				s.clock.Now().UTC(),
				name,
				key,
				s.nodeID)
			if err != nil {
				s.config.Logger.Error("failed to update heartbeat", "job", name, "key", key, "error", err)
			}
		case <-stop:
			return
		}
	}
}

func (s *Scheduler) startHeartbeatMonitor() {
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

func (s *Scheduler) checkAndResetTimedOutJobs() error {
	query := fmt.Sprintf(`
        WITH timed_out_jobs AS (
            UPDATE %s
            SET picked = false, 
                picked_by = $1, 
                next_run = $2,
                status = $3
            WHERE picked = true 
            AND heartbeat < $4
            RETURNING name, key, status as prev_status
        )
        SELECT name, key, prev_status FROM timed_out_jobs`,
		s.tableName)

	rows, err := s.db.Query(query,
		uuid.Nil,
		s.clock.Now().UTC(),
		StatusPending,
		s.clock.Now().UTC().Add(-s.config.NoHeartbeatTimeout))

	if err != nil {
		return fmt.Errorf("failed to reset timed-out jobs: %w", err)
	}
	defer rows.Close()

	// Process each timed out job and send notifications
	for rows.Next() {
		var job struct {
			Name       string
			Key        string
			PrevStatus Status
		}
		if err := rows.Scan(&job.Name, &job.Key, &job.PrevStatus); err != nil {
			return fmt.Errorf("failed to scan timed-out job: %w", err)
		}

		notifyQuery, err := s.prepareStatusChangeNotification(job.Name, job.Key, job.PrevStatus, StatusPending)
		if err != nil {
			return fmt.Errorf("failed to prepare status change notification: %w", err)
		}

		if notifyQuery != nil {
			_, err = s.db.ExecContext(s.ctx, notifyQuery.Query, notifyQuery.Params...)
			if err != nil {
				return fmt.Errorf("failed to execute notification: %w", err)
			}
		}

		s.config.Logger.Debug("reset timed-out job", "job", job.Name, "key", job.Key)
	}

	return nil
}
func (s *Scheduler) startFailedAndCompletedJobCleaner() {
	defer s.wg.Done()
	ticker := s.clock.NewTicker(s.config.FailedAndCompletedJobCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			if err := s.cleanFailedAndCompletedJobs(); err != nil {
				s.config.Logger.Error("error cleaning failed and completed jobs", "error", err)
			}
		case <-s.shutdownChan:
			return
		}
	}
}

func (s *Scheduler) cleanFailedAndCompletedJobs() error {
	query := fmt.Sprintf(`
            DELETE FROM %s 
            WHERE next_run IS NULL 
            AND status IN ($1, $2) 
        AND last_run < $3`,
		s.tableName)

	_, err := s.db.Exec(query,
		StatusFailed,
		StatusCompleted,
		s.clock.Now().UTC().Add(-s.config.FailedAndCompletedJobCleanupInterval))

	if err != nil {
		return fmt.Errorf("failed to clean failed and completed jobs: %w", err)
	}
	return nil
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
	s.jobRegistryRWLock.RLock()
	jobNames := make([]string, 0, len(s.jobRegistry))
	for name := range s.jobRegistry {
		jobNames = append(jobNames, name)
	}
	s.jobRegistryRWLock.RUnlock()

	if len(jobNames) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
            DELETE FROM %s
            WHERE name NOT IN (?)
        AND heartbeat < ?`, s.tableName)

	query, args, err := sqlx.In(query, jobNames, s.clock.Now().UTC().Add(-s.config.OrphanedJobTimeout))
	if err != nil {
		return fmt.Errorf("failed to expand IN clause: %w", err)
	}

	query = s.db.Rebind(query)
	result, err := s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to clean orphaned jobs: %w", err)
	}

	if rowsAffected, err := result.RowsAffected(); err == nil && rowsAffected > 0 {
		s.config.Logger.Info("cleaned orphaned jobs", "count", rowsAffected)
	}

	return nil
}
