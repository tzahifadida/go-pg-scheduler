# PGScheduler

PGScheduler is a robust and flexible distributed job scheduling library for Go applications using PostgreSQL as a backend. It supports both recurring and one-time jobs, with features like automatic retries, heartbeat monitoring, and orphaned job cleanup.

⭐️ Please Star This Project

If you find this project useful, please consider giving it a star ⭐️ on GitHub. It helps others find the project and shows your support!

## Features

- Support for recurring (cron-based) and one-time jobs
- Ensures only one instance of a job runs at a time across distributed nodes
- Automatic retries for failed jobs
- Heartbeat monitoring to detect and reset stalled jobs
- Orphaned job cleanup (delayed, to support rollbacks)
- Configurable node job concurrency limits
- Support for both `lib/pq` and `jackc/pgx` or any other database/sql based drivers
- Custom schema support for job tables
- Optional PGLN integration for improved responsiveness
- Support for job-specific keys to allow multiple instances of the same job type

### Distributed Execution

PGScheduler ensures that only one instance of a job runs at a time across distributed nodes. This is achieved through a locking mechanism in the database. When a node picks up a job to run, it sets a lock in the database. Other nodes will see this lock and skip the job, preventing multiple executions of the same job across different nodes.

### PGLN Integration

PGScheduler optionally integrates with PGLN (PostgreSQL Listen/Notify) to provide immediate responsiveness for job scheduling and cancellation. When enabled:

- Jobs start processing immediately upon scheduling
- Job cancellations are processed instantly
- Maintains polling as a fallback mechanism for reliability
- Automatic recovery of missed events during disconnections
- Transactional notifications ensure consistency

### Status Change Callbacks

When PGLN is configured, PGScheduler can notify your application about job status changes immediately through callbacks. This helps reduce polling overhead by enabling reactive behavior:

```go
config := pgscheduler.SchedulerConfig{
    // ... other configuration
    PGLNInstance: pglnInstance, // Required for status callbacks
    JobStatusChangeCallback: func(name, key string, prevStatus, newStatus Status) {
        // React to status changes immediately instead of waiting for next poll
        if newStatus == StatusCompleted {
            triggerNextAction()
        }
    },
}
```

**Note:** Since PGLN can disconnect and miss events, implement fallback polling for critical operations.

Status transitions: `"" -> "pending"` (scheduled), `"pending" -> "running"` (started), `"running" -> "pending/failed/completed"` (finished).

## Installation

To install PGScheduler, use `go get`:

```bash
go get github.com/tzahifadida/go-pg-scheduler
```

### Optional PGLN Dependency

If you want to use the PGLN integration for improved responsiveness:

```bash
go get github.com/tzahifadida/pgln
```

## Job Interface

To create a job, implement the `Job` interface:

```go
type Job interface {
    Name() string
    Key() string
    Run(ctx context.Context) error
    NextRunCalculator() NextRunCalculator
    Parameters() interface{}
    MaxRetries() int
}
```

The interface includes:
- `Name()`: Returns the job type identifier
- `Key()`: Returns a unique key for this job instance
- `Run()`: Contains the job's execution logic
- `NextRunCalculator()`: Defines when the job should run next (cron or interval)
- `Parameters()`: Provides job-specific parameters
- `MaxRetries()`: Specifies maximum retry attempts on failure

### NextRunCalculator Interface

```go
type NextRunCalculator interface {
    NextRun(now time.Time) (time.Time, error)
}
```

PGScheduler provides two implementations:
- `CronSchedule`: For cron-based scheduling
- `FixedInterval`: For interval-based scheduling

## Configuration

```go
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
    Schema                               string
    JobStatusChangeCallback              func(name, key string, prevStatus, newStatus Status)
    PGLNInstance                         *pgln.PGListenNotify // Required for status callbacks
}
```

## Usage Examples

### Basic Scheduler Setup

```go
import (
    "database/sql"
    "github.com/tzahifadida/go-pg-scheduler"
    _ "github.com/lib/pq"
)

func main() {
    db, err := sql.Open("postgres", "postgres://user:password@localhost/dbname?sslmode=disable")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    config := pgscheduler.SchedulerConfig{
        DB:           db,
        DBDriverName: "postgres",
        Schema:       "custom_schema", // Optional
        // ... other configuration
    }

    scheduler, err := pgscheduler.NewScheduler(config)
    if err != nil {
        panic(err)
    }

    // Initialize and start
    if err := scheduler.Init(); err != nil {
        panic(err)
    }
    if err := scheduler.Start(); err != nil {
        panic(err)
    }
    defer scheduler.Shutdown()
}
```

### Setup with PGLN Integration

```go
import (
    "github.com/tzahifadida/pgln"
    "github.com/tzahifadida/go-pg-scheduler"
)

func main() {
    // Create PGLN instance
    pglnBuilder := pgln.NewPGListenNotifyBuilder().
        SetContext(ctx).
        SetDB(db).
        SetReconnectInterval(5 * time.Second)

    pglnInstance, err := pglnBuilder.Build()
    if err != nil {
        panic(err)
    }
    err = pglnInstance.Start()
    if err != nil {
        panic(err)
    }
    defer pglnInstance.Shutdown()

    // Create scheduler with PGLN
    config := pgscheduler.SchedulerConfig{
        DB:           db,
        DBDriverName: "postgres",
        PGLNInstance: pglnInstance,
        // ... other configuration
    }

    scheduler, err := pgscheduler.NewScheduler(config)
    // ... rest of setup
}
```

### Creating a Job

```go
type MyJob struct {
    name       string
    key        string
    calculator pgscheduler.NextRunCalculator
    parameters interface{}
    maxRetries int
}

func NewMyJob(name, key string, cronExpr string) (*MyJob, error) {
    calculator, err := pgscheduler.NewCronSchedule(cronExpr)
    if err != nil {
        return nil, err
    }
    return &MyJob{
        name:       name,
        key:        key,
        calculator: calculator,
        maxRetries: 3,
    }, nil
}

// Implement Job interface
func (j *MyJob) Name() string { return j.name }
func (j *MyJob) Key() string { return j.key }
func (j *MyJob) NextRunCalculator() pgscheduler.NextRunCalculator { return j.calculator }
func (j *MyJob) Parameters() interface{} { return j.parameters }
func (j *MyJob) MaxRetries() int { return j.maxRetries }
func (j *MyJob) Run(ctx context.Context) error {
    // Job implementation
    return nil
}

// Register and schedule the job
job, _ := NewMyJob("my_job", "instance_1", "*/5 * * * *")
scheduler.RegisterJob(job)
scheduler.ScheduleJob(job)
```

## Best Practices

1. Always call `scheduler.Shutdown()` when your application is terminating
2. Use unique job keys for different instances of the same job type
3. Consider using PGLN integration for time-sensitive operations
4. Keep job execution times reasonable
5. Use appropriate retry counts and intervals
6. Implement proper error handling in your job's `Run` method
7. Use the context passed to the `Run` method for cancellation
8. Consider your PostgreSQL connection pool size when setting MaxRunningJobs
9. Implement fallback polling for critical operations when using status callbacks

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.