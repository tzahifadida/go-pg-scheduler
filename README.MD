# PGScheduler

PGScheduler is a robust and flexible job scheduling library for Go applications using PostgreSQL as a backend. It supports both recurring and one-time jobs, with features like automatic retries, heartbeat monitoring, and orphaned job cleanup.

## Features

- Support for recurring (cron-based) and one-time jobs
- Automatic retries for failed jobs
- Heartbeat monitoring to detect and reset stalled jobs
- Orphaned job cleanup
- Configurable concurrency limits
- Support for both `lib/pq` and `jackc/pgx` database drivers
- Ensures only one instance of a recurring job runs at a time across distributed nodes

## Installation

To install PGScheduler, use `go get`:

```bash
go get github.com/tzahifadida/go-pg-scheduler
```

## Configuration

PGScheduler is highly configurable. Here's an explanation of the `SchedulerConfig` struct:

```go
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
    Logger                               *slog.Logger
    RunImmediately                       bool
    TablePrefix                          string
    ShutdownTimeout                      time.Duration
    FailedAndCompletedOneTimeJobInterval time.Duration
}
```

- `Ctx`: The context for the scheduler. If not provided, a background context will be used.
- `DB`: A `*sql.DB` instance for database operations.
- `DBDriverName`: The name of the database driver ("postgres" for `lib/pq`, "pgx" for `jackc/pgx`).
- `MaxConcurrentJobs`: The maximum number of jobs that can run concurrently.
- `JobCheckInterval`: How often to check for new jobs to run.
- `OrphanedJobTimeout`: The duration after which a job is considered orphaned (default: 14 days).
- `HeartbeatInterval`: How often to update the heartbeat for running jobs.
- `NoHeartbeatTimeout`: The duration after which a job with no heartbeat is considered failed.
- `CreateSchema`: Whether to automatically create the required database schema.
- `Logger`: A `slog.Logger` instance for logging.
- `RunImmediately`: Whether to run jobs immediately upon scheduler start.
- `TablePrefix`: A prefix for the database table name.
- `ShutdownTimeout`: The maximum duration to wait for jobs to complete during shutdown.
- `FailedAndCompletedOneTimeJobInterval`: How often to clean up failed and completed one-time jobs.

## Usage

### Creating a Scheduler

```go
import (
    "database/sql"
    "github.com/tzahifadida/go-pg-scheduler"
    _ "github.com/lib/pq"
)

func main() {
    db, err := sql.Open("postgres", "your_connection_string")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    config := pgscheduler.SchedulerConfig{
        DB:                db,
        DBDriverName:      "postgres",
        MaxConcurrentJobs: 10,
        JobRetentionDays:  7,
        JobCheckInterval:  time.Minute,
        CreateSchema:      true,
    }

    scheduler, err := pgscheduler.NewScheduler(config)
    if err != nil {
        panic(err)
    }

    err = scheduler.Init()
    if err != nil {
        panic(err)
    }

    // Use the scheduler...

    scheduler.Shutdown()
}
```

### Defining Jobs

To create a job, implement the `Job` interface:

```go
type Job interface {
    Name() string
    CronSchedule() string
    JobType() JobType
    Parameters() interface{}
    Retries() int
    Init(ctx context.Context) error
    Run(ctx context.Context) error
    Shutdown(ctx context.Context) error
}
```

### Recurring Job Example

```go
type MyRecurringJob struct {
    pgscheduler.JobConfig
}

func (j *MyRecurringJob) Init(ctx context.Context) error {
    // Initialization logic
    return nil
}

func (j *MyRecurringJob) Run(ctx context.Context) error {
    // Job logic
    return nil
}

func (j *MyRecurringJob) Shutdown(ctx context.Context) error {
    // Cleanup logic
    return nil
}

// Create and schedule the job
jobConfig, err := pgscheduler.NewRecurringJobConfig("my_recurring_job", "*/5 * * * *")
if err != nil {
    panic(err)
}

job := &MyRecurringJob{JobConfig: *jobConfig}
err = scheduler.ScheduleJob(job)
if err != nil {
    panic(err)
}
```

### One-Time Job Example with Parameter Passing

```go
type MyOneTimeJob struct {
    pgscheduler.JobConfig
}

func (j *MyOneTimeJob) Init(ctx context.Context) error {
    // Initialization logic
    return nil
}

func (j *MyOneTimeJob) Run(ctx context.Context) error {
    // Retrieve parameters from context
    params := ctx.Value(pgscheduler.ClusterSchedulerParametersKey)
    if params != nil {
        // Use the parameters
        fmt.Printf("Job parameters: %v\n", params)
    }
    // Job logic
    return nil
}

func (j *MyOneTimeJob) Shutdown(ctx context.Context) error {
    // Cleanup logic
    return nil
}

// Create and schedule the job
params := map[string]interface{}{
    "key1": "value1",
    "key2": 42,
}
jobConfig, err := pgscheduler.NewOneTimeJobConfig("my_one_time_job", params, 3)
if err != nil {
    panic(err)
}

job := &MyOneTimeJob{JobConfig: *jobConfig}
err = scheduler.ScheduleJob(job)
if err != nil {
    panic(err)
}
```

### Distributed Execution of Recurring Jobs

PGScheduler ensures that only one instance of a recurring job runs at a time across distributed nodes. This is achieved through a locking mechanism in the database. When a node picks up a job to run, it sets a lock in the database. Other nodes will see this lock and skip the job, preventing multiple executions of the same job across different nodes.

This feature is particularly useful in distributed environments where you have multiple instances of your application running, each with its own scheduler. It prevents job duplication and ensures consistent execution of recurring tasks.

## Database Driver Examples

### Using `lib/pq`

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
        // ... other configuration options
    }

    scheduler, err := pgscheduler.NewScheduler(config)
    if err != nil {
        panic(err)
    }

    // Use the scheduler...
}
```

### Using `jackc/pgx`

```go
import (
    "github.com/jackc/pgx/v4/stdlib"
    "github.com/tzahifadida/go-pg-scheduler"
)

func main() {
    connConfig, err := pgx.ParseConfig("postgres://user:password@localhost/dbname")
    if err != nil {
        panic(err)
    }

    db := stdlib.OpenDB(*connConfig)
    defer db.Close()

    config := pgscheduler.SchedulerConfig{
        DB:           db,
        DBDriverName: "pgx",
        // ... other configuration options
    }

    scheduler, err := pgscheduler.NewScheduler(config)
    if err != nil {
        panic(err)
    }

    // Use the scheduler...
}
```

## Best Practices

1. Always call `scheduler.Shutdown()` when your application is terminating to ensure proper cleanup.
2. Use appropriate retry counts for one-time jobs to handle transient failures.
3. Implement proper error handling in your job's `Run` method.
4. Use the context passed to the `Run` method for cancellation and timeout handling.
5. Keep job execution times reasonable. For long-running jobs, consider breaking them into smaller tasks.
6. In distributed environments, be aware that only one instance of a recurring job will run at a time across all nodes.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.