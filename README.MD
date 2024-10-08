# PGScheduler

PGScheduler is a robust and flexible distributed job scheduling library for Go applications using PostgreSQL as a backend. It supports both recurring and one-time jobs, with features like automatic retries, heartbeat monitoring, and orphaned job cleanup.

⭐️ Please Star This Project

If you find this project useful, please consider giving it a star ⭐️ on GitHub. It helps others find the project and shows your support!

## Features

- Support for recurring (cron-based) and one-time jobs
- Ensures only one instance of a job runs at the same time across distributed nodes
- Automatic retries for failed jobs
- Heartbeat monitoring to detect and reset stalled jobs
- Orphaned job cleanup (delayed, to support rollbacks)
- Configurable node job concurrency limits
- Support for both `lib/pq` and `jackc/pgx` or any other database/sql based drivers
- Custom schema support for job tables

### Distributed Execution

PGScheduler ensures that only one instance of a job runs at a time across distributed nodes. This is achieved through a locking mechanism in the database. When a node picks up a job to run, it sets a lock in the database. Other nodes will see this lock and skip the job, preventing multiple executions of the same job across different nodes.

This feature is particularly useful in distributed environments where you have multiple instances of your application running, each with its own scheduler. It prevents job duplication and ensures consistent execution of recurring tasks.

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
    Schema                               string
}
```

- `Ctx`: The context for the scheduler. If not provided, a background context will be used.
- `DB`: A `*sql.DB` instance for database operations.
- `DBDriverName`: The name of the database driver ("postgres" for `lib/pq`, "pgx" for `jackc/pgx`).
- `MaxConcurrentOneTimeJobs`: The maximum number of one-time jobs that can run concurrently.
- `MaxConcurrentRecurringJobs`: The maximum number of recurring jobs that can run concurrently.
- `JobCheckInterval`: How often to check for new jobs to run.
- `OrphanedJobTimeout`: The duration after which a job is considered orphaned (default: 14 days).
- `HeartbeatInterval`: How often to update the heartbeat for running jobs.
- `NoHeartbeatTimeout`: The duration after which a job with no heartbeat is considered failed.
- `CreateSchema`: Whether to automatically create the required database schema.
- `Logger`: A `Logger` interface instance for logging.
- `RunImmediately`: Whether to run jobs immediately upon scheduler start.
- `TablePrefix`: A prefix for the database table name.
- `ShutdownTimeout`: The maximum duration to wait for jobs to complete during shutdown.
- `FailedAndCompletedOneTimeJobInterval`: How often to clean up failed and completed one-time jobs.
- `Schema`: The PostgreSQL schema to use for the job tables (default: "public").

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
        JobCheckInterval:  time.Minute,
        CreateSchema:      true,
        Schema:            "custom_schema", // Optional: specify a custom schema
    }

    scheduler, err := pgscheduler.NewScheduler(config)
    if err != nil {
        panic(err)
    }

    // also creates the schema if missing
    err = scheduler.Init()
    if err != nil {
        panic(err)
    }
    // call ScheduleJob here if you specified RunImmediately = true

    // starts job processing
    err = scheduler.Start()
    if err != nil {
        panic(err)
    }
    // call ScheduleJob here

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
    Run(ctx context.Context) error
}
```

### Recurring Job Example

```go
type MyRecurringJob struct {
    pgscheduler.JobConfig
}

func (j *MyRecurringJob) Run(ctx context.Context) error {
    // Job logic
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
        Schema:       "custom_schema", // Optional: specify a custom schema
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
        Schema:       "custom_schema", // Optional: specify a custom schema
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
2. Don't create a job Name for jobs with slightly different behavior. For one time jobs you can use parameters passing instead. Excessive job "Name"s will damage performance since each job code is kept in memory with Name as key and queries use a filter such as "IN (Names...)".
3. Use appropriate retry counts for one-time jobs to handle transient failures.
4. Implement proper error handling in your job's `Run` method.
5. Use the context passed to the `Run` method for cancellation and timeout handling.
6. Keep job execution times reasonable. For long-running jobs, consider breaking them into smaller tasks.
7. In distributed environments, be aware that only one instance of a recurring job will run at a time across all nodes.
8. When using custom schemas, ensure that your database user has the necessary permissions to create and access the specified schema.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.