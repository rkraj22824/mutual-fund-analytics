> ### Rate Limiting Strategy and Proof of Correctness
>
> The system uses a unified sliding window rate limiter to enforce three limits:
>
> - 2 requests per second  
> - 50 requests per minute  
> - 300 requests per hour  
>
> A single in-memory list of request timestamps is maintained for the last hour.
>
> Before each request:
> - Count requests in last 1 second
> - Count requests in last 1 minute
> - Count requests in last 1 hour
>
> If any limit is exceeded, the system waits for the required duration and retries.
>
> #### Proof of Correctness
>
> After every request, the following invariant holds:
>
> - Requests in last 1 second < limit  
> - Requests in last 1 minute < limit  
> - Requests in last 1 hour < limit  
>
> This is enforced atomically using a mutex, ensuring no race conditions.
>
> The system always re-evaluates all constraints before proceeding, guaranteeing correctness.





> ### Coordinating Three Concurrent Limits
>
> All three rate limits are enforced using a single shared data structure.
>
> Instead of independent limiters, the system:
>
> - Evaluates all three windows together
> - Computes required wait time for each violated limit
> - Applies the maximum wait duration
>
> This ensures:
>
> - No limit is violated
> - No unnecessary waiting occurs
>
> A loop re-checks all limits after sleeping, ensuring convergence only when all constraints are satisfied.


> ### Backfill Orchestration Within Quota Constraints
>
> Backfill is handled using a single-worker sequential pipeline.
>
> Key characteristics:
>
> - One job processed at a time  
> - Worker runs every 5 seconds  
> - Each job fetches data for one scheme  
>
> Jobs are stored in a persistent queue with states:
>
> - pending  
> - running  
> - completed  
> - failed  
>
> On restart:
>
> - Running jobs are reset to pending  
> - Retries are incremented  
>
> Idempotent database writes ensure safe retries without duplication.
>
> Backfill is skipped when sufficient recent data already exists, reducing API usage.


> ### Storage Schema for Time-Series NAV Data
>
> NAV data is stored using a composite primary key:
>
> - scheme_code  
> - date  
>
> This ensures:
>
> - No duplicate records  
> - Natural data integrity  
>
> Dates are stored in ISO-8601 format:
>
> - Enables efficient sorting  
> - Supports range queries  
>
> SQLite is used because:
>
> - Dataset is small  
> - No operational overhead  
> - High read performance  
>
> A single database connection is used to avoid write contention.



> ### Pre-computation vs On-Demand Trade-offs
>
> All analytics are pre-computed after data ingestion.
>
> This avoids expensive runtime calculations such as rolling returns.
>
> Benefits:
>
> - Fast API responses  
> - Predictable latency  
>
> Trade-off:
>
> - Data may be slightly stale  
>
> This is acceptable because NAV data updates only once per day.
>
> On-demand computation is intentionally avoided to prevent latency spikes.




> ### Handling Schemes with Insufficient History
>
> Some schemes do not have enough historical data for full analysis.
>
> The system checks minimum required data before computation.
>
> If insufficient:
>
> - A default analytics record is stored  
> - All values are set to zero  
>
> This ensures:
>
> - No missing records  
> - Consistent API responses  
>
> In rankings:
>
> - Such schemes naturally appear at the bottom  
>
> Data gaps are tolerated within limits, and incomplete windows are excluded to maintain accuracy.


