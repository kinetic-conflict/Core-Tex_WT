# Scheduler plan

## What are we trying to do?
We want to stop choosing a worker manually.
Instead, a scheduler should automatically pick the best worker for every task.

## How do we achieve it?
1. Workers already register with the master through heartbeat.
2. A separate metrics agent sends CPU and memory stats to the scheduler.
3. When a task is submitted, the scheduler reads:
   - active nodes from `nodes`
   - route cost from `topology`
   - load stats from `worker_metrics`
4. The scheduler computes a weighted score.
5. The node with the lowest score gets the task.
6. The scheduler calls that worker's existing `/execute` endpoint.
7. The result is saved in `jobs`.

## Why this algorithm?
Weighted least-load scheduling is simple, explainable, and good for hackathon demos.
It is better than round robin because machines are not equally free.

## Score used
score =
- 0.35 * CPU%
- 0.25 * Memory%
- 12 * active_tasks
- 6 * queued_tasks
- 0.20 * network_weight
- plus GPU penalty if the task needs GPU

Lowest score wins.
