# TaskMaple

A simple Dockerized Go app, which you point to a MySQL/PgSQL database table, commonly named `task_pool`, which has a certain accepted shape, this table is then processed by the `TaskMaple` instance.

Upon picking up a task, the `TaskMaple` instance will initiate a POST request to the configured HTTP destination, your application, usually expected to live behind a load balancer (e.g. hitting an ALB which has a ECS instance cluster behind it), then responds synchronously with either the task failure, or the task success.

Using HTTP is admittedly not the most efficient technology, however, it is extremely simple, and allows for the use of `TaskMaple` in pretty much any small to medium production environment.

## Features

### Task Insertion

`TaskMaple` owns the post-insertion logic around picking up tasks, handling exponential back-offs, throttling based on load, and updating the DB to mark success/failure.

The insertion of tasks is left entirely up to you, this on one hand, gives you flexibility to BYOORM (Bring your own ORM), or insert with RawSQL, or insert from a no-code autoamtion, `TaskMaple` does not care.

There are many permutations of how companies may do this, theres no one-size-fits-all solution, so we leave this up to you.

> You can also utilise transactions for task insertion, letting your application atomically save DB state to other tables, whilst inserting tasks.

### Tasks vs Other solutions

The `Task` model is effectively just a mostly-FIFO queue, nothing special, especially coming from other solutions such as SQS.

`Tasks` have some fundamental advantages, depending on your use case:

- Ability to transactionally insert tasks with other DB operations
- Idempotency for task insertion, so you don't insert duplicates
- Processing invokes your application over HTTP, no short polling of SQS/etc needed.
- No client libraries required, we provide some examples for Go/Node/Nest to help you get started.
- Very visible, if you've got monitoring of your incoming HTTP requests you've got monitoring of task processing
- No need to create new infrastructure to add a new Task, just add a row to the DB with a new "type" string value, it's entirely up to your application how you wish to serialize/de-serialize these tasks

There are some disadvantages:

- At very high scale, this may not be as suitable as a fully distributed system such as SQS
- `TaskMaple` relies upon polling tasks from a MySQL/PgSQL so more load will be incurred on your DB
