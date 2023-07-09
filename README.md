## What is Airflow ?
Airflow is an open-source platform designed for **programmatically authoring**, **scheduling**, and **monitoring** workflows. It provides a flexible framework for defining complex data pipelines as **Directed Acyclic Graphs (DAGs)** and executing tasks within those pipelines. Airflow enables the orchestration of tasks, tracks their dependencies, handles retries and failures, and offers a web-based user interface for monitoring and managing workflows. It is widely used in the data engineering and data science communities for building and managing scalable, reliable, and maintainable **data pipelines**.

## Airflow Components
![Core Components](./assets/core-components.png)

- Airflow is an orchestrator, not a processing framework. Process your gigabytes of data outside of Airflow (i.e. You have a Spark cluster, you use an operator to execute a Spark job, and the data is processed in Spark).
- A **DAG** is a data pipeline, an **Operator** is a task.
- An **Executor** defines how your tasks are executed, whereas a **worker** is a process executing your task
- The **Scheduler** schedules your tasks, the **web server** serves the UI, and the **database** stores the metadata of Airflow.

### One Node Architecture
![One Node Architecture](./assets/one-node-architecture.png)

### Multi Node Architecture
![Multi Node Architecture](./assets/multi-node-architecture.png)

### How does it Work ?
![Step 01](./assets/flow-step-1.png)
![Step 02](./assets/flow-step-2.png)
![Step 03](./assets/flow-step-3.png)
![Step 04](./assets/flow-step-4.png)

### DAG
![DAG Concept](./assets/dag-concept.png)

### Operator
![Operator Do AND Don't](./assets/operator-do-dont.png)

#### 3 Types of Operator
- **Action Operators**: _Execute_ an action (Python Operators & Bash Operators)
- **Transfer Operators**: _Transfer_ data
- **Sensors**: _Wait_ for a Condition to be met

### Hooks
![Hook Concept](./assets/hook.png)   

In Apache Airflow, a hook is a way to interact with external systems or services within your workflows. It provides a high-level interface to connect and interact with various systems, such as databases, cloud services, message queues, and more. Hooks abstract the implementation details of interacting with these systems, providing a consistent and simplified interface.

### DAG Scheduling
- **start_date**: the timestamp from which the scheduler will attempt to backfill
- **scheduler_interval**: How often a DAG runs
- **end_date**: The timestamp from which a DAG ends

![Dag Scheduling Concept 01](./assets/dag-schedule-1.png)

> A DAG is triggered **AFTER** the `start_date/last_run + the schedule_interval`

![Dag Scheduling Concept 02](./assets/dag-schedule-2.png)

### Backfilling
![Backfill and Catchup](./assets/backfill-catchup.png)


### Dataset

In Apache Airflow, a dataset refers to a collection of data that is used or manipulated within a workflow. It represents a logical unit of data that can be read, transformed, or written during the execution of tasks within a data pipeline.   
Datasets in Airflow are typically represented as variables or parameters within operators or as inputs and outputs between tasks. The task dependencies and relationships are defined based on the datasets, ensuring that the tasks are executed in the correct order, with the necessary inputs available.   

- Two Property
  - `URI` 
    - Unique identifier of your data
    - Path to your data
    - Must compose of only ASCII characters
    - The URI scheme cannot be `airflow`
    - Case Sensitive
  - `Extra`
    ```python
    from airflow import Dataset
    my_file = Dataset(
        uri='s3://dataset/file.csv',
        extra={'owner': 'nilanjan.deb'}
    )   
    ```
![Dataset](./assets/dataset-workflow.png)

#### Limitations
- DAGs can only use Datasets in the same Airflow instance. A DAG cannot wait for a Dataset defined in another Airflow instance.
- Consumer DAGs are triggered every time a task that updates datasets completes successfully. Airflow doesn't check whether the data has been effectively updated. 
- You can't combine different schedules like datasets with cron expressions. 
- If two tasks update the same dataset, as soon as one is done, that triggers the Consumer DAG immediately without waiting for the second task to complete. 
- Airflow monitors datasets only within the context of DAGs and Tasks. If an external tool updates the actual data represented by a Dataset, Airflow has no way of knowing that.
