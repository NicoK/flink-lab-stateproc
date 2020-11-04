# Lab: State Processor API

This project consists of 3 modules:

- `bootstrap` creates a new savepoint via the state processor API
- `flink-job` defines a Flink job that should be able to resume from the savepoint created by `bootstrap`
- `common` defines shared classes that are used by both modules, e.g. data objects

## Usage

1. Create a new savepoint by running `BootstrapJob`:

    ```
    ./gradlew :bootstrap:run
    ```

   This will create a new savepoint under `/tmp/flink-test/state-proc/savepoint1`
2. Start a Flink cluster.
3. Submit the job and resume from the created savepoint:

    ```bash
    flink run -p 1 -s "file:///tmp/flink-test/state-proc/savepoint1" flink-job/build/libs/lab-stateproc-flink-job-1.11-SNAPSHOT-all.jar
    ```

4. Check that the job is running and produces this output:

    ```
    (1,1.5)
    (2,1.0)
    (3,1.0)
    (4,1.0)
    ```