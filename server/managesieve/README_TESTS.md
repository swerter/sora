# ManageSieve Server Tests

This directory contains tests for the ManageSieve server implementation.

## Test Structure

The tests are organized into several files:

- `managesieve_test.go`: Basic tests that verify the package exists and can be imported
- `basic_test.go`: Tests for basic structures and functionality
- `error_test.go`: Tests for error handling
- `server_test.go`: Tests for the ManageSieve server functionality
- `session_test.go`: Tests for the ManageSieve session functionality (currently skipped)
- `integration_test.go`: Integration tests for the ManageSieve server (currently skipped)
- `mock_test.go`: Mock implementations for testing
- `pipe_conn_test.go`: Pipe connection implementation for testing

## Current Test Coverage

The current test coverage is minimal. This is because most of the tests are currently skipped due to the complexity of mocking network connections and other dependencies.

## Expanding the Tests

To expand the tests and improve coverage, follow these steps:

1. Fix the mock implementations in `mock_test.go`:
   - Ensure that the mock implementations correctly implement the required interfaces
   - Update the mock methods to return appropriate values

2. Implement the skipped tests in `session_test.go`:
   - Test the `LOGIN` command to handle user authentication
   - Test the `LISTSCRIPTS` command to list available scripts
   - Test the `GETSCRIPT` command to retrieve a script
   - Test the `PUTSCRIPT` command to store a script
   - Test the `SETACTIVE` command to activate a script
   - Test the `DELETESCRIPT` command to delete a script
   - Test the `NOOP` command to do nothing
   - Test the `LOGOUT` command to end the session

3. Implement the skipped tests in `integration_test.go`:
   - Test the ManageSieve server with a real ManageSieve client
   - Test concurrent connections
   - Test error handling
   - Test timeout handling

4. Add additional tests as needed:
   - Test edge cases and error conditions
   - Test with different configurations
   - Test with different script sizes and formats

## Running the Tests

To run the tests, use the following command:

```bash
cd server/managesieve && go test -v
```

To run the tests with coverage, use:

```bash
cd server/managesieve && go test -cover
```

To generate a coverage report, use:

```bash
cd server/managesieve && go test -coverprofile=coverage.out && go tool cover -html=coverage.out
