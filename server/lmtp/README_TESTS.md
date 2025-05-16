# LMTP Server Tests

This directory contains tests for the LMTP server implementation.

## Test Structure

The tests are organized into several files:

- `lmtp_test.go`: Basic tests that verify the package exists and can be imported
- `basic_test.go`: Tests for basic structures and functionality
- `error_test.go`: Tests for error handling
- `server_test.go`: Tests for the LMTP server functionality (currently skipped)
- `session_test.go`: Tests for the LMTP session functionality (currently skipped)
- `integration_test.go`: Integration tests for the LMTP server (currently skipped)
- `mock_test.go`: Mock implementations for testing

## Current Test Coverage

The current test coverage is minimal (around 2.0% of statements). This is because most of the tests are currently skipped due to issues with the mock implementations.

## Expanding the Tests

To expand the tests and improve coverage, follow these steps:

1. Fix the mock implementations in `mock_test.go`:
   - Ensure that the mock implementations correctly implement the required interfaces
   - Update the mock methods to return appropriate values

2. Implement the skipped tests in `server_test.go`:
   - Test the `New` function to create a new LMTP server
   - Test the `LMTPServerBackend_NewSession` function to create a new session

3. Implement the skipped tests in `session_test.go`:
   - Test the `Mail` method to handle the MAIL FROM command
   - Test the `Rcpt` method to handle the RCPT TO command
   - Test the `Data` method to handle the DATA command
   - Test the `Reset` method to reset the session
   - Test the `Logout` method to handle the QUIT command

4. Implement the skipped tests in `integration_test.go`:
   - Test the LMTP server with a real LMTP client
   - Test concurrent connections
   - Test error handling

5. Add additional tests as needed:
   - Test edge cases and error conditions
   - Test with different configurations
   - Test with different message sizes and formats

## Running the Tests

To run the tests, use the following command:

```bash
cd server/lmtp && go test -v
```

To run the tests with coverage, use:

```bash
cd server/lmtp && go test -cover
```

To generate a coverage report, use:

```bash
cd server/lmtp && go test -coverprofile=coverage.out && go tool cover -html=coverage.out
