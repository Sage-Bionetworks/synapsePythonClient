<!-- Last reviewed: 2026-03 -->

## Project

Authentication credential providers implementing a chain-of-responsibility pattern for token resolution.

## Conventions

### Provider chain order (priority)
1. **UserArgsCredentialsProvider** — explicit login args passed to `syn.login()`
2. **ConfigFileCredentialsProvider** — `~/.synapseConfig` file (profile-aware via sections)
3. **EnvironmentVariableCredentialsProvider** — `SYNAPSE_AUTH_TOKEN` env var
4. **AWSParameterStoreCredentialsProvider** — AWS SSM Parameter Store (via `SYNAPSE_TOKEN_AWS_SSM_PARAMETER_NAME` env var)

### Profile selection
Select profile via `SYNAPSE_PROFILE` env var or `--profile` CLI arg. If username provided in login args differs from config file username, config credentials are rejected — prevents ambiguity.

### Token handling
`SynapseAuthTokenCredentials` implements `requests.auth.AuthBase`, adding `Authorization: Bearer` header. JWT validation failure is silent (logs warning, does not raise) — allows tokens with unrecognized formats to attempt API calls.

## Constraints

- Bearer tokens must never appear in logs — redact with `BEARER_TOKEN_PATTERN` regex before logging.
