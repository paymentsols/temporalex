# Client backends: Temporal Core (NIF) and gRPC

A `Temporalex.Client` talks to Temporal through a backend, chosen per client
with the `backend:` option. Two backends speak to a real server:

| | `Temporalex.Backend.TemporalCore` (default) | `Temporalex.Backend.Grpc` |
| --- | --- | --- |
| Transport | Temporal's Rust SDK core and client, via a Rustler NIF | Temporal's WorkflowService over gRPC, in Elixir (`grpc` with the Mint adapter, protos from hex `temporalio`) |
| Clients | yes | yes |
| Workers | yes | **no** — client-only |
| Dependencies | the NIF (precompiled, or built with Rust) | optional deps `:temporalio`, `:grpc`, `:protobuf`, `:mint` |

The gRPC backend replaces only the NIF's **client** half. Workers always run on
Temporal's Rust core through `Temporalex.Backend.TemporalCore`: a worker started
on a gRPC client fails with an error saying so. `Temporalex.Backend.Test` is the
in-memory backend for tests and implements no client calls.

## Operations

"Same" means the same `{:ok, value}` and the same `{:error, %Temporalex.…Error{}}`
struct for the same call; the parity is checked by running the client
integration suites against both backends (see "Tests" below).

| Operation (`Temporalex.Client`) | NIF | gRPC |
| --- | --- | --- |
| `start_workflow` | yes | same |
| `start_workflow` with `start_signal:` (signal-with-start) | yes | same |
| `get_result` (long-polls the close event, follows cron/retry/continue-as-new runs) | yes | same; plus `follow_runs: false` to stop at the first run and get `WorkflowContinuedAsNewError` |
| `signal_workflow` | yes | same |
| `query_workflow` (incl. `reject_condition:`) | yes | same |
| `update_workflow` (execute-update: accepted, then polled to completion) | yes | same |
| `cancel_workflow` | yes | same |
| `terminate_workflow` | yes | same |
| `describe_workflow` | yes | same map: `workflow_id`, `run_id`, `workflow_type`, `status`, `task_queue`, `history_length`, `start_time_ms`, `execution_time_ms`, `close_time_ms`, `memo` |
| `fetch_workflow_history` (decoded, or `raw: true` bytes) | yes | same decoded events; raw bytes are an equivalent `History` encoding (decode to the same events, not necessarily byte-identical) |
| `fetch_history_page` (one page; `page_token`, `wait_new_event`, `maximum_page_size`, `event_filter: :all \| :close`) | **no** — `:unsupported` | yes |
| `list_workflows` (visibility query, `page_size`, `page_token`) | **no** — `:unsupported` | yes; executions in the `describe_workflow` shape |
| `update_workflow_options` (Versioning Override: `{:pinned, deployment, build_id}`, `:auto_upgrade`, `:unset`) | **no** — `:unsupported` | yes |
| `reset_workflow` (`event_id`, `reason`, `request_id`) | **no** — `:unsupported` | yes |
| `set_worker_deployment_current_version` | **no** — `:unsupported` | yes |
| `describe_worker_deployment` | **no** — `:unsupported` | yes |
| `memo:` on `start_workflow` | **no** — silently dropped | yes |
| `request_id:` on `start_workflow` (a resent start with the same id is the same start) | **no** — dropped | yes |
| `request_id:` on `signal_workflow` / `cancel_workflow` | yes | same |
| `request_id:` on `terminate_workflow` | **no** — dropped | yes, see below |
| `priority:` on signal-with-start | **no** — dropped by sdk-rust (the `Temporalex` start surface refuses the combination) | carried (the surface refusal still applies) |
| Workers (`Temporalex.Worker`), activities, replay | yes | **no** |

An operation a backend does not support answers
`{:error, %Temporalex.TransportError{category: :unsupported}}` whose message
names the backend that does.

### Terminate and request ids

Temporal's `TerminateWorkflowExecutionRequest` has no request-id field. The gRPC
backend carries `:request_id` in the request's `identity`
(`"<identity> request_id=<id>"`) and makes a resend good: a terminate that finds
the run already closed answers `:ok` when the close event is a termination
recorded with that same request id, and `WorkflowNotFoundError` otherwise.

`reset_workflow`'s `:request_id` is passed to the server, which decides what a
resend means; measured on server 1.32, a second reset of the same base run with
the same request id starts another run.

## Payload encoding

| | NIF | gRPC |
| --- | --- | --- |
| Client payloads (input, signal/query/update args, headers, terminate details) with `payload_codec: :etf` | `binary/erlang-eterm` | `binary/erlang-eterm` |
| …with `payload_codec: :json` | **still `binary/erlang-eterm`** — the NIF client ignores the codec | `json/plain`; a value JSON cannot represent is an **error**, never sent as ETF |
| Memo on start | — | encoded with the client's codec |
| Search attributes | `json/plain` (Temporal requires it) | same |
| Decoding results, failures' details, memo | `json/plain` or ETF by the payload's `encoding`; anything else tried as ETF | same, but ETF is decoded with `binary_to_term(data, [:safe])`; `binary/null` is `nil`; `binary/plain` is returned as the binary (the NIF reports a payload conversion error) |

**The deliberate difference.** With `payload_codec: :json` the gRPC backend
refuses — `{:error, %Temporalex.TransportError{category: :payload_conversion}}`
— any value `Jason` cannot encode: tuples, keyword lists, pids, references,
functions, structs without a `Jason.Encoder`, binaries that are not UTF-8. The
NIF backend sends every client payload as ETF whatever the codec, and the
worker's JSON encoder falls back to ETF for such values. A deployment whose
specification forbids ETF on the wire (DSF's TMP-007) needs the refusal: a
silent fallback would put ETF in history where nothing else can read it. The
error message names the kind of value, never the value itself. `Jason` encodes
atoms (other than `true`, `false`, `nil`) as strings, so they arrive as strings.

**Safe decoding.** ETF from the server is decoded with `[:safe]`, so a payload
cannot create atoms. A result naming an atom this node has never loaded is a
`:payload_conversion` error rather than a new atom. Workflow code shared between
the worker and the client node is loaded on both, so its atoms exist.

The worker path is unchanged on both backends: workers run on the NIF.

## Errors

Both backends report failures as the same public structs
(`WorkflowNotFoundError`, `WorkflowAlreadyStartedError` with the existing run
id, `WorkflowFailedError` with the `Temporalex.Failure.*` tree,
`WorkflowCancelledError`, `WorkflowTerminatedError`, `WorkflowTimedOutError`,
`QueryRejectedError`, `UpdateFailedError`, `TransportError`,
`ClientUnavailableError`), built from the same internal reasons. The gRPC
backend maps gRPC `NOT_FOUND` to not-found, `ALREADY_EXISTS` on a start to
already-started (decoding the run id from the status details),
`DEADLINE_EXCEEDED` and its own wait limit to the same timeout error the NIF's
await produces, and the rest to `TransportError` with category `:rpc`.

The **message text** of an `:rpc` or `:connect` error differs: the NIF formats
tonic's status, the gRPC backend writes `"<StatusName>: <server message>"`.
Match on struct and category, not on message.

## What the NIF client does not implement

Through `Temporalex.Backend.TemporalCore`, a client cannot: fetch one page of
history or long-poll for new events (`fetch_history_page`); list or query
workflows by visibility query (`list_workflows`); set or clear a Versioning
Override (`update_workflow_options`); reset a workflow (`reset_workflow`); set
or read a Worker Deployment's routing (`set_worker_deployment_current_version`,
`describe_worker_deployment`); set a memo on start; or make a start or terminate
idempotent with a request id. Payloads it sends are always ETF.

## Choosing a backend, and falling back to the NIF

The backend is a per-client option, so one application can run both:

```elixir
children = [
  # Hosts the worker: workers need the NIF.
  {Temporalex.Client,
   name: MyApp.TemporalWorkerClient,
   backend: Temporalex.Backend.TemporalCore,
   target: "https://temporal.internal:7233",
   tls: tls, api_key: token},
  {Temporalex.Worker, client: MyApp.TemporalWorkerClient, workflows: [...], activities: [...]},

  # Application-side calls over gRPC: listing, reset, versioning, JSON payloads.
  {Temporalex.Client,
   name: MyApp.Temporal,
   backend: Application.get_env(:my_app, :temporal_client_backend, Temporalex.Backend.Grpc),
   target: "https://temporal.internal:7233",
   tls: tls, api_key: token,
   payload_codec: :json}
]
```

Both backends take the same client options — `:target` (or `:url` /
`:address`), `:namespace`, `:task_queue`, `:api_key` (sent as
`authorization: Bearer <key>`), `:headers`, `:tls` (`true`, or a keyword list
with inline or `_file` PEMs and `:domain` as the server name to verify), the
`:connect_timeout`, `:start_timeout`, `:completion_timeout` and
`:workflow_result_timeout`, and `:payload_codec`. The gRPC backend also takes
`:identity` (default `"temporalex-<os pid>"`, as the NIF) and
`:reconnect_attempts` (default 10). So falling back is a one-line change:
set the client's `backend:` to `Temporalex.Backend.TemporalCore` — or point the
configuration key above at it — and restart the client. Code that calls only
the operations both support keeps working; calls to gRPC-only operations then
return `:unsupported`, and with `payload_codec: :json` payloads go out as ETF
again.

## Dependencies

The gRPC backend's dependencies are **optional** in Temporalex's `mix.exs`, so a
NIF-only application fetches and compiles none of them, and
`Temporalex.Backend.Grpc` is not compiled. To use it, add them to the
application:

```elixir
{:temporalio, "~> 1.63"},   # Temporal API protos (protobuf-elixir)
{:grpc, "~> 1.0.5"},
{:protobuf, "~> 0.17.0"},
{:mint, "~> 1.7"}           # grpc's pure-Elixir HTTP/2 client; gun is not needed
```

An application that runs no workers and whose clients all use the gRPC
backend can compile Temporalex **without the NIF**, so it needs no Rust
toolchain and downloads no precompiled library:

```elixir
# config/config.exs
config :temporalex, nif: false
```

It is a compile-time setting (recompile Temporalex after changing it:
`mix deps.compile temporalex --force`). With it, `Temporalex.Backend.TemporalCore`
refuses to start a client, replay or worker with an error naming the setting,
and `rustler` can be left out of the application's dependencies.

Notes:

- `grpc` pulls in `googleapis`, which declares `elixir: "~> 1.18"`. The backend
  is built and tested on Elixir 1.18.4 / OTP 27; on 1.17 Mix warns about that
  requirement.
- `tls: true` without a CA uses the OS trust store (`:public_key.cacerts_get/0`);
  `castore` is not required.
- The gRPC channel lives under the `:grpc` application's supervisor; the backend
  ties it to the client process, so it is closed when the client stops or dies.
- The backend connects with `GetSystemInfo`, as the NIF's client does, so a
  wrong address or a refused credential fails the client's start.

## Tests

`Temporalex.TestSupport.Backends` runs the client integration suites —
`client_api`, `temporal_client_semantics`, `fetch_history`,
`signal_with_start`, `structured_errors`, `json_codec`, `client_tls` — once per
backend that is compiled in, with the worker on the NIF client.
`grpc_backend_test.exs` covers the gRPC-only operations, request ids, the
`:json` refusal and cross-backend parity of describe and history;
`test/temporalex/backend/grpc_test.exs` covers the conversions without a
server.
