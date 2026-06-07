# ota-telemetry-schema

| Field | Value |
|---|---|
| Revision | 1 |
| Created | 2026-06-07 |
| Status | scaffold |
| Part of | [Helix OTA](https://github.com/HelixDevelopment/helix_ota) |
| Language | go |
| License | Apache-2.0 |

## Purpose

Telemetry event/metric schema + codecs shared by server and agents (download/install/verify/success/failure + health).

## Boundary (decoupling)

Schema + (de)serialization only; no ingestion, no storage. Shared contract between agents and the control plane.

This is a **reusable, independently versioned** building brick (HelixConstitution
§11.4.28 submodules-as-equal-codebase). It is consumed by Helix OTA and is designed
to be reusable by other projects. It must ship in-depth documentation, user guides,
and full test coverage (§1 four-layer) before leaving `scaffold` status.

## Status

Scaffold. Implementation tracked in the Helix OTA spec corpus
(`docs/research/main_specs/`). See the master design and the submodule reuse map.

## Mirrors

- GitHub: https://github.com/HelixDevelopment/ota-telemetry-schema
- GitLab: https://gitlab.com/helixdevelopment1/ota-telemetry-schema
