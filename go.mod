// =============================================================================
// BERMUDA Stealth Gateway NG — Production Module Manifest
// Go 1.24 Language Version Baseline
//
// Architectural Mandate: Zero Third-Party Dependencies
// The entire edge data plane, process supervisor, and L7 reverse proxy
// rely exclusively on the Go 1.24 standard library to ensure:
//  - Minimal static binary footprint (~9MB stripped with AVX2 acceleration)
//  - Deterministic memory usage (confined within the 128MiB soft ceiling)
//  - Zero external supply-chain attack surface
//  - Sub-second deterministic Docker builds without transitive module fetches
// =============================================================================

module bermuda-gateway

go 1.24
