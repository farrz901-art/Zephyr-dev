# P6K Execution Plan

P6K is a Zephyr-dev core hardening line for future Pro/Web common-kernel needs.
It does not modify Zephyr-base and does not add commercial logic.

## P6K-M0: Baseline Audit

- Objective: Freeze the current Zephyr-dev kernel baseline and the next-step hardening plan.
- Input prerequisites: P6 M1-M3 complete, No dependency upgrade in scope
- Deliverables: P6K scope, roadmap, non-goal, and execution-plan docs, Gap audit tools and validation JSON outputs, Aggregate baseline audit report
- Completion criteria: Current kernel baseline is documented, Unstructured 0.22.28 gap is documented without runtime change, PackageManifest, it-stream, destination, and Base boundaries are documented
- Dependencies: None
- Non-goals: No runtime implementation, No CLI enhancement, No dependency upgrade
- Risk: Audit can drift if later milestones change implementation without updating the frozen baseline.
- Parallelizable: no

## P6K-M1: Unstructured 0.22.28 Enhanced Partition Profile

- Objective: Upgrade to unstructured 0.22.28 and add selective enhanced partition profiles.
- Input prerequisites: P6K-M0 audit approved, Benchmark input path recorded
- Deliverables: Version upgrade and compatibility layer, Profile selection layer, CLI flags for enhanced partition parameters, Metadata preservation tests
- Completion criteria: Selective enhanced profiles work, Governance metadata is preserved, Base regression boundary remains intact
- Dependencies: P6K-M0
- Non-goals: No PackageManifest yet, No workflow runtime yet, No commercial logic
- Risk: Version and metadata-shape drift can regress Base bundle compatibility if not bounded.
- Parallelizable: no

## P6K-M2: PackageManifestV1 / package_id / artifact descriptors

- Objective: Add package-aware artifact identity without changing commercial boundaries.
- Input prerequisites: P6K-M1 partition outputs stabilized
- Deliverables: PackageManifestV1, package_id and artifact descriptor model, package-aware content evidence plan
- Completion criteria: Artifact identity collision risk is removed, Destination mapping can consume package-aware descriptors
- Dependencies: P6K-M1
- Non-goals: No destination remap yet, No workflow runtime yet
- Risk: Artifact layout changes can break replay and Base compatibility if wrapper paths are not preserved.
- Parallelizable: no

## P6K-M3: Partitioner enhanced profiles real benchmark

- Objective: Benchmark enhanced partition behavior against planned Chinese invoice and contract samples.
- Input prerequisites: P6K-M1 complete, Benchmark samples available locally
- Deliverables: Benchmark scripts, Evidence snapshots, Regression summary
- Completion criteria: Expected element classes appear, Metadata richness improves on target samples
- Dependencies: P6K-M1
- Non-goals: No new runtime surface beyond M1
- Risk: Sample variance can hide regressions if evidence is not normalized.
- Parallelizable: yes

## P6K-M4: it-stream Airbyte-like structured sync hardening

- Objective: Add bounded Airbyte-like structured sync semantics on the existing it-stream path.
- Input prerequisites: P6K-M0 gap audit, Checkpoint semantics remain bounded
- Deliverables: Structured sync contract layer, Per-stream/per-slice state model, Bounded retry and recovery semantics
- Completion criteria: Airbyte-like subset is explicit, No uncontrolled runtime expansion occurs
- Dependencies: P6K-M0
- Non-goals: No batch workflow runtime yet, No SaaS sync platform behavior
- Risk: Broadening into consumer-group or platform semantics would violate current it-stream boundaries.
- Parallelizable: no

## P6K-M5: Package-aware destination mapping

- Objective: Map future package-aware artifacts into retained destinations.
- Input prerequisites: P6K-M2
- Deliverables: Destination-specific mapping rules, Schema/index/table/key migration plan, Compatibility wrappers where needed
- Completion criteria: Each retained destination has a package-aware mapping plan, Replay and delivery contracts remain bounded
- Dependencies: P6K-M2
- Non-goals: No new destination families
- Risk: Destination schema churn can fragment replay and inspection surfaces.
- Parallelizable: yes

## P6K-M6: HTML Composer Node

- Objective: Add an internal HTML composition node after package-aware artifacts exist.
- Input prerequisites: P6K-M2
- Deliverables: HTML node contract, Artifact mapping, Node-level tests
- Completion criteria: HTML node is bounded and package-aware
- Dependencies: P6K-M2
- Non-goals: No public Base HTML support
- Risk: HTML behavior can blur kernel vs product boundaries if surfaced too early.
- Parallelizable: yes

## P6K-M7: Chunker Node + ZephyrChunk

- Objective: Add typed chunk artifacts for downstream embed/search work.
- Input prerequisites: P6K-M2
- Deliverables: ZephyrChunk contract, Chunker node, Chunk artifact descriptors
- Completion criteria: Chunk artifacts are package-aware and replay-safe
- Dependencies: P6K-M2
- Non-goals: No vector DB product layer
- Risk: Chunk identity drift can multiply collision and replay complexity.
- Parallelizable: yes

## P6K-M8: Embedder Node + EmbeddingProvider

- Objective: Add core embedding provider abstractions without commercial gating.
- Input prerequisites: P6K-M7
- Deliverables: EmbeddingProvider interface, Embedder node, Embedding artifacts
- Completion criteria: Embeddings are typed, package-aware, and optional
- Dependencies: P6K-M7
- Non-goals: No Pro billing or quota logic
- Risk: Heavy dependency and model-surface creep can violate Base non-regression boundaries.
- Parallelizable: no

## P6K-M9: Batch Workflow Runtime minimum

- Objective: Introduce the minimum workflow runtime after node and package primitives exist.
- Input prerequisites: P6K-M2, P6K-M6, P6K-M7, P6K-M8
- Deliverables: Workflow model, Node execution chain, Batch runtime minimum
- Completion criteria: Workflow execution remains bounded and replayable
- Dependencies: P6K-M2, P6K-M6, P6K-M7, P6K-M8
- Non-goals: No SaaS orchestration plane
- Risk: Runtime expansion can reopen orchestration boundaries if introduced before package identity is stable.
- Parallelizable: no

## P6K-M10: Pro/Web SDK/API surface

- Objective: Expose the hardened kernel to future Pro/Web product layers without commercial logic in Zephyr-dev.
- Input prerequisites: P6K-M9
- Deliverables: Stable SDK/API surfaces, Compatibility wrappers, Integration guidance
- Completion criteria: Kernel surfaces are stable and product-facing layers remain external
- Dependencies: P6K-M9
- Non-goals: No Web-core entitlement or billing in Zephyr-dev
- Risk: Surface leakage can collapse kernel and commercial boundaries.
- Parallelizable: no

## P6K-M11: Chinese benchmark suite

- Objective: Expand benchmark coverage with Chinese-focused document samples and evidence capture.
- Input prerequisites: P6K-M1, P6K-M3
- Deliverables: Benchmark suite, Evidence matrix, Regression thresholds
- Completion criteria: Benchmark evidence is reproducible and versioned
- Dependencies: P6K-M1, P6K-M3
- Non-goals: No product feature expansion
- Risk: Benchmark sprawl can turn into a hidden runtime contract if not scoped.
- Parallelizable: yes

## P6K-M12: LLMProvider / Extract / NER / Table Description

- Objective: Add bounded AI extraction provider contracts after workflow primitives exist.
- Input prerequisites: P6K-M9
- Deliverables: Provider interfaces, Extract/NER/table-description nodes
- Completion criteria: Provider layer is typed, optional, and non-commercial
- Dependencies: P6K-M9
- Non-goals: No billing or entitlement logic
- Risk: Model-provider coupling can leak product policy into the kernel.
- Parallelizable: yes

## P6K-M13: DeepSeek-OCR2 fallback experiment

- Objective: Run a bounded OCR fallback experiment without changing the default runtime path.
- Input prerequisites: P6K-M1, P6K-M12
- Deliverables: Experiment harness, Evidence summary, Boundary assessment
- Completion criteria: Fallback remains experimental and optional
- Dependencies: P6K-M1, P6K-M12
- Non-goals: No default OCR route change
- Risk: Experimental OCR can silently broaden dependency and runtime surfaces.
- Parallelizable: yes

## P6K-M14: Feishu UNS + IT sources

- Objective: Add bounded Feishu document and structured sources after prior kernel layers are ready.
- Input prerequisites: P6K-M4, P6K-M9
- Deliverables: Feishu source contracts, Flow-specific adapters, Focused tests
- Completion criteria: Feishu sources fit existing flow ownership rules
- Dependencies: P6K-M4, P6K-M9
- Non-goals: No SaaS workspace sync platform
- Risk: Feishu source breadth can blur structured vs document flow boundaries.
- Parallelizable: no
