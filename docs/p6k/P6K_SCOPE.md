# P6K Scope

P6K is the Zephyr-dev Pro-pre core hardening line.

Its purpose is to strengthen the common kernel that future Pro and Web product layers can consume
without moving commercial logic into Zephyr-dev.

P6K does:

- harden Zephyr-dev kernel contracts and bounded runtime surfaces
- prepare future common-kernel needs for Pro and Web
- keep Zephyr-dev as the pure core
- focus on AI data workflow kernel hardening

P6K does not:

- modify Zephyr-base in this line
- replace the P6 six-repo commercial architecture
- replace Zephyr-Web-core entitlement, license, billing, or gating
- add SaaS, multi-tenant, cloud, or Kubernetes behavior
- add commercial logic to Zephyr-dev

P6K-M0 scope is audit-only:

- document the current kernel baseline
- document the current gap versus unstructured 0.22.28 enhanced partition support
- document current package-manifest, structured-sync, destination-mapping, and Base non-regression
  boundaries
- freeze the milestone execution sequence

P6K-M1 is the first implementation slice and targets selective Enhanced Partition Profiles on
`unstructured==0.22.28`.
