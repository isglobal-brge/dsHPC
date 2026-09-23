# Container runner network isolation

`dshpc.container_network = "none"` is the default. Docker and Podman receive
`--network none`. Apptainer and Singularity receive
`--containall --net --network none`; their default host networking is therefore
disabled for runner execution. The admitted runner environment is passed
explicitly after containment, preserving input/output paths and configuration
without inheriting the worker environment.

Apptainer/Singularity isolation requires a Linux runtime host with network
namespaces, and either permitted unprivileged user namespaces or an
administrator-approved setuid runtime installation. Site policies that disable
network namespace creation are incompatible with the isolated default. The
runtime host needs `readlink` and Linux procfs; the container image needs
`/bin/sh`, `readlink`, `/usr/bin/env`, and mounted Linux procfs. Minimal images
without these tools fail closed.

Before starting runner code, a guard in the **same container invocation** checks
that `/proc/self/ns/net` differs from the runtime host's namespace and that
`/proc/net/dev` contains only loopback. Missing inspection facilities, refused
namespace flags, a runtime silently sharing the host namespace, or an additional
network interface cause failure. The generated step script records the exit
status and emits a clear isolation requirement message. It never retries with
networking enabled. Conflicting network arguments in administrator runner
`extra_args` are rejected when the network mode is `none`.

The checks execute on the actual runtime host, including an external HPC unit;
the submitting Rock's namespace capabilities are not used to infer remote
support. Administrators may explicitly select `container_network = "host"`
to restore the runtime's host-network behavior. For Apptainer/Singularity, other
non-`none` values retain the existing runtime/default argument behavior; they do
not select a named CNI network automatically.

This policy governs the runner's network namespace. Runtime image retrieval may
use the host network before execution. Use administrator-staged, pinned local
images when image retrieval must also be offline. Network namespaces do not
replace the existing trusted-runner, mount-confinement, or output-disclosure
contracts.

Runtime references:

- [Apptainer network virtualization](https://apptainer.org/docs/user/main/networking.html)
- [Apptainer exec containment options](https://apptainer.org/docs/user/main/cli/apptainer_exec.html)
- [Apptainer namespace requirements](https://apptainer.org/docs/admin/main/user_namespace.html)
- [SingularityCE network virtualization](https://docs.sylabs.io/guides/latest/user-guide/networking.html)
