"""Top-level package for custom Apache Beam components.

This package contains modular, reusable, and well-tested building blocks for
constructing robust Apache Beam pipelines. It is organized into subpackages
based on purpose and abstraction level:

- `transforms`: A collection of reusable `PTransform` components for common data processing
    patterns.

- `dofn`: Low-level `DoFn` classes that encapsulate custom logic,
    often with state or timer support.

Goals:
- Promote reuse and consistency across Beam pipelines.
- Encourage clean separation of concerns between processing logic (`DoFn`)
  and composition (`PTransform`).
- Ensure all components are easy to test and reason about.

This package is intended to scale with complex data processing needs while remaining maintainable.
"""
