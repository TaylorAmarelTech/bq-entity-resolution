"""Pipeline package: DAG-based pipeline orchestration.

Core components:
- dag: Build a StageDAG from config
- plan: Create an immutable PipelinePlan from a DAG
- executor: Execute a plan against a Backend
- validator: Compile-time contract validation
- gates: Runtime data quality assertions
- diagnostics: Structured error reporting
"""
