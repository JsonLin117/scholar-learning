# TaskFlow API — Study Notes (2026-05-24)

## Core Concept
TaskFlow API = Python decorators (`@task` / `@dag`) for DAG authoring.
- Function calls = dependency declarations
- Return values = automatic XCom push/pull
- 50% less boilerplate than traditional PythonOperator

## Key Mechanisms
1. `@dag` replaces `with DAG()` context manager
2. `@task` turns function into Task + auto XCom
3. `XComArg` object returned on function call (lazy, not immediate execution)
4. `multiple_outputs=True` splits dict into individual XComs
5. `.override()` for task reuse with different params
6. `@task.virtualenv`, `@task.docker`, `@task.kubernetes` for isolated envs

## Dynamic Task Mapping Integration
```python
@task
def add(x: int, y: int): return x + y
add.partial(y=10).expand(x=[1, 2, 3])  # 3 parallel tasks
```

## Anti-patterns
- Don't pass large objects through XCom → pass paths only
- Don't define @task outside @dag function
- Don't forget `multiple_outputs=True` when returning dict for key access
- Don't use TaskFlow for non-Python tasks (Bash, SparkSubmit)

## ODM Supply Chain Application
- Multi-vendor dynamic ETL with expand()
- BOM change notification pipeline with multiple_outputs
- Gradual migration: new DAGs = TaskFlow, old DAGs = mix
