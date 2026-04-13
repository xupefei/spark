# Apache Spark

## Pre-flight Checks

Before the first code read, edit, or test in a session, ensure a clean working environment. DO NOT skip these checks:

1. Run `git remote -v` to identify the personal fork and upstream (`apache/spark`). If unclear, ask the user to configure their remotes following the standard convention (`origin` for the fork, `upstream` for `apache/spark`).
2. If the latest commit on `<upstream>/master` is more than a day old (check with `git log -1 --format="%ci" <upstream>/master`), run `git fetch <upstream> master`.
3. If there are uncommitted changes (check with `git status`), ask the user to stash them before proceeding.
4. Switch to the appropriate branch:
   - **Existing PR**: resolve the PR branch name via `gh api repos/databricks-eng/runtime/pulls/<number> --jq '.head.ref'`, then look for a local branch matching that name. If found, switch to it and inform the user. If not found, ask whether to fetch it or if there is a local branch under a different name.
   - **New edits**: ask the user to choose: create a new git worktree from `<upstream>/master` and work from there (recommended), or create and switch to a new branch from `<upstream>/master`.
   - **Reading code or running tests**: use `<upstream>/master`.

## Development Notes

SQL golden file tests are managed by `SQLQueryTestSuite` and its variants. Read the class documentation before running or updating these tests. DO NOT edit the generated golden files (`.sql.out`) directly. Always regenerate them when needed, and carefully review the diff to make sure it's expected.

Spark Connect protocol is defined in proto files under `sql/connect/common/src/main/protobuf/`. Read the README there before modifying proto definitions.

## Build and Test

Build and tests can take a long time. Before running tests, ask the user if they have more changes to make.

Prefer SBT over Maven for faster incremental compilation. Module names are defined in `project/SparkBuild.scala`.

Compile a single module:

    build/sbt <module>/compile

Compile test code for a single module:

    build/sbt <module>/Test/compile

Run test suites by wildcard or full class name:

    build/sbt '<module>/testOnly *MySuite'
    build/sbt '<module>/testOnly org.apache.spark.sql.MySuite'

Run test cases matching a substring:

    build/sbt '<module>/testOnly *MySuite -- -z "test name"'

For faster iteration, keep SBT open in interactive mode:

    build/sbt
    > project <module>
    > testOnly *MySuite

### PySpark Tests

PySpark tests require building Spark with Hive support first:

    build/sbt -Phive package

Activate the virtual environment specified by the user, or default to `.venv`:

    source <venv>/bin/activate

If the default venv does not exist, create it:

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r dev/requirements.txt

Run a single test suite:

    python/run-tests --testnames pyspark.sql.tests.arrow.test_arrow

Run a single test case:

    python/run-tests --testnames "pyspark.sql.tests.test_catalog CatalogTests.test_current_database"

## Investigating PR CI Failures

Do NOT download full job logs to grep for errors — they are very large and slow. Instead, use the test report annotations on the fork.

Step 1 — Get the fork owner and the latest commit SHA of the PR:

    gh api repos/apache/spark/pulls/<PR_NUMBER> --jq '{owner: .head.repo.owner.login, sha: .head.sha}'

Step 2 — Find the "Report test results" check run on the fork's commit:

    gh api repos/<OWNER>/spark/commits/<SHA>/check-runs \
      --jq '.check_runs[] | select(.name == "Report test results") | {id: .id, annotations: .output.annotations_count}'

Step 3 — Fetch failure annotations:

    gh api repos/<OWNER>/spark/check-runs/<CHECK_RUN_ID>/annotations

Each annotation contains the test class, test name, and failure message.

## Pull Request Workflow

PR title requires a JIRA ticket ID (e.g., `[SPARK-xxxx][SQL] Title`). Ask the user to create a new ticket or provide an existing one if not given. Before writing the PR description, read `.github/PULL_REQUEST_TEMPLATE` and fill in every section from that file.

DO NOT push to the upstream repo. Always push to the personal fork. Open PRs against `master` on the upstream repo.

DO NOT force push or use `--amend` on pushed commits unless the user explicitly asks. If the remote branch has new commits, fetch and rebase before pushing.

Always get user approval before external operations such as pushing commits, creating PRs, or posting comments. Use `gh pr create` to open PRs. If `gh` is not installed, generate the GitHub PR URL for the user and recommend installing the GitHub CLI.

---

## Code Style & Review Checklist

- Names: meaningful yet concise.
- Method length: avoid big chunky methods that do multiple things.
- Line length: at most 100 chars.
- Class/method/variable access: Pick the least open modifier that works.
- Backward compatibility: New APIs don't break existing code, maintain compatibility with overloads.
- Always estimate the number of times a method will be called. If it is expected to be called in a tight loop (thousands of times), pay extra attention to performance!
- Method names
  - Descriptive yet short verb-noun: buildReplaceDataPlan, extractInputType
  - Conversion methods: Use to prefix → toInstruction, toGroupFilterCondition
  - NO getXXX: Avoid getter prefixes for simple accessors (use property-style)
  - Use `obj.property` over `obj.property()` in Scala while calling getters or simple methods without any mutation action. Even when calling Java objects from Scala.
- Variable Naming
  - Descriptive yet concise names: `groupFilterCond`, `matchedInstructions`
  - Plural words for collections: `args`, `assignments`, `metadataAttrs`
- Code Comments
  - Use ScalaDoc or Javadoc for **public APIs** with parameter descriptions
  - Use inline comments for private methods and code blocks that require explanation
  - Document non-obvious design decisions, complex logic, edge cases, performance considerations
- Import Organization (follow Databricks Scala Guide)
  - Group imports: `java.*`/`javax.*` → `scala.*` → third-party (`org.*`, `com.*`) → project (`org.apache.spark.*`)
  - Within each group: alphabetical order by **full package path**
  - For `org.apache.spark.sql.*` imports: `catalyst` comes before `connector`
  - Use absolute paths, separate groups with blank lines
- If method/class args have to be split on multiple lines, **never** leave trailing `)` on a new line. Instead, put close parenthesis on the same line with the last arg like `argN)`.
- **ALWAYS** try to find existing logical/physical plan nodes and/or expressions before creating new ones.

---

## Spark-Specific Knowledge

### LogicalPlan

- Base: `QueryPlan[LogicalPlan]`
- Key traits: `AnalysisHelper`, `LogicalPlanStats`, `QueryPlanConstraints`, `Logging`
- Node types: `LeafNode`, `UnaryNode`, `BinaryNode`
- Essential properties: `output`, `resolved`, `isStreaming`, `maxRows`
- Core methods: `resolveOperators[Up|Down|WithPruning]`, `resolveExpressions[...]`
- Analysis skips already-analyzed subtrees for efficiency.

### Analyzer

- Located in `sql/catalyst/analysis/Analyzer.scala`.
- Converts **unresolved** plans → **resolved** logical plans.
- Organizes rules in **batches** (`fixedPoint` or `Once`) for resolution and cleanup.
- Uses **TreePattern-based pruning**, **relation caching**, and **stateful two-pass rules** for performance.

### Optimizer

- Located in `sql/catalyst/optimizer/Optimizer.scala`.
- Transforms **analyzed** plans → **optimized** plans using **rule-based fixed-point execution**.
- Key optimizations: predicate/projection pushdown, join reordering, constant folding, operator elimination, subquery rewriting.

### Physical Planning (`SparkPlan`)

- Located in `sql/core/execution`.
- Represents **executable** operations (`RDD[InternalRow]`), vs. LogicalPlan (semantic only).
- Node types: `LeafExecNode`, `UnaryExecNode`, `BinaryExecNode`
- Strategies map `LogicalPlan` → `SparkPlan` (e.g., `JoinSelection`, `Aggregation`, `SpecialLimits`).

### Data Source V2 (DSv2)

Everything under `org.apache.spark.sql.connector` is considered DSv2.

Key interfaces:
- `Table`, `SupportsRead`, `SupportsWrite`, `SupportsRowLevelOperations`
- `RowLevelOperation` — logical representation of DELETE, UPDATE, or MERGE
- `SupportsDelta` — mix-in for connectors that can handle row deltas
- `RequiresAggregateFiltering` — mix-in for connectors that need aggregate pre-computation

**DSv2 Relation Lifecycle**:

```
Table (analysis) → DataSourceV2Relation (logical) → DataSourceV2ScanRelation (scan planned) → BatchScanExec (physical)
```

### Development Tips

- **Rule patterns:**
    - Single-pass: `plan.resolveOperators { case ... => ... }`
    - Two-pass: Collect state → Transform plan
- **Performance:**
    - Use pruning (`_.containsPattern`) and caches to skip redundant traversals.
