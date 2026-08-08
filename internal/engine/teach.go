package engine

// The teach layer. Every verdict Brace produces carries an explanation, because
// "UNSAFE" on its own teaches nobody anything — the product's actual output is
// the reader understanding which lock was taken, who had to wait for it, and
// what the safe form does differently.
//
// Everything in this file is about PostgreSQL 16 specifically. Several of these
// facts are version-dependent (ADD COLUMN with a constant default stopped
// rewriting the table in PG11; REINDEX CONCURRENTLY arrived in PG12), so this
// map must be revisited if Brace ever targets another major version.

// Classification names a migration shape Brace recognises. It is the key into
// the teach map and the thing the UI branches on.
type Classification string

const (
	ClassCreateIndex             Classification = "create_index"
	ClassCreateIndexConcurrently Classification = "create_index_concurrently"
	ClassDropIndex               Classification = "drop_index"
	ClassDropIndexConcurrently   Classification = "drop_index_concurrently"
	ClassReindex                 Classification = "reindex"
	ClassReindexConcurrently     Classification = "reindex_concurrently"
	ClassAlterColumnType         Classification = "alter_column_type"
	ClassAddColumnConstDefault   Classification = "add_column_const_default"
	ClassAddColumnVolatile       Classification = "add_column_volatile_default"
	ClassAddConstraint           Classification = "add_constraint"
	ClassAddConstraintNotValid   Classification = "add_constraint_not_valid"
	ClassValidateConstraint      Classification = "validate_constraint"
	ClassSetNotNull              Classification = "set_not_null"
	ClassAlterTableConstantTime  Classification = "alter_table_constant_time"
	ClassAlterTableOther         Classification = "alter_table_other"
	ClassAlterIndex              Classification = "alter_index"
	ClassAlterIndexRewrite       Classification = "alter_index_rewrite"
	ClassVacuumFull              Classification = "vacuum_full"
	ClassVacuum                  Classification = "vacuum"
	ClassAnalyze                 Classification = "analyze"
	ClassComment                 Classification = "comment"
	ClassGeneric                 Classification = "generic"
)

// Lesson is one entry in the teach map.
type Lesson struct {
	// Title is a short human name for the shape, for the UI's heading.
	Title string `json:"title"`
	// ExpectedLock is the strongest lock this shape takes, in PostgreSQL's own
	// spelling. Empty when the shape is too broad to predict.
	ExpectedLock string `json:"expected_lock,omitempty"`
	// BlocksWrites is Brace's PREDICTION, from the shape alone. The rehearsal
	// is what actually decides the verdict; disagreement between the two is
	// interesting and is surfaced rather than hidden.
	BlocksWrites bool `json:"blocks_writes"`
	// TriviallySafe marks shapes where a rehearsal would show a flat line: the
	// statement touches catalog rows only. The API uses this to answer "this
	// one's already safe" instead of burning a twin on it.
	TriviallySafe bool `json:"trivially_safe"`
	// ScalesWithRows says the work this shape does is O(rows): an index build, a
	// table rewrite, a validation scan. It is what makes a measurement taken on
	// a sample extrapolable to a production table — and, just as importantly,
	// what makes extrapolation ILLEGITIMATE when it is false. A catalog-only
	// ADD COLUMN takes the same microseconds at ten million rows and at five
	// hundred million, so scaling its measurement by 50x would manufacture an
	// outage that cannot happen. See scale.go.
	//
	// Note this is about the DURATION of the lock, not its presence: a shape can
	// take ACCESS EXCLUSIVE and still not scale (ALTER INDEX ... RENAME), and a
	// shape can scale without blocking writes at all (VALIDATE CONSTRAINT,
	// CREATE INDEX CONCURRENTLY) — where the growing cost is wall-clock and I/O
	// rather than a freeze.
	ScalesWithRows bool `json:"scales_with_rows"`
	// ScaleUncertain marks the catch-all shapes (ClassAlterTableOther,
	// ClassGeneric) where ScalesWithRows is a can't-rule-it-out default rather than
	// a known fact: some forms under them rewrite the table, some are catalog-only,
	// and the SQL alone does not say which. For these the scale layer must NOT
	// manufacture a freeze by projecting the 250ms resolution floor — that would
	// be a false-UNSAFE on a statement that did no measurable work. It may still
	// project a freeze the rehearsal actually MEASURED above the floor, because a
	// real measurement outranks any prediction. The definitely-O(rows) shapes
	// (CREATE INDEX, SET NOT NULL, ALTER COLUMN TYPE, ...) leave this false, so
	// their floor measurements still project as an upper bound.
	ScaleUncertain bool `json:"scale_uncertain,omitempty"`
	// Explanation is the teach text: which lock, who waits, why the safe form
	// differs. One short paragraph, technically accurate.
	Explanation string `json:"explanation"`
	// Suggestion is the safe rewrite, when one exists.
	Suggestion string `json:"suggestion,omitempty"`
}

// lessons is the curated map. Every Classification constant MUST have an entry
// with a non-empty Explanation — TestTeachMapIsComplete enforces it.
var lessons = map[Classification]Lesson{
	ClassCreateIndex: {
		Title:          "CREATE INDEX",
		ExpectedLock:   "ShareLock",
		BlocksWrites:   true,
		ScalesWithRows: true,
		Explanation: "A plain CREATE INDEX takes a SHARE lock on the table and holds it for the entire build. " +
			"SHARE conflicts with ROW EXCLUSIVE, which is the lock every INSERT, UPDATE and DELETE takes — so for as " +
			"long as the index is building, every write queues, and any write with a lock_timeout or a request deadline " +
			"dies waiting. Reads are untouched: SELECT takes ACCESS SHARE, which does not conflict with SHARE, which is " +
			"why this outage looks like \"the site is up but nothing saves\".",
		Suggestion: "CREATE INDEX CONCURRENTLY",
	},
	ClassCreateIndexConcurrently: {
		Title:          "CREATE INDEX CONCURRENTLY",
		ExpectedLock:   "ShareUpdateExclusiveLock",
		BlocksWrites:   false,
		ScalesWithRows: true,
		Explanation: "CONCURRENTLY takes only SHARE UPDATE EXCLUSIVE, which does not conflict with the ROW EXCLUSIVE " +
			"lock writes take — so writes keep committing throughout. It pays for that with two table passes plus a wait " +
			"for every transaction older than the build, so it takes longer and cannot run inside a transaction block. " +
			"The one catch: if the build fails (a unique violation, a cancelled session), it leaves an INVALID index " +
			"behind that still costs write overhead but is never used for queries. Drop it and retry — that is a " +
			"recoverable annoyance, not an outage.",
	},
	ClassDropIndex: {
		Title:          "DROP INDEX",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: false,
		Explanation: "DROP INDEX takes ACCESS EXCLUSIVE on the parent table. The drop itself is fast — it is a catalog " +
			"update plus unlinking files — but ACCESS EXCLUSIVE conflicts with everything, including SELECT, so it must " +
			"wait for every in-flight statement on the table to finish before it can start. On a busy table that wait is " +
			"where the damage happens: the pending ACCESS EXCLUSIVE request queues ahead of new arrivals, so a single " +
			"long-running query can turn an instant DROP into a total stall.",
		Suggestion: "DROP INDEX CONCURRENTLY",
	},
	ClassDropIndexConcurrently: {
		Title:          "DROP INDEX CONCURRENTLY",
		ExpectedLock:   "ShareUpdateExclusiveLock",
		BlocksWrites:   false,
		ScalesWithRows: false,
		Explanation: "DROP INDEX CONCURRENTLY marks the index dead, waits for every transaction that could still be " +
			"using it, and only then removes it — holding SHARE UPDATE EXCLUSIVE rather than ACCESS EXCLUSIVE, so reads " +
			"and writes carry on. It is slower in wall-clock terms and cannot run in a transaction block, which is a " +
			"trade almost every production system should take.",
	},
	ClassReindex: {
		Title:          "REINDEX",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: true,
		Explanation: "A plain REINDEX rebuilds the index in place under an ACCESS EXCLUSIVE lock on the index and a " +
			"SHARE lock on the table, so writes are blocked for the whole rebuild and, for REINDEX TABLE, reads too. " +
			"REINDEX CONCURRENTLY builds a replacement alongside the original and swaps it in, keeping traffic alive.",
		Suggestion: "REINDEX INDEX CONCURRENTLY",
	},
	ClassReindexConcurrently: {
		Title:          "REINDEX CONCURRENTLY",
		ExpectedLock:   "ShareUpdateExclusiveLock",
		BlocksWrites:   false,
		ScalesWithRows: true,
		Explanation: "REINDEX CONCURRENTLY (PostgreSQL 12+) builds a fresh copy of the index under SHARE UPDATE " +
			"EXCLUSIVE, then swaps it for the old one under a brief ACCESS EXCLUSIVE that lasts a catalog update rather " +
			"than a rebuild. Writes keep flowing; a failure can leave an INVALID leftover index, which is cleaned up " +
			"with DROP INDEX.",
	},
	ClassAlterColumnType: {
		Title:          "ALTER TABLE ... ALTER COLUMN ... TYPE",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: true,
		Explanation: "Changing a column's type takes ACCESS EXCLUSIVE and, in almost every case, rewrites the entire " +
			"table plus every index on it. ACCESS EXCLUSIVE conflicts with every other lock mode, so reads AND writes " +
			"stop for the full duration of that rewrite — minutes or hours on a large table. The safe pattern is to add " +
			"a new column, backfill it in batches, keep it in sync, and swap the names in a short transaction.",
		Suggestion: "ALTER TABLE t ADD COLUMN new_col <type>;  -- then backfill in batches, sync, and rename",
	},
	ClassAddColumnConstDefault: {
		Title:          "ALTER TABLE ... ADD COLUMN (constant or no default)",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   false,
		ScalesWithRows: false,
		TriviallySafe:  true,
		Explanation: "Since PostgreSQL 11 a new column with a constant default is a catalog-only change: the default is " +
			"stored as the column's \"missing value\" and existing rows are never touched. The statement still takes " +
			"ACCESS EXCLUSIVE, but only for the microseconds it takes to write a catalog row — the risk is not the lock " +
			"itself, it is waiting to GET the lock behind a long-running query. There is nothing here for Brace to crash.",
	},
	ClassAddColumnVolatile: {
		Title:          "ALTER TABLE ... ADD COLUMN with a volatile DEFAULT",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: true,
		Explanation: "The PostgreSQL 11 fast path only applies to a default that is constant. A volatile default — " +
			"now(), random(), gen_random_uuid(), nextval() — has to produce a different value per row, so the whole " +
			"table is rewritten under ACCESS EXCLUSIVE, blocking reads and writes for the duration. The same is true " +
			"of a SERIAL / BIGSERIAL column (which is just a nextval() default), a GENERATED ... AS IDENTITY column, " +
			"and a GENERATED ALWAYS AS (expr) STORED column: each computes a value for every existing row. Add the " +
			"column without a default, backfill in batches, then set the default for future rows.",
		Suggestion: "ALTER TABLE t ADD COLUMN c <type>;  -- backfill in batches, then ALTER COLUMN c SET DEFAULT ...",
	},
	ClassAddConstraint: {
		Title:          "ALTER TABLE ... ADD CONSTRAINT",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: true,
		Explanation: "Adding a CHECK or FOREIGN KEY constraint the ordinary way takes ACCESS EXCLUSIVE (and, for a " +
			"foreign key, a lock on the referenced table too) and holds it while every existing row is validated. " +
			"UNIQUE and PRIMARY KEY are worse still: they build an index under that same lock. The safe path is two " +
			"steps — ADD CONSTRAINT ... NOT VALID takes the lock only long enough to record the constraint, then " +
			"VALIDATE CONSTRAINT scans the table under SHARE UPDATE EXCLUSIVE, which does not block writes.",
		Suggestion: "ALTER TABLE t ADD CONSTRAINT c ... NOT VALID;  -- then: ALTER TABLE t VALIDATE CONSTRAINT c;",
	},
	ClassAddConstraintNotValid: {
		Title:          "ALTER TABLE ... ADD CONSTRAINT ... NOT VALID",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   false,
		ScalesWithRows: false,
		Explanation: "NOT VALID is the first half of the safe two-step: the constraint is recorded and enforced for new " +
			"and updated rows, but existing rows are not scanned, so the ACCESS EXCLUSIVE lock is held for a catalog " +
			"write rather than a table scan. Finish with VALIDATE CONSTRAINT, which takes the weaker SHARE UPDATE " +
			"EXCLUSIVE and lets writes continue while it verifies the backlog.",
		Suggestion: "ALTER TABLE t VALIDATE CONSTRAINT c;",
	},
	ClassValidateConstraint: {
		Title:          "ALTER TABLE ... VALIDATE CONSTRAINT",
		ExpectedLock:   "ShareUpdateExclusiveLock",
		BlocksWrites:   false,
		ScalesWithRows: true,
		Explanation: "VALIDATE CONSTRAINT is the second half of the safe two-step. It scans the whole table to prove " +
			"the existing rows satisfy a NOT VALID constraint, but it does so under SHARE UPDATE EXCLUSIVE, which does " +
			"not conflict with the ROW EXCLUSIVE lock writes take. The scan can be long; nobody is blocked while it runs.",
	},
	ClassSetNotNull: {
		Title:          "ALTER TABLE ... SET NOT NULL",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: true,
		Explanation: "SET NOT NULL takes ACCESS EXCLUSIVE and scans every row to prove none is null, blocking reads and " +
			"writes for the length of that scan. Since PostgreSQL 12 the scan is skipped when an existing, already " +
			"validated CHECK (col IS NOT NULL) constraint proves the same thing — so the safe path is to add that CHECK " +
			"as NOT VALID, VALIDATE it without blocking writes, then SET NOT NULL, which becomes a catalog-only change.",
		Suggestion: "ALTER TABLE t ADD CONSTRAINT c CHECK (col IS NOT NULL) NOT VALID;  -- VALIDATE, then SET NOT NULL",
	},
	ClassAlterTableConstantTime: {
		Title:          "ALTER TABLE (catalog-only)",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: false,
		Explanation: "This ALTER TABLE form — DROP COLUMN, RENAME, SET/DROP DEFAULT, SET STORAGE and the like — changes " +
			"catalog metadata only: it never scans or rewrites a row, so it finishes in microseconds regardless of table " +
			"size. It still takes ACCESS EXCLUSIVE, which conflicts with every other lock mode including plain SELECT, so " +
			"the real risk is not the work, it is the wait to ACQUIRE that lock behind a long-running query — while the " +
			"request is queued, every statement that arrives after it queues too. Because the work is not O(rows), a " +
			"bigger table does not make it slower, and Brace does not project a scale freeze for it.",
	},
	ClassAlterTableOther: {
		Title:          "ALTER TABLE",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: true,
		ScaleUncertain: true,
		Explanation: "Most forms of ALTER TABLE take ACCESS EXCLUSIVE, the strongest lock PostgreSQL has: it conflicts " +
			"with every other mode, so reads and writes both stop. Whether that is a blip or an outage depends entirely " +
			"on whether the statement also rewrites the table — which is exactly the thing that is hard to predict from " +
			"the SQL, and exactly what this rehearsal measures.",
	},
	ClassAlterIndex: {
		Title:          "ALTER INDEX ... RENAME",
		ExpectedLock:   "ShareUpdateExclusiveLock",
		BlocksWrites:   false,
		ScalesWithRows: false,
		Explanation: "On PostgreSQL 12+ ALTER INDEX ... RENAME takes only SHARE UPDATE EXCLUSIVE, which does NOT conflict " +
			"with the ROW EXCLUSIVE lock that INSERT, UPDATE and DELETE take — so writes keep flowing while the rename " +
			"happens. It is a catalog-only change that finishes in microseconds; the rename does not block traffic and " +
			"does not grow with the table. (Other ALTER INDEX forms, such as SET TABLESPACE, take ACCESS EXCLUSIVE and " +
			"are a different story — see below.)",
	},
	ClassAlterIndexRewrite: {
		Title:          "ALTER INDEX (ACCESS EXCLUSIVE forms)",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: true,
		Explanation: "Unlike a plain RENAME, forms such as ALTER INDEX ... SET TABLESPACE take ACCESS EXCLUSIVE on the " +
			"index and physically move or rewrite it, so reads and writes against the table wait for the whole operation, " +
			"and its cost grows with the size of the index. ACCESS EXCLUSIVE conflicts with every other lock mode, so a " +
			"long-running query in front of it makes the wait worse still. Move indexes during a maintenance window, or " +
			"rebuild CONCURRENTLY into the new location instead.",
	},
	ClassVacuumFull: {
		Title:          "VACUUM FULL",
		ExpectedLock:   "AccessExclusiveLock",
		BlocksWrites:   true,
		ScalesWithRows: true,
		Explanation: "VACUUM FULL rewrites the entire table into a fresh file under ACCESS EXCLUSIVE. Reads and writes " +
			"are blocked for the whole rewrite, and it needs enough free disk for a second copy of the table. Plain " +
			"VACUUM reclaims space for reuse under SHARE UPDATE EXCLUSIVE without blocking anybody; it does not return " +
			"space to the filesystem, which is nearly always the right trade.",
		Suggestion: "VACUUM (ANALYZE) t;",
	},
	ClassVacuum: {
		Title:          "VACUUM",
		ExpectedLock:   "ShareUpdateExclusiveLock",
		BlocksWrites:   false,
		ScalesWithRows: true,
		Explanation: "Plain VACUUM takes SHARE UPDATE EXCLUSIVE, which does not conflict with reads or writes — traffic " +
			"continues throughout. It competes for I/O rather than for locks, so the visible cost is a shallow dip in " +
			"throughput, not a freeze. Only one VACUUM at a time may run on a given table.",
	},
	ClassAnalyze: {
		Title:          "ANALYZE",
		ExpectedLock:   "ShareUpdateExclusiveLock",
		BlocksWrites:   false,
		ScalesWithRows: false,
		TriviallySafe:  true,
		Explanation: "ANALYZE samples rows to refresh the planner's statistics under SHARE UPDATE EXCLUSIVE, which does " +
			"not conflict with reads or writes. It is one of the few things you can run on a hot table at any hour " +
			"without thinking about it.",
	},
	ClassComment: {
		Title:          "COMMENT ON",
		BlocksWrites:   false,
		ScalesWithRows: false,
		TriviallySafe:  true,
		Explanation: "COMMENT ON writes a single row of documentation into the catalog and touches no table data. It " +
			"holds a lock on the commented object only long enough for that catalog write.",
	},
	ClassGeneric: {
		Title:          "Unrecognised migration",
		BlocksWrites:   true,
		ScalesWithRows: true,
		ScaleUncertain: true,
		Explanation: "Brace does not recognise this statement's shape, so it has no prediction to offer — which is " +
			"precisely when a rehearsal is worth the most. The twin will tell you what lock it takes and what it costs " +
			"live traffic, and the verdict below is measured, not guessed.",
	},
}

// Explain returns the lesson for a classification. An unknown classification
// falls back to the generic entry rather than an empty Lesson, so no verdict
// ever ships without teach text.
func Explain(c Classification) Lesson {
	if l, ok := lessons[c]; ok {
		return l
	}
	return lessons[ClassGeneric]
}

// Classifications lists every classification Brace knows, for tests and for an
// API that wants to publish the teach map.
func Classifications() []Classification {
	return []Classification{
		ClassCreateIndex, ClassCreateIndexConcurrently,
		ClassDropIndex, ClassDropIndexConcurrently,
		ClassReindex, ClassReindexConcurrently,
		ClassAlterColumnType,
		ClassAddColumnConstDefault, ClassAddColumnVolatile,
		ClassAddConstraint, ClassAddConstraintNotValid, ClassValidateConstraint,
		ClassSetNotNull, ClassAlterTableConstantTime, ClassAlterTableOther,
		ClassAlterIndex, ClassAlterIndexRewrite,
		ClassVacuumFull, ClassVacuum, ClassAnalyze, ClassComment,
		ClassGeneric,
	}
}
