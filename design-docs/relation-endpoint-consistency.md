# Relational endpoint consistency

Metadata relations and statistics use database IDs rather than foreign keys. A writer that
resolves an ID before its transaction can otherwise commit a new relation after another server
deletes that object. The existing garbage collector repairs such orphan rows, but a successful
write can be immediately unobservable to the caller. We therefore require synchronous endpoint
liveness for new tag assignments and role privileges. Statistic writes will use the same endpoint
fence in #13177. Garbage collection remains the recovery mechanism for historical rows and
failures outside these write paths.

`LiveEndpointService.lockLiveEndpoint` must run inside the same transaction as its dependent
write. It locks the observed metalake, catalog, schema, and target IDs in hierarchy order using
the backend's shared row lock (`FOR UPDATE` on H2). It then resolves the name directly from the
database and compares the entire ID chain. A deleted row, a same-name replacement, or a move
causes the transaction to fail. A column uses its table as the locking row and verifies its
column ID after the table is locked. Deletion or rename of an ancestor has to update a locked
ancestor or target row before it can commit. The guarantee ends when the dependent transaction
commits: a later delete may remove the object and its relations normally.

Lock ordering is ancestor to descendant, followed by relation rows. Tag-to-policy updates already
lock tag rows before policy rows; tag assignments to policies preserve that order after locking
the metalake. Role privilege writes lock their endpoints before inserting securable-object rows.
In #13177, statistic writers will use the shared endpoint lock rather than taking a schema lock
first, which would invert the hierarchy order. Sequential statistic value replacement remains
allowed, while concurrent updates will use `current_version` CAS.

Owner assignment already has its own metalake, target-row, and principal-row fencing. Its
single-owner semantics and any remaining name/parent identity checks belong to #13002.
Policy-to-tag endpoint locking is covered by #12679 and is not duplicated here. Existing GC
continues to repair legacy or externally produced orphan relations. No database foreign keys are
introduced because metadata deletion is soft and asynchronous.

The endpoint helper is tested against deletion, rename, and same-name replacement, including a
schema-scoped table. Controlled tag-assignment and role-privilege tests pause behind a concurrent
catalog deletion and verify that the dependent write fails without leaving a relation. The
policy-to-tag suite covers its existing lock protocol on H2, MySQL, and PostgreSQL. Statistic
tests belong to #13177.
