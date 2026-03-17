/// Compile-time adapter contract consumed by the Java core emitter.
///
/// Holds the small set of language-level constants that vary per JVM language.
/// The JDBC layer is engine-agnostic, so there is no generated helper file and
/// no target-specific SQL normalization for JVM backends.
pub(super) struct JvmCoreContract {
    /// Statement-end token appended after JDBC bind calls (`";"` for Java).
    pub(super) statement_end: &'static str,
    /// Fallback row type used when a query has no result columns (`"Object[]"` for Java).
    pub(super) fallback_type: &'static str,
}

/// Resolve the Java adapter contract.
pub(super) fn resolve_java_contract() -> JvmCoreContract {
    JvmCoreContract { statement_end: ";", fallback_type: "Object[]" }
}
