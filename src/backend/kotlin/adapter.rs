/// Compile-time adapter contract consumed by the Kotlin core emitter.
///
/// Holds the small set of language-level constants that vary per JVM language.
/// The JDBC layer is engine-agnostic, so there is no generated helper file and
/// no target-specific SQL normalization for JVM backends.
pub(super) struct JvmCoreContract {
    /// Statement-end token appended after JDBC bind calls (`""` for Kotlin).
    pub(super) statement_end: &'static str,
    /// Fallback row type used when a query has no result columns (`"Any"` for Kotlin).
    pub(super) fallback_type: &'static str,
}

/// Resolve the Kotlin adapter contract.
pub(super) fn resolve_kotlin_contract() -> JvmCoreContract {
    JvmCoreContract { statement_end: "", fallback_type: "Any" }
}
