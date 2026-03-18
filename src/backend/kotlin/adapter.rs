pub(super) use crate::backend::jdbc::JvmCoreContract;

/// Resolve the Kotlin adapter contract.
pub(super) fn resolve_kotlin_contract() -> JvmCoreContract {
    JvmCoreContract { statement_end: "", fallback_type: "Any", size_access: ".size" }
}
