pub(super) use crate::backend::jdbc::JvmCoreContract;

/// Resolve the Java adapter contract.
pub(super) fn resolve_java_contract() -> JvmCoreContract {
    JvmCoreContract { statement_end: ";", fallback_type: "Object[]", size_access: ".size()" }
}
