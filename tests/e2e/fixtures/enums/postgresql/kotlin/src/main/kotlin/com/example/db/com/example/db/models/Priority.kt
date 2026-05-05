package com.example.db.models

enum class Priority(val value: String) {
    LOW("low"),
    MEDIUM("medium"),
    HIGH("high"),
    CRITICAL("critical");

    companion object {
        fun fromValue(value: String): Priority =
            entries.first { it.value == value }
    }
}
