package com.example.db.models

enum class Status(val value: String) {
    OPEN("open"),
    IN_PROGRESS("in_progress"),
    DONE("done"),
    CANCELLED("cancelled");

    companion object {
        fun fromValue(value: String): Status =
            entries.first { it.value == value }
    }
}
