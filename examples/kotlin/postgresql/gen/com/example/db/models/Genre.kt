package com.example.db.models

enum class Genre(val value: String) {
    FICTION("fiction"),
    NON_FICTION("non_fiction"),
    SCIENCE("science"),
    HISTORY("history"),
    BIOGRAPHY("biography");

    companion object {
        fun fromValue(value: String): Genre =
            entries.first { it.value == value }
    }
}
