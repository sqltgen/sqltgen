package com.example.db.models;

public record Task(
    long id,
    String title,
    Priority priority,
    Status status,
    String description,
    java.util.List<Priority> tags
) {}
