package db.models;

public record BookSummaries(
    long id,
    String title,
    String genre,
    String authorName
) {}
