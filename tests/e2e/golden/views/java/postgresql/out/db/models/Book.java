package db.models;

public record Book(
    long id,
    long authorId,
    String title,
    String genre
) {}
