package db.models;

public record Author(
    long id,
    String name,
    String bio,
    Integer birthYear
) {}
