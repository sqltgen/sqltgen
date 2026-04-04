package db.models;

public record Customer(
    long id,
    String name,
    String email
) {}
