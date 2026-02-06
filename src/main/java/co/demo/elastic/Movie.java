package co.demo.elastic;

public record Movie(
    String movie_id,
    String movie_name,
    Integer year,
    String genre,
    String description,
    String director
) {
}
