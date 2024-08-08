package com.real.matcher;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MatcherImpl implements Matcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MatcherImpl.class);
  private final Map<Integer, List<Movie>> movieDb = new HashMap<>();
  private final Map<Integer, List<Role>> roleDb = new HashMap<>();
  private final Map<String, List<Integer>> directorDb = new HashMap<>();

  public MatcherImpl(CsvStream movieDb, CsvStream actorAndDirectorDb) {
    LOGGER.info("Importing database");
    importMovieDatabase(movieDb);
    importActorsAndDirectors(actorAndDirectorDb);
    LOGGER.info("Database imported");
  }

  private void importMovieDatabase(CsvStream movieDb) {
    try (CSVParser parser = CSVParser.parse(movieDb.getHeaderRow() + "\n" + String.join("\n", movieDb.getDataRows().collect(Collectors.toList())), CSVFormat.DEFAULT.withHeader())) {
      for (CSVRecord record : parser) {
        int id = Integer.parseInt(record.get("id"));
        String title = record.get("title");
        int year = Optional.ofNullable(record.get("year"))
            .filter(s -> !s.isEmpty() && !s.equals("NULL"))
            .map(Integer::parseInt)
            .orElse(0);

        Movie movie = new Movie(id, title, year);
        this.movieDb.computeIfAbsent(year, k -> new ArrayList<>()).add(movie);
      }
    } catch (IOException e) {
      LOGGER.error("Error importing movie database", e);
    }
  }

  private void importActorsAndDirectors(CsvStream actorAndDirectorDb) {
    try (CSVParser parser = CSVParser.parse(actorAndDirectorDb.getHeaderRow() + "\n" + String.join("\n", actorAndDirectorDb.getDataRows().collect(Collectors.toList())), CSVFormat.DEFAULT.withHeader())) {
      for (CSVRecord record : parser) {
        int movieId = Integer.parseInt(record.get("movie_id"));
        String name = record.get("name");
        String role = record.get("role");

        Role roleObj = new Role(movieId, name, role);
        this.roleDb.computeIfAbsent(movieId, k -> new ArrayList<>()).add(roleObj);

        if ("director".equals(role)) {
          this.directorDb.computeIfAbsent(MatcherUtils.normalizeTitle(name), k -> new ArrayList<>()).add(movieId);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Error importing actors and directors database", e);
    }
  }

  @Override
  public List<IdMapping> match(DatabaseType databaseType, CsvStream externalDb) {
    Map<String, Integer> idMappings = new ConcurrentHashMap<>(); // Thread-safe map
    MatcherConfig.MatchCriteria criteria = MatcherConfig.getCriteriaForDatabase(databaseType);

    try (CSVParser parser = CSVParser.parse(externalDb.getHeaderRow() + "\n" + String.join("\n", externalDb.getDataRows().collect(Collectors.toList())), CSVFormat.DEFAULT.withHeader())) {
      List<CSVRecord> records = parser.getRecords();

      records.parallelStream().forEach(record -> {
        findMatch(record, criteria).ifPresent(movie ->
            idMappings.putIfAbsent(record.get("MediaId"), movie.getId())
        );
//        long i = records.indexOf(record);
//        if (i % 1000 == 0) {
//          LOGGER.info("Processed {} records", i);
//        }
      });
    } catch (IOException e) {
      LOGGER.error("Error parsing external database", e);
    }

    // Convert Map to List<IdMapping>
    return idMappings.entrySet().stream()
        .map(entry -> new IdMapping(entry.getValue(), entry.getKey()))
        .collect(Collectors.toList());
  }

  private Optional<Movie> findMatch(CSVRecord externalMovie, MatcherConfig.MatchCriteria criteria) {
    String externalTitle = externalMovie.get(criteria.getTitleField());
    int externalYear = MatcherUtils.extractYear(externalMovie.get(criteria.getDateField()));
    String externalDirector = criteria.getDirectorField() != null ? externalMovie.get(criteria.getDirectorField()) : null;

    List<Movie> moviesInYear = movieDb.getOrDefault(externalYear, Collections.emptyList());
    Set<Integer> movieIdsInYear = moviesInYear.stream().map(Movie::getId).collect(Collectors.toSet());

    Set<Integer> directorMovieIds = new HashSet<>();
    if (externalDirector != null) {
      directorMovieIds.addAll(directorDb.getOrDefault(MatcherUtils.normalizeTitle(externalDirector), Collections.emptyList()));
    }

    Set<Integer> intersectionMovieIds;
    if (!directorMovieIds.isEmpty()) {
      intersectionMovieIds = new HashSet<>(movieIdsInYear);
      intersectionMovieIds.retainAll(directorMovieIds);
    } else {
      intersectionMovieIds = movieIdsInYear;
    }

    return moviesInYear.parallelStream()
        .filter(movie -> intersectionMovieIds.contains(movie.getId()))
        .filter(movie -> MatcherUtils.isTitleMatch(movie.getTitle(), externalTitle))
        .findAny();
  }

  public static class Movie {
    private final int id;
    private final String title;
    private final int year;

    public Movie(int id, String title, int year) {
      this.id = id;
      this.title = title;
      this.year = year;
    }

    public int getId() { return id; }
    public String getTitle() { return title; }
    public int getYear() { return year; }
  }

  public static class Role {
    private final int movieId;
    private final String name;
    private final String role;

    public Role(int movieId, String name, String role) {
      this.movieId = movieId;
      this.name = name;
      this.role = role;
    }

    public int getMovieId() { return movieId; }
    public String getName() { return name; }
    public String getRole() { return role; }
  }
}