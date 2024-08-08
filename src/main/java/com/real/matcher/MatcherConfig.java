package com.real.matcher;

import java.util.HashMap;
import java.util.Map;

public class MatcherConfig {

  private static final Map<Matcher.DatabaseType, MatchCriteria> DATABASE_CRITERIA = new HashMap<>();

  static {
    DATABASE_CRITERIA.put(Matcher.DatabaseType.XBOX, new MatchCriteria("Title", "OriginalReleaseDate", null));
    DATABASE_CRITERIA.put(Matcher.DatabaseType.GOOGLE_PLAY, new MatchCriteria("Title", "ReleaseDate", "Director"));
    // Add more database types and their criteria here
  }

  public static MatchCriteria getCriteriaForDatabase(Matcher.DatabaseType databaseType) {
    return DATABASE_CRITERIA.getOrDefault(databaseType,
        new MatchCriteria("Title", "OriginalReleaseDate", null));
  }

  public static class MatchCriteria {
    private final String titleField;
    private final String dateField;
    private final String directorField;

    public MatchCriteria(String titleField, String dateField, String directorField) {
      this.titleField = titleField;
      this.dateField = dateField;
      this.directorField = directorField;
    }

    public String getTitleField() {
      return titleField;
    }

    public String getDateField() {
      return dateField;
    }

    public String getDirectorField() {
      return directorField;
    }
  }
}