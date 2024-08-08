package com.real.matcher;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MatcherUtils {

  private static final List<DateTimeFormatter>  DATE_FORMATTERS = Arrays.asList(
      DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd"),
      DateTimeFormatter.ofPattern("yyyy/MM/dd"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy")
  );

  public static boolean isTitleMatch(String title1, String title2) {
    return StringUtils.getJaroWinklerDistance(
        normalizeTitle(title1),
        normalizeTitle(title2)
    ) > 0.9;
  }

  public static String normalizeTitle(String title) {
    return title.toLowerCase()
        .replaceAll("\\s+", " ")
        .replaceAll("[^a-z0-9 ]", "")
        .trim();
  }

  public static int extractYear(String dateString) {
    for (DateTimeFormatter formatter : DATE_FORMATTERS) {
      try {
        // Try parsing as LocalDateTime first
        LocalDateTime dateTime = LocalDateTime.parse(dateString, formatter);
        return dateTime.getYear();
      } catch (DateTimeParseException e) {
        try {
          // If that fails, try parsing as LocalDate
          LocalDate date = LocalDate.parse(dateString, formatter);
          return date.getYear();
        } catch (DateTimeParseException ex) {
          // Continue to the next formatter
        }
      }
    }
    return extractYearNumber(dateString);
  }

  public static int extractYearNumber(String dateString) {
    Pattern pattern = Pattern.compile("\\b\\d{4}\\b");
    Matcher matcher = pattern.matcher(dateString);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group());
    }
    return 0;
  }

  public static boolean isYearMatch(int year1, int year2) {
    return Math.abs(year1 - year2) <= 1; // Allow 1 year difference
  }
}