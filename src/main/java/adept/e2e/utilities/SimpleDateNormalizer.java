package adept.e2e.utilities;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by azamania on 7/31/17.
 */
public final class SimpleDateNormalizer {

  private final static Pattern referenceDateRegex = Pattern.compile("(19|20)\\d\\d(0|1)\\d(0|1|2|3)\\d");

  private final static String monthRegexString = "January|Jan\\.?|February|Feb\\.?|March|Mar\\.?|" +
      "April|Apr\\.?|May|June|Jun\\.?|July|Jul\\.?|August|Aug\\.?|September|Sept\\.?|Sep\\.?|" +
      "October|Oct\\.?|November|Nov\\.?|December|Dec\\.?";

  private final static Pattern singleYearRegex = Pattern.compile("(start of|end of)? ?(\\d{4})", Pattern.CASE_INSENSITIVE);
  private final static Pattern singleMonthRegex = Pattern.compile("(" + monthRegexString + ")", Pattern.CASE_INSENSITIVE);
  private final static Pattern monthAndDayRegex = Pattern.compile("(" + monthRegexString + ") (\\d\\d?)(st|nd|rd|th)?", Pattern.CASE_INSENSITIVE);
  private final static Pattern monthDayYearRegex = Pattern.compile("(" + monthRegexString + ") (\\d\\d?)(st|nd|rd|th)?,? (\\d{4})", Pattern.CASE_INSENSITIVE);
  private final static Pattern monthAndYearRegex = Pattern.compile("(" + monthRegexString + ") (\\d{4})", Pattern.CASE_INSENSITIVE);
  private final static Pattern todayRegex = Pattern.compile("today", Pattern.CASE_INSENSITIVE);
  private final static Pattern thisYearRegex = Pattern.compile("(early|late|earlier|later)? ?this year", Pattern.CASE_INSENSITIVE);
  private final static Pattern lastYearRegex = Pattern.compile("(early|late)? ?last year", Pattern.CASE_INSENSITIVE);
  private final static Pattern dayOfWeekRegex =
      Pattern.compile("(the)? ?(early|late|last|previous)? ?(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)", Pattern.CASE_INSENSITIVE);
  private final static Pattern lastMonthRegex = Pattern.compile("(early|late)? ?last month", Pattern.CASE_INSENSITIVE);


  private static String getReferenceDateFromDocID(String docid) {
    Matcher m = referenceDateRegex.matcher(docid);
    if (m.find())
      return m.group(0);
    return null;
  }

  private static String getTwoDigitMonth(String monthString) {
    String ms = monthString.toLowerCase().substring(0, 3);
    if (ms.equals("jan"))
      return "01";
    if (ms.equals("feb"))
      return "02";
    if (ms.equals("mar"))
      return "03";
    if (ms.equals("apr"))
      return "04";
    if (ms.equals("may"))
      return "05";
    if (ms.equals("jun"))
      return "06";
    if (ms.equals("jul"))
      return "07";
    if (ms.equals("aug"))
      return "08";
    if (ms.equals("sep"))
      return "09";
    if (ms.equals("oct"))
      return "10";
    if (ms.equals("nov"))
      return "11";
    if (ms.equals("dec"))
      return "12";

    return "XX";
  }

  private static String getTwoDigitDay(String dayString) {
    int dayInt = Integer.valueOf(dayString);
    if (dayInt <= 0 || dayInt > 31)
      return "XX";
    if (dayInt < 10)
      return "0" + String.valueOf(dayInt);
    return String.valueOf(dayInt);
  }

  private static boolean dayOfWeekMatches(int dayInt, String dayString) {
    String ds = dayString.toLowerCase().substring(0, 3);
    return
        (dayInt == 1 && ds.equals("sun")) ||
        (dayInt == 2 && ds.equals("mon")) ||
        (dayInt == 3 && ds.equals("tue")) ||
        (dayInt == 4 && ds.equals("wed")) ||
        (dayInt == 5 && ds.equals("thu")) ||
        (dayInt == 6 && ds.equals("fri")) ||
        (dayInt == 7 && ds.equals("sat"));
  }

  public static String normalizeDate(String documentID, String date) {
    String referenceDate = getReferenceDateFromDocID(documentID);
    if (referenceDate == null)
      return null;

    String referenceYear = referenceDate.substring(0, 4);
    String referenceMonth = referenceDate.substring(4, 6);
    String referenceDay = referenceDate.substring(6, 8);

    Matcher m;

    m = singleYearRegex.matcher(date);
    if (m.matches()) {
      String yearString = m.group(2);
      return yearString + "-XX-XX";
    }

    m = singleMonthRegex.matcher(date);
    if (m.matches()) {
      String twoDigitMonth = getTwoDigitMonth(date);
      return referenceYear + "-" + twoDigitMonth + "-XX";
    }

    m = monthAndDayRegex.matcher(date);
    if (m.matches()) {
      String monthString = m.group(1);
      String dayString = m.group(2);
      return referenceYear + "-" + getTwoDigitMonth(monthString) + "-" + getTwoDigitDay(dayString);
    }

    m = monthDayYearRegex.matcher(date);
    if (m.matches()) {
      String monthString = m.group(1);
      String dayString = m.group(2);
      String yearString = m.group(4);
      return yearString + "-" + getTwoDigitMonth(monthString) + "-" + getTwoDigitDay(dayString);
    }

    m = monthAndYearRegex.matcher(date);
    if (m.matches()) {
      String monthString = m.group(1);
      String yearString = m.group(2);

      return yearString + "-" + getTwoDigitMonth(monthString) + "-XX";
    }

    m = todayRegex.matcher(date);
    if (m.matches()) {
      return referenceYear + "-" + referenceMonth + "-" + referenceDay;
    }

    m = thisYearRegex.matcher(date);
    if (m.matches()) {
      return referenceYear + "-XX-XX";
    }

    m = lastYearRegex.matcher(date);
    if (m.matches()) {
      int yearInt = Integer.valueOf(referenceYear);
      String yearString = String.valueOf(yearInt - 1);
      return yearString + "-XX-XX";
    }

    m = dayOfWeekRegex.matcher(date);
    if (m.matches()) {
      String dayOfWeekString = m.group(3);
      //System.out.println("day of week matches");
      Calendar c = Calendar.getInstance();
      try {
        Date d = new SimpleDateFormat("yyyyMMdd").parse(referenceDate);
        c.setTime(d);
        for (int i = 0; i < 7; i++) {
          int dayOfWeekInt = c.get(Calendar.DAY_OF_WEEK);
          //System.out.println("Day of week int: " + dayOfWeekInt);
          if (dayOfWeekMatches(dayOfWeekInt, dayOfWeekString)) {
            return new SimpleDateFormat("yyyy-MM-dd").format(d);
          }
          c.add(Calendar.DATE, -1);
          d = c.getTime();
        }
      } catch (ParseException e) {
        return null;
      }
    }

    m = lastMonthRegex.matcher(date);
    if (m.matches()) {
      int monthInt = Integer.valueOf(referenceMonth);
      monthInt -= 1;
      if (monthInt == 0) {
        int yearInt = Integer.valueOf(referenceYear);
        String yearString = String.valueOf(yearInt - 1);
        return yearString + "-12-XX";
      } else {
        String monthString = String.valueOf(monthInt);
        if (monthString.length() == 1)
          monthString = "0" + monthString;
        return referenceYear + "-" + monthString + "-XX";
      }
    }

    return null;
  }

}
