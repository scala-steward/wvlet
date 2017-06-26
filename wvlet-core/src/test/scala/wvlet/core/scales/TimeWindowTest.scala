package wvlet.core.scales

import java.time.ZonedDateTime

import wvlet.test.WvletSpec

/**
  *
  */
class TimeWindowTest extends WvletSpec {

  val t = TimeWindow.ofSystem.withCurrentTime("2016-06-26 01:23:45-0700")
  val zone = t.zone
  info(s"now: ${t.now}")

  def parse(s:String, expected:String): TimeWindow = {
    val w = t.parse(s)
    info(s"str:${s}, window:${w}")
    w.toString shouldBe expected
    w
  }

  "TimeWindow" should {

    "parse string repl" in {
      // duration/offset

      // DURATION := (+ | -)?(INTEGER)(UNIT)
      // UNIT     := s | m | h | d | w | M | y
      //
      // OFFSET   := DURATION | DATE_TIME
      // RANGE    := (DURATION) (/ (OFFSET))?
      // DATE_TIME := yyyy-MM-dd( HH:mm:ss(.ZZZ| ' ' z)?)?
      //

      // The following tests are the results if the current time is 2016-06-26 01:23:45-0700

      // The default offset is 0(UNIT) (the beginning of the given time unit)

      // 7 days ago until at the beginning of today.
      // 0d := the beginning of the day
      // [-7d, 0d)
      parse("7d", "[2016-06-19 00:00:00-0700,2016-06-26 00:00:00-0700)")

      // 7d and -7d have the same meaning
      // [-7d, 0d)
      // |-------------|
      // -7d -- ... -- 0d ---- now  ------
      parse("-7d", "[2016-06-19 00:00:00-0700,2016-06-26 00:00:00-0700)")

      // Since 7 days ago + time fragment from [-7d, now)
      //  |-------------------|
      // -7d - ... - 0d ---- now  ------
      parse("7d/now", "[2016-06-19 00:00:00-0700,2016-06-26 01:23:45-0700)")

      // '+' indicates forward time range
      // +7d = [0d, +7d)
      //      |------------------------------|
      // ---  0d --- now --- 1d ---  ... --- 7da
      parse("+7d", "[2016-06-26 00:00:00-0700,2016-07-03 00:00:00-0700)")

      // [now, +7d)
      //         |---------------------|
      // 0d --- now --- 1d ---  ... --- 7d
      parse("+7d/now", "[2016-06-26 01:23:45-0700,2016-07-03 00:00:00-0700)")

      // [-1h, 0h)
      parse("1h", "[2016-06-26 00:00:00-0700,2016-06-26 01:00:00-0700)")
      // [-1h, now)
      parse("1h/now", "[2016-06-26 00:00:00-0700,2016-06-26 01:23:45-0700)")


      // -12h/now  (last 12 hours + fraction until now)

      parse("-12h/now", "[2016-06-25 13:00:00-0700,2016-06-26 01:23:45-0700)")
      parse("-12h", "[2016-06-25 13:00:00-0700,2016-06-26 01:00:00-0700)")
      parse("-12h/now", "[2016-06-25 13:00:00-0700,2016-06-26 01:23:45-0700)")
      parse("+12h/now", "[2016-06-26 01:23:45-0700,2016-06-26 13:00:00-0700)")

      // Absolute offset
      // 3d:2017-04-07 [2017-04-04,2017-04-07)
      parse("3d/2017-04-07", "[2017-04-04 00:00:00-0700,2017-04-07 00:00:00-0700)")

      // The offset can be specified using a duration
      // -1M:-1M  [2017-04-01, 2017-05-01) if today is 2017-05-20
      parse("1M/0M", "[2016-05-01 00:00:00-0700,2016-06-01 00:00:00-0700)")
      // -1M:-1M  [2017-03-01, 2017-04-01) if today is 2017-05-20
      parse("1M/1M","[2016-04-01 00:00:00-0700,2016-05-01 00:00:00-0700)")

      // -1h/2017-01-23 01:00:00 -> [2017-01-23 00:00:00,2017-01-23 01:00:00]
      // -1h/2017-01-23 01:23:45 -> [2017-01-23 00:00:00,2017-01-23 01:23:45]
      // 60m/2017-01-23 01:23:45 -> [2017-01-23 00:23:45,2017-01-23 01:23:45]
      parse("-1h/2017-01-23 01:00:00", "[2017-01-23 00:00:00-0700,2017-01-23 01:00:00-0700)")
      parse("-1h/2017-01-23 01:23:45", "[2017-01-23 00:00:00-0700,2017-01-23 01:23:45-0700)")
      parse("60m/2017-01-23 01:23:45", "[2017-01-23 00:23:00-0700,2017-01-23 01:23:45-0700)")
    }

    "support human-friendly range" in {
      parse("today","[2016-06-26 00:00:00-0700,2016-06-27 00:00:00-0700)")
      parse("today/now", "[2016-06-26 00:00:00-0700,2016-06-26 01:23:45-0700)")
      parse("thisHour", "[2016-06-26 01:00:00-0700,2016-06-26 02:00:00-0700)")
      parse("thisWeek", "[2016-06-20 00:00:00-0700,2016-06-27 00:00:00-0700)")
      parse("thisMonth", "[2016-06-01 00:00:00-0700,2016-07-01 00:00:00-0700)")
      parse("thisMonth/now", "[2016-06-01 00:00:00-0700,2016-06-26 01:23:45-0700)")
      parse("thisYear", "[2016-01-01 00:00:00-0700,2017-01-01 00:00:00-0700)")

      parse("yesterday", "[2016-06-25 00:00:00-0700,2016-06-26 00:00:00-0700)")
      parse("yesterday/now", "[2016-06-25 00:00:00-0700,2016-06-26 01:23:45-0700)")
      parse("lastHour", "[2016-06-26 00:00:00-0700,2016-06-26 01:00:00-0700)")
      parse("lastWeek", "[2016-06-13 00:00:00-0700,2016-06-20 00:00:00-0700)")
      parse("lastMonth", "[2016-05-01 00:00:00-0700,2016-06-01 00:00:00-0700)")
      parse("lastYear", "[2015-01-01 00:00:00-0700,2016-01-01 00:00:00-0700)")

      parse("tomorrow","[2016-06-27 00:00:00-0700,2016-06-28 00:00:00-0700)")
      parse("tomorrow/now", "[2016-06-26 01:23:45-0700,2016-06-28 00:00:00-0700)")
      parse("nextHour", "[2016-06-26 02:00:00-0700,2016-06-26 03:00:00-0700)")
      parse("nextWeek", "[2016-06-27 00:00:00-0700,2016-07-04 00:00:00-0700)")
      parse("nextMonth", "[2016-07-01 00:00:00-0700,2016-08-01 00:00:00-0700)")
      parse("nextYear", "[2017-01-01 00:00:00-0700,2018-01-01 00:00:00-0700)")
    }

    "split time windows" in {
      val weeks = t.parse("5w").splitIntoWeeks
      info(weeks.mkString("\n"))

      val weeks2 = t.parse("5w/2017-06-01").splitIntoWeeks
      info(weeks2.mkString("\n"))

      val months = t.parse("thisYear/thisMonth").splitIntoMonths
      info(months.mkString("\n"))
      val months2 = t.parse("thisYear/0M").splitIntoMonths
      info(months2.mkString("\n"))

      val days = t.parse("thisMonth").splitIntoWeeks
      info(days.mkString("\n"))
    }

    "parse timezone" in {
      // Sanity tests
      TimeWindow.withZone("UTC")
      TimeWindow.withZone("PST")
      TimeWindow.withZone("PDT")
      TimeWindow.withZone("JST")
      TimeWindow.withZone("EDT")
      TimeWindow.withZone("BST")
      TimeWindow.withZone("CDT")
      TimeWindow.withZone("MDT")
    }
  }

}
