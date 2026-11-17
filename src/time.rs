//! UTC calendar arithmetic for file names and day directories, without a date library.

pub const NS_PER_SEC: i64 = 1_000_000_000;
pub const NS_PER_HOUR: i64 = 3600 * NS_PER_SEC;
pub const NS_PER_DAY: i64 = 24 * NS_PER_HOUR;

/// UTC date and time of a timestamp in nanoseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Utc {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
}

impl Utc {
    pub fn from_ns(ns: i64) -> Self {
        let days = ns.div_euclid(NS_PER_DAY);
        let secs = ns.rem_euclid(NS_PER_DAY) / NS_PER_SEC;
        // civil_from_days, Howard Hinnant (public domain)
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
        let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
        let year = yoe + era * 400 + i64::from(month <= 2);
        Self {
            year,
            month,
            day,
            hour: (secs / 3600) as u32,
            minute: (secs / 60 % 60) as u32,
            second: (secs % 60) as u32,
        }
    }

    /// `2026-11-21`
    pub fn date(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    /// `13-05-12`, safe in file names on every system.
    pub fn time(&self) -> String {
        format!("{:02}-{:02}-{:02}", self.hour, self.minute, self.second)
    }
}

/// Midnight UTC that begins `date` (`2026-11-21`), in ns.
pub fn day_start(date: &str) -> Option<i64> {
    let mut parts = date.splitn(3, '-');
    let (y, m, d) = (parts.next()?, parts.next()?, parts.next()?);
    if (y.len(), m.len(), d.len()) != (4, 2, 2) {
        return None;
    }
    let (y, m, d): (i64, i64, i64) = (y.parse().ok()?, m.parse().ok()?, d.parse().ok()?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // days_from_civil, Howard Hinnant (public domain)
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let ns = (era * 146_097 + doe - 719_468) * NS_PER_DAY;
    // reject dates that do not exist, such as 2026-02-30
    (Utc::from_ns(ns).date() == date).then_some(ns)
}

/// Start of the UTC hour containing `ns`.
pub fn hour_start(ns: i64) -> i64 {
    ns - ns.rem_euclid(NS_PER_HOUR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_dates() {
        let t = Utc::from_ns(
            1_795_219_200 * NS_PER_SEC + 13 * NS_PER_HOUR + 5 * 60 * NS_PER_SEC + 12 * NS_PER_SEC,
        );
        assert_eq!(
            (t.date(), t.time()),
            ("2026-11-21".into(), "13-05-12".into())
        );
        assert_eq!(Utc::from_ns(0).date(), "1970-01-01");
        assert_eq!(
            Utc::from_ns(951_782_400 * NS_PER_SEC).date(),
            "2000-02-29",
            "leap day"
        );
        assert_eq!(Utc::from_ns(-NS_PER_SEC).date(), "1969-12-31");
        assert_eq!(Utc::from_ns(-NS_PER_SEC).time(), "23-59-59");
    }

    #[test]
    fn every_day_follows_the_previous_one() {
        // 1970 .. 2100, day by day: dates are strictly increasing and well-formed
        let mut prev = Utc::from_ns(0);
        for d in 1..47_482 {
            let t = Utc::from_ns(d * NS_PER_DAY);
            assert!((1..=12).contains(&t.month) && (1..=31).contains(&t.day));
            assert!((t.year, t.month, t.day) > (prev.year, prev.month, prev.day));
            assert_eq!((t.hour, t.minute, t.second), (0, 0, 0));
            prev = t;
        }
        assert_eq!(prev.date(), "2099-12-31");
    }

    #[test]
    fn day_start_inverts_date() {
        for days in [-1000, 0, 20_000, 20_778, 20_779, 30_000] {
            let ns = days * NS_PER_DAY;
            assert_eq!(day_start(&Utc::from_ns(ns).date()), Some(ns));
        }
        assert_eq!(day_start("2026-11-21"), Some(1_795_219_200_000_000_000));
        for bad in ["2026-02-30", "2026-13-01", "26-11-21", "2026-11-2", "x"] {
            assert_eq!(day_start(bad), None, "{bad}");
        }
    }

    #[test]
    fn hour_boundaries() {
        let h = 1_795_219_200 * NS_PER_SEC;
        assert_eq!(hour_start(h), h);
        assert_eq!(hour_start(h + NS_PER_HOUR - 1), h);
        assert_eq!(hour_start(h + NS_PER_HOUR), h + NS_PER_HOUR);
    }
}
