//! Common utility functions.

use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the current UTC time in GeneralizedTime format (`YYYYMMDDHHMMSSZ`).
///
/// The Kerberos protocol requires GeneralizedTime format, which must end with 'Z' to indicate UTC.
pub fn utc_now_generalized() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{}Z", utc_calendar(secs))
}

/// Returns the current microsecond fraction (for `cusec`, the fractional microseconds after `YYYYMMDDHHMMSS`).
///
/// Used for the microsecond portion of Kerberos timestamps.
pub fn now_micros() -> i32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| (d.as_micros() % 1_000_000) as i32)
        .unwrap_or(0)
}

/// Returns the UTC time `n` days from now in GeneralizedTime format.
pub fn utc_add_days(n: u64) -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() + n * 86400)
        .unwrap_or(0);
    format!("{}Z", utc_calendar(secs))
}

/// Converts a Unix timestamp to Kerberos GeneralizedTime format (without the 'Z' suffix).
fn utc_calendar(secs: u64) -> String {
    let days = secs / 86400;
    let mut y = 1970u32;
    let mut rem = days;
    loop {
        let leap = if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
            366
        } else {
            365
        };
        if rem < leap {
            break;
        }
        rem -= leap;
        y += 1;
    }
    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let mut m = 1u32;
    let mut d = rem;
    loop {
        let md = month_days[(m - 1) as usize] + if m == 2 && leap { 1 } else { 0 };
        if d < md {
            break;
        }
        d -= md;
        m += 1;
    }
    let h = (secs % 86400) / 3600;
    let mi = (secs % 3600) / 60;
    let s = secs % 60;
    format!(
        "{y:04}{m:02}{d:02}{h:02}{mi:02}{s:02}",
        y = y,
        m = m,
        d = d + 1,
        h = h,
        mi = mi,
        s = s
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utc_now_generalized_format() {
        let time = utc_now_generalized();
        // 格式: YYYYMMDDHHMMSSZ
        assert!(time.ends_with('Z'));
        assert_eq!(time.len(), 15); // 14 字符 + 'Z'
    }

    #[test]
    fn test_now_micros_range() {
        let micros = now_micros();
        assert!(micros >= 0 && micros < 1_000_000);
    }

    #[test]
    fn test_utc_add_days() {
        let today = utc_now_generalized();
        let tomorrow = utc_add_days(1);
        // 至少年份和月份应该相同 (除非跨年)
        assert_eq!(&today[..6], &tomorrow[..6]);
    }
}
