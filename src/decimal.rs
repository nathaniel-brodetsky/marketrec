//! Prices and quantities from Binance decimal strings to integer steps, without floating point.
//!
//! Binance Spot sends prices and quantities as decimal strings with at most 8 fractional
//! digits, for example `"97123.45000000"`. A value is first read exactly as a count of
//! atoms (units of 1e-8), then divided by the instrument's step (tick size for prices,
//! step size for quantities). A value that is not a whole number of steps is an error, not
//! a rounding: an off-grid price means the step in the manifest is wrong.
//!
//! The parser works on bytes, never allocates and never panics on any input.

use std::fmt;

/// Number of fractional digits Binance Spot uses in price and quantity strings.
pub const DECIMALS: u32 = 8;

/// Atoms in one whole unit: 10^DECIMALS.
pub const ATOMS_PER_UNIT: u64 = 10u64.pow(DECIMALS);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    /// The input is empty.
    Empty,
    /// A byte that is neither a digit nor the decimal point; `pos` is its index.
    InvalidByte { pos: usize },
    /// No digit before or after the decimal point, or a second decimal point.
    Format,
    /// A non-zero digit beyond the 8th fractional place.
    TooPrecise,
    /// The value does not fit into the integer range.
    Overflow,
    /// The value is not a whole number of steps.
    OffGrid,
    /// A step of zero.
    ZeroStep,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Empty => write!(f, "empty decimal"),
            ParseError::InvalidByte { pos } => write!(f, "invalid byte at position {pos}"),
            ParseError::Format => write!(f, "malformed decimal"),
            ParseError::TooPrecise => {
                write!(f, "more than {DECIMALS} significant fractional digits")
            }
            ParseError::Overflow => write!(f, "value out of range"),
            ParseError::OffGrid => write!(f, "value is not a whole number of steps"),
            ParseError::ZeroStep => write!(f, "step must be positive"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse a non-negative decimal string into atoms (units of 1e-8).
///
/// Accepted: `"0"`, `"12"`, `"0.5"`, `"97123.45000000"`. Fractional digits beyond the 8th
/// are accepted only if they are zeros. Rejected: signs, exponents, spaces, `".5"`, `"1."`.
pub fn parse_atoms(s: &[u8]) -> Result<u64, ParseError> {
    if s.is_empty() {
        return Err(ParseError::Empty);
    }
    let mut value: u64 = 0;
    let mut int_digits = 0usize;
    let mut frac_digits = 0u32;
    let mut seen_point = false;
    for (pos, &b) in s.iter().enumerate() {
        match b {
            b'0'..=b'9' => {
                let d = u64::from(b - b'0');
                if !seen_point {
                    int_digits += 1;
                    value = value
                        .checked_mul(10)
                        .and_then(|v| v.checked_add(d))
                        .ok_or(ParseError::Overflow)?;
                } else if frac_digits < DECIMALS {
                    frac_digits += 1;
                    value = value
                        .checked_mul(10)
                        .and_then(|v| v.checked_add(d))
                        .ok_or(ParseError::Overflow)?;
                } else {
                    frac_digits += 1;
                    if d != 0 {
                        return Err(ParseError::TooPrecise);
                    }
                }
            }
            b'.' => {
                if seen_point || int_digits == 0 {
                    return Err(ParseError::Format);
                }
                seen_point = true;
            }
            _ => return Err(ParseError::InvalidByte { pos }),
        }
    }
    if seen_point && frac_digits == 0 {
        return Err(ParseError::Format);
    }
    let missing = DECIMALS - frac_digits.min(DECIMALS);
    value
        .checked_mul(10u64.pow(missing))
        .ok_or(ParseError::Overflow)
}

/// A price or quantity grid: the tick size or step size of one instrument, in atoms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Grid {
    step_atoms: u64,
}

impl Grid {
    /// Grid from the step as Binance writes it in exchangeInfo, for example `"0.01000000"`.
    pub fn parse(step: &[u8]) -> Result<Self, ParseError> {
        Self::from_atoms(parse_atoms(step)?)
    }

    pub fn from_atoms(step_atoms: u64) -> Result<Self, ParseError> {
        if step_atoms == 0 {
            return Err(ParseError::ZeroStep);
        }
        Ok(Self { step_atoms })
    }

    pub fn step_atoms(&self) -> u64 {
        self.step_atoms
    }

    /// Decimal string to a whole number of steps.
    pub fn to_steps(&self, s: &[u8]) -> Result<i64, ParseError> {
        let atoms = parse_atoms(s)?;
        if atoms % self.step_atoms != 0 {
            return Err(ParseError::OffGrid);
        }
        i64::try_from(atoms / self.step_atoms).map_err(|_| ParseError::Overflow)
    }

    /// Steps back to atoms, for display and checks.
    pub fn to_atoms(&self, steps: i64) -> Option<u64> {
        u64::try_from(steps).ok()?.checked_mul(self.step_atoms)
    }
}

/// Write atoms as a decimal string with exactly 8 fractional digits, as Binance does.
pub fn write_atoms(atoms: u64, out: &mut impl fmt::Write) -> fmt::Result {
    write!(
        out,
        "{}.{:08}",
        atoms / ATOMS_PER_UNIT,
        atoms % ATOMS_PER_UNIT
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn atoms(s: &str) -> Result<u64, ParseError> {
        parse_atoms(s.as_bytes())
    }

    fn text(a: u64) -> String {
        let mut s = String::new();
        write_atoms(a, &mut s).unwrap();
        s
    }

    #[test]
    fn binance_examples() {
        let tick = Grid::parse(b"0.01000000").unwrap();
        let step = Grid::parse(b"0.00001000").unwrap();
        assert_eq!(tick.to_steps(b"97123.45000000"), Ok(9_712_345));
        assert_eq!(step.to_steps(b"0.00012000"), Ok(12));
        assert_eq!(step.to_steps(b"0.00000000"), Ok(0));
    }

    #[test]
    fn accepted_forms() {
        assert_eq!(atoms("0"), Ok(0));
        assert_eq!(atoms("1"), Ok(ATOMS_PER_UNIT));
        assert_eq!(atoms("0.5"), Ok(50_000_000));
        assert_eq!(atoms("0001.50"), Ok(150_000_000));
        assert_eq!(atoms("0.00000001"), Ok(1));
        assert_eq!(atoms("1.1234567800"), Ok(112_345_678));
    }

    #[test]
    fn rejected_forms() {
        assert_eq!(atoms(""), Err(ParseError::Empty));
        assert_eq!(atoms("-1"), Err(ParseError::InvalidByte { pos: 0 }));
        assert_eq!(atoms("1e5"), Err(ParseError::InvalidByte { pos: 1 }));
        assert_eq!(atoms("1 "), Err(ParseError::InvalidByte { pos: 1 }));
        assert_eq!(atoms(".5"), Err(ParseError::Format));
        assert_eq!(atoms("1."), Err(ParseError::Format));
        assert_eq!(atoms("1.2.3"), Err(ParseError::Format));
        assert_eq!(atoms("1.123456789"), Err(ParseError::TooPrecise));
        assert_eq!(atoms("99999999999999999999"), Err(ParseError::Overflow));
        assert_eq!(atoms("184467440738"), Err(ParseError::Overflow));
    }

    #[test]
    fn largest_value_fits() {
        let max_units = u64::MAX / ATOMS_PER_UNIT;
        assert_eq!(
            atoms(&max_units.to_string()),
            Ok(max_units * ATOMS_PER_UNIT)
        );
    }

    #[test]
    fn off_grid_and_zero_step() {
        let tick = Grid::parse(b"0.01").unwrap();
        assert_eq!(tick.to_steps(b"97123.455"), Err(ParseError::OffGrid));
        assert_eq!(Grid::parse(b"0.00000000"), Err(ParseError::ZeroStep));
    }

    #[test]
    fn to_atoms_reports_overflow_and_negative() {
        let grid = Grid::from_atoms(ATOMS_PER_UNIT).unwrap();
        assert_eq!(grid.to_atoms(i64::MAX), None);
        assert_eq!(grid.to_atoms(-1), None);
        assert_eq!(grid.to_atoms(3), Some(3 * ATOMS_PER_UNIT));
    }

    #[test]
    fn writes_like_binance() {
        assert_eq!(text(9_712_345_000_000), "97123.45000000");
        assert_eq!(text(12_000), "0.00012000");
        assert_eq!(text(0), "0.00000000");
    }

    proptest! {
        #[test]
        fn atoms_roundtrip(a in any::<u64>()) {
            prop_assert_eq!(atoms(&text(a)), Ok(a));
        }

        #[test]
        // step up to 1000 units and k below 1e8 keep step * k under u64::MAX
        fn steps_roundtrip(step in 1u64..=ATOMS_PER_UNIT * 1000, k in 0i64..100_000_000) {
            let grid = Grid::from_atoms(step).unwrap();
            let a = grid.to_atoms(k).unwrap();
            prop_assert_eq!(grid.to_steps(text(a).as_bytes()), Ok(k));
        }

        #[test]
        fn never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..40)) {
            let _ = parse_atoms(&bytes);
        }

        #[test]
        fn never_panics_on_decimal_like_input(s in "[0-9.]{0,30}") {
            let _ = parse_atoms(s.as_bytes());
        }
    }
}
