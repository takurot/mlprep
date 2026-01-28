use polars::prelude::*;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn minmax_scale_simd(s: &Series, min: f64, max: f64) -> PolarsResult<Series> {
    // 1. Check for constant column case to match scalar behavior
    let diff = max - min;
    if diff.abs() < f64::EPSILON {
        // Return 0.5 for all non-null values
        let ca = s.f64()?;
        let out: Float64Chunked = ca.apply_values(|_| 0.5);
        return Ok(out.into_series());
    }

    // ------------------------------------------------------------------
    // X86_64 Dispatch
    // ------------------------------------------------------------------
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // Safety: Checked feature
            return unsafe { minmax_scale_avx2(s, min, max) };
        }
    }

    // ------------------------------------------------------------------
    // AArch64 Dispatch
    // ------------------------------------------------------------------
    #[cfg(target_arch = "aarch64")]
    {
        // NEON is standard on aarch64
        unsafe { minmax_scale_neon(s, min, max) }
    }

    // Default fallback (reachable on x86_64 without AVX2, or other arches)
    #[cfg(not(target_arch = "aarch64"))]
    minmax_scale_scalar(s, min, max)
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn minmax_scale_simd(s: &Series, min: f64, max: f64) -> PolarsResult<Series> {
    minmax_scale_scalar(s, min, max)
}

pub fn minmax_scale_scalar(s: &Series, min: f64, max: f64) -> PolarsResult<Series> {
    let diff = max - min;
    // Check constant column
    if diff.abs() < f64::EPSILON {
        let ca = s.f64()?;
        let out: Float64Chunked = ca.apply_values(|_| 0.5);
        return Ok(out.into_series());
    }

    let scale = 1.0 / diff;
    let ca = s.f64()?;
    // Polars apply_values preserves nulls automatically
    let out: Float64Chunked = ca.apply_values(|v| (v - min) * scale);
    Ok(out.into_series())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn minmax_scale_avx2(s: &Series, min: f64, max: f64) -> PolarsResult<Series> {
    let ca = s.f64()?;
    let diff = max - min;
    let scale = if diff.abs() < f64::EPSILON {
        1.0
    } else {
        1.0 / diff
    };

    let mut builder = PrimitiveChunkedBuilder::<Float64Type>::new(s.name().clone(), s.len());

    for arr in ca.downcast_iter() {
        let slice = arr.values().as_slice();
        let len = slice.len();
        let mut out_vec: Vec<f64> = Vec::with_capacity(len);
        let mut i = 0;

        // AVX2 constants
        let v_min = _mm256_set1_pd(min);
        let v_scale = _mm256_set1_pd(scale);

        while i + 4 <= len {
            let v_x = _mm256_loadu_pd(slice.as_ptr().add(i));
            let v_res = _mm256_sub_pd(v_x, v_min);
            let v_res = _mm256_mul_pd(v_res, v_scale);
            _mm256_storeu_pd(out_vec.as_mut_ptr().add(i), v_res);
            i += 4;
        }
        out_vec.set_len(i); // Update len to what we processed

        // Remainder
        while i < len {
            let val = (slice[i] - min) * scale;
            out_vec.push(val);
            i += 1;
        }

        // Append to builder handling validity
        if let Some(validity) = arr.validity() {
            for (val, valid) in out_vec.into_iter().zip(validity.iter()) {
                if valid {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
        } else {
            // Fast path: no nulls
            // Need to convert &[f64] to iterator or slice?
            // append_slice is not always available on Builder?
            // PrimitiveChunkedBuilder has append_value/null/option.
            // Check docs: usually manual loop is needed if no bulk method.
            // But iteration is fine.
            for val in out_vec {
                builder.append_value(val);
            }
        }
    }

    Ok(builder.finish().into_series())
}

#[cfg(target_arch = "aarch64")]
unsafe fn minmax_scale_neon(s: &Series, min: f64, max: f64) -> PolarsResult<Series> {
    let ca = s.f64()?;
    let diff = max - min;
    let scale = if diff.abs() < f64::EPSILON {
        1.0
    } else {
        1.0 / diff
    };

    let mut builder = PrimitiveChunkedBuilder::<Float64Type>::new(s.name().clone(), s.len());

    for arr in ca.downcast_iter() {
        let slice = arr.values().as_slice();
        let len = slice.len();
        let mut out_vec: Vec<f64> = Vec::with_capacity(len);
        let mut i = 0;

        let v_min = vdupq_n_f64(min);
        let v_scale = vdupq_n_f64(scale);

        while i + 2 <= len {
            let v_x = vld1q_f64(slice.as_ptr().add(i));
            let v_res = vsubq_f64(v_x, v_min);
            let v_res = vmulq_f64(v_res, v_scale);
            vst1q_f64(out_vec.as_mut_ptr().add(i), v_res);
            i += 2;
        }
        out_vec.set_len(i);

        while i < len {
            let val = (slice[i] - min) * scale;
            out_vec.push(val);
            i += 1;
        }

        if let Some(validity) = arr.validity() {
            for (val, valid) in out_vec.into_iter().zip(validity.iter()) {
                if valid {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
        } else {
            for val in out_vec {
                builder.append_value(val);
            }
        }
    }

    Ok(builder.finish().into_series())
}

// ------------------------------------------------------------------
// Standard Scale
// ------------------------------------------------------------------

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn standard_scale_simd(s: &Series, mean: f64, std: f64) -> PolarsResult<Series> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { standard_scale_avx2(s, mean, std) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { standard_scale_neon(s, mean, std) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    standard_scale_scalar(s, mean, std)
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn standard_scale_simd(s: &Series, mean: f64, std: f64) -> PolarsResult<Series> {
    standard_scale_scalar(s, mean, std)
}

pub fn standard_scale_scalar(s: &Series, mean: f64, std: f64) -> PolarsResult<Series> {
    let scale = if std.abs() < f64::EPSILON {
        1.0
    } else {
        1.0 / std
    };
    let ca = s.f64()?;
    let out: Float64Chunked = ca.apply_values(|v| (v - mean) * scale);
    Ok(out.into_series())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn standard_scale_avx2(s: &Series, mean: f64, std: f64) -> PolarsResult<Series> {
    let ca = s.f64()?;
    let scale = if std.abs() < f64::EPSILON {
        1.0
    } else {
        1.0 / std
    };

    let mut builder = PrimitiveChunkedBuilder::<Float64Type>::new(s.name().clone(), s.len());

    for arr in ca.downcast_iter() {
        let slice = arr.values().as_slice();
        let len = slice.len();
        let mut out_vec: Vec<f64> = Vec::with_capacity(len);
        let mut i = 0;

        let v_mean = _mm256_set1_pd(mean);
        let v_scale = _mm256_set1_pd(scale);

        while i + 4 <= len {
            let v_x = _mm256_loadu_pd(slice.as_ptr().add(i));
            let v_res = _mm256_sub_pd(v_x, v_mean);
            let v_res = _mm256_mul_pd(v_res, v_scale);
            _mm256_storeu_pd(out_vec.as_mut_ptr().add(i), v_res);
            i += 4;
        }
        out_vec.set_len(i);

        while i < len {
            let val = (slice[i] - mean) * scale;
            out_vec.push(val);
            i += 1;
        }

        if let Some(validity) = arr.validity() {
            for (val, valid) in out_vec.into_iter().zip(validity.iter()) {
                if valid {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
        } else {
            for val in out_vec {
                builder.append_value(val);
            }
        }
    }

    Ok(builder.finish().into_series())
}

#[cfg(target_arch = "aarch64")]
unsafe fn standard_scale_neon(s: &Series, mean: f64, std: f64) -> PolarsResult<Series> {
    let ca = s.f64()?;
    let scale = if std.abs() < f64::EPSILON {
        1.0
    } else {
        1.0 / std
    };

    let mut builder = PrimitiveChunkedBuilder::<Float64Type>::new(s.name().clone(), s.len());

    for arr in ca.downcast_iter() {
        let slice = arr.values().as_slice();
        let len = slice.len();
        let mut out_vec: Vec<f64> = Vec::with_capacity(len);
        let mut i = 0;

        let v_mean = vdupq_n_f64(mean);
        let v_scale = vdupq_n_f64(scale);

        while i + 2 <= len {
            let v_x = vld1q_f64(slice.as_ptr().add(i));
            let v_res = vsubq_f64(v_x, v_mean);
            let v_res = vmulq_f64(v_res, v_scale);
            vst1q_f64(out_vec.as_mut_ptr().add(i), v_res);
            i += 2;
        }
        out_vec.set_len(i);

        while i < len {
            let val = (slice[i] - mean) * scale;
            out_vec.push(val);
            i += 1;
        }

        if let Some(validity) = arr.validity() {
            for (val, valid) in out_vec.into_iter().zip(validity.iter()) {
                if valid {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
        } else {
            for val in out_vec {
                builder.append_value(val);
            }
        }
    }

    Ok(builder.finish().into_series())
}

// ------------------------------------------------------------------
// Regex & OneHot (Safe Implementations + Fallback)
// ------------------------------------------------------------------

// These implementations use standard Rust iterators/methods which are safe
// and compiler-optimized (auto-vectorization). They can be used on any architecture.

pub fn regex_validate_simd(s: &Series, pattern: &str) -> PolarsResult<Series> {
    // For complex patterns, use the regex crate as fallback
    let regex = regex::Regex::new(pattern)
        .map_err(|e| polars_err!(ComputeError: "Invalid regex pattern: {}", e))?;

    let str_ca = s.str()?;
    let mut out: Vec<bool> = Vec::with_capacity(s.len());

    // Check if pattern is a simple literal that we can optimize
    let is_simple_literal = !pattern.chars().any(|c| {
        matches!(
            c,
            '.' | '*' | '+' | '?' | '[' | ']' | '(' | ')' | '{' | '}' | '|' | '^' | '$' | '\\'
        )
    });

    if is_simple_literal && !pattern.is_empty() {
        // Simple literal search
        let pattern_bytes = pattern.as_bytes();
        let pattern_len = pattern_bytes.len();

        for arr in str_ca.downcast_iter() {
            for opt_val in arr.iter() {
                match opt_val {
                    Some(val) => {
                        let bytes = val.as_bytes();
                        // Safe: standard windows iterator
                        let found = bytes.windows(pattern_len).any(|w| w == pattern_bytes);
                        out.push(found);
                    }
                    None => out.push(false),
                }
            }
        }
    } else {
        // Complex pattern - use regex crate
        for arr in str_ca.downcast_iter() {
            for opt_val in arr.iter() {
                match opt_val {
                    Some(val) => out.push(regex.is_match(val)),
                    None => out.push(false),
                }
            }
        }
    }

    Ok(Series::new(s.name().clone(), out))
}

pub fn onehot_lookup_simd(
    s: &Series,
    vocab: &[String],
    base_name: &str,
) -> PolarsResult<Vec<Series>> {
    let str_ca = s.str()?;
    let n_rows = s.len();

    // Pre-allocate output vectors for each category
    let mut outputs: Vec<Vec<i32>> = vocab.iter().map(|_| Vec::with_capacity(n_rows)).collect();

    // Process each row
    for arr in str_ca.downcast_iter() {
        for opt_val in arr.iter() {
            match opt_val {
                Some(val) => {
                    // Check each category
                    for (cat_idx, category) in vocab.iter().enumerate() {
                        if val == category.as_str() {
                            outputs[cat_idx].push(1);
                        } else {
                            outputs[cat_idx].push(0);
                        }
                    }
                }
                None => {
                    // Null value -> all zeros
                    for output in outputs.iter_mut() {
                        output.push(0);
                    }
                }
            }
        }
    }

    // Convert to Series
    let result: Vec<Series> = vocab
        .iter()
        .enumerate()
        .map(|(idx, category)| {
            let col_name = format!("{}_{}", base_name, category);
            Series::new(col_name.into(), outputs[idx].clone())
        })
        .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use polars::prelude::*;

    #[test]
    fn test_minmax_scale_simd() {
        let s = Series::new("a".into(), vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let min = 1.0;
        let max = 5.0;

        // Expected: (x - min) / (max - min)
        // 1.0 -> 0.0
        // 5.0 -> 1.0
        // 3.0 -> 0.5

        let scaled = minmax_scale_simd(&s, min, max).unwrap();
        let ca = scaled.f64().unwrap();

        assert_eq!(ca.get(0), Some(0.0));
        assert_eq!(ca.get(2), Some(0.5));
        assert_eq!(ca.get(4), Some(1.0));
    }

    #[test]
    fn test_standard_scale_simd() {
        let s = Series::new("a".into(), vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let mean = 3.0;
        let std = 1.5811388300841898; // sqrt(2.5) approx

        // Expected: (x - mean) / std
        // 3.0 -> 0.0

        let scaled = standard_scale_simd(&s, mean, std).unwrap();
        let ca = scaled.f64().unwrap();

        // Tolerance for float
        let epsilon = 1e-6;
        assert!((ca.get(2).unwrap() - 0.0).abs() < epsilon);
        assert!((ca.get(0).unwrap() - (-1.264911)).abs() < epsilon);
    }

    #[test]
    fn test_regex_validate_simd_simple_literal() {
        let s = Series::new(
            "email".into(),
            vec!["alice@example.com", "bob@test.org", "invalid-email"],
        );

        // Simple literal pattern "@" - should use SIMD-optimized path
        let result = regex_validate_simd(&s, "@").unwrap();
        let ca = result.bool().unwrap();

        assert_eq!(ca.get(0), Some(true)); // alice@example.com contains @
        assert_eq!(ca.get(1), Some(true)); // bob@test.org contains @
        assert_eq!(ca.get(2), Some(false)); // invalid-email does NOT contain @
    }

    #[test]
    fn test_regex_validate_simd_complex_pattern() {
        let s = Series::new(
            "email".into(),
            vec!["alice@example.com", "bob@test.org", "invalid"],
        );

        // Complex regex pattern - uses regex crate fallback
        let result = regex_validate_simd(&s, r"^[a-z]+@[a-z]+\.[a-z]+$").unwrap();
        let ca = result.bool().unwrap();

        assert_eq!(ca.get(0), Some(true)); // valid email format
        assert_eq!(ca.get(1), Some(true)); // valid email format
        assert_eq!(ca.get(2), Some(false)); // invalid
    }

    #[test]
    fn test_onehot_lookup_simd() {
        let s = Series::new("category".into(), vec!["A", "B", "A", "C", "B"]);
        let vocab = vec!["A".to_string(), "B".to_string(), "C".to_string()];

        let result = onehot_lookup_simd(&s, &vocab, "cat").unwrap();

        assert_eq!(result.len(), 3); // 3 categories

        // Check cat_A column
        let cat_a = result[0].i32().unwrap();
        assert_eq!(cat_a.get(0), Some(1)); // "A"
        assert_eq!(cat_a.get(1), Some(0)); // "B"
        assert_eq!(cat_a.get(2), Some(1)); // "A"
        assert_eq!(cat_a.get(3), Some(0)); // "C"
        assert_eq!(cat_a.get(4), Some(0)); // "B"

        // Check cat_B column
        let cat_b = result[1].i32().unwrap();
        assert_eq!(cat_b.get(0), Some(0)); // "A"
        assert_eq!(cat_b.get(1), Some(1)); // "B"
        assert_eq!(cat_b.get(4), Some(1)); // "B"

        // Check cat_C column
        let cat_c = result[2].i32().unwrap();
        assert_eq!(cat_c.get(3), Some(1)); // "C"
    }
}
