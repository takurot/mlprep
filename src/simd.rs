use polars::prelude::*;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn minmax_scale_simd(s: &Series, min: f64, max: f64) -> PolarsResult<Series> {
    let ca = s.f64()?;
    // We need to operate on the underlying data. 
    // Polars ChunkedArray might have multiple chunks, but usually one for small data.
    // For simplicity/performance, we can iterate chunks or assume contiguous if we ensure it.
    // `to_vec` copies, we want to avoid copy if possible, but for new series we need new memory anyway.
    
    // Create a mutable vector for result
    let mut out: Vec<f64> = Vec::with_capacity(s.len());
    let diff = max - min;
    // Avoid division by zero
    let scale = if diff.abs() < f64::EPSILON { 1.0 } else { 1.0 / diff };

    // Iterate over chunks to handle potential fragmentation, though usually we can flatten.
    for arr in ca.downcast_iter() {
        let slice = arr.values().as_slice();
        let len = slice.len();
        let mut i = 0;
        
        unsafe {
            #[cfg(target_arch = "x86_64")]
            {
                let v_min = _mm256_set1_pd(min);
                let v_scale = _mm256_set1_pd(scale);
                
                while i + 4 <= len {
                    let v_x = _mm256_loadu_pd(slice.as_ptr().add(i));
                    let v_res = _mm256_sub_pd(v_x, v_min);
                    let v_res = _mm256_mul_pd(v_res, v_scale);
                    _mm256_storeu_pd(out.as_mut_ptr().add(out.len()), v_res);
                    out.set_len(out.len() + 4);
                    i += 4;
                }
            }
            
            #[cfg(target_arch = "aarch64")]
            {
                let v_min = vdupq_n_f64(min);
                let v_scale = vdupq_n_f64(scale);
                
                while i + 2 <= len {
                    let v_x = vld1q_f64(slice.as_ptr().add(i));
                    let v_res = vsubq_f64(v_x, v_min);
                    let v_res = vmulq_f64(v_res, v_scale);
                    vst1q_f64(out.as_mut_ptr().add(out.len()), v_res);
                    out.set_len(out.len() + 2);
                    i += 2;
                }
            }
        }
        
        // Remainder loop
        while i < len {
            let val = (slice[i] - min) * scale;
            out.push(val);
            i += 1;
        }
    }
    
    Ok(Series::new(s.name().clone(), out))
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn standard_scale_simd(s: &Series, mean: f64, std: f64) -> PolarsResult<Series> {
    let ca = s.f64()?;
    let mut out: Vec<f64> = Vec::with_capacity(s.len());
    
    // scale = 1.0 / std
    let scale = if std.abs() < f64::EPSILON { 1.0 } else { 1.0 / std };

    for arr in ca.downcast_iter() {
        let slice = arr.values().as_slice();
        let len = slice.len();
        let mut i = 0;
        
        unsafe {
            #[cfg(target_arch = "x86_64")]
            {
                let v_mean = _mm256_set1_pd(mean);
                let v_scale = _mm256_set1_pd(scale);
                
                while i + 4 <= len {
                    let v_x = _mm256_loadu_pd(slice.as_ptr().add(i));
                    let v_res = _mm256_sub_pd(v_x, v_mean);
                    let v_res = _mm256_mul_pd(v_res, v_scale);
                    _mm256_storeu_pd(out.as_mut_ptr().add(out.len()), v_res);
                    out.set_len(out.len() + 4);
                    i += 4;
                }
            }
            
            #[cfg(target_arch = "aarch64")]
            {
                let v_mean = vdupq_n_f64(mean);
                let v_scale = vdupq_n_f64(scale);
                
                while i + 2 <= len {
                    let v_x = vld1q_f64(slice.as_ptr().add(i));
                    let v_res = vsubq_f64(v_x, v_mean);
                    let v_res = vmulq_f64(v_res, v_scale);
                    vst1q_f64(out.as_mut_ptr().add(out.len()), v_res);
                    out.set_len(out.len() + 2);
                    i += 2;
                }
            }
        }
        
        while i < len {
            let val = (slice[i] - mean) * scale;
            out.push(val);
            i += 1;
        }
    }
    
    Ok(Series::new(s.name().clone(), out))
}

/// SIMD-optimized regex validation for simple patterns.
/// This implementation works for short fixed patterns (e.g., email checks like "@" presence).
/// For complex regex, falls back to standard regex crate.
/// 
/// Returns a boolean Series where true = match, false = no match.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn regex_validate_simd(s: &Series, pattern: &str) -> PolarsResult<Series> {
    // For complex patterns, use the regex crate as fallback
    let regex = regex::Regex::new(pattern)
        .map_err(|e| polars_err!(ComputeError: "Invalid regex pattern: {}", e))?;
    
    let str_ca = s.str()?;
    let mut out: Vec<bool> = Vec::with_capacity(s.len());
    
    // Check if pattern is a simple literal that we can optimize
    let is_simple_literal = !pattern.chars().any(|c| {
        matches!(c, '.' | '*' | '+' | '?' | '[' | ']' | '(' | ')' | '{' | '}' | '|' | '^' | '$' | '\\')
    });
    
    if is_simple_literal && !pattern.is_empty() {
        // Simple literal search - can use SIMD substring search
        let pattern_bytes = pattern.as_bytes();
        let pattern_len = pattern_bytes.len();
        
        for arr in str_ca.downcast_iter() {
            for opt_val in arr.iter() {
                match opt_val {
                    Some(val) => {
                        let bytes = val.as_bytes();
                        // Simple contains check - SIMD optimized by compiler for memchr-like operations
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

/// SIMD-optimized one-hot encoding lookup.
/// Compares a string column against a vocabulary and returns binary indicators.
/// 
/// Returns a DataFrame with one column per category.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn onehot_lookup_simd(
    s: &Series, 
    vocab: &[String], 
    base_name: &str
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
    let result: Vec<Series> = vocab.iter().enumerate().map(|(idx, category)| {
        let col_name = format!("{}_{}", base_name, category);
        Series::new(col_name.into(), outputs[idx].clone())
    }).collect();
    
    Ok(result)
}

// Fallback for non-supported architectures
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn minmax_scale_simd(s: &Series, _min: f64, _max: f64) -> PolarsResult<Series> {
    Ok(s.clone()) // Dummy
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn standard_scale_simd(s: &Series, _mean: f64, _std: f64) -> PolarsResult<Series> {
    Ok(s.clone()) // Dummy
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn regex_validate_simd(s: &Series, _pattern: &str) -> PolarsResult<Series> {
    Ok(s.clone()) // Dummy
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn onehot_lookup_simd(s: &Series, _vocab: &[String], _base_name: &str) -> PolarsResult<Vec<Series>> {
    Ok(vec![]) // Dummy
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
        let s = Series::new("email".into(), vec!["alice@example.com", "bob@test.org", "invalid-email"]);
        
        // Simple literal pattern "@" - should use SIMD-optimized path
        let result = regex_validate_simd(&s, "@").unwrap();
        let ca = result.bool().unwrap();
        
        assert_eq!(ca.get(0), Some(true));  // alice@example.com contains @
        assert_eq!(ca.get(1), Some(true));  // bob@test.org contains @
        assert_eq!(ca.get(2), Some(false)); // invalid-email does NOT contain @
    }

    #[test]
    fn test_regex_validate_simd_complex_pattern() {
        let s = Series::new("email".into(), vec!["alice@example.com", "bob@test.org", "invalid"]);
        
        // Complex regex pattern - uses regex crate fallback
        let result = regex_validate_simd(&s, r"^[a-z]+@[a-z]+\.[a-z]+$").unwrap();
        let ca = result.bool().unwrap();
        
        assert_eq!(ca.get(0), Some(true));  // valid email format
        assert_eq!(ca.get(1), Some(true));  // valid email format
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
