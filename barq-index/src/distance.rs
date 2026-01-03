use crate::DistanceMetric;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// Detected SIMD capability
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimdCapability {
    Avx512,
    Avx2,
    Sse4,
    Neon,
    Scalar,
}

/// Detect the best SIMD capability at runtime
pub fn detect_simd_capability() -> SimdCapability {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            return SimdCapability::Avx512;
        }
        if is_x86_feature_detected!("avx2") {
            return SimdCapability::Avx2;
        }
        if is_x86_feature_detected!("sse4.1") {
            return SimdCapability::Sse4;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return SimdCapability::Neon;
        }
    }
    SimdCapability::Scalar
}

#[inline]
pub fn score_with_metric(metric: DistanceMetric, lhs: &[f32], rhs: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => -l2_distance(lhs, rhs),
        DistanceMetric::Cosine => cosine_similarity(lhs, rhs),
        DistanceMetric::Dot => dot_product(lhs, rhs),
    }
}

pub fn l2_distance(lhs: &[f32], rhs: &[f32]) -> f32 {
    match detect_simd_capability() {
        SimdCapability::Avx512 => unsafe { l2_avx512(lhs, rhs) },
        SimdCapability::Avx2 => unsafe { l2_avx2(lhs, rhs) },
        SimdCapability::Sse4 => unsafe { l2_sse4(lhs, rhs) },
        SimdCapability::Neon => unsafe { l2_neon(lhs, rhs) },
        SimdCapability::Scalar => l2_scalar(lhs, rhs),
    }
}

pub fn dot_product(lhs: &[f32], rhs: &[f32]) -> f32 {
    match detect_simd_capability() {
        SimdCapability::Avx512 => unsafe { dot_avx512(lhs, rhs) },
        SimdCapability::Avx2 => unsafe { dot_avx2(lhs, rhs) },
        SimdCapability::Sse4 => unsafe { dot_sse4(lhs, rhs) },
        SimdCapability::Neon => unsafe { dot_neon(lhs, rhs) },
        SimdCapability::Scalar => dot_scalar(lhs, rhs),
    }
}

pub fn cosine_similarity(lhs: &[f32], rhs: &[f32]) -> f32 {
    let dot = dot_product(lhs, rhs);
    // Use the same capability for norms
    let (norm_l, norm_r) = match detect_simd_capability() {
        SimdCapability::Avx512 => unsafe { (norm_avx512(lhs), norm_avx512(rhs)) },
        SimdCapability::Avx2 => unsafe { (norm_avx2(lhs), norm_avx2(rhs)) },
        SimdCapability::Sse4 => unsafe { (norm_sse4(lhs), norm_sse4(rhs)) },
        SimdCapability::Neon => unsafe { (norm_neon(lhs), norm_neon(rhs)) },
        SimdCapability::Scalar => (norm_scalar(lhs), norm_scalar(rhs)),
    };
    
    if norm_l == 0.0 || norm_r == 0.0 {
        return 0.0;
    }
    dot / (norm_l * norm_r)
}

// --- Scalar Implementations ---

fn l2_scalar(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum = 0.0;
    for i in 0..len {
        let diff = lhs[i] - rhs[i];
        sum += diff * diff;
    }
    sum.sqrt()
}

fn dot_scalar(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum = 0.0;
    for i in 0..len {
        sum += lhs[i] * rhs[i];
    }
    sum
}

fn norm_scalar(vec: &[f32]) -> f32 {
    dot_scalar(vec, vec).sqrt()
}

// --- SIMD Stubs that panic if called on wrong arch (should be guarded) ---

#[cfg(not(target_arch = "x86_64"))]
unsafe fn l2_avx512(_lhs: &[f32], _rhs: &[f32]) -> f32 { panic!("AVX512 not supported") }
#[cfg(not(target_arch = "x86_64"))]
unsafe fn l2_avx2(_lhs: &[f32], _rhs: &[f32]) -> f32 { panic!("AVX2 not supported") }
#[cfg(not(target_arch = "x86_64"))]
unsafe fn l2_sse4(_lhs: &[f32], _rhs: &[f32]) -> f32 { panic!("SSE4 not supported") }
#[cfg(not(target_arch = "x86_64"))]
unsafe fn dot_avx512(_lhs: &[f32], _rhs: &[f32]) -> f32 { panic!("AVX512 not supported") }
#[cfg(not(target_arch = "x86_64"))]
unsafe fn dot_avx2(_lhs: &[f32], _rhs: &[f32]) -> f32 { panic!("AVX2 not supported") }
#[cfg(not(target_arch = "x86_64"))]
unsafe fn dot_sse4(_lhs: &[f32], _rhs: &[f32]) -> f32 { panic!("SSE4 not supported") }
#[cfg(not(target_arch = "x86_64"))]
unsafe fn norm_avx512(_vec: &[f32]) -> f32 { panic!("AVX512 not supported") }
#[cfg(not(target_arch = "x86_64"))]
unsafe fn norm_avx2(_vec: &[f32]) -> f32 { panic!("AVX2 not supported") }
#[cfg(not(target_arch = "x86_64"))]
unsafe fn norm_sse4(_vec: &[f32]) -> f32 { panic!("SSE4 not supported") }

#[cfg(not(target_arch = "aarch64"))]
unsafe fn l2_neon(_lhs: &[f32], _rhs: &[f32]) -> f32 { panic!("NEON not supported") }
#[cfg(not(target_arch = "aarch64"))]
unsafe fn dot_neon(_lhs: &[f32], _rhs: &[f32]) -> f32 { panic!("NEON not supported") }
#[cfg(not(target_arch = "aarch64"))]
unsafe fn norm_neon(_vec: &[f32]) -> f32 { panic!("NEON not supported") }


// --- NEON Implementations ---

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn l2_neon(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum_v = vdupq_n_f32(0.0);
    let mut i = 0;
    while i + 4 <= len {
        let l_ptr = lhs.as_ptr().add(i);
        let r_ptr = rhs.as_ptr().add(i);
        let l_vec = vld1q_f32(l_ptr);
        let r_vec = vld1q_f32(r_ptr);
        let diff = vsubq_f32(l_vec, r_vec);
        sum_v = vfmaq_f32(sum_v, diff, diff);
        i += 4;
    }
    let mut sum = vaddvq_f32(sum_v);
    for j in i..len {
        let diff = *lhs.get_unchecked(j) - *rhs.get_unchecked(j);
        sum += diff * diff;
    }
    sum.sqrt()
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_neon(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum_v = vdupq_n_f32(0.0);
    let mut i = 0;
    while i + 4 <= len {
        let l_ptr = lhs.as_ptr().add(i);
        let r_ptr = rhs.as_ptr().add(i);
        let l_vec = vld1q_f32(l_ptr);
        let r_vec = vld1q_f32(r_ptr);
        sum_v = vfmaq_f32(sum_v, l_vec, r_vec);
        i += 4;
    }
    let mut sum = vaddvq_f32(sum_v);
    for j in i..len {
        sum += *lhs.get_unchecked(j) * *rhs.get_unchecked(j);
    }
    sum
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn norm_neon(vec: &[f32]) -> f32 {
    dot_neon(vec, vec).sqrt()
}

// --- x86_64 Implementations ---

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn l2_avx2(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum_v = _mm256_setzero_ps();
    let mut i = 0;
    while i + 8 <= len {
        let l_ptr = lhs.as_ptr().add(i);
        let r_ptr = rhs.as_ptr().add(i);
        let l_vec = _mm256_loadu_ps(l_ptr);
        let r_vec = _mm256_loadu_ps(r_ptr);
        let diff = _mm256_sub_ps(l_vec, r_vec);
        sum_v = _mm256_fmadd_ps(diff, diff, sum_v);
        i += 8;
    }
    let mut arr = [0.0; 8];
    _mm256_storeu_ps(arr.as_mut_ptr(), sum_v);
    let mut sum: f32 = arr.iter().sum();
    
    for j in i..len {
        let diff = *lhs.get_unchecked(j) - *rhs.get_unchecked(j);
        sum += diff * diff;
    }
    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn dot_avx2(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum_v = _mm256_setzero_ps();
    let mut i = 0;
    while i + 8 <= len {
        let l_ptr = lhs.as_ptr().add(i);
        let r_ptr = rhs.as_ptr().add(i);
        let l_vec = _mm256_loadu_ps(l_ptr);
        let r_vec = _mm256_loadu_ps(r_ptr);
        sum_v = _mm256_fmadd_ps(l_vec, r_vec, sum_v);
        i += 8;
    }
    let mut arr = [0.0; 8];
    _mm256_storeu_ps(arr.as_mut_ptr(), sum_v);
    let mut sum: f32 = arr.iter().sum();
    
    for j in i..len {
        sum += *lhs.get_unchecked(j) * *rhs.get_unchecked(j);
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn norm_avx2(vec: &[f32]) -> f32 {
    dot_avx2(vec, vec).sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn l2_sse4(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum_v = _mm_setzero_ps();
    let mut i = 0;
    while i + 4 <= len {
        let l_ptr = lhs.as_ptr().add(i);
        let r_ptr = rhs.as_ptr().add(i);
        let l_vec = _mm_loadu_ps(l_ptr);
        let r_vec = _mm_loadu_ps(r_ptr);
        let diff = _mm_sub_ps(l_vec, r_vec);
        // fmadd not available in SSE4.1, use mul+add
        let sq = _mm_mul_ps(diff, diff);
        sum_v = _mm_add_ps(sum_v, sq);
        i += 4;
    }
    let mut arr = [0.0; 4];
    _mm_storeu_ps(arr.as_mut_ptr(), sum_v);
    let mut sum: f32 = arr.iter().sum();
    for j in i..len {
        let diff = *lhs.get_unchecked(j) - *rhs.get_unchecked(j);
        sum += diff * diff;
    }
    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn dot_sse4(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum_v = _mm_setzero_ps();
    let mut i = 0;
    while i + 4 <= len {
        let l_ptr = lhs.as_ptr().add(i);
        let r_ptr = rhs.as_ptr().add(i);
        let l_vec = _mm_loadu_ps(l_ptr);
        let r_vec = _mm_loadu_ps(r_ptr);
        let prod = _mm_mul_ps(l_vec, r_vec);
        sum_v = _mm_add_ps(sum_v, prod);
        i += 4;
    }
    let mut arr = [0.0; 4];
    _mm_storeu_ps(arr.as_mut_ptr(), sum_v);
    let mut sum: f32 = arr.iter().sum();
    for j in i..len {
        sum += *lhs.get_unchecked(j) * *rhs.get_unchecked(j);
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn norm_sse4(vec: &[f32]) -> f32 {
    dot_sse4(vec, vec).sqrt()
}

// AVX-512 Stub (implement if environment supports)
// For now, mapping to AVX2 logic but compiled with avx512f feature
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn l2_avx512(lhs: &[f32], rhs: &[f32]) -> f32 {
    // Basic implementation using 512-bit registers
    let len = lhs.len().min(rhs.len());
    let mut sum_v = _mm512_setzero_ps();
    let mut i = 0;
    while i + 16 <= len {
        let l_ptr = lhs.as_ptr().add(i);
        let r_ptr = rhs.as_ptr().add(i);
        let l_vec = _mm512_loadu_ps(l_ptr);
        let r_vec = _mm512_loadu_ps(r_ptr);
        let diff = _mm512_sub_ps(l_vec, r_vec);
        sum_v = _mm512_fmadd_ps(diff, diff, sum_v);
        i += 16;
    }
    let mut idx = i;
    // Reduce 512 register to scalar
    let sum_128 = _mm512_extractf32x4_ps(sum_v, 0); // Need proper reduction
    // Simplified reduction:
    let mut arr = [0.0; 16];
    _mm512_storeu_ps(arr.as_mut_ptr(), sum_v);
    let mut sum: f32 = arr.iter().sum();

    for j in idx..len {
        let diff = *lhs.get_unchecked(j) - *rhs.get_unchecked(j);
        sum += diff * diff;
    }
    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn dot_avx512(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum_v = _mm512_setzero_ps();
    let mut i = 0;
    while i + 16 <= len {
        let l_ptr = lhs.as_ptr().add(i);
        let r_ptr = rhs.as_ptr().add(i);
        let l_vec = _mm512_loadu_ps(l_ptr);
        let r_vec = _mm512_loadu_ps(r_ptr);
        sum_v = _mm512_fmadd_ps(l_vec, r_vec, sum_v);
        i += 16;
    }
    let mut arr = [0.0; 16];
    _mm512_storeu_ps(arr.as_mut_ptr(), sum_v);
    let mut sum: f32 = arr.iter().sum();
    for j in i..len {
        sum += *lhs.get_unchecked(j) * *rhs.get_unchecked(j);
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn norm_avx512(vec: &[f32]) -> f32 {
    dot_avx512(vec, vec).sqrt()
}
