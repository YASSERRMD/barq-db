use crate::DistanceMetric;

#[inline]
pub fn score_with_metric(metric: DistanceMetric, lhs: &[f32], rhs: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => -l2_distance(lhs, rhs),
        DistanceMetric::Cosine => cosine_similarity(lhs, rhs),
        DistanceMetric::Dot => dot_product(lhs, rhs),
    }
}

pub fn l2_distance(lhs: &[f32], rhs: &[f32]) -> f32 {
    if lhs.len() != rhs.len() {
        return 0.0; 
    }
    let mut sum = 0.0;
    // Explicit loop for better auto-vectorization
    for i in 0..lhs.len() {
        let diff = lhs[i] - rhs[i];
        sum += diff * diff;
    }
    sum.sqrt()
}

pub fn dot_product(lhs: &[f32], rhs: &[f32]) -> f32 {
    #[cfg(target_arch = "aarch64")]
    {
        if is_aarch64_feature_detected!("neon") {
            return unsafe { dot_product_neon(lhs, rhs) };
        }
    }
    dot_product_naive(lhs, rhs)
}

fn dot_product_naive(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum = 0.0;
    for i in 0..len {
        sum += lhs[i] * rhs[i];
    }
    sum
}

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "aarch64")]
use std::arch::is_aarch64_feature_detected;

#[cfg(target_arch = "aarch64")]
unsafe fn dot_product_neon(lhs: &[f32], rhs: &[f32]) -> f32 {
    let len = lhs.len().min(rhs.len());
    let mut sum_v = vdupq_n_f32(0.0);
    // Process 4 floats at a time
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
    // Handle remaining elements
    for j in i..len {
        sum += *lhs.get_unchecked(j) * *rhs.get_unchecked(j);
    }
    sum
}

pub fn cosine_similarity(lhs: &[f32], rhs: &[f32]) -> f32 {
    let dot = dot_product(lhs, rhs);
    let norm_l = dot_product(lhs, lhs).sqrt();
    let norm_r = dot_product(rhs, rhs).sqrt();
    if norm_l == 0.0 || norm_r == 0.0 {
        return 0.0;
    }
    dot / (norm_l * norm_r)
}
