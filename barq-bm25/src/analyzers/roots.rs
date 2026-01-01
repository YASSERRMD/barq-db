//! Arabic Root Extraction
//!
//! Implements Arabic root extraction based on morphological patterns.
//! Arabic words are typically derived from 3-letter (trilateral) or 4-letter
//! (quadrilateral) roots according to specific patterns (أوزان).

use std::collections::HashSet;

/// Common Arabic verb patterns (أوزان الأفعال)
/// These are the most common patterns used in Arabic morphology
static TRILITERAL_PATTERNS: &[&str] = &[
    // Form I - فَعَلَ (basic)
    "فعل",
    // Form II - فَعَّلَ (intensive/causative)
    "فعّل",
    // Form III - فاعَلَ (reciprocal)
    "فاعل",
    // Form IV - أَفْعَلَ (causative)
    "أفعل",
    // Form V - تَفَعَّلَ (reflexive of II)
    "تفعّل",
    // Form VI - تَفاعَلَ (reciprocal/reflexive)
    "تفاعل",
    // Form VII - اِنْفَعَلَ (passive/reflexive)
    "انفعل",
    // Form VIII - اِفْتَعَلَ (reflexive)
    "افتعل",
    // Form X - اِسْتَفْعَلَ (request/consider)
    "استفعل",
];



/// Arabic root extractor.
pub struct ArabicRootExtractor {
    /// Known roots dictionary (optional, for validation)
    known_roots: Option<HashSet<String>>,
    /// Minimum root length
    min_root_length: usize,
    /// Maximum root length
    max_root_length: usize,
}

impl Default for ArabicRootExtractor {
    fn default() -> Self {
        Self {
            known_roots: None,
            min_root_length: 3,
            max_root_length: 4,
        }
    }
}

impl ArabicRootExtractor {
    /// Create a new root extractor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a set of known roots for validation.
    pub fn with_known_roots(roots: HashSet<String>) -> Self {
        Self {
            known_roots: Some(roots),
            ..Default::default()
        }
    }

    /// Extract the root from an Arabic word.
    /// 
    /// This uses a simplified algorithm:
    /// 1. Remove known prefixes (ال، و، ب، ك، ف، ل، س)
    /// 2. Remove known suffixes (ات، ون، ين، ة، ي، ه)
    /// 3. Remove pattern letters (ا، و، ي، ت، م، ن، س، أ)
    /// 4. Keep consonants that form the root
    pub fn extract(&self, word: &str) -> Option<String> {
        let chars: Vec<char> = word.chars().collect();
        let len = chars.len();

        if len < 3 {
            return None;
        }

        // Step 1: Remove common prefixes
        let word = self.remove_prefixes(word);
        let chars: Vec<char> = word.chars().collect();
        let len = chars.len();

        if len < 3 {
            return None;
        }

        // Step 2: Remove common suffixes
        let word = self.remove_suffixes(&word);
        let chars: Vec<char> = word.chars().collect();
        let len = chars.len();

        if len < 3 {
            return None;
        }

        // Step 3: Extract root consonants
        let root = self.extract_root_consonants(&chars);

        // Validate root length
        let root_len = root.chars().count();
        if root_len >= self.min_root_length && root_len <= self.max_root_length {
            // Validate against known roots if available
            if let Some(ref known) = self.known_roots {
                if known.contains(&root) {
                    return Some(root);
                }
            } else {
                return Some(root);
            }
        }

        // Fallback: return first 3 consonants
        let fallback = self.fallback_extraction(&chars);
        if fallback.is_empty() {
            None
        } else {
            Some(fallback)
        }
    }

    fn remove_prefixes(&self, word: &str) -> String {
        let chars: Vec<char> = word.chars().collect();
        let len = chars.len();

        if len < 4 {
            return word.to_string();
        }

        // Remove multi-character prefixes first
        // استـ (Form X prefix)
        if len >= 6 && chars[0] == 'ا' && chars[1] == 'س' && chars[2] == 'ت' {
            return chars[3..].iter().collect();
        }

        // ال (definite article)
        if len >= 4 && chars[0] == 'ا' && chars[1] == 'ل' {
            return chars[2..].iter().collect();
        }

        // تـ (imperfect prefix)
        if len >= 4 && chars[0] == 'ت' {
            return chars[1..].iter().collect();
        }

        // يـ (imperfect prefix)
        if len >= 4 && chars[0] == 'ي' {
            return chars[1..].iter().collect();
        }

        // نـ (imperfect prefix)
        if len >= 4 && chars[0] == 'ن' {
            return chars[1..].iter().collect();
        }

        // أـ (imperfect/Form IV prefix)
        if len >= 4 && chars[0] == 'أ' {
            return chars[1..].iter().collect();
        }

        // مـ (participle prefix)
        if len >= 4 && chars[0] == 'م' {
            return chars[1..].iter().collect();
        }

        // Single character prefixes
        // وـ، بـ، كـ، فـ، لـ، سـ
        if len >= 6 {
            let first = chars[0];
            if first == 'و' || first == 'ب' || first == 'ك' || first == 'ف' || first == 'ل' || first == 'س' {
                return chars[1..].iter().collect();
            }
        }

        word.to_string()
    }

    fn remove_suffixes(&self, word: &str) -> String {
        let chars: Vec<char> = word.chars().collect();
        let len = chars.len();

        if len < 4 {
            return word.to_string();
        }

        // Multi-character suffixes
        // ـات (feminine plural)
        if len >= 4 && chars[len - 2] == 'ا' && chars[len - 1] == 'ت' {
            return chars[..len - 2].iter().collect();
        }

        // ـون / ـين (masculine plural)
        if len >= 4 {
            let suffix: String = chars[len - 2..].iter().collect();
            if suffix == "ون" || suffix == "ين" {
                return chars[..len - 2].iter().collect();
            }
        }

        // ـان (dual)
        if len >= 4 && chars[len - 2] == 'ا' && chars[len - 1] == 'ن' {
            return chars[..len - 2].iter().collect();
        }

        // Single character suffixes
        // ـة، ـي، ـه، ـا
        let last = chars[len - 1];
        if len >= 4 && (last == 'ة' || last == 'ي' || last == 'ه' || last == 'ا') {
            return chars[..len - 1].iter().collect();
        }

        word.to_string()
    }

    fn extract_root_consonants(&self, chars: &[char]) -> String {
        // Pattern letters (weak letters and inserted letters)
        let pattern_letters: HashSet<char> = [
            'ا', 'و', 'ي', 'ء',  // Weak letters
            'أ', 'إ', 'آ', 'ى',  // Alef variants
        ].iter().copied().collect();

        let mut root = String::new();
        
        for &ch in chars {
            if !pattern_letters.contains(&ch) && is_arabic_letter(ch) {
                root.push(ch);
                if root.chars().count() >= self.max_root_length {
                    break;
                }
            }
        }

        root
    }

    fn fallback_extraction(&self, chars: &[char]) -> String {
        // Simple fallback: take first 3 Arabic consonants
        let mut root = String::new();
        
        for &ch in chars {
            if is_arabic_consonant(ch) {
                root.push(ch);
                if root.chars().count() >= 3 {
                    break;
                }
            }
        }

        root
    }
}

/// Check if a character is an Arabic letter.
fn is_arabic_letter(ch: char) -> bool {
    matches!(ch as u32, 0x0621..=0x064A)
}

/// Check if a character is an Arabic consonant (not a weak letter).
fn is_arabic_consonant(ch: char) -> bool {
    is_arabic_letter(ch) && !matches!(ch, 'ا' | 'و' | 'ي' | 'ء' | 'أ' | 'إ' | 'آ' | 'ى')
}

/// Common 3-letter Arabic roots.
/// This is a small subset for demonstration and validation.
pub fn common_arabic_roots() -> HashSet<String> {
    [
        // Common verbs
        "كتب", "قرأ", "علم", "فهم", "درس", "عمل", "أكل", "شرب",
        "ذهب", "جلس", "نظر", "سمع", "فتح", "غلق", "خرج", "دخل",
        // Common nouns
        "بيت", "كلب", "قلم", "كتب", "طلب", "حمل", "حمد",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_root_extraction() {
        let extractor = ArabicRootExtractor::new();
        
        // كاتب (writer) -> كتب
        let root = extractor.extract("كاتب");
        assert!(root.is_some());
        assert_eq!(root.unwrap(), "كتب");
    }

    #[test]
    fn test_prefix_removal() {
        let extractor = ArabicRootExtractor::new();
        
        // يكتب (he writes) -> كتب
        let root = extractor.extract("يكتب");
        assert!(root.is_some());
        assert_eq!(root.unwrap(), "كتب");
    }

    #[test]
    fn test_suffix_removal() {
        let extractor = ArabicRootExtractor::new();
        
        // كتابة (writing) -> كتب
        let root = extractor.extract("كتابة");
        assert!(root.is_some());
        // After removing ة and extracting consonants
        let r = root.unwrap();
        assert!(r.contains('ك') && r.contains('ت') && r.contains('ب'));
    }

    #[test]
    fn test_with_known_roots() {
        let known = common_arabic_roots();
        let extractor = ArabicRootExtractor::with_known_roots(known);
        
        // مكتوب (written) -> كتب
        let root = extractor.extract("مكتوب");
        assert!(root.is_some());
    }

    #[test]
    fn test_short_word() {
        let extractor = ArabicRootExtractor::new();
        
        // Short words return None
        let root = extractor.extract("من");
        assert!(root.is_none());
    }
}
