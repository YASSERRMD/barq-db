//! Advanced Arabic Text Analyzer
//!
//! Provides comprehensive Arabic text analysis including:
//! - Diacritics (tashkeel) removal
//! - Tatweel (kashida) removal
//! - Character normalization (alef, yaa, taa marbuta)
//! - Arabic stemming (light and aggressive)
//! - Stop words removal
//! - Proper tokenization for Arabic script

use super::traits::{Analyzer, AnalyzerConfig};




/// Arabic text normalizer.
pub struct ArabicNormalizer {
    /// Remove diacritical marks (tashkeel)
    pub remove_diacritics: bool,
    /// Remove tatweel (kashida)
    pub remove_tatweel: bool,
    /// Normalize alef variants to bare alef
    pub normalize_alef: bool,
    /// Normalize yaa variants
    pub normalize_yaa: bool,
    /// Normalize taa marbuta to haa
    pub normalize_taa_marbuta: bool,
    /// Normalize hamza variants
    pub normalize_hamza: bool,
}

impl Default for ArabicNormalizer {
    fn default() -> Self {
        Self {
            remove_diacritics: true,
            remove_tatweel: true,
            normalize_alef: true,
            normalize_yaa: true,
            normalize_taa_marbuta: true,
            normalize_hamza: true,
        }
    }
}

impl ArabicNormalizer {
    /// Create a new Arabic normalizer with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Normalize Arabic text.
    pub fn normalize(&self, text: &str) -> String {
        let mut result = String::with_capacity(text.len());

        for ch in text.chars() {
            // Skip diacritical marks (tashkeel)
            if self.remove_diacritics && is_arabic_diacritic(ch) {
                continue;
            }

            // Skip tatweel (kashida)
            if self.remove_tatweel && ch == '\u{0640}' {
                continue;
            }

            // Normalize alef variants
            if self.normalize_alef && is_alef_variant(ch) {
                result.push('\u{0627}'); // bare alef
                continue;
            }

            // Normalize yaa variants (alef maqsura to yaa)
            if self.normalize_yaa && ch == '\u{0649}' {
                result.push('\u{064A}'); // yaa
                continue;
            }

            // Normalize taa marbuta to haa
            if self.normalize_taa_marbuta && ch == '\u{0629}' {
                result.push('\u{0647}'); // haa
                continue;
            }

            // Normalize hamza variants
            if self.normalize_hamza {
                if let Some(normalized) = normalize_hamza(ch) {
                    result.push(normalized);
                    continue;
                }
            }

            result.push(ch);
        }

        result
    }
}

/// Check if a character is an Arabic diacritical mark.
fn is_arabic_diacritic(ch: char) -> bool {
    matches!(ch,
        '\u{064B}'..='\u{0652}' | // Fathatan to Sukun
        '\u{0670}' |              // Superscript alef
        '\u{0653}'..='\u{0655}' | // Maddah, Hamza above/below
        '\u{065F}'                // Wavy hamza below
    )
}

/// Check if a character is an alef variant.
fn is_alef_variant(ch: char) -> bool {
    matches!(ch,
        '\u{0622}' | // Alef with madda
        '\u{0623}' | // Alef with hamza above
        '\u{0625}' | // Alef with hamza below
        '\u{0671}'   // Alef wasla
    )
}

/// Normalize hamza variants.
fn normalize_hamza(ch: char) -> Option<char> {
    match ch {
        '\u{0624}' => Some('\u{0648}'), // Waw with hamza -> waw
        '\u{0626}' => Some('\u{064A}'), // Yaa with hamza -> yaa
        _ => None,
    }
}

/// Arabic stemmer implementing Light10 stemming algorithm.
pub struct ArabicStemmer {
    /// Enable prefix removal
    pub remove_prefixes: bool,
    /// Enable suffix removal
    pub remove_suffixes: bool,
    /// Minimum word length after stemming
    pub min_stem_length: usize,
}

impl Default for ArabicStemmer {
    fn default() -> Self {
        Self {
            remove_prefixes: true,
            remove_suffixes: true,
            min_stem_length: 2,
        }
    }
}

impl ArabicStemmer {
    /// Create a new Arabic stemmer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stem an Arabic word.
    pub fn stem(&self, word: &str) -> String {
        let mut result = word.to_string();

        // Remove definite article prefix (ال)
        if self.remove_prefixes {
            result = self.remove_prefix(&result);
        }

        // Remove common suffixes
        if self.remove_suffixes {
            result = self.remove_suffix(&result);
        }

        // Ensure minimum length
        if result.chars().count() < self.min_stem_length {
            return word.to_string();
        }

        result
    }

    fn remove_prefix(&self, word: &str) -> String {
        let chars: Vec<char> = word.chars().collect();
        let len = chars.len();

        if len < 3 {
            return word.to_string();
        }

        // Remove "ال" (definite article)
        if len >= 3 && chars[0] == '\u{0627}' && chars[1] == '\u{0644}' {
            return chars[2..].iter().collect();
        }

        // Remove "و" (conjunction waw) at beginning
        if len >= 3 && chars[0] == '\u{0648}' {
            // Check if followed by alef-lam
            if chars[1] == '\u{0627}' && chars[2] == '\u{0644}' && len >= 4 {
                return chars[3..].iter().collect();
            }
            return chars[1..].iter().collect();
        }

        // Remove "ب" (preposition ba) at beginning
        if len >= 3 && chars[0] == '\u{0628}' {
            // Check if followed by alef-lam
            if len >= 4 && chars[1] == '\u{0627}' && chars[2] == '\u{0644}' {
                return chars[3..].iter().collect();
            }
        }

        // Remove "ك" (preposition kaf) at beginning
        if len >= 3 && chars[0] == '\u{0643}' {
            if len >= 4 && chars[1] == '\u{0627}' && chars[2] == '\u{0644}' {
                return chars[3..].iter().collect();
            }
        }

        // Remove "ل" (preposition lam) at beginning
        if len >= 3 && chars[0] == '\u{0644}' {
            if len >= 4 && chars[1] == '\u{0644}' {
                return chars[2..].iter().collect();
            }
            if len >= 4 && chars[1] == '\u{0627}' && chars[2] == '\u{0644}' {
                return chars[3..].iter().collect();
            }
        }

        word.to_string()
    }

    fn remove_suffix(&self, word: &str) -> String {
        let chars: Vec<char> = word.chars().collect();
        let len = chars.len();

        if len < 3 {
            return word.to_string();
        }

        // Remove plural and dual suffixes
        // ات (feminine plural)
        if len >= 4 && chars[len - 2] == '\u{0627}' && chars[len - 1] == '\u{062A}' {
            return chars[..len - 2].iter().collect();
        }

        // ون / ين (masculine plural/dual)
        if len >= 4 {
            let last_two: String = chars[len - 2..].iter().collect();
            if last_two == "ون" || last_two == "ين" {
                return chars[..len - 2].iter().collect();
            }
        }

        // ان (dual)
        if len >= 4 && chars[len - 2] == '\u{0627}' && chars[len - 1] == '\u{0646}' {
            return chars[..len - 2].iter().collect();
        }

        // ة (taa marbuta - already normalized to haa)
        if len >= 3 && chars[len - 1] == '\u{0647}' {
            return chars[..len - 1].iter().collect();
        }

        // ي (possessive yaa)
        if len >= 3 && chars[len - 1] == '\u{064A}' {
            return chars[..len - 1].iter().collect();
        }

        // ه / ها (possessive pronouns)
        if len >= 3 && chars[len - 1] == '\u{0647}' {
            return chars[..len - 1].iter().collect();
        }
        if len >= 4 && chars[len - 2] == '\u{0647}' && chars[len - 1] == '\u{0627}' {
            return chars[..len - 2].iter().collect();
        }

        word.to_string()
    }
}

use super::roots::ArabicRootExtractor;
use super::stop_words::StopWords;

/// Advanced Arabic text analyzer.
pub struct ArabicAnalyzer {
    config: AnalyzerConfig,
    normalizer: ArabicNormalizer,
    stemmer: ArabicStemmer,
    root_extractor: ArabicRootExtractor,
    stop_words: StopWords,
}

impl Default for ArabicAnalyzer {
    fn default() -> Self {
        Self::new(AnalyzerConfig::arabic())
    }
}

impl ArabicAnalyzer {
    /// Create a new Arabic analyzer with the given configuration.
    pub fn new(config: AnalyzerConfig) -> Self {
        let normalizer = ArabicNormalizer {
            remove_diacritics: config.remove_diacritics,
            remove_tatweel: config.remove_tatweel,
            normalize_alef: config.normalize_arabic,
            normalize_yaa: config.normalize_arabic,
            normalize_taa_marbuta: config.normalize_arabic,
            normalize_hamza: config.normalize_arabic,
        };

        Self {
            config,
            normalizer,
            stemmer: ArabicStemmer::new(),
            root_extractor: ArabicRootExtractor::new(),
            stop_words: StopWords::arabic(),
        }
    }

    /// Create with custom stop words.
    pub fn with_stop_words(mut self, words: StopWords) -> Self {
        self.stop_words = words;
        self
    }

    /// Check if a character is Arabic.
    fn is_arabic_char(ch: char) -> bool {
        matches!(ch as u32,
            0x0600..=0x06FF |  // Arabic
            0x0750..=0x077F |  // Arabic Supplement
            0x08A0..=0x08FF |  // Arabic Extended-A
            0xFB50..=0xFDFF |  // Arabic Presentation Forms-A
            0xFE70..=0xFEFF    // Arabic Presentation Forms-B
        )
    }

    /// Check if a character is part of a token (Arabic letter or number).
    fn is_token_char(ch: char) -> bool {
        Self::is_arabic_char(ch) || ch.is_ascii_alphanumeric()
    }
}

impl Analyzer for ArabicAnalyzer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let mut current_token = String::new();

        // First normalize the entire text
        let normalized = self.normalizer.normalize(text);

        // Tokenize on whitespace and non-Arabic characters
        for ch in normalized.chars() {
            if Self::is_token_char(ch) {
                current_token.push(ch);
            } else if !current_token.is_empty() {
                tokens.push(current_token.clone());
                current_token.clear();
            }
        }

        if !current_token.is_empty() {
            tokens.push(current_token);
        }

        // Process tokens
        tokens
            .into_iter()
            .filter_map(|token| {
                // Check length constraints
                let len = token.chars().count();
                if len < self.config.min_token_length || len > self.config.max_token_length {
                    return None;
                }

                // Remove stop words
                if self.config.remove_stop_words && self.stop_words.contains(&token) {
                    return None;
                }

                // Apply stemming or root extraction
                let processed = if self.config.stemming {
                    // Try root extraction first (more aggressive)
                    if let Some(root) = self.root_extractor.extract(&token) {
                        root
                    } else {
                        // Fallback to light stemming
                        self.stemmer.stem(&token)
                    }
                } else {
                    token
                };

                // Final length check after stemming
                if processed.chars().count() < self.config.min_token_length {
                    return None;
                }

                Some(processed)
            })
            .collect()
    }

    fn config(&self) -> &AnalyzerConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_diacritics() {
        let normalizer = ArabicNormalizer::new();
        // kitab with diacritics
        let input = "كِتَابٌ";
        let result = normalizer.normalize(input);
        assert_eq!(result, "كتاب");
    }

    #[test]
    fn test_normalize_alef_variants() {
        let normalizer = ArabicNormalizer::new();
        // Different alef forms
        assert_eq!(normalizer.normalize("أحمد"), "احمد");
        assert_eq!(normalizer.normalize("إسلام"), "اسلام");
        assert_eq!(normalizer.normalize("آمين"), "امين");
    }

    #[test]
    fn test_remove_tatweel() {
        let normalizer = ArabicNormalizer::new();
        let input = "العـــربية"; // With tatweel
        let result = normalizer.normalize(input);
        assert_eq!(result, "العربيه");
    }

    #[test]
    fn test_stemmer_definite_article() {
        let stemmer = ArabicStemmer::new();
        assert_eq!(stemmer.stem("الكتاب"), "كتاب");
        // Note: Stemmer removes article, suffix removal handles taa marbuta
        // "المدرسة" -> removes "ال" = "مدرسة" -> taa marbuta is ة not ه
        assert_eq!(stemmer.stem("المدرسة"), "مدرسة");
    }

    #[test]
    fn test_stemmer_conjunction() {
        let stemmer = ArabicStemmer::new();
        assert_eq!(stemmer.stem("والكتاب"), "كتاب");
    }

    #[test]
    fn test_full_analysis() {
        let analyzer = ArabicAnalyzer::default();
        let text = "الكُتُبُ العَرَبِيَّةُ جَمِيلَةٌ";
        let tokens = analyzer.tokenize(text);
        
        // Should remove diacritics, stem, and remove stop words
        assert!(!tokens.is_empty());
        // "الكتب" should become "كتب" (stem removes article and suffix)
        assert!(tokens.iter().any(|t| t == "كتب"));
    }

    #[test]
    fn test_stop_words() {
        let analyzer = ArabicAnalyzer::default();
        let text = "هذا هو الكتاب الذي قرأته";
        let tokens = analyzer.tokenize(text);
        
        // Stop words should be removed
        assert!(!tokens.iter().any(|t| t == "هذا"));
        assert!(!tokens.iter().any(|t| t == "هو"));
        assert!(!tokens.iter().any(|t| t == "الذي"));
    }

    #[test]
    fn test_mixed_arabic_english() {
        let analyzer = ArabicAnalyzer::default();
        let text = "تعلم Python البرمجة";
        let tokens = analyzer.tokenize(text);
        
        // Should handle both Arabic and ASCII
        assert!(tokens.contains(&"python".to_string()) || tokens.contains(&"Python".to_string()));
    }
}
