//! Multilingual Text Analyzer
//!
//! Provides automatic language detection and appropriate analysis.

use super::arabic::ArabicAnalyzer;
use super::english::EnglishAnalyzer;
use super::traits::{Analyzer, AnalyzerConfig, Language};

/// Multilingual text analyzer with automatic language detection.
pub struct MultilingualAnalyzer {
    config: AnalyzerConfig,
    arabic_analyzer: ArabicAnalyzer,
    english_analyzer: EnglishAnalyzer,
}

impl Default for MultilingualAnalyzer {
    fn default() -> Self {
        Self::new(AnalyzerConfig::default())
    }
}

impl MultilingualAnalyzer {
    /// Create a new multilingual analyzer.
    pub fn new(config: AnalyzerConfig) -> Self {
        let arabic_config = AnalyzerConfig::arabic();
        let english_config = AnalyzerConfig::english();

        Self {
            config,
            arabic_analyzer: ArabicAnalyzer::new(arabic_config),
            english_analyzer: EnglishAnalyzer::new(english_config),
        }
    }

    /// Detect the primary language of text.
    pub fn detect_language(text: &str) -> Language {
        let mut arabic_chars = 0;
        let mut latin_chars = 0;
        let mut total_chars = 0;

        for ch in text.chars() {
            if ch.is_alphabetic() {
                total_chars += 1;
                if Self::is_arabic_char(ch) {
                    arabic_chars += 1;
                } else if ch.is_ascii_alphabetic() {
                    latin_chars += 1;
                }
            }
        }

        if total_chars == 0 {
            return Language::Universal;
        }

        let arabic_ratio = arabic_chars as f32 / total_chars as f32;
        let latin_ratio = latin_chars as f32 / total_chars as f32;

        if arabic_ratio > 0.5 {
            Language::Arabic
        } else if latin_ratio > 0.5 {
            Language::English
        } else {
            Language::Universal
        }
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
}

impl Analyzer for MultilingualAnalyzer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        let detected = Self::detect_language(text);

        match detected {
            Language::Arabic => self.arabic_analyzer.tokenize(text),
            Language::English => self.english_analyzer.tokenize(text),
            _ => {
                // For mixed content, tokenize separately and combine
                let mut tokens = Vec::new();
                let mut current_segment = String::new();
                let mut current_lang: Option<Language> = None;

                for ch in text.chars() {
                    let char_lang = if Self::is_arabic_char(ch) {
                        Some(Language::Arabic)
                    } else if ch.is_ascii_alphabetic() {
                        Some(Language::English)
                    } else if ch.is_whitespace() || !ch.is_alphanumeric() {
                        None
                    } else {
                        current_lang
                    };

                    if char_lang != current_lang && !current_segment.is_empty() {
                        // Process current segment
                        let segment_tokens = match current_lang {
                            Some(Language::Arabic) => {
                                self.arabic_analyzer.tokenize(&current_segment)
                            }
                            Some(Language::English) => {
                                self.english_analyzer.tokenize(&current_segment)
                            }
                            _ => vec![current_segment.clone()],
                        };
                        tokens.extend(segment_tokens);
                        current_segment.clear();
                    }

                    if char_lang.is_some() {
                        current_segment.push(ch);
                        current_lang = char_lang;
                    }
                }

                // Process remaining segment
                if !current_segment.is_empty() {
                    let segment_tokens = match current_lang {
                        Some(Language::Arabic) => {
                            self.arabic_analyzer.tokenize(&current_segment)
                        }
                        Some(Language::English) => {
                            self.english_analyzer.tokenize(&current_segment)
                        }
                        _ => vec![current_segment],
                    };
                    tokens.extend(segment_tokens);
                }

                tokens
            }
        }
    }

    fn config(&self) -> &AnalyzerConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_arabic() {
        let lang = MultilingualAnalyzer::detect_language("مرحبا بالعالم");
        assert_eq!(lang, Language::Arabic);
    }

    #[test]
    fn test_detect_english() {
        let lang = MultilingualAnalyzer::detect_language("Hello World");
        assert_eq!(lang, Language::English);
    }

    #[test]
    fn test_mixed_content() {
        let analyzer = MultilingualAnalyzer::default();
        let tokens = analyzer.tokenize("Hello مرحبا World عالم");
        
        // Should handle both languages
        assert!(!tokens.is_empty());
    }

    #[test]
    fn test_arabic_text() {
        let analyzer = MultilingualAnalyzer::default();
        let tokens = analyzer.tokenize("الكتاب العربي");
        
        // Should use Arabic analyzer
        assert!(!tokens.is_empty());
    }

    #[test]
    fn test_english_text() {
        let analyzer = MultilingualAnalyzer::default();
        let tokens = analyzer.tokenize("The quick brown fox");
        
        // Should use English analyzer and remove stop words
        assert!(!tokens.contains(&"the".to_string()));
    }
}
