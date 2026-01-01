//! Analyzer Traits
//!
//! Common interfaces for all text analyzers.

use serde::{Deserialize, Serialize};

/// Supported languages for text analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Language {
    /// English (default)
    English,
    /// Arabic with morphological analysis
    Arabic,
    /// Auto-detect language
    Auto,
    /// Process all languages without specific rules
    Universal,
}

impl Default for Language {
    fn default() -> Self {
        Language::English
    }
}

impl Language {
    /// Parse language from string.
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "ar" | "arabic" => Language::Arabic,
            "en" | "english" => Language::English,
            "auto" => Language::Auto,
            _ => Language::Universal,
        }
    }

    /// Get language code.
    pub fn code(&self) -> &'static str {
        match self {
            Language::English => "en",
            Language::Arabic => "ar",
            Language::Auto => "auto",
            Language::Universal => "universal",
        }
    }
}

/// Configuration for text analyzers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzerConfig {
    /// Target language
    pub language: Language,
    /// Enable stemming
    pub stemming: bool,
    /// Remove stop words
    pub remove_stop_words: bool,
    /// Lowercase all tokens
    pub lowercase: bool,
    /// Minimum token length
    pub min_token_length: usize,
    /// Maximum token length
    pub max_token_length: usize,
    /// Enable diacritics removal (for Arabic)
    pub remove_diacritics: bool,
    /// enable tatweel removal (for Arabic)
    pub remove_tatweel: bool,
    /// Normalize Arabic characters
    pub normalize_arabic: bool,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            language: Language::English,
            stemming: true,
            remove_stop_words: true,
            lowercase: true,
            min_token_length: 1,
            max_token_length: 100,
            remove_diacritics: true,
            remove_tatweel: true,
            normalize_arabic: true,
        }
    }
}

impl AnalyzerConfig {
    /// Create config for Arabic text.
    pub fn arabic() -> Self {
        Self {
            language: Language::Arabic,
            stemming: true,
            remove_stop_words: true,
            lowercase: false, // Arabic doesn't have case
            min_token_length: 2,
            max_token_length: 50,
            remove_diacritics: true,
            remove_tatweel: true,
            normalize_arabic: true,
        }
    }

    /// Create config for English text.
    pub fn english() -> Self {
        Self {
            language: Language::English,
            stemming: true,
            remove_stop_words: true,
            lowercase: true,
            min_token_length: 2,
            max_token_length: 50,
            remove_diacritics: false,
            remove_tatweel: false,
            normalize_arabic: false,
        }
    }
}

/// Common trait for all text analyzers.
pub trait Analyzer: Send + Sync {
    /// Tokenize text into individual tokens.
    fn tokenize(&self, text: &str) -> Vec<String>;

    /// Get the analyzer configuration.
    fn config(&self) -> &AnalyzerConfig;

    /// Get the target language.
    fn language(&self) -> Language {
        self.config().language
    }

    /// Process text: tokenize, normalize, stem, and filter.
    fn analyze(&self, text: &str) -> Vec<String> {
        self.tokenize(text)
    }
}
