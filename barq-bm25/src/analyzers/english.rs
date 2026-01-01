//! English Text Analyzer
//!
//! Basic English text analyzer with stemming and stop words support.

use super::traits::{Analyzer, AnalyzerConfig};
use std::collections::HashSet;

/// English stop words list.
static ENGLISH_STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
    "to", "was", "were", "will", "with", "the", "this", "but", "they",
    "have", "had", "what", "when", "where", "who", "which", "why", "how",
    "all", "each", "every", "both", "few", "more", "most", "other", "some",
    "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too",
    "very", "can", "just", "should", "now", "or", "if", "then", "else",
    "do", "does", "did", "doing", "would", "could", "should", "might",
    "must", "shall", "may", "here", "there", "am", "been", "being",
];

/// English text analyzer.
pub struct EnglishAnalyzer {
    config: AnalyzerConfig,
    stop_words: HashSet<String>,
}

impl Default for EnglishAnalyzer {
    fn default() -> Self {
        Self::new(AnalyzerConfig::english())
    }
}

impl EnglishAnalyzer {
    /// Create a new English analyzer.
    pub fn new(config: AnalyzerConfig) -> Self {
        let stop_words: HashSet<String> = ENGLISH_STOP_WORDS
            .iter()
            .map(|s| s.to_string())
            .collect();

        Self { config, stop_words }
    }

    /// Apply basic Porter-like stemming rules.
    fn stem(&self, word: &str) -> String {
        let mut result = word.to_string();

        // Simple suffix rules (not a full Porter stemmer, but covers common cases)
        if result.len() > 4 {
            if result.ends_with("ing") {
                result = result[..result.len() - 3].to_string();
                if result.ends_with("e") {
                    result.pop();
                }
            } else if result.ends_with("ed") {
                result = result[..result.len() - 2].to_string();
            } else if result.ends_with("ly") {
                result = result[..result.len() - 2].to_string();
            } else if result.ends_with("ies") {
                result = format!("{}y", &result[..result.len() - 3]);
            } else if result.ends_with("es") && !result.ends_with("ies") {
                result = result[..result.len() - 2].to_string();
            } else if result.ends_with("s") && !result.ends_with("ss") {
                result = result[..result.len() - 1].to_string();
            } else if result.ends_with("ness") {
                result = result[..result.len() - 4].to_string();
            } else if result.ends_with("ment") {
                result = result[..result.len() - 4].to_string();
            } else if result.ends_with("tion") {
                result = format!("{}t", &result[..result.len() - 3]);
            }
        }

        result
    }
}

impl Analyzer for EnglishAnalyzer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter_map(|token| {
                let processed = if self.config.lowercase {
                    token.to_lowercase()
                } else {
                    token.to_string()
                };

                // Check length
                let len = processed.len();
                if len < self.config.min_token_length || len > self.config.max_token_length {
                    return None;
                }

                // Remove stop words
                if self.config.remove_stop_words && self.stop_words.contains(&processed) {
                    return None;
                }

                // Apply stemming
                let result = if self.config.stemming {
                    self.stem(&processed)
                } else {
                    processed
                };

                if result.is_empty() {
                    None
                } else {
                    Some(result)
                }
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
    fn test_basic_tokenization() {
        let analyzer = EnglishAnalyzer::default();
        let tokens = analyzer.tokenize("Hello World");
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn test_stop_words_removal() {
        let analyzer = EnglishAnalyzer::default();
        let tokens = analyzer.tokenize("The quick brown fox");
        assert!(!tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
    }

    #[test]
    fn test_stemming() {
        let analyzer = EnglishAnalyzer::default();
        
        let tokens = analyzer.tokenize("running");
        assert!(tokens.contains(&"runn".to_string()) || tokens.contains(&"run".to_string()));
        
        let tokens = analyzer.tokenize("played");
        assert!(tokens.contains(&"play".to_string()));
    }

    #[test]
    fn test_punctuation_handling() {
        let analyzer = EnglishAnalyzer::default();
        let tokens = analyzer.tokenize("Hello, World! How are you?");
        // Should tokenize on punctuation
        assert!(tokens.len() >= 2);
    }
}
