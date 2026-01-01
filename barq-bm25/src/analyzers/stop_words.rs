//! Configurable Stop Words
//!
//! Provides configurable stop words loading from files or custom lists.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// Default Arabic stop words.
pub static DEFAULT_ARABIC_STOP_WORDS: &[&str] = &[
    // Particles and prepositions
    "من", "في", "على", "إلى", "عن", "مع", "بين", "حتى", "منذ",
    // Conjunctions  
    "و", "أو", "ثم", "لكن", "بل", "أم", "إما", "لو", "لولا",
    // Pronouns
    "هو", "هي", "هم", "هن", "أنا", "نحن", "أنت", "أنتم", "أنتن",
    // Demonstratives
    "هذا", "هذه", "ذلك", "تلك", "هؤلاء", "أولئك",
    // Relative pronouns
    "الذي", "التي", "الذين", "اللواتي", "اللاتي",
    // Question words
    "ما", "ماذا", "أين", "متى", "كيف", "لماذا", "كم", "أي",
    // Auxiliary verbs
    "كان", "يكون", "كانت", "كانوا", "ليس", "ليست",
    // Common particles
    "قد", "لقد", "سوف", "لن", "لم", "لا", "إن", "أن", "إذا", "إذ",
    // Articles and prefixes
    "ال", "ب", "ل", "ك", "ف",
    // Common words
    "كل", "بعض", "غير", "عند", "ذات", "هناك", "هنا",
    "فقط", "أيضا", "جدا", "معظم", "كثير", "قليل",
    "بعد", "قبل", "خلال", "أثناء", "ضد", "نحو", "حول",
    "فوق", "تحت", "أمام", "خلف", "بجانب",
];

/// Default English stop words.
pub static DEFAULT_ENGLISH_STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
    "to", "was", "were", "will", "with", "this", "but", "they",
    "have", "had", "what", "when", "where", "who", "which", "why", "how",
    "all", "each", "every", "both", "few", "more", "most", "other", "some",
    "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too",
    "very", "can", "just", "should", "now", "or", "if", "then", "else",
    "do", "does", "did", "doing", "would", "could", "might",
    "must", "shall", "may", "here", "there", "am", "been", "being",
];

/// Configurable stop words collection.
#[derive(Debug, Clone)]
pub struct StopWords {
    words: HashSet<String>,
    case_insensitive: bool,
}

impl Default for StopWords {
    fn default() -> Self {
        Self::new()
    }
}

impl StopWords {
    /// Create an empty stop words collection.
    pub fn new() -> Self {
        Self {
            words: HashSet::new(),
            case_insensitive: false,
        }
    }

    /// Create from a slice of words.
    pub fn from_slice(words: &[&str]) -> Self {
        let words = words.iter().map(|s| s.to_string()).collect();
        Self {
            words,
            case_insensitive: false,
        }
    }

    /// Create with default Arabic stop words.
    pub fn arabic() -> Self {
        Self::from_slice(DEFAULT_ARABIC_STOP_WORDS)
    }

    /// Create with default English stop words.
    pub fn english() -> Self {
        let mut sw = Self::from_slice(DEFAULT_ENGLISH_STOP_WORDS);
        sw.case_insensitive = true;
        sw
    }

    /// Load stop words from a file (one word per line).
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let content = fs::read_to_string(path)?;
        let words: HashSet<String> = content
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|s| s.to_string())
            .collect();

        Ok(Self {
            words,
            case_insensitive: false,
        })
    }

    /// Set case sensitivity.
    pub fn case_insensitive(mut self, value: bool) -> Self {
        self.case_insensitive = value;
        self
    }

    /// Add a word to the stop words list.
    pub fn add(&mut self, word: impl Into<String>) {
        self.words.insert(word.into());
    }

    /// Add multiple words.
    pub fn add_words(&mut self, words: &[&str]) {
        for word in words {
            self.words.insert(word.to_string());
        }
    }

    /// Remove a word from the stop words list.
    pub fn remove(&mut self, word: &str) {
        self.words.remove(word);
    }

    /// Check if a word is a stop word.
    pub fn contains(&self, word: &str) -> bool {
        if self.case_insensitive {
            self.words.contains(&word.to_lowercase())
        } else {
            self.words.contains(word)
        }
    }

    /// Get the number of stop words.
    pub fn len(&self) -> usize {
        self.words.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    /// Get an iterator over the stop words.
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.words.iter()
    }

    /// Merge with another stop words collection.
    pub fn merge(&mut self, other: &StopWords) {
        for word in &other.words {
            self.words.insert(word.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arabic_stop_words() {
        let sw = StopWords::arabic();
        assert!(sw.contains("من"));
        assert!(sw.contains("في"));
        assert!(sw.contains("هذا"));
        assert!(!sw.contains("كتاب"));
    }

    #[test]
    fn test_english_stop_words() {
        let sw = StopWords::english();
        assert!(sw.contains("the"));
        assert!(sw.contains("The")); // case insensitive
        assert!(sw.contains("and"));
        assert!(!sw.contains("book"));
    }

    #[test]
    fn test_custom_stop_words() {
        let mut sw = StopWords::new();
        sw.add("custom");
        sw.add("words");
        
        assert!(sw.contains("custom"));
        assert!(sw.contains("words"));
        assert!(!sw.contains("other"));
    }

    #[test]
    fn test_add_and_remove() {
        let mut sw = StopWords::arabic();
        let original_len = sw.len();
        
        sw.add("جديد");
        assert_eq!(sw.len(), original_len + 1);
        assert!(sw.contains("جديد"));
        
        sw.remove("جديد");
        assert_eq!(sw.len(), original_len);
        assert!(!sw.contains("جديد"));
    }

    #[test]
    fn test_merge() {
        let mut arabic = StopWords::arabic();
        let english = StopWords::english();
        let arabic_len = arabic.len();
        let english_len = english.len();
        
        arabic.merge(&english);
        assert!(arabic.len() >= arabic_len);
        assert!(arabic.contains("the"));
        assert!(arabic.contains("من"));
    }
}
