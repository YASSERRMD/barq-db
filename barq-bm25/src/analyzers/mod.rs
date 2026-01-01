//! Text Analyzers Module
//!
//! Provides various text analyzers for different languages,
//! including Arabic with advanced morphological support.

mod arabic;
mod english;
mod multilingual;
mod traits;
mod roots;
mod stop_words;

pub use arabic::{ArabicAnalyzer, ArabicNormalizer, ArabicStemmer};
pub use english::EnglishAnalyzer;
pub use multilingual::MultilingualAnalyzer;
pub use traits::{Analyzer, AnalyzerConfig, Language};
pub use roots::{ArabicRootExtractor, common_arabic_roots};
pub use stop_words::{StopWords, DEFAULT_ARABIC_STOP_WORDS, DEFAULT_ENGLISH_STOP_WORDS};
