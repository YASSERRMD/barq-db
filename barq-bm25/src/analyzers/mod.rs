//! Text Analyzers Module
//!
//! Provides various text analyzers for different languages,
//! including Arabic with advanced morphological support.

mod arabic;
mod english;
mod multilingual;
mod traits;

pub use arabic::{ArabicAnalyzer, ArabicNormalizer, ArabicStemmer, ArabicStopWords};
pub use english::EnglishAnalyzer;
pub use multilingual::MultilingualAnalyzer;
pub use traits::{Analyzer, AnalyzerConfig, Language};
