use std::collections::HashMap;

use log::info;
use tantivy::tokenizer::{TextAnalyzer, SimpleTokenizer, LowerCaser, RemoveLongFilter, StopWordFilter, Language, Stemmer};

pub(crate) fn default_tokenizer() -> TextAnalyzer {
    // 创建一个新的 TextAnalyzer
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .filter(RemoveLongFilter::limit(40))
        // 使用英语停用词
        .filter(StopWordFilter::remove(vec![
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", 
            "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", 
            "they", "this", "to", "was", "will", "with"
        ].into_iter().map(String::from).collect::<Vec<String>>()))
        // 使用英语词干提取器
        .filter(Stemmer::new(Language::English))
        .build()
}

fn jieba_tokenizer() -> TextAnalyzer {
    tantivy_jieba::JiebaTokenizer {}.into()
}

pub(crate) fn create_tokenizer(params: &HashMap<String, String>) -> Option<TextAnalyzer> {
    match params.get("tokenizer") {
        Some(tokenizer_name) => match tokenizer_name.as_str() {
            "default" => {
                return Some(default_tokenizer());
            }
            "jieba" => return Some(jieba_tokenizer()),
            _ => {
                return None;
            }
        },
        None => {
            info!("no tokenizer is specific, use default tokenizer");
            return Some(default_tokenizer());
        }
    }
}
