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
            "i","me","my","myself","we","our","ours","ourselves","you","you're","you've","you'll","you'd","your","yours","yourself",
            "yourselves","he","him","his","himself","she","she's","her","hers","herself","it","it's","its","itself","they","them",
            "their","theirs","themselves","what","which","who","whom","this","that","that'll","these","those","am","is","are","was",
            "were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if",
            "or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during",
            "before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then",
            "once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such",
            "no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","don't","should",
            "should've","now","d","ll","m","o","re","ve","y","ain","aren","aren't","couldn","couldn't","didn","didn't","doesn",
            "doesn't","hadn","hadn't","hasn","hasn't","haven","haven't","isn","isn't","ma","mightn","mightn't","mustn","mustn't",
            "needn","needn't","shan","shan't","shouldn","shouldn't","wasn","wasn't","weren","weren't","won","won't","wouldn","wouldn't"
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
