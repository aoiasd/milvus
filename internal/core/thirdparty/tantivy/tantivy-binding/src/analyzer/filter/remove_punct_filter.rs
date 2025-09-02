use lazy_static::lazy_static;
use regex::Regex;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

lazy_static! {
    static ref RE: Regex = Regex::new(r"[\p{Punct}\s]+").unwrap();
}

pub struct RemovePunctFilter;

pub struct RemovePunctFilterStream<T> {
    tail: T,
}

impl TokenFilter for RemovePunctFilter {
    type Tokenizer<T: Tokenizer> = RemovePunctFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> RemovePunctFilterWrapper<T> {
        RemovePunctFilterWrapper(tokenizer)
    }
}
#[derive(Clone)]
pub struct RemovePunctFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for RemovePunctFilterWrapper<T> {
    type TokenStream<'a> = RemovePunctFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        RemovePunctFilterStream {
            tail: self.0.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for RemovePunctFilterStream<T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if !RE.is_match(&self.tail.token().text) {
                return true;
            }
        }

        false
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_remove_punct_filter() {
        use crate::analyzer::analyzer::create_analyzer;
        let params = r#"{
            "tokenizer": {
                "type": "jieba"
            },
            "filter": ["removepunct"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("中文，标点。\"测试\"。\n");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }
}
