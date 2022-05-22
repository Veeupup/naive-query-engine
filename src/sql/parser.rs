/*
 * @Author: Veeupup
 * @Date: 2022-05-13 19:35:41
 * @Email: code@tanweime.com
*/

use sqlparser::{
    ast::Statement,
    dialect::GenericDialect,
    parser::{Parser, ParserError},
    tokenizer::Tokenizer,
};

/// SQL Parser
pub struct SQLParser;

impl SQLParser {
    /// Parse the specified tokens and return statement
    pub fn parse(sql: &str) -> Result<Statement, ParserError> {
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens, &dialect);
        parser.parse_statement()
    }
}
