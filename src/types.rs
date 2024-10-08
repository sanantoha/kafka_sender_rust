use clap::Parser;
use colored::ColoredString;
use serde::{Serialize, Deserialize};
use error_stack::Report;
use thiserror::Error;


#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long)]
    pub bootstrap: Option<String>,

    #[clap(short, long)]
    pub topic_name: Option<String>,

    #[clap(short, long)]
    pub is_ssl: Option<bool>,

    #[clap(long)]
    pub ca_cert_location: Option<String>,

    #[clap(long)]
    pub service_key_location: Option<String>,

    #[clap(long)]
    pub key_cert_location: Option<String>,

    #[clap(short, long)]
    pub consumer: bool,

    #[clap(short, long)]
    pub producer: bool,

    #[clap(short, long)]
    pub file_path_messages: Option<String>,

    #[clap(long)]
    pub csv_msg_file: bool
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub bootstrap: String,
    pub topic_name: String,
    pub is_ssl: bool,
    pub ca_cert_location: String,
    pub service_key_location: String,
    pub key_cert_location: String
}


impl Default for Config {
    fn default() -> Self {
        Config {
            bootstrap: "localhost:9092".to_string(),
            topic_name: "test.topic".to_string(),
            is_ssl: false,
            ca_cert_location: "".to_string(),
            service_key_location: "".to_string(),
            key_cert_location: "".to_string()
        }
    }
}


// #[derive(Debug, Clone, PartialEq, Eq)]
// pub enum AppError {
//     EncodingError(String),
//     KafkaError(String),
//     ConfigError(String),
//     CsvReadError(String),
//     MsgFileReadError(String),
//     MsgReadError(String)
// }

#[derive(Error, Debug)]
pub enum AppError {
    #[error("encoding error: {msg}")]
    EncodingError { msg: &'static str },
    #[error("kafka error: {msg}")]
    KafkaError {msg: &'static str },
    #[error("config error: {msg}")]
    ConfigError {msg: &'static str },
    #[error("csv read error")]
    CsvReadError,
    #[error("error read message file: {path}")]
    MsgFileReadError {path: ColoredString },
    #[error("error read message")]
    MsgReadError,
}

#[derive(Debug)]
pub struct Msg {
    pub key: Result<String, Report<AppError>>,
    pub header: Option<Result<String, Report<AppError>>>,
    pub value: Result<String, Report<AppError>>
}


// impl fmt::Display for AppError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         let str = match &*self {
//             AppError::EncodingError(s) => s,
//             AppError::KafkaError(s) => s,
//             AppError::ConfigError(s) => s,
//             AppError::CsvReadError(s) => s,
//             AppError::MsgFileReadError(s) => s,
//             AppError::MsgReadError(s) => s
//         };
//
//
//         write!(f, "{}", str)
//     }
// }

#[derive(Debug, Deserialize)]
pub struct CsvRecord {
    pub key: String,
    pub value: String
}