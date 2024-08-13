use clap::Parser;
use std::{path::Path, fs::File, io::BufReader, io::BufRead};
use colored::Colorize;

mod kafka_consumer;
mod kafka_producer;

mod types;
use types::{Args, Config, AppError, Msg, CsvRecord};
extern crate confy;

use log::warn;
use env_logger;
use error_stack::{Report, ResultExt};


// default path /Users/san/Library/Application\ Support/rs.kafka_sender_rust/default-config.yml
fn main() -> Result<(), Report<AppError>> {
    env_logger::init();

    let args = Args::parse();

    if args.consumer && args.producer {
        return Err(Report::new(AppError::ConfigError {msg: "error you can not use consumer and producer at the same time" }));
    }
    

    let cfg: Config = confy::load("kafka_sender_rust", None).change_context(AppError::ConfigError {msg: "can not read config file"})?;

    let bootstrap = &args.bootstrap.unwrap_or(cfg.bootstrap);
    let topic_name =  &args.topic_name.unwrap_or(cfg.topic_name);
    let is_ssl = args.is_ssl.unwrap_or(cfg.is_ssl);
    let ca_cert_location = &args.ca_cert_location.unwrap_or(cfg.ca_cert_location);
    let service_key_location = &args.service_key_location.unwrap_or(cfg.service_key_location);
    let key_cert_location = &args.key_cert_location.unwrap_or(cfg.key_cert_location);

    if args.consumer {
        kafka_consumer::start_consuming(
            bootstrap, 
            topic_name,
            is_ssl,
            ca_cert_location,
            service_key_location,
            key_cert_location       
        );
    } else if args.producer {
        let msgs =
            args.file_path_messages.ok_or(Report::new(AppError::ConfigError { msg: "file is not specified, use -h to get help" }))
                .and_then(|x| {
                    if args.csv_msg_file {
                        read_messages_csv_file(&x)
                    } else {
                        read_messages_file(&x)
                    }                    
                })?;
                // .unwrap_or(Ok(vec![]));

        // let msgs = match msgs_res {
        //     Ok(vec) => vec,
        //     Err(e) => {
        //         error!("error reading csv file {} {}", file_path_messages_str, e);
        //         vec![]
        //     }
        // };

        kafka_producer::start_producing(
            bootstrap, 
            topic_name,
            is_ssl,
            ca_cert_location,
            service_key_location,
            key_cert_location,
            &msgs
        );
    } else {
        warn!("please use `--consumer` or `--producer` argument");
    }
    
        
    Ok(())
}

fn read_messages_csv_file(file_path_messages: &str) -> Result<Vec<Msg>, Report<AppError>> {
    let path = Path::new(file_path_messages);

    let mut rdr = csv::ReaderBuilder::new()
        .escape(Some(b'\\'))
        .from_path(path)
        .change_context(AppError::CsvReadError)?;
        

    let mut vec: Vec<Msg> = Vec::new();

    for result in rdr.deserialize() {
        let record: CsvRecord = result.change_context(AppError::CsvReadError)?;

        vec.push(Msg {
            key: Ok(record.key),
            header: None,
            value: Ok(record.value)
        });
    }

    Ok(vec)
}

fn read_messages_file(file_path: &str) -> Result<Vec<Msg>, Report<AppError>> {
    let path = Path::new(file_path);

    let file = File::open(path).change_context(AppError::MsgFileReadError {path: file_path.to_string().red() })?;

    let msgs = BufReader::new(file).lines().map(|line|{
        Msg {
            key: Ok("".to_string()),
            header: None,
            value: line.change_context(AppError::MsgReadError)
        }
    }).collect();

    Ok(msgs)
}
