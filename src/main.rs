use clap::Parser;
use std::path::Path;

mod kafka_consumer;
mod kafka_producer;

mod types;
use types::{Args, Config, AppError, Msg, CsvRecord};
extern crate confy;




// default path /Users/san/Library/Application\ Support/rs.kafka_sender_rust/default-config.yml
fn main() -> Result<(), AppError> {
    let args = Args::parse();

    if args.consumer && args.producer {
        return Err(AppError::ConfigError("error you can not use consumer and producer at the same time".to_string()));
    }
    

    let cfg: Config = confy::load("kafka_sender_rust", None)
            .map_err(|e| AppError::ConfigError(e.to_string()))?;

    let bootstrap = &args.bootstrap.unwrap_or(cfg.bootstrap);
    let topic_name =  &args.topic_name.unwrap_or(cfg.topic_name);
    let is_ssl = &args.is_ssl.unwrap_or(cfg.is_ssl);
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
        let file_path_messages_str = args.file_path_messages.clone().unwrap_or("".to_string());

        let msgs_res = 
            args.file_path_messages.map(|x| read_messages_file(&x)).unwrap_or(Ok(vec![]));

        let msgs = match msgs_res {
            Ok(vec) => vec,
            Err(e) => {
                eprintln!("error reading csv file {} {}", file_path_messages_str, e);
                vec![]
            }
        };

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
        println!("please use `--consumer` or `--producer` argument");
    }
    
        
    Ok(())
}

fn read_messages_file(file_path_messages: &str) -> Result<Vec<Msg>, AppError> {
    let path = Path::new(file_path_messages);

    let mut rdr = csv::ReaderBuilder::new()
        .escape(Some(b'\\'))
        .from_path(path)
        .map_err(csv_msg_err)?;        
        

    let mut vec: Vec<Msg> = Vec::new();

    for result in rdr.deserialize() {
        let record: CsvRecord = result.map_err(csv_msg_err)?;

        vec.push(Msg {
            key: Ok(record.key),
            header: None,
            value: Ok(record.value)
        });
    }

    Ok(vec)
}

fn csv_msg_err(e: csv::Error) -> AppError {
    AppError::CsvReadError(format!("{}", e))
}
