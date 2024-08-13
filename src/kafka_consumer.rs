use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig, 
    Message, message::{Headers, BorrowedHeaders}
};
    
use rdkafka::config::RDKafkaLogLevel;

use rand::Rng;
use colored::Colorize;
use std::str;

use std::{thread, time::Duration};
use error_stack::{Report, ResultExt};

use crate::types::{AppError, Msg};
use log::{info, warn};

const EMPTY: &str = "<empty>";


pub fn start_consuming(bootstrap_server: &str, 
                       topic_name: &str,
                       is_ssl: bool,
                       ca_cert_location: &str,
                       service_key_location: &str,
                       key_cert_location: &str
                    ) {
                        
    info!("bootstrap: {}", bootstrap_server);
    info!("topic name: {}", topic_name);
    info!("ssl: {}", is_ssl);


    let mut rng = rand::thread_rng();
    let group_id: String = format!("my_consumer_group_{}", rng.gen::<u64>());
    info!("group_id: {}", group_id);


    let mut config: ClientConfig = ClientConfig::new();
        

    if is_ssl {
        info!("ssl.ca.location: {}", ca_cert_location);
        info!("ssl.certificate.location: {}", service_key_location);
        info!("ssl.key.location: {}", key_cert_location);

        config.set("security.protocol", "SSL")
            .set("ssl.ca.location", ca_cert_location)
            .set("ssl.certificate.location", service_key_location)
            .set("ssl.key.location", key_cert_location);
    }

    let consumer: BaseConsumer = config
        .set("bootstrap.servers", bootstrap_server)        
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        // .set("debug", "consumer,cgrp,topic,fetch,security,metadata") // does not work
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("invalid consumer config");



    consumer
        .subscribe(&[topic_name])
        .expect("topic subscribe failed");


    thread::spawn(move || loop {
        info!("start consumer thread {:?}", thread::current().id());

        loop {
            let msg_result = consumer.poll(Duration::from_secs(15));
            if msg_result.is_none() {
                warn!("there is no any messages, next check in 15 sec");
                continue;
            }

            let msge = msg_result.unwrap().change_context(AppError::KafkaError { msg: "can not read message from kafka" });

            let res = msge.map(|msg| {
                let key = msg.key().map(bytes_to_string).unwrap_or(Ok(EMPTY.to_string()));
                let val = msg.payload().map(bytes_to_string).unwrap_or(Ok(EMPTY.to_string()));
                let header = msg.headers().map(read_headers).unwrap_or(Ok(EMPTY.to_string()));
                Msg {
                    key: key,
                    header: Some(header),
                    value: val
                }
            });


            if let Ok(ref msg) = res {
                let empty = String::from("");
                let empty_result = Ok(empty.clone());

                let key = msg.key.as_ref().unwrap_or(&empty);
                let header = msg.header.as_ref().unwrap_or(&empty_result).as_ref().unwrap_or(&empty);
                let val = msg.value.as_ref().unwrap_or(&empty);

                println!(
                    "{} {}\n{} {}\n{} {}\n", "key:".bold().bright_green(), key.blink().blue(),
                    "headers:".bold().bright_green(), header.yellow(),
                    "value:".bold().bright_green(), val.green()
                )
            }

        }
    });    
    
    thread::sleep(Duration::MAX);
}

fn bytes_to_string(arr: &[u8]) -> Result<String, Report<AppError>> {
    return str::from_utf8(arr).map(String::from)
        .change_context(AppError::EncodingError { msg: "can not encode value" });
}

fn read_headers(hm: &BorrowedHeaders) -> Result<String, Report<AppError>> {
    let cnt = hm.count();
    let mut idx = 0;

    let mut res = String::new();

    while idx < cnt {
        let header = hm.get(idx);
        let key = header.key;
        if let Some(v) = header.value {
            let val = bytes_to_string(v)?;
            let vstr = format!("{}={}\n", key, val);
            res.push_str(&vstr)
        }

        idx += 1;
    }

    Ok(res)
}