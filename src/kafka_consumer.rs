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

use crate::types::{AppError, Msg};

const EMPTY: &str = "<empty>";


pub fn start_consuming(bootstrap_server: &str, 
                       topic_name: &str,
                       is_ssl: &bool,
                       ca_cert_location: &str,
                       service_key_location: &str,
                       key_cert_location: &str
                    ) {
                        
    println!("bootstrap: {}", bootstrap_server);
    println!("topic name: {}", topic_name);
    println!("ssl: {}", is_ssl);    


    let mut rng = rand::thread_rng();
    let group_id: String = format!("my_consumer_group_{}", rng.gen::<u64>());
    println!("group_id: {}", group_id);


    let mut config: ClientConfig = ClientConfig::new();
        

    if *is_ssl {
        println!("ssl.ca.location: {}", ca_cert_location);
        println!("ssl.certificate.location: {}", service_key_location);
        println!("ssl.key.location: {}", key_cert_location);

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
        println!("start consumer thread {:?}", thread::current().id());

        // for msg_result in consumer.iter() {
        loop {
            let msg_result = consumer.poll(Duration::from_secs(15));
            if msg_result.is_none() {
                println!("there is no any messages, next check in 15 sec");
                continue;
            }

            let msge = msg_result.unwrap()
                    .map_err(|e| AppError::KafkaError(format!("kafka value error{}", e)));

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

            
            let key = res.clone().and_then(|msg| msg.key)
                .map_err(|e| format!("key error {}", e))
                .unwrap_or("".to_string());

            let header = res.clone().and_then(|msg| msg.header.unwrap_or(Ok("".to_string())))
                .map_err(|e| format!("header error {}", e))
                .unwrap_or("".to_string());

            let val = res.and_then(|msg| msg.value)
                .map_err(|e| format!("value error {}", e))
                .unwrap_or("".to_string());

            println!(
                "{} {}\n{} {}\n{} {}\n", "key:".bold().bright_green(), key.blink().blue(),
                            "headers:".bold().bright_green(), header.yellow(), 
                            "value:".bold().bright_green(), val.green()
            )
        }
        // println!("end consumer thread");
    });    
    
    thread::sleep(Duration::MAX);
}

fn bytes_to_string(arr: &[u8]) -> Result<String, AppError> {
    str::from_utf8(arr).map(String::from).map_err(|e| AppError::EncodingError(format!("enc error: {}", e)))
}

fn read_headers(hm: &BorrowedHeaders) -> Result<String, AppError> {
    let cnt = hm.count();
    let mut idx = 0;

    let mut res = String::new();

    while idx < cnt {
        if let Some(header_str) = hm.get(idx)
            .map(|(k, v)| format!("{}={}\n", k, bytes_to_string(v).unwrap_or(EMPTY.to_string()))) {
                res.push_str(&header_str);
        }                            

        idx += 1;
    }

    Ok(res)
}