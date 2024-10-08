use rdkafka::{
    producer::{BaseProducer, BaseRecord},
    ClientConfig,
};
use crate::types::Msg;
use rdkafka::config::RDKafkaLogLevel;
use std::thread;
use std::time::Duration;
use log::{error, info};

const EMPTY_MSG: &str = "";

pub fn start_producing(bootstrap_server: &str, 
                       topic_name: &str,
                       is_ssl: bool,
                       ca_cert_location: &str,
                       service_key_location: &str,
                       key_cert_location: &str,
                       msgs: &Vec<Msg>
                    ) {
    
    info!("bootstrap: {}", bootstrap_server);
    info!("topic name: {}", topic_name);
    info!("ssl: {}", is_ssl);


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

    let producer: BaseProducer = config
        .set("bootstrap.servers", bootstrap_server)        
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("invalid producer config");

    let empty_msg_val = EMPTY_MSG.to_string();
    
    for msg in msgs.iter() {
        let mut msg_base_record = BaseRecord::to(topic_name);
        msg_base_record = match &msg.key {
            Ok(k) => msg_base_record.key(k),
            Err(e) => {
                error!("key not found {}", e);
                msg_base_record
            }                
        };
        msg_base_record = match &msg.value {
            Ok(v) => msg_base_record.payload(v),
            Err(e) => {
                error!("value not found {}", e);
                msg_base_record.payload(&empty_msg_val)
            }                
        };

        producer
            .send(msg_base_record)
            .expect("failed to send message");
            
        info!("message sent {:?}", msg);
    }
        
    thread::sleep(Duration::from_secs(3));
}