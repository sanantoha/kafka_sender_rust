use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    // message::ToBytes,
    // producer::{BaseProducer, BaseRecord, Producer, ProducerContext, ThreadedProducer},
    ClientConfig, 
    // ClientContext, 
    Message,
};

use rand::Rng;

use std::{thread, time::Duration};


pub fn test_consumer() {

    let mut rng = rand::thread_rng();
    let group_id: u64 = rng.gen();


    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:29092")
        //for auth
        /*.set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "<update>")
        .set("sasl.password", "<update>")*/
        .set("group.id", format!("my_consumer_group_{}", group_id))
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("invalid consumer config");



    consumer
        .subscribe(&["rust"])
        .expect("topic subscribe failed");


    thread::spawn(move || loop {
        println!("start consumer thread {:?}", thread::current().id());
        for msg_result in consumer.iter() {
            let msg = msg_result.unwrap();
            let key: &str = msg.key_view().unwrap().unwrap();
            let value: &[u8] = msg.payload().unwrap();
            let val_str = std::str::from_utf8(value).unwrap_or("");
            // let user: User = serde_json::from_slice(value).expect("failed to deser JSON to User");
            println!(
                "received key {} with value {:?} in offset {:?} from partition {}",
                key,
                val_str,
                msg.offset(),
                msg.partition()
            )
        }
        println!("end consumer thread");
    });    
    
    thread::sleep(Duration::MAX);
}