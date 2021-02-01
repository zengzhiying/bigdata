#include <iostream>
#include <cstdio>

#include "librdkafka/rdkafkacpp.h"

using std::string;
using std::cout;
using std::cerr;
using std::endl;

/**
 * g++ -Wl,-rpath=/usr/local/librdkafka/lib/ -L /usr/local/lib -lrdkafka++ producer.cpp -o producer
 */

class ProduceDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb (RdKafka::Message &message) {
        if (message.err())
          cerr << "% Message delivery failed: " << message.errstr() << endl;
        // else
        //   cerr << "% Message delivered to topic " << message.topic_name() <<
        //     " [" << message.partition() << "] at offset " <<
        //     message.offset() << endl;
      }
    
};

int main(int argc, char const *argv[])
{
    string brokers = "bigdata1:9092,bigdata2:9092";
    string topicName = "message";

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    string err;
    if(conf->set("bootstrap.servers", brokers, err) != 
        RdKafka::Conf::CONF_OK) {
        cerr << err << endl;
        exit(1);
    }

    ProduceDeliveryReportCb producerCb;
    if(conf->set("dr_cb", &producerCb, err) != RdKafka::Conf::CONF_OK) {
        cerr << err << endl;
        exit(1);
    }

    RdKafka::Producer *producer = RdKafka::Producer::create(conf, err);
    if(!producer) {
        cerr << "Failed to create producer: " << err << endl;
        exit(1);
    }

    delete conf;
    conf = NULL;

    cout << "start produce message:" << endl;
    string fmt("{\"code\": %d, \"message\": \"消息: %d\"}");
    for(int i = 1; i <= 100; i++) {
        int length = snprintf(nullptr, 0, fmt.c_str(), i, i);
        char *buf = new char[length + 1];
        snprintf(buf, length + 1, fmt.c_str(), i, i);
        string msg(buf);
        delete[] buf;

        // produce message to kafka
        RdKafka::ErrorCode errCode = producer->produce(
            topicName,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char *>(msg.c_str()), msg.size(),
            // Key
            NULL, 0,
            0,
            NULL,
            NULL);
        if(errCode != RdKafka::ERR_NO_ERROR) {
            cerr << "Failed to producer: " << topicName << RdKafka::err2str(errCode) << endl;
            if(errCode == RdKafka::ERR__QUEUE_FULL) {
                producer->poll(1000);
                cerr << "Local Buffer Queue is fill!" << endl;
            }
        } else {
            // success
        }

        producer->poll(0);
    }

    // 定期或一定条数执行flush
    producer->flush(10 * 1000 /* 10s */);
    if(producer->outq_len() > 0) {
        cerr << producer->outq_len() << " messages were not delivered." << endl;
    }

    delete producer;
    producer = NULL;

    cout << "message is produced." << endl;

    return 0;
}
