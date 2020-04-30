package com.felixzh;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

public class JMeterProducer extends AbstractJavaSamplerClient {
    private static Logger log = LoggerFactory.getLogger(JMeterProducer.class);
    private KafkaProducer<String, String> kafkaProducer;
    private String topic;
    private boolean flag = true;
    private boolean asyncSendFlag = true;

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        arguments.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        arguments.addArgument("topic", "test");
        arguments.addArgument("async.send.flag", "true");
        arguments.addArgument(ProducerConfig.ACKS_CONFIG, "1");
        arguments.addArgument(ProducerConfig.CLIENT_ID_CONFIG, "com.felixzh.jmeter.producer");
        arguments.addArgument(ProducerConfig.RETRIES_CONFIG, "0");
        arguments.addArgument(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        arguments.addArgument(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        arguments.addArgument(ProducerConfig.LINGER_MS_CONFIG, "0");
        arguments.addArgument(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        arguments.addArgument(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        arguments.addArgument(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        log.info("Arguments : " + arguments.toString());
        return arguments;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getParameter(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.ACKS_CONFIG, context.getParameter(ProducerConfig.ACKS_CONFIG));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, context.getParameter(ProducerConfig.CLIENT_ID_CONFIG));
        props.put(ProducerConfig.RETRIES_CONFIG, context.getParameter(ProducerConfig.RETRIES_CONFIG));
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, context.getParameter(ProducerConfig.RETRY_BACKOFF_MS_CONFIG));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, context.getParameter(ProducerConfig.BATCH_SIZE_CONFIG));
        props.put(ProducerConfig.LINGER_MS_CONFIG, context.getParameter(ProducerConfig.LINGER_MS_CONFIG));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, context.getParameter(ProducerConfig.BUFFER_MEMORY_CONFIG));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        kafkaProducer = new KafkaProducer<>(props);
        topic = context.getParameter("topic");
        asyncSendFlag = Boolean.valueOf(context.getParameter("async.send.flag"));
    }

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult sampleResult = new SampleResult();
        sampleResult.sampleStart();
        log.info("开始时间： {}", new Date());

        boolean startSendFlag = true;
        while (flag) {
            Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<>(topic, "felixzh report current time : " + System.currentTimeMillis()));
            if (!asyncSendFlag) {
                try {
                    future.get();
                } catch (InterruptedException ex) {
                    log.info("线程退出，时间：{}", new Date());
                    break;
                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);
                }
            }

            if (startSendFlag) {
                log.info("正在发送数据……");
                startSendFlag = false;
            }
        }

        if (kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close(Duration.ofMillis(3000));
        }

        sampleResult.setResponseData("结束时间：" + new Date(), StandardCharsets.UTF_8.name());
        sampleResult.setSuccessful(true);
        sampleResult.setResponseCodeOK();
        sampleResult.sampleEnd();
        return sampleResult;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        flag = false;
        super.teardownTest(context);
    }
}
