package org.mysamples.common;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Common {

// log4j 2
//    public static void log4jClientConfig(Level level) {
//        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
//        Configuration config = ctx.getConfiguration();
//        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
//        loggerConfig.setLevel(level);
//        ctx.updateLoggers();
//    }

//log4j1
    public static void log4jClientConfig(Level level) {
        org.slf4j.Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
        Logger.getRootLogger().getLoggerRepository().resetConfiguration();
        ConsoleAppender console = new ConsoleAppender();
        console.setLayout(new PatternLayout("%d [%p|%c|%C{1}] %m%n"));
        console.activateOptions();
        Logger.getRootLogger().setLevel(level);
        Logger.getRootLogger().addAppender(console);
    }

    public static Map<String,Object> newSaslConfigExecInPod() throws IOException {
        Map clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", "kafka-0.kafka-headless.default.svc.cluster.local:9192");
        Properties props = new Properties();
        props.load(new FileInputStream("/config-data/config.properties"));
        clientProps.putAll(props);
        return clientProps;
    }

    public static Map<String,Object> newSaslConfig(String broker, String apiKey) {
        Map<String,Object> clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", broker);
        Common.jaasClientConfig(clientProps, "token", apiKey);

        clientProps.put("sasl.mechanism", "PLAIN");
        clientProps.put("security.protocol", "SASL_SSL");
        clientProps.put("ssl.protocol","TLSv1.2");
        clientProps.put("ssl.enabled.protocols","TLSv1.2");
        clientProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,"HTTPS");

        return clientProps;
    }

    public static void jaasClientConfig(Map props, String username, String apikey) {
        props.put(SaslConfigs.SASL_JAAS_CONFIG, 
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + 
        username+"\" password=\"" +
        apikey + "\";"); 
    }


//    OAUTHBEARER settings
//        clientProps.put("security.protocol","SASL_PLAINTEXT");
//        clientProps.put("sasl.mechanism", "OAUTHBEARER");
//        clientProps.put("sasl.jaas.config","org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
//                + " unsecuredLoginStringClaim_sub=\"edo\";");
//        clientProps.put("sasl.client.callback.handler.class", MyClientAuthenticateCallbackHandler.class);
//        clientProps.put("sasl.login.refresh.buffer.seconds","5");
//        clientProps.put("sasl.login.refresh.min.period.seconds","5");


}
