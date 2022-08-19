package com.solace.swim.service.googlePubSub;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.solace.swim.service.IService;
import com.solacesystems.jms.message.SolMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

@Service
@ConditionalOnProperty(prefix = "service.google-pubsub", value = "enabled", havingValue = "true")
public class GooglePubSubService implements IService {

    @Autowired
    Hashtable<String,String> envProducer;
    private Publisher publisher;

    private static final Logger logger = LoggerFactory.getLogger(GooglePubSubService.class);

    @PostConstruct
    private void init() {
        Properties props = new Properties();
        props.putAll(envProducer);

        ProjectTopicName topicName = ProjectTopicName.of(props.getProperty("google.messaging.pubsub.project"), props.getProperty("google.messaging.pubsub.topic"));
        try {
            this.publisher = Publisher.newBuilder(topicName).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void publish(String payload) {
        ByteString data = ByteString.copyFromUtf8(payload);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        this.publisher.publish(pubsubMessage);
    }

    @Override
    public void invoke(Message<?> message) {
        String payload;
        if (message.getPayload() instanceof String) {
            payload = (String)message.getPayload();
        } else if (message.getPayload() instanceof SolMessage) {
            SolMessage obj = (SolMessage) message.getPayload();
            payload = obj.dump();
        } else {
            payload = message.getPayload().toString();
        }
        publish(payload);
    }
}
