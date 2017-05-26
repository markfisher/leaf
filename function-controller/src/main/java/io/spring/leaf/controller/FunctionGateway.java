/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.spring.leaf.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Mark Fisher
 */
@EnableBinding
public class FunctionGateway {

	@Autowired
	private BinderAwareChannelResolver resolver;

	private AtomicLong counter = new AtomicLong();

	private Map<Long, ArrayBlockingQueue<String>> replies = new HashMap<>();

	public String sendRequest(String topic, String message) {
		ArrayBlockingQueue<String> replyHolder = new ArrayBlockingQueue<String>(1);
		long id = this.counter.incrementAndGet(); 
		this.replies.put(id, replyHolder);
		Message<?> requestMessage = MessageBuilder.withPayload(message)
				.setHeader("gatewayReplyTo", "http://localhost:5323/replies/" + id)
				.build();
		sendMessage(topic, requestMessage);
		try {
			String reply = replyHolder.poll(10, TimeUnit.SECONDS);
			this.replies.remove(id);
			if (reply == null) {
				throw new IllegalStateException("timed out waiting for reply");
			}
			return reply + "\n";
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return "interrupted exception occurred\n";
		}
	}

	public void sendEvent(String topic, String event) {
		this.sendMessage(topic, MessageBuilder.withPayload(event).build());
	}

	public String handleReply(long id, String reply) {
		try {
			this.replies.get(id).put(reply);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return "ack\n";
	}

	private void sendMessage(String topic, Message<?> message) {
		MessageChannel channel = resolver.resolveDestination(topic);
		channel.send(message);
	}
}
