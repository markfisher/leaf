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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.client.RestTemplate;

import io.spring.leaf.controller.repository.BindingRepository;

/**
 * @author Mark Fisher
 */
@EnableBinding
public class FunctionGateway {

	private static final String RUNNER_CHANNEL_PREFIX = "runner-";

	private static final String FUNCTION_CHANNEL_PREFIX = "function-";

	@Autowired
	private BinderAwareChannelResolver resolver;

	@Autowired
	private BindingRepository bindingRepository;

	private final RestTemplate restTemplate = new RestTemplate();

	private Set<String> seen = new HashSet<>();

	private Map<String, MessageChannel> bootstrapChannels = new HashMap<>();

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

	private void sendMessage(String topic, Message<?> message) {
		Set<Binding> bindings = this.bindingRepository.findByInput(topic);
		for (Binding binding : bindings) {
			if (!this.seen.contains(binding.getName())) {
				this.deployRunner(binding.getRunner());
				this.deploy(binding);
			}
		}
		MessageChannel channel = resolver.resolveDestination(FUNCTION_CHANNEL_PREFIX + topic);
		channel.send(message);
	}

	public String reply(long id, String reply) {
		try {
			this.replies.get(id).put(reply);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return "ack\n";
	}

	public String scale(String bindingName, int count) {
		for (int i = 0; i < count; i++) {
			this.deploy(this.bindingRepository.get(bindingName));
		}
		return String.format("incremented pool for binding %s by %d\n", bindingName, count);
	}

	private void deployRunner(String name) {
		this.restTemplate.postForObject(
				"http://localhost:5323/pools/runner/{name}/1", "", String.class, name);		
	}

	private void deploy(Binding binding) {
		MessageChannel channel = this.bootstrapChannels.get(binding.getName());
		if (channel == null) {
			channel = this.resolver.resolveDestination(RUNNER_CHANNEL_PREFIX + binding.getRunner());
			this.bootstrapChannels.put(binding.getName(), channel);
		}
		// TODO: get from registry
		String resource = "file:///tmp/function-repo/" + binding.getFunction();
		Map<String, String> functionDeploymentRequest = new HashMap<>();
		functionDeploymentRequest.put("function", resource);
		functionDeploymentRequest.put("input", binding.getInput());
		if (binding.getOutput() != null) {
			functionDeploymentRequest.put("output", binding.getOutput());
		}
		channel.send(MessageBuilder.withPayload(functionDeploymentRequest).build());
		this.seen.add(binding.getName());
	}
}
