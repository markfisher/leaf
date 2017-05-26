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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.core.AppDefinition;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import io.spring.leaf.controller.repository.BindingRepository;

/**
 * @author Mark Fisher
 */
@Component
public class Deployer {

	private static final String RUNNER_CHANNEL_PREFIX = "runner-";

	@Autowired
	private BindingRepository bindingRepository;

	private final Map<String, Resource> runnerResources = new HashMap<>();

	@Autowired
	private BinderAwareChannelResolver channelResolver;

	private final Map<String, List<String>> runnerDeployments = new HashMap<>();

	private Set<String> seen = new HashSet<>();

	private Map<String, MessageChannel> bootstrapChannels = new HashMap<>();

	@Autowired
	private AppDeployer appDeployer;

	@Autowired
	private ResourceLoader resourceLoader;

	private final Map<String, SubscribableChannel> monitorChannels = new HashMap<>();

	public String getRunnerNames() {
		return StringUtils.collectionToCommaDelimitedString(this.runnerResources.keySet()) + "\n";
	}

	public String getRunnerStatus(String runner) {
		List<String> deploymentIds = this.runnerDeployments.get(runner);
		StringBuilder builder = new StringBuilder();
		for (String deploymentId : deploymentIds) {
			builder.append(deploymentId.replaceFirst("null\\.", ""));
			builder.append("\n");
		}
		return builder.toString();
	}

	public String scaleRunnerPool(String runner, int count) {
		// TODO: support negative count, and undeploy
		for (int i = 0; i < count; i++) {
			this.deployRunner(runner);
		}
		return String.format("incremented pool for runner %s by %d\n", runner, count);
	}

	public String scaleBindingPool(String bindingName, int count) {
		Binding binding = this.bindingRepository.get(bindingName);
		Assert.notNull(binding, "no such binding: " + bindingName);
		for (int i = 0; i < count; i++) {
			this.deployRunner(binding.getRunner());
			this.deployBinding(binding);
		}
		return String.format("incremented pool for binding %s by %d\n", bindingName, count);
	}

	public void deployRunner(String runner, String location) {
		Resource resource = this.resourceLoader.getResource(location);
		this.runnerResources.put(runner, resource);
		this.deployRunner(runner);
	}

	public void deployRunner(String runner) {
		Resource resource = this.runnerResources.get(runner);
		Map<String, String> properties = new HashMap<>();
		properties.put("spring.cloud.deployer.group", "runner");
		properties.put("spring.cloud.stream.bindings.input.destination", "runner-" + runner);
		properties.put("spring.cloud.stream.bindings.input.group", "default");
		this.runnerDeployments.putIfAbsent(runner, new ArrayList<String>());
		int index = runnerDeployments.get(runner).size();
		AppDefinition definition = new AppDefinition(runner + "-" + index, properties);
		AppDeploymentRequest appDeploymentRequest = new AppDeploymentRequest(definition, resource);
		String deploymentId = this.appDeployer.deploy(appDeploymentRequest);
		this.runnerDeployments.get(runner).add(deploymentId);
	}

	private void deployBinding(Binding binding) {
		MessageChannel channel = this.bootstrapChannels.get(binding.getName());
		if (channel == null) {
			channel = this.channelResolver.resolveDestination(RUNNER_CHANNEL_PREFIX + binding.getRunner());
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void monitor(final String topic, Binder binder) {
		DirectChannel channel = new DirectChannel();
		this.monitorChannels.put(topic, channel);
		channel.subscribe(new MessageHandler() {
			
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				deployIfNecessary(topic);
			}
		});
		binder.bindConsumer(topic, "leaf-monitor", channel,
				new ExtendedConsumerProperties<RabbitConsumerProperties>(new RabbitConsumerProperties()));
	}

	private void deployIfNecessary(String topic) {
		Set<Binding> bindings = this.bindingRepository.findByInput(topic);
		for (Binding binding : bindings) {
			if (!this.seen.contains(binding.getName())) {
				this.deployRunner(binding.getRunner());
				this.deployBinding(binding);
			}
		}
	}
}
