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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.core.AppDefinition;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.FieldCallback;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.spring.leaf.controller.repository.BindingRepository;

/**
 * @author Mark Fisher
 */
@RestController
public class FunctionController {

	@Autowired
	private BindingRepository repository;

	private final Map<String, Resource> runnerResources = new HashMap<>();

	private final Map<String, List<String>> runnerDeployments = new HashMap<>();

	@Autowired
	private FunctionGateway gateway;

	@Autowired
	private FunctionRegistryController registry;

	@Autowired
	private AppDeployer deployer;

	@Autowired
	private ResourceLoader resourceLoader;

	@Autowired
	private BinderFactory binderFactory;

	@GetMapping("/runners")
	public String listRunners() {
		return StringUtils.collectionToCommaDelimitedString(this.runnerResources.keySet()) + "\n";
	}

	@GetMapping("/runners/{name}")
	public String runnerStatus(@PathVariable String name) {
		List<String> deploymentIds = this.runnerDeployments.get(name);
		StringBuilder builder = new StringBuilder();
		for (String deploymentId : deploymentIds) {
			builder.append(deploymentId.replaceFirst("null\\.", ""));
			builder.append("\n");
		}
		return builder.toString();
	}

	@PostMapping("/runners/{name}")
	public void createRunner(@PathVariable String name, @RequestBody String location) {
		Resource resource = this.resourceLoader.getResource(location);
		this.runnerResources.put(name, resource);
		this.deployRunner(name);
	}

	@PostMapping(value="/pools/runner/{name}/{count}") // todo: accept JSON body
	public String incrementRunnerPool(@PathVariable String name, @PathVariable int count) {
		for (int i = 0; i < count; i++) {
			this.deployRunner(name);
		}
		return String.format("incremented pool for runner %s by %d\n", name, count);
	}

	@PostMapping(value="/pools/binding/{name}/{count}") // todo: accept JSON body
	public String incrementFunctionPool(@PathVariable String name, @PathVariable int count) {
		Binding binding = this.repository.get(name);
		Assert.notNull(binding, "no such binding: " + name);
		for (int i = 0; i < count; i++) {
			this.deployRunner(binding.getRunner());
		}
		return this.gateway.scale(name, count);
	}

	@GetMapping("/functions")
	public String listFunctions() {
		return StringUtils.arrayToCommaDelimitedString(this.registry.list()) + "\n";
	}

	@PostMapping("/functions/{name}")
	public void registerFunction(@PathVariable String name, @RequestBody String code) {
		this.registry.compile(name, code);
	}

	@GetMapping("/bindings")
	public String listBindings() {
		return StringUtils.collectionToCommaDelimitedString(this.repository.names()) + "\n";
	}

	@GetMapping("/bindings/{name}")
	public String getBinding(@PathVariable String name) {
		return this.repository.get(name).toString();
	}

	@PostMapping("/bindings/{name}") // TODO: pass JSON body instead of params
	public void createBinding(@PathVariable String name, @RequestParam String function, @RequestParam String runner,
			@RequestParam(required = false) String input, @RequestParam(required = false) String output, @RequestBody String code) {
		Binding binding = new Binding(name, function, runner);
		if (input != null) {
			binding.setInput(input);
		}
		else { // default to the binding name itself
			binding.setInput(name);
		}
		if (!StringUtils.isEmpty(output)) {
			binding.setOutput(output);
		}
		this.repository.save(name, binding);
		this.createTopicForConsumer(binding.getInput(), "default");
	}

	@GetMapping("/topics")
	public Set<String> listTopics() {
		SortedSet<String> topics = new TreeSet<>();
		for (String name : this.repository.names()) {
			Binding binding = this.repository.get(name);
			if (binding.getInput() != null) {
				topics.add(binding.getInput());
			}
			if (binding.getOutput() != null) {
				topics.add(binding.getOutput());
			}
		}
		return topics;
	}
	@PostMapping("/events/{topic}")
	public void publishEvent(@PathVariable String topic, @RequestBody String event) {
		this.gateway.sendEvent(topic, event);
	}

	@PostMapping("/requests/{topic}")
	public String publishRequest(@PathVariable String topic, @RequestBody String request) {
		return this.gateway.sendRequest(topic, request);
	}

	@PostMapping("/replies/{id}")
	public String handleReply(@PathVariable String id, @RequestBody String reply) {
		return this.gateway.reply(Long.parseLong(id), reply);
	}

	private void deployRunner(String runner) {
		Resource resource = this.runnerResources.get(runner);
		Map<String, String> properties = new HashMap<>();
		properties.put("spring.cloud.deployer.group", "runner");
		properties.put("spring.cloud.stream.bindings.input.destination", "runner-" + runner);
		properties.put("spring.cloud.stream.bindings.input.group", "default");
		this.runnerDeployments.putIfAbsent(runner, new ArrayList<String>());
		int index = runnerDeployments.get(runner).size();
		AppDefinition definition = new AppDefinition(runner + "-" + index, properties);
		AppDeploymentRequest appDeploymentRequest = new AppDeploymentRequest(definition, resource);
		String deploymentId = this.deployer.deploy(appDeploymentRequest);
		this.runnerDeployments.get(runner).add(deploymentId);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void createTopicForConsumer(String topic, String group) {
		Binder binder = this.binderFactory.getBinder("rabbit", MessageChannel.class);
		final AtomicReference<Field> provisionerField = new AtomicReference<>();
		ReflectionUtils.doWithFields(binder.getClass(), new FieldCallback() {

			@Override
			public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
				if (ProvisioningProvider.class.isAssignableFrom(field.getType())) {
					field.setAccessible(true);
					provisionerField.set(field);
				}
			}
		});
		ProvisioningProvider provisioner = (ProvisioningProvider) ReflectionUtils.getField(provisionerField.get(), binder);
		provisioner.provisionConsumerDestination(topic, group,
				new ExtendedConsumerProperties<RabbitConsumerProperties>(new RabbitConsumerProperties()));
	}
}
