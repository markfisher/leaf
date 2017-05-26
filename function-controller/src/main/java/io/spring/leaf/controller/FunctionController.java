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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.messaging.MessageChannel;
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

	@Autowired
	private FunctionGateway gateway;

	@Autowired
	private FunctionRegistryController registry;

	@Autowired
	private Deployer deployer;

	@Autowired
	private BinderFactory binderFactory;

	@GetMapping("/runners")
	public String listRunners() {
		return this.deployer.getRunnerNames();
	}

	@GetMapping("/runners/{name}")
	public String runnerStatus(@PathVariable String name) {
		return this.deployer.getRunnerStatus(name);
	}

	@PostMapping("/runners/{name}")
	public void createRunner(@PathVariable String name, @RequestBody String location) {
		this.deployer.deployRunner(name, location);
	}

	@PostMapping(value="/pools/runner/{name}/{count}") // todo: accept JSON body
	public String scaleRunnerPool(@PathVariable String runner, @PathVariable int count) {
		return this.deployer.scaleRunnerPool(runner, count);
	}

	@PostMapping(value="/pools/binding/{name}/{count}") // todo: accept JSON body
	public String scaleBindingPool(@PathVariable String bindingName, @PathVariable int count) {
		return this.deployer.scaleBindingPool(bindingName, count);
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
		return this.gateway.handleReply(Long.parseLong(id), reply);
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
		this.deployer.monitor(topic, binder);
	}
}
