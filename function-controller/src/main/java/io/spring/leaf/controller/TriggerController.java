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
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Mark Fisher
 */
@RestController
public class TriggerController {

	@Autowired
	private BinderAwareChannelResolver resolver;

	private final TaskScheduler scheduler = new ConcurrentTaskScheduler();

	private final Map<String, Trigger> triggers = new HashMap<>();

	@GetMapping("/triggers")
	public String listTriggers() {
		return "[" + StringUtils.collectionToCommaDelimitedString(this.triggers.values()) + "]\n";
	}

	@PostMapping("/triggers/{topic}")
	public String createTrigger(@PathVariable String topic, @RequestBody String cron) {
		String id = UUID.randomUUID().toString();
		Trigger trigger = new Trigger(id, topic, cron);
		this.triggers.put(id, trigger);
		this.scheduler.schedule(new TriggerTask(trigger), new CronTrigger(cron));
		return id;
	}

	public class Trigger {

		private final String id;

		private final String topic;

		private final String cron;

		public Trigger(String id, String topic, String cron) {
			this.id = id;
			this.topic = topic;
			this.cron = cron;
		}

		public String getId() {
			return id;
		}

		public String getTopic() {
			return topic;
		}

		public String getCron() {
			return cron;
		}

		@Override
		public String toString() {
			return String.format("{\"id\":\"%s\",\"topic\":\"%s\",\"cron\":\"%s\"}", this.id, this.topic, this.cron);
		}
	}

	private class TriggerTask implements Runnable {

		private final String topic;

		private final String triggerId;

		TriggerTask(Trigger trigger) {
			this.topic = trigger.getTopic();
			this.triggerId = trigger.getId();
		}

		@Override
		public void run() {
			MessageChannel channel = resolver.resolveDestination(this.topic);
			channel.send(MessageBuilder.withPayload(this.triggerId).build());
		}
	}
}
