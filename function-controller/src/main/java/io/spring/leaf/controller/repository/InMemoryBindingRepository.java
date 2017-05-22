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

package io.spring.leaf.controller.repository;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Repository;

import io.spring.leaf.controller.Binding;

/**
 * @author Mark Fisher
 */
@Repository
public class InMemoryBindingRepository implements BindingRepository {

	private final Map<String, Binding> map = new HashMap<>();

	@Override
	public Set<String> names() {
		return this.map.keySet();
	}

	@Override
	public Binding get(String name) {
		return this.map.get(name);
	}

	@Override
	public void save(String name, Binding binding) {
		this.map.put(name, binding);
	}

	@Override
	public Set<Binding> findByInput(String topic) {
		Set<Binding> results = new HashSet<>();
		for (Binding binding : this.map.values()) {
			if (topic.equals(binding.getInput())) {
				results.add(binding);
			}
		}
		return results;
	}
}
