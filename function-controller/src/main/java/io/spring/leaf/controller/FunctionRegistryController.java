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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.function.compiler.CompiledFunctionFactory;
import org.springframework.cloud.function.compiler.FunctionCompiler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.spring.leaf.controller.repository.FunctionRepository;
import reactor.core.publisher.Flux;

/**
 * @author Mark Fisher
 */
@RestController
public class FunctionRegistryController {

	@Autowired
	private FunctionRepository repository;

	private final FunctionCompiler<Flux<?>, Flux<?>> compiler = new FunctionCompiler<>();

	@GetMapping("/registry")
	public String[] list() {
		return this.repository.names();
	}

	@GetMapping(value="/registry/{name}", produces="application/octet-stream")
	public byte[] lookup(@PathVariable String name) {
		return this.repository.find(name);
	}

	@PostMapping(value="/registry/{name}", consumes="text/plain")
	public void compile(@PathVariable String name, @RequestBody String lambda) {
		CompiledFunctionFactory<?> factory = this.compiler.compile(name, lambda);//, types);
		this.repository.save(name, factory.getGeneratedClassBytes());
	}

	@PostMapping(value="/registry/{name}", consumes="application/octet-stream")
	public void register(@PathVariable String name, @RequestBody byte[] bytecode) {
		this.repository.save(name, bytecode);
	}
}
