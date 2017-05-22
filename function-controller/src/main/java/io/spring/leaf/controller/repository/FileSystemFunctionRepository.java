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

import java.io.File;
import java.io.IOException;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Repository;
import org.springframework.util.FileCopyUtils;

/**
 * @author Mark Fisher
 */
@Repository
public class FileSystemFunctionRepository implements FunctionRepository, InitializingBean {

	private final String directory = "/tmp/function-repo";

	@Override
	public void afterPropertiesSet() throws Exception {
		File base = new File(this.directory);
		if (!base.exists()) {
			base.mkdirs();
		}
	}

	@Override
	public String[] names() {
		return new File(this.directory).list();
	}

	@Override
	public byte[] find(String name) {
		File file = new File(this.directory, name);
		try {
			return (file.exists()) ? FileCopyUtils.copyToByteArray(file) : null;
		}
		catch (IOException e) {
			throw new IllegalStateException("failed to read function from file", e);
		}
	}

	@Override
	public void save(String name, byte[] bytecode) {
		File file = new File(this.directory, name);
		try {
			FileCopyUtils.copy(bytecode, file);
		}
		catch (IOException e) {
			throw new IllegalStateException("failed to write function to file", e);
		}
	}
}
