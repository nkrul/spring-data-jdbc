/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import lombok.NonNull;
import lombok.Value;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Exception thrown when building a {@link PathNode} fails
 *
 * @author Nicholas Krul
 */
@Value
public class PropertyResolutionException extends RuntimeException {

	@NonNull
	final PersistentPropertyPath<RelationalPersistentProperty> path;

	@NonNull
	final PathNode node;

	/**
	 * @param path The path within the current node. Must not be {@code null}.
	 * @param node the current node being processed. Must not be {@code null}.
	 */
	public PropertyResolutionException(PersistentPropertyPath<RelationalPersistentProperty> path, PathNode node) {
		super("Failed to access property " + path.toDotPath());
		this.path = path;
		this.node = node;
	}
}
