/*
 * Copyright 2019 the original author or authors.
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

import lombok.*;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

import java.util.*;
/**
 * Represents a single entity in an aggregate along with its property path from the root entity and the chain of
 * objects to traverse a long this path.
 *
 * @author Jens Schauder
 * @author Nicholas Krul
 */
@Getter
@ToString
public class PathNode {

	@NonNull
	private RelationalMappingContext context;

	private PathNode parent;

	private PersistentPropertyPath<RelationalPersistentProperty> propertyPath;

	@NonNull
	private ValueIdentificationStrategy identifier;

	public PathNode(RelationalMappingContext context, Object value) {
		this.context = context;
		this.identifier = new KnownValueIdentificationStrategy(value);
	}

	public PathNode(PathNode parent, PersistentPropertyPath<RelationalPersistentProperty> propertyPath, ValueIdentificationStrategy identifier) {
		this.context = parent.context;
		this.parent = parent;
		this.propertyPath = propertyPath;
		this.identifier = identifier;
	}

	public Object getValue() {
		if (parent == null) {
			return identifier.convert(null);
		}
		Object parentValue = getParent().getValue();
		Object thisValue = getPropertyPath().getRequiredLeafProperty().getOwner().getPropertyAccessor(parentValue).getProperty(getPropertyPath().getRequiredLeafProperty());
		return identifier.convert(thisValue);
	}

	public void setValue(Object value) {
		PathNode node = this;
		while(node.parent != null) {
			PersistentPropertyAccessor persistentPropertyAccessor = node.getPropertyPath().getRequiredLeafProperty().getOwner().getPropertyAccessor(node.getParent().getValue());
			Object wrapperValue = persistentPropertyAccessor.getProperty(node.getPropertyPath().getRequiredLeafProperty());
			wrapperValue = node.identifier.update(wrapperValue, value);
			persistentPropertyAccessor.setProperty(node.getPropertyPath().getRequiredLeafProperty(), wrapperValue);
			value = persistentPropertyAccessor.getBean();
			node = node.parent;
		}
		node.identifier = new KnownValueIdentificationStrategy(value);
	}

	public List<PathNode> calculateChildNodes() {
		return context
				.findPersistentPropertyPaths(getValue().getClass(), (p) -> p.isEntity() && !p.isEmbedded())
				.stream()
				.filter(p -> p.getLength() == 1)
				.map(this::calculateChildNodesForProperty)
				.reduce(new ArrayList<>(), (l, r) -> {l.addAll(r); return l;});
	}

	private List<PathNode> calculateChildNodesForProperty(PersistentPropertyPath<RelationalPersistentProperty> path) {
		Object childValue = path.getRequiredLeafProperty().getOwner().getPropertyAccessor(getValue()).getProperty(path.getRequiredLeafProperty());

		if (childValue == null) {
			return Collections.emptyList();
		}

		List<PathNode> children = new ArrayList<>();
		if (path.getRequiredLeafProperty().isEmbedded()) {

			children.add(new PathNode(this, path, new DirectValueIdentificationStrategy()));
		} else if (path.getRequiredLeafProperty().isQualified()) {

			if (path.getRequiredLeafProperty().isMap()) {
				((Map<?, ?>) childValue).keySet()
						.stream()
						.map(MappedValueIdentificationStrategy::new)
						.map(valueIdentificationStrategy -> new PathNode(this, path, valueIdentificationStrategy))
						.forEach(childNode -> children.add(childNode));
			} else {

				int size = ((List) childValue).size();
				for (int k = 0; k < size; k++) {
					children.add(new PathNode(this, path, new ListValueIdentificationStrategy(k)));
				}
			}
		} else if (path.getRequiredLeafProperty().isCollectionLike()) { // collection value

			if (childValue instanceof Set) {
				if (!(path.getRequiredLeafProperty().isImmutable() || childValue instanceof SortedSet)) {
					LinkedHashSet newValue = new LinkedHashSet((Set)childValue);
					PersistentPropertyAccessor propertyAccessor = path.getRequiredLeafProperty().getOwner().getPropertyAccessor(getValue());
					propertyAccessor.setProperty(path.getRequiredLeafProperty(), newValue);

					//ensure any fun with immutables is propagated upwards
					setValue((propertyAccessor).getBean());
				}
				int index = 0;
				Iterator it = ((Collection<?>) childValue).iterator();
				while (it.hasNext()) {
					it.next();
					children.add(new PathNode(this, path, new LinkedHashSetValueIdentificationStrategy(index++)));
				}
			} else {
				// some odd kind of collection perhaps?
				throw new PropertyResolutionException(path, getParent());
			}
		} else { // single entity value
			children.add(new PathNode(this, path, new DirectValueIdentificationStrategy()));
		}

		return children;
	}

	public interface ValueIdentificationStrategy {
		Object convert(Object value);
		Object getIdentifier();
		Object update(Object wrapperValue, Object identifiedValue);
	}

	public static class DirectValueIdentificationStrategy implements ValueIdentificationStrategy {
		@Override
		public Object convert(Object value) {
			return value;
		}

		@Override
		public Object getIdentifier() {
			return null;
		}

		@Override
		public Object update(Object wrapperValue, Object identifiedValue) {
			return identifiedValue;
		}
	}

	@Value
	@AllArgsConstructor
	public static class MappedValueIdentificationStrategy implements ValueIdentificationStrategy {
		private Object key;

		@Override
		public Object convert(Object value) {
			return ((Map) value).get(key);
		}

		@Override
		public Object getIdentifier() {
			return key;
		}

		@Override
		public Object update(Object wrapperValue, Object identifiedValue) {
			((Map) wrapperValue).put(key, identifiedValue);
			return wrapperValue;
		}
	}

	@Value
	@AllArgsConstructor
	public static class ListValueIdentificationStrategy implements ValueIdentificationStrategy {
		private int index;

		@Override
		public Object convert(Object value) {
			return ((List) value).get(index);
		}

		@Override
		public Object getIdentifier() {
			return index;
		}

		@Override
		public Object update(Object wrapperValue, Object identifiedValue) {
			((List) wrapperValue).set(index, identifiedValue);
			return wrapperValue;
		}
	}

	@Value
	@AllArgsConstructor
	public static class LinkedHashSetValueIdentificationStrategy implements ValueIdentificationStrategy {
		private int index;

		@Override
		public Object convert(Object value) {
			Iterator it = ((Collection) value).iterator();
			for(int i = 0; i < index; i++)
				it.next();
			return it.next();
		}

		@Override
		public Object getIdentifier() { //there isn't REALLY an identifier...
			return null;
		}

		@Override
		public Object update(Object wrapperValue, Object identifiedValue) {
			Set newValue = new LinkedHashSet();
			Iterator it = ((Collection) wrapperValue).iterator();
			int i = 0;
			while (it.hasNext()) {
				Object listValue = it.next();
				if (i == index) listValue = identifiedValue;
				newValue.add(listValue);
				i++;
			}
			return newValue;
		}
	}

	@Data
	@AllArgsConstructor
	public static class KnownValueIdentificationStrategy implements ValueIdentificationStrategy {
		Object value;

		@Override
		public Object convert(Object value) {
			return this.value;
		}

		@Override
		public Object getIdentifier() {
			return null;
		}

		@Override
		public Object update(Object wrapperValue, Object identifiedValue) {
			value = identifiedValue;
			return value;
		}
	}

}
