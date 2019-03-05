/*
 * Copyright 2017-2019 the original author or authors.
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

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Represents the change happening to the aggregate (as used in the context of Domain Driven Design) as a whole.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Nicholas Krul
 */
@Getter
public class AggregateChange<T> {

	private final Kind kind;

	/** Type of the aggregate root to be changed */
	private final Class<T> entityType;

	/** Aggregate root, to which the change applies, if available */
	@Nullable private T entity;

	private final List<DbAction<?>> actions = new ArrayList<>();

	public AggregateChange(Kind kind, Class<T> entityType, @Nullable T entity) {

		this.kind = kind;
		this.entityType = entityType;
		this.entity = entity;
	}

	@SuppressWarnings("unchecked")
	public void executeWith(Interpreter interpreter, RelationalMappingContext context, RelationalConverter converter) {

		PathNode rootUpdateNode = null;

		RelationalPersistentEntity<T> persistentEntity = entity != null
				? (RelationalPersistentEntity<T>) context.getRequiredPersistentEntity(entity.getClass())
				: null;

		PersistentPropertyAccessor<T> propertyAccessor = //
				persistentEntity != null //
						? converter.getPropertyAccessor(persistentEntity, entity) //
						: null;

		for(DbAction a: actions) {

			a.executeWith(interpreter);

			if (a instanceof DbAction.InsertRoot || a instanceof  DbAction.UpdateRoot) {
				PathNode node = ((DbAction.WithPathNode)a).getPathNode();
				if (entityType.equals(node.getValue().getClass()))
					rootUpdateNode = node;
			}

			if (a instanceof DbAction.WithGeneratedId) {

				Assert.notNull(persistentEntity,
						"For statements triggering database side id generation a RelationalPersistentEntity must be provided.");
				Assert.notNull(propertyAccessor, "propertyAccessor must not be null");

				Object generatedId = ((DbAction.WithGeneratedId<?>) a).getGeneratedId();

				if (generatedId != null) {

					if (a instanceof DbAction.InsertRoot && a.getEntityType().equals(entityType)) {
						propertyAccessor.setProperty(persistentEntity.getRequiredIdProperty(), generatedId);
						rootUpdateNode.setValue(propertyAccessor.getBean());
					} else if (a instanceof DbAction.WithDependingOn) {

						setId(context, converter, (DbAction.WithDependingOn<?>) a, generatedId);
					}
				}
			}
		}

		if (rootUpdateNode != null) {
			entity = (T)rootUpdateNode.getValue();
		} else if (propertyAccessor != null) {
			entity = propertyAccessor.getBean();
		}
	}

	public void addAction(DbAction<?> action) {
		actions.add(action);
	}

	@SuppressWarnings("unchecked")
	static void setId(
			RelationalMappingContext context,
			RelationalConverter converter,
			DbAction.WithDependingOn<?> action,
			Object generatedId
	) {

		Object currentPropertyValue = action.getPathNode().getValue();

		RelationalPersistentEntity persistentEntity = context.getRequiredPersistentEntity(action.getEntity().getClass());
		PersistentPropertyAccessor propertyAccessor = converter.getPropertyAccessor(persistentEntity, action.getEntity());

		Assert.notNull(currentPropertyValue, "Trying to set an ID for an element that does not exist");

		RelationalPersistentProperty requiredIdProperty = context
				.getRequiredPersistentEntity(action.getPathNode().getPropertyPath().getRequiredLeafProperty().getActualType()) //propertyPathToEntity
				.getRequiredIdProperty();

		PersistentPropertyPath<RelationalPersistentProperty> pathToId = context.getPersistentPropertyPath(
				requiredIdProperty.getName(),
				requiredIdProperty.getOwner().getType());

		propertyAccessor.setProperty(pathToId, generatedId);

		action.getPathNode().setValue(propertyAccessor.getBean());
	}

	/**
	 * Sets the id of the entity referenced in the action and uses the {@link PersistentPropertyAccessor} used for that.
	 */
	private static <T> PersistentPropertyAccessor<T> setId(RelationalConverter converter,
			DbAction.WithDependingOn<T> action, Object generatedId) {

		Object originalElement = action.getEntity();

		RelationalPersistentEntity<T> persistentEntity = (RelationalPersistentEntity<T>) converter.getMappingContext()
				.getRequiredPersistentEntity(action.getEntityType());
		PersistentPropertyAccessor<T> intermediateAccessor = converter.getPropertyAccessor(persistentEntity,
				(T) originalElement);

		RelationalPersistentProperty idProperty = persistentEntity.getIdProperty();
		if (idProperty != null) {
			intermediateAccessor.setProperty(idProperty, generatedId);
		}

		return intermediateAccessor;
	}

	/**
	 * The kind of action to be performed on an aggregate.
	 */
	public enum Kind {
		/**
		 * A {@code SAVE} of an aggregate typically involves an {@code insert} or {@code update} on the aggregate root plus
		 * {@code insert}s, {@code update}s, and {@code delete}s on the other elements of an aggregate.
		 */
		SAVE,

		/**
		 * A {@code DELETE} of an aggregate typically involves a {@code delete} on all contained entities.
		 */
		DELETE
	}
}
