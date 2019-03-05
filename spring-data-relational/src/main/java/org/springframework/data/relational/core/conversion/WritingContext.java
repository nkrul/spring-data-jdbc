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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Holds context information for the current save operation.
 *
 * @author Jens Schauder
 * @author Bastian Wilhelm
 * @author Nicholas Krul
 */
class WritingContext {

	private final RelationalMappingContext context;
	private final Object entity;
	private final Map<PathNode, DbAction> previousActions = new HashMap<>();

	WritingContext(RelationalMappingContext context, Object root, AggregateChange<?> aggregateChange) {

	    //root and entity are *always* the same???  nK TRACE
		this.context = context;
		this.entity = aggregateChange.getEntity();
	}

	/**
	 * Leaves out the isNew check as defined in #DATAJDBC-282
	 *
	 * @return List of {@link DbAction}s
	 * @see <a href="https://jira.spring.io/browse/DATAJDBC-282">DAJDBC-282</a>
	 */
	List<DbAction<?>> insert() {

		List<DbAction<?>> actions = new ArrayList<>();
		PathNode rootNode = new PathNode(context, entity);
		actions.add(setRootAction(rootNode, new DbAction.InsertRoot<>(rootNode)));
		actions.addAll(insertReferenced(rootNode));
		return actions;
	}

	/**
	 * Leaves out the isNew check as defined in #DATAJDBC-282
	 *
	 * @return List of {@link DbAction}s
	 * @see <a href="https://jira.spring.io/browse/DATAJDBC-282">DAJDBC-282</a>
	 */
	List<DbAction<?>> update() {

		PathNode rootNode = new PathNode(context, entity);
		List<DbAction<?>> actions = new ArrayList<>(deleteReferenced(rootNode));
		actions.add(setRootAction(rootNode, new DbAction.UpdateRoot<>(rootNode)));
		actions.addAll(insertReferenced(rootNode));
		return actions;
	}

	List<DbAction<?>> save() {

		List<DbAction<?>> actions = new ArrayList<>();
		PathNode rootNode = new PathNode(context, entity);
		if (isNew(entity)) {

			actions.add(setRootAction(rootNode, new DbAction.InsertRoot<>(rootNode)));
			actions.addAll(insertReferenced(rootNode));
		} else {

			actions.addAll(deleteReferenced(rootNode));
			actions.add(setRootAction(rootNode, new DbAction.UpdateRoot<>(rootNode)));
			actions.addAll(insertReferenced(rootNode));
		}

		return actions;
	}

	private boolean isNew(Object o) {
		return context.getRequiredPersistentEntity(o.getClass()).isNew(o);
	}

	//// Operations on all paths

	private List<DbAction<?>> insertReferenced(PathNode parent) {

		List<DbAction<?>> actions = new ArrayList<>();
		parent.calculateChildNodes().forEach(path -> actions.addAll(insertAll(path)));
		return actions;
	}

	private List<DbAction<?>> insertAll(PathNode node) {

		List<DbAction<?>> actions = new ArrayList<>();

		DbAction.Insert<Object> insert;
		if (node.getPropertyPath().getRequiredLeafProperty().isQualified()) {

			insert = new DbAction.Insert<>(node.getValue(), node, getAction(node.getParent()));
			insert.getAdditionalValues().put(node.getPropertyPath().getRequiredLeafProperty().getKeyColumn(), node.getIdentifier().getIdentifier());

		} else {
			insert = new DbAction.Insert<>(node.getValue(), node, getAction(node.getParent()));
		}
		previousActions.put(node, insert);
		actions.add(insert);

		node.calculateChildNodes().forEach(path -> actions.addAll(insertAll(path)));

		return actions;
	}

	private List<DbAction<?>> deleteReferenced(PathNode node) {

		List<DbAction<?>> deletes = new ArrayList<>();

		Object value = node.getValue();
		Object id = context.getRequiredPersistentEntity(entity.getClass()).getIdentifierAccessor(entity).getIdentifier();

		context
				.findPersistentPropertyPaths(value.getClass(), (p) -> p.isEntity() && !p.isEmbedded())
				.forEach(path -> deletes.add(0, new DbAction.Delete<>(id, path)));

		return deletes;
	}

	//// methods not directly related to the creation of DbActions

	private DbAction<?> setRootAction(PathNode node, DbAction<?> dbAction) {

		previousActions.put(node, dbAction);
		return dbAction;
	}

	@Nullable
	private DbAction.WithEntity<?> getAction(PathNode parent) {

		DbAction action = previousActions.get(parent);

		if (action != null) {

			Assert.isInstanceOf( //
					DbAction.WithEntity.class, //
					action, //
					"dependsOn action is not a WithEntity, but " + action.getClass().getSimpleName() //
			);

			return (DbAction.WithEntity<?>) action;
		}

		return null;
	}

}
