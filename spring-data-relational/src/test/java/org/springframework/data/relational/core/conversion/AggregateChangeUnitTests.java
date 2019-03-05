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

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Unit tests for the {@link AggregateChange}.
 *
 * @author Jens Schauder
 */
public class AggregateChangeUnitTests {

	DummyEntity entity = new DummyEntity();
	Content content = new Content();

	RelationalMappingContext context = new RelationalMappingContext();
	RelationalConverter converter = new BasicRelationalConverter(context);

	PersistentPropertyAccessor<DummyEntity> propertyAccessor = context.getRequiredPersistentEntity(DummyEntity.class)
			.getPropertyAccessor(entity);
	Object id = 23;

	PathNode rootNode = new PathNode(context, entity);
	DbAction.WithEntity<?> rootInsert = new DbAction.InsertRoot<>(rootNode);

	DbAction.Insert<?> createInsert(String propertyName, Object value, PathNode.ValueIdentificationStrategy identificationStrategy) {

		////differen action needed based on key...

		DbAction.Insert<Object> insert = new DbAction.Insert<>(value,
				new PathNode(
						rootNode,
						context.getPersistentPropertyPath(propertyName, DummyEntity.class),
						identificationStrategy),
				rootInsert);
		insert.getAdditionalValues().put("dummy_entity_key", identificationStrategy.getIdentifier());

		return insert;
	}

	@Test // DATAJDBC-241
	public void setIdForSimpleReference() {

		entity.single = content;

		DbAction.Insert<?> insert = createInsert("single", content, new PathNode.DirectValueIdentificationStrategy());

		AggregateChange.setId(context, converter, insert, id);

		DummyEntity result = propertyAccessor.getBean();

		assertThat(result.single.id).isEqualTo(id);
	}

	@Test // DATAJDBC-241
	public void setIdForSingleElementSet() {

		entity.contentSet.add(content);

		DbAction.Insert<?> insert = createInsert("contentSet", content, new PathNode.LinkedHashSetValueIdentificationStrategy(0));

		AggregateChange.setId(context, converter, insert, id);

		DummyEntity result = propertyAccessor.getBean();
		assertThat(result.contentSet).isNotNull();
		assertThat(result.contentSet).extracting(c -> c == null ? "null" : c.id).containsExactlyInAnyOrder(23);
	}

	@Test // DATAJDBC-241
	public void setIdForSingleElementList() {

		entity.contentList.add(content);

		DbAction.Insert<?> insert = createInsert("contentList", content, new PathNode.ListValueIdentificationStrategy(0));

		AggregateChange.setId(context, converter, insert, id);

		DummyEntity result = propertyAccessor.getBean();
		assertThat(result.contentList).extracting(c -> c.id).containsExactlyInAnyOrder(23);
	}

	@Test // DATAJDBC-241
	public void setIdForSingleElementMap() {

		entity.contentMap.put("one", content);

		DbAction.Insert<?> insert = createInsert("contentMap", content, new PathNode.MappedValueIdentificationStrategy("one"));

		AggregateChange.setId(context, converter, insert, id);

		DummyEntity result = propertyAccessor.getBean();
		assertThat(result.contentMap.entrySet()).extracting(e -> e.getKey(), e -> e.getValue().id)
				.containsExactlyInAnyOrder(tuple("one", 23));
	}

	private static class DummyEntity {

		@Id Integer rootId;

		Content single;

		Set<Content> contentSet = new HashSet<>();

		List<Content> contentList = new ArrayList<>();

		Map<String, Content> contentMap = new HashMap<>();
	}

	private static class Content {

		@Id Integer id;
	}
}
