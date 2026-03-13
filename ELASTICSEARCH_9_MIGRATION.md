# Elasticsearch 7 to 9 Migration Guide

## Migration Status

### ✅ COMPLETED

1. **pom.xml** - Updated dependencies from ES 7.9.3 to ES 9.0.0
   - Replaced `elasticsearch-rest-high-level-client` with `elasticsearch-java` (new Java API Client)
   - Added required dependencies: `jakarta.json-api`

2. **ElasticSearchClient.java** - Completely rewritten for ES 9
   - No longer extends `RestHighLevelClient` (deprecated/removed)
   - Now uses `ElasticsearchClient` from the new Java API
   - Implements `AutoCloseable` for proper resource management
   - Provides `getClient()` method to access the new client

3. **ESModuleServeletContextListener.java** - Updated client initialization
   - Simplified RestClient builder usage
   - Removed deprecated import statements

4. **ElasticSearchQueryUtil.java** - Completely migrated to ES 9 API
   - Replaced all `QueryBuilder` classes with new `Query` functional builders
   - `GeoPolygonQueryBuilder` → `GeoShapeQuery` (deprecated query fixed)
   - All boolean, range, terms, match phrase queries migrated to lambda-based builders
   - Aggregations updated to use new `Aggregation` builders
   - Geographic queries use new coordinate system

### ⚠️ CRITICAL - REQUIRES MANUAL MIGRATION

The following files contain extensive ES client usage and require complete rewrites.
The old High Level REST Client API is fundamentally different from the new Java API Client.

#### 1. **ElasticSearchServiceImpl.java** (~2500 lines)

**Major Changes Required:**
- All `client.index()`, `client.update()`, `client.delete()`, `client.search()` calls need rewriting
- Method signatures changed from imperative to functional/lambda style
- `SearchRequest` → Use `client.search(s -> s.index(...).query(...))`
- `IndexRequest` → Use `client.index(i -> i.index(...).document(...))`
- `UpdateRequest` → Use `client.update(u -> u.index(...).doc(...))`
- `DeleteRequest` → Use `client.delete(d -> d.index(...).id(...))`
- `BulkRequest` → Use `client.bulk(b -> b.operations(...))`
- `SearchResponse` → Different structure for accessing hits, aggregations
- `DateHistogramInterval` → Use `CalendarInterval` or `FixedInterval`
- All aggregation result parsing needs updating

**Example Migration Pattern:**

**Old (ES 7)**:
```java
IndexRequest request = new IndexRequest(index);
request.id(documentId);
request.source(document, XContentType.JSON);
IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
```

**New (ES 9)**:
```java
IndexResponse indexResponse = client.getClient().index(i -> i
    .index(index)
    .id(documentId)
    .document(parseDocument(document))  // Need to parse JSON to object
);
```

**Files Affected:**
- `create()` method - Index document creation
- `fetch()` method - Get document by ID
- `update()` method - Update document
- `delete()` method - Delete document
- `bulk()` method - Bulk operations
- `search()` method - Search queries
- All aggregation methods (temporal, geohash, terms, etc.)
- `getMappings()` method - Index mapping retrieval

#### 2. **ElasticSearchGeoServiceImpl.java** (~500 lines)

**Changes Required:**
- Geographic aggregations (GeoHashGrid)
- Search with geo filters
- GeoBounds aggregations
- All use new `client.getClient().search()` pattern

#### 3. **ElasticAdminSearchServiceImpl.java** (~200 lines)

**Changes Required:**
- Uses `RestClient` for low-level operations
- Can use `client.getLowLevelClient()` from updated ElasticSearchClient
- Request/Response parsing may need updates

#### 4. **GeojsonServiceImpl.java** (~300 lines)

**Changes Required:**
- GeoJSON search queries
- Bounding box queries updated in ElasticSearchQueryUtil but service integration needed

### 📝 DOCUMENTATION UPDATES NEEDED

#### **elasticsearch.md**
Current documentation shows ES 6.0 mapping syntax:
```json
{
  "mappings": {
    "observation": {  // Type name removed in ES 8+
      "properties": { ... }
    }
  }
}
```

Should be updated to ES 9 syntax:
```json
{
  "mappings": {
    "properties": { ... }  // No type wrapper
  }
}
```

## Breaking Changes Summary

### 1. Client Library
- **Old:** `org.elasticsearch.client.RestHighLevelClient`
- **New:** `co.elastic.clients.elasticsearch.ElasticsearchClient`

### 2. Query Builders
- **Old:** `QueryBuilders.boolQuery()` returns `BoolQueryBuilder`
- **New:** `BoolQuery.of(b -> b.must(...))` returns `Query`

### 3. Geo Queries
- **Old:** `GeoPolygonQueryBuilder` (REMOVED)
- **New:** `GeoShapeQuery` with polygon shape

### 4. Package Changes
- **Old:** `org.elasticsearch.common.xcontent.*`
- **New:** `org.elasticsearch.xcontent.*` (in elasticsearch-x-content library)

### 5. Aggregations
- **Old:** `AggregationBuilders.dateHistogram().interval(DateHistogramInterval.MONTH)`
- **New:** `DateHistogramAggregation.of(d -> d.calendarInterval(CalendarInterval.Month))`

### 6. Request Pattern
- **Old:** Imperative - create request object, set properties, execute
- **New:** Functional - lambda builders in single expression

## Migration Steps for Service Implementations

### Pattern 1: Index/Create Document

```java
// NEW ES 9 Pattern
public MapQueryResponse create(String index, String type, String documentId, String document) throws IOException {
    // Parse JSON string to Map or specific class
    Map<String, Object> docMap = objectMapper.readValue(document, Map.class);

    IndexResponse response = client.getClient().index(i -> i
        .index(index)
        .id(documentId)
        .document(docMap)
    );

    // Handle response
    return new MapQueryResponse(
        MapQueryStatus.valueOf(response.result().name()),
        ""
    );
}
```

### Pattern 2: Search with Query

```java
// NEW ES 9 Pattern
public MapResponse search(String index, Query query, int from, int size) throws IOException {
    SearchResponse<Map> response = client.getClient().search(s -> s
        .index(index)
        .query(query)
        .from(from)
        .size(size),
        Map.class
    );

    // Process hits
    List<Hit<Map>> hits = response.hits().hits();
    // ... process results
}
```

### Pattern 3: Aggregations

```java
// NEW ES 9 Pattern
SearchResponse<Void> response = client.getClient().search(s -> s
    .index(index)
    .query(query)
    .size(0)  // Only want aggregations
    .aggregations("my_agg", a -> a
        .terms(t -> t
            .field("field_name")
            .size(100)
        )
    ),
    Void.class
);

// Extract aggregation results
Map<String, Aggregate> aggregations = response.aggregations();
StringTermsAggregate termsAgg = aggregations.get("my_agg").sterms();
```

## Testing Requirements

After completing the migration, test:

1. **Document Operations**
   - Create/Index documents
   - Update documents
   - Delete documents
   - Bulk operations

2. **Search Operations**
   - Simple term queries
   - Boolean queries (must, should, filter)
   - Range queries
   - Nested queries
   - Match phrase queries

3. **Geographic Operations**
   - Geo bounding box queries
   - Geo polygon/shape queries
   - GeoHash grid aggregations
   - GeoBounds aggregations

4. **Aggregations**
   - Terms aggregations
   - Date histogram aggregations
   - Nested aggregations
   - Pipeline aggregations

5. **Edge Cases**
   - Empty results
   - Large result sets
   - Complex nested queries
   - Multi-field aggregations

## Dependencies

### Updated in pom.xml:
```xml
<!-- ES 9 Java API Client -->
<dependency>
    <groupId>co.elastic.clients</groupId>
    <artifactId>elasticsearch-java</artifactId>
    <version>9.0.0</version>
</dependency>

<dependency>
    <groupId>jakarta.json</groupId>
    <artifactId>jakarta.json-api</artifactId>
    <version>2.1.3</version>
</dependency>

<!-- Jackson for JSON processing -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.15.2</version>
</dependency>
```

## Additional Resources

- [Elasticsearch Java API Client Documentation](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html)
- [Migration Guide from HLRC](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/migrate-hlrc.html)
- [Breaking Changes in ES 8](https://www.elastic.co/guide/en/elasticsearch/reference/8.0/breaking-changes-8.0.html)
- [Breaking Changes in ES 9](https://www.elastic.co/guide/en/elasticsearch/reference/9.0/breaking-changes-9.0.html)

## Next Steps

1. **Phase 1:** Test the completed migrations (ElasticSearchClient, ElasticSearchQueryUtil)
2. **Phase 2:** Migrate ElasticSearchServiceImpl.java (largest file)
3. **Phase 3:** Migrate remaining service implementations
4. **Phase 4:** Integration testing
5. **Phase 5:** Update documentation

## Notes

- The new Java API Client uses a **fluent functional builder pattern** throughout
- All queries return `Query` objects, not specific builder types
- Type safety is improved with generics (e.g., `SearchResponse<MyClass>`)
- JSON processing requires explicit mapping (use Jackson ObjectMapper)
- Some convenience methods from HLRC don't exist - must build requests explicitly
