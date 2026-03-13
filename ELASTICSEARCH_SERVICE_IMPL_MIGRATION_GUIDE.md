# ElasticSearchServiceImpl.java - ES 9 Migration Guide

## Status

Due to the file's size (2034 lines) and complexity, a **template-based incremental migration** is recommended.

**Backup Created**: `ElasticSearchServiceImpl.java.bak`

## Migration Approach

The file requires migrating **all** Elasticsearch 7 High Level REST Client (HLRC) calls to Elasticsearch 9 Java API Client. This includes:

- Index/Create/Update/Delete operations
- Search operations with various query types
- Bulk operations
- Aggregations (date histogram, terms, geo, etc.)
- Suggestions/autocomplete
- Mapping retrieval
- Complex nested queries

## Core Migration Patterns

### Pattern 1: Create/Index Document

**Old (ES 7)**:
```java
IndexRequest request = new IndexRequest(index);
request.id(documentId);
request.source(document, XContentType.JSON);
IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
```

**New (ES 9)**:
```java
Map<String, Object> docMap = objectMapper.readValue(document, Map.class);
IndexResponse response = client.getClient().index(i -> i
    .index(index)
    .id(documentId)
    .document(docMap)
);
```

### Pattern 2: Fetch/Get Document

**Old (ES 7)**:
```java
GetRequest request = new GetRequest(index, documentId);
GetResponse response = client.get(request, RequestOptions.DEFAULT);
String source = response.getSourceAsString();
```

**New (ES 9)**:
```java
GetResponse<Map> response = client.getClient().get(g -> g
    .index(index)
    .id(documentId),
    Map.class
);
if (response.found() && response.source() != null) {
    String jsonString = objectMapper.writeValueAsString(response.source());
}
```

### Pattern 3: Update Document

**Old (ES 7)**:
```java
UpdateRequest request = new UpdateRequest(index, documentId);
request.doc(document);
UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
```

**New (ES 9)**:
```java
UpdateResponse<Map> updateResponse = client.getClient().update(u -> u
    .index(index)
    .id(documentId)
    .doc(document),
    Map.class
);
```

### Pattern 4: Delete Document

**Old (ES 7)**:
```java
DeleteRequest request = new DeleteRequest(index, documentId);
DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
```

**New (ES 9)**:
```java
DeleteResponse deleteResponse = client.getClient().delete(d -> d
    .index(index)
    .id(documentId)
);
```

### Pattern 5: Bulk Operations

**Old (ES 7)**:
```java
BulkRequest request = new BulkRequest();
for (JsonNode json : jsons) {
    IndexRequest ir = new IndexRequest(index);
    ir.id(json.get("id").asText());
    ir.source(json.toString(), XContentType.JSON);
    request.add(ir);
}
BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
```

**New (ES 9)**:
```java
List<BulkOperation> operations = new ArrayList<>();
for (JsonNode json : jsons) {
    String docId = json.get("id").asText();
    Map<String, Object> docMap = objectMapper.convertValue(json, Map.class);

    operations.add(BulkOperation.of(b -> b
        .index(idx -> idx
            .index(index)
            .id(docId)
            .document(docMap)
        )
    ));
}

BulkResponse bulkResponse = client.getClient().bulk(b -> b
    .operations(operations)
);
```

### Pattern 6: Search with Query

**Old (ES 7)**:
```java
SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
sourceBuilder.query(query);
sourceBuilder.from(from);
sourceBuilder.size(size);
sourceBuilder.sort(sortField, sortOrder);

SearchRequest searchRequest = new SearchRequest(index);
searchRequest.source(sourceBuilder);

SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
long totalHits = searchResponse.getHits().getTotalHits().value;

for (SearchHit hit : searchResponse.getHits().getHits()) {
    String source = hit.getSourceAsString();
}
```

**New (ES 9)**:
```java
SearchResponse<Map> searchResponse = client.getClient().search(s -> s
    .index(index)
    .query(query)  // query is now of type Query, not QueryBuilder
    .from(from)
    .size(size)
    .sort(so -> so.field(f -> f.field(sortField).order(sortOrder))),
    Map.class
);

long totalHits = searchResponse.hits().total() != null ?
    searchResponse.hits().total().value() : 0;

for (Hit<Map> hit : searchResponse.hits().hits()) {
    if (hit.source() != null) {
        String jsonString = objectMapper.writeValueAsString(hit.source());
    }
}
```

### Pattern 7: Date Histogram Aggregation

**Old (ES 7)**:
```java
AggregationBuilder aggregation = AggregationBuilders.dateHistogram(Constants.TEMPORAL_AGG)
    .field("created_on")
    .calendarInterval(DateHistogramInterval.days(1))
    .format("yyyy-MM-dd");

sourceBuilder.aggregation(aggregation);
SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

Histogram histogram = searchResponse.getAggregations().get(Constants.TEMPORAL_AGG);
for (Histogram.Bucket bucket : histogram.getBuckets()) {
    String dateString = bucket.getKeyAsString();
    long count = bucket.getDocCount();
}
```

**New (ES 9)**:
```java
SearchResponse<Void> searchResponse = client.getClient().search(s -> s
    .index(index)
    .query(query)
    .size(0)
    .aggregations("temporal_agg", a -> a
        .dateHistogram(d -> d
            .field("created_on")
            .calendarInterval(CalendarInterval.Day)
            .format("yyyy-MM-dd")
        )
    ),
    Void.class
);

if (searchResponse.aggregations() != null &&
    searchResponse.aggregations().get("temporal_agg") != null) {
    Aggregate agg = searchResponse.aggregations().get("temporal_agg");
    if (agg.isDateHistogram()) {
        for (DateHistogramBucket bucket : agg.dateHistogram().buckets().array()) {
            String dateString = bucket.keyAsString();
            long count = bucket.docCount();
        }
    }
}
```

### Pattern 8: Terms Aggregation

**Old (ES 7)**:
```java
TermsAggregationBuilder termsAgg = AggregationBuilders.terms(field)
    .field(field)
    .size(size);

SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
Terms terms = searchResponse.getAggregations().get(field);
for (Terms.Bucket bucket : terms.getBuckets()) {
    String key = bucket.getKeyAsString();
    long count = bucket.getDocCount();
}
```

**New (ES 9)**:
```java
SearchResponse<Void> searchResponse = client.getClient().search(s -> s
    .index(index)
    .query(query)
    .size(0)
    .aggregations(field, a -> a
        .terms(t -> t
            .field(field)
            .size(size)
        )
    ),
    Void.class
);

if (searchResponse.aggregations() != null &&
    searchResponse.aggregations().get(field) != null) {
    Aggregate agg = searchResponse.aggregations().get(field);
    if (agg.isSterms()) {
        for (StringTermsBucket bucket : agg.sterms().buckets().array()) {
            String key = bucket.key().stringValue();
            long count = bucket.docCount();
        }
    }
}
```

### Pattern 9: GeoHash Grid Aggregation

**Old (ES 7)**:
```java
GeoGridAggregationBuilder geohashGrid = AggregationBuilders.geohashGrid(field + "-" + precision);
geohashGrid.field(field);
geohashGrid.precision(precision);

SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
ParsedGeoHashGrid geoHashGrid = searchResponse.getAggregations().get(field + "-" + precision);
for (GeoGrid.Bucket bucket : geoHashGrid.getBuckets()) {
    String hash = bucket.getKeyAsString();
    long count = bucket.getDocCount();
}
```

**New (ES 9)**:
```java
SearchResponse<Void> searchResponse = client.getClient().search(s -> s
    .index(index)
    .query(query)
    .size(0)
    .aggregations(field + "-" + precision, a -> a
        .geohashGrid(g -> g
            .field(field)
            .precision(precision)
        )
    ),
    Void.class
);

if (searchResponse.aggregations() != null) {
    Aggregate agg = searchResponse.aggregations().get(field + "-" + precision);
    if (agg.isGeohashGrid()) {
        GeoHashGridAggregate geoHashGrid = agg.geohashGrid();
        for (GeoHashGridBucket bucket : geoHashGrid.buckets().array()) {
            String hash = bucket.key();
            long count = bucket.docCount();
        }
    }
}
```

### Pattern 10: Geo Distance Query & Sort

**Old (ES 7)**:
```java
GeoDistanceQueryBuilder geoQuery = QueryBuilders.geoDistanceQuery("location")
    .point(lat, lon)
    .distance("100km");

GeoDistanceSortBuilder sortBuilder = SortBuilders.geoDistanceSort("location", lat, lon)
    .order(SortOrder.ASC);

sourceBuilder.query(geoQuery);
sourceBuilder.sort(sortBuilder);
```

**New (ES 9)**:
```java
Query geoQuery = GeoDistanceQuery.of(g -> g
    .field("location")
    .location(l -> l.latlon(ll -> ll.lat(lat).lon(lon)))
    .distance("100km")
)._toQuery();

SearchResponse<Map> searchResponse = client.getClient().search(s -> s
    .index(index)
    .query(geoQuery)
    .sort(so -> so.geoDistance(gd -> gd
        .field("location")
        .location(l -> l.latlon(ll -> ll.lat(lat).lon(lon)))
        .order(SortOrder.Asc)
    )),
    Map.class
);
```

### Pattern 11: Autocomplete/Suggestions

**Old (ES 7)**:
```java
CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders
    .completionSuggestion(field)
    .prefix(text)
    .size(10);

SuggestBuilder suggestBuilder = new SuggestBuilder();
suggestBuilder.addSuggestion(field + "_suggest", suggestionBuilder);

sourceBuilder.suggest(suggestBuilder);
SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

Suggest suggest = searchResponse.getSuggest();
CompletionSuggestion suggestion = suggest.getSuggestion(field + "_suggest");
for (CompletionSuggestion.Entry entry : suggestion.getEntries()) {
    for (CompletionSuggestion.Entry.Option option : entry.getOptions()) {
        // Process suggestions
    }
}
```

**New (ES 9)**:
```java
SearchResponse<T> searchResponse = client.getClient().search(s -> s
    .index(index)
    .suggest(sg -> sg
        .suggesters(field + "_suggest", sug -> sug
            .prefix(text)
            .completion(c -> c.field(field).size(10))
        )
    ),
    classMapped
);

// Process suggestions from response
if (searchResponse.suggest() != null) {
    Map<String, List<Suggestion<T>>> suggestions = searchResponse.suggest();
    // Process suggestions
}
```

### Pattern 12: Get Mappings

**Old (ES 7)**:
```java
GetMappingsRequest request = new GetMappingsRequest();
request.indices(index);
GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);

Map<String, MappingMetadata> mappings = response.mappings();
for (Map.Entry<String, MappingMetadata> entry : mappings.entrySet()) {
    Map<String, Object> sourceAsMap = entry.getValue().sourceAsMap();
}
```

**New (ES 9)**:
```java
GetMappingResponse response = client.getClient().indices().getMapping(m -> m.index(index));

for (Map.Entry<String, IndexMappingRecord> entry : response.result().entrySet()) {
    String jsonMapping = objectMapper.writeValueAsString(entry.getValue().mappings());
    Map<String, Object> mappingMap = objectMapper.readValue(jsonMapping, Map.class);
}
```

## Required Import Changes

**Remove all these ES 7 imports**:
```java
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.*;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.suggest.*;
```

**Add these ES 9 imports**:
```java
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.*;
import co.elastic.clients.elasticsearch.core.search.*;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.JsonData;
```

## Step-by-Step Migration Instructions

1. **Backup**: Already created at `ElasticSearchServiceImpl.java.bak`

2. **Update imports**: Replace all ES 7 imports with ES 9 imports (see above)

3. **Update client reference**:
   - Change: `client.index()` → `client.getClient().index()`
   - Change: `client.search()` → `client.getClient().search()`
   - etc.

4. **Migrate methods one by one**:
   - Start with `create()` - use Pattern 1
   - Then `fetch()` - use Pattern 2
   - Then `update()` - use Pattern 3
   - Then `delete()` - use Pattern 4
   - Then `bulkUpload()` and `bulkUpdate()` - use Pattern 5
   - Then all search methods - use Pattern 6
   - Then aggregation methods - use Patterns 7, 8, 9
   - etc.

5. **Update QueryBuilder usage**:
   - All methods in ElasticSearchQueryUtil now return `Query` instead of `QueryBuilder`
   - Use `getBoolQuery()` instead of `getBoolQueryBuilder()`

6. **Test each method** after migration before moving to the next

## Methods Requiring Migration

Based on the file analysis, here are all methods that need migration:

### CRUD Operations
- `create()` - Index document
- `fetch()` - Get document
- `update()` - Update document
- `delete()` - Delete document
- `bulkUpload()` - Bulk index
- `bulkUpdate()` - Bulk update

### Search Operations
- `termSearch()` - Term query search
- `boolSearch()` - Boolean query search
- `rangeSearch()` - Range query search
- `search()` - General search with MapSearchQuery
- `querySearch()` - Internal search helper

### Aggregation Operations
- `aggregationByDay()` - Day-level date histogram
- `aggregationByMonth()` - Month-level date histogram
- `aggregation()` - General aggregation
- `geohashAggregation()` - Geohash grid aggregation
- `termsAggregation()` - Terms aggregation
- `aggregateSearch()` - Helper method
- `groupAggregation()` - Helper method

### Specialized Queries
- `getObservationRightPan()` - Complex observation query
- `observationNearBy()` - Geo distance query
- `autoCompletion()` - Autocomplete suggestions
- `identifierInfo()` - Identifier aggregations
- `uploaderInfo()` - Uploader aggregations

### Admin Operations
- `getMappings()` - Get index mappings

## Notes

- Use the migrated `ElasticSearchQueryUtil` - it now returns `Query` objects
- All aggregation parsing is different - check types with `agg.isSterms()`, `agg.isDateHistogram()`, etc.
- Response handling changed - `hits().hits()` returns `List<Hit<T>>` not `SearchHit[]`
- Track total hits: `searchResponse.hits().total().value()` (may be null, check first)
- Use `ObjectMapper` to convert between JSON strings and Maps
- The new API uses generics - specify document type: `SearchResponse<Map>`, `GetResponse<Map>`, etc.

## Testing Checklist

After migration, test:
- [ ] Document CRUD (create, read, update, delete)
- [ ] Bulk operations
- [ ] Simple term searches
- [ ] Boolean queries
- [ ] Range queries
- [ ] Date histogram aggregations
- [ ] Terms aggregations
- [ ] Geohash aggregations
- [ ] Geo distance queries
- [ ] Autocomplete
- [ ] Mapping retrieval

## Estimated Effort

- **Time**: 8-16 hours for careful, tested migration
- **Complexity**: High - requires understanding both old and new APIs
- **Risk**: Medium - comprehensive testing required

## Support

- Refer to: `ELASTICSEARCH_9_MIGRATION.md` for general guidance
- Use migrated files as reference:
  - `ElasticSearchQueryUtil.java` - Query building patterns
  - `ElasticSearchGeoServiceImpl.java` - Geo operations
  - `GeojsonServiceImpl.java` - Simple search
- Official docs: https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html
