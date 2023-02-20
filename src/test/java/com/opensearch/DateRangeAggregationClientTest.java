package com.opensearch;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.opensearch.model.ProductDetails;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Node;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ExpandWildcard;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.aggregations.DateRangeAggregation;
import org.opensearch.client.opensearch._types.aggregations.DateRangeExpression;
import org.opensearch.client.opensearch._types.aggregations.FieldDateMath;
import org.opensearch.client.opensearch.cat.IndicesResponse;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;

public class DateRangeAggregationClientTest {

    private OpenSearchClient client;

    @Test
    public void testDateRangeAggregation() throws Exception {
        var index = "test-date-range-aggregation";
        createDateRangeDocuments(index);
        var searchResponse = sendAggregateRequest(index, "expiry_ranges", getExpiryDateRangeAggregation());
        var expiryRangesAggregations = searchResponse.aggregations().get("expiry_ranges");
        var buckets = expiryRangesAggregations._get()
                ._toAggregate()
                .dateRange()
                .buckets()
                .array();

        assertEquals(3, buckets.size());
    }

    private Aggregation getExpiryDateRangeAggregation() {
        DateRangeAggregation expiryDateRangeAggregation = new DateRangeAggregation.Builder()
                .field("expDate")
                .ranges(getDateAggregationRanges())
                .build();
        return new Aggregation.Builder().dateRange(expiryDateRangeAggregation).build();
    }

    private SearchResponse<Void> sendAggregateRequest(String index, String key, Aggregation value) throws IOException {
        return client.search(
                request -> request.index(index)
                        .size(0)
                        .aggregations(key, value),
                Void.class);
    }

    private List<DateRangeExpression> getDateAggregationRanges() {
        return List.of(
                new DateRangeExpression.Builder()
                        .from(builder -> builder.value((double) getDatePlusDays(1).getTime()))
                        .to(FieldDateMath.of(builder -> builder.value((double) getDatePlusDays(3).getTime() - 1000)))
                        .key("from-1-to-2-days")
                        .build(),
                new DateRangeExpression.Builder()
                        .from(builder -> builder.value((double) getDatePlusDays(3).getTime()))
                        .to(FieldDateMath.of(builder -> builder.value((double) getDatePlusDays(5).getTime() - 1000)))
                        .key("from-3-to-4-days")
                        .build(),
                new DateRangeExpression.Builder()
                        .from(builder -> builder.value((double) getDatePlusDays(5).getTime()))
                        .to(FieldDateMath.of(builder -> builder.value((double) getDatePlusDays(7).getTime() - 1000)))
                        .key("from-5-to-6-days")
                        .build()
        );
    }

    private void createDateRangeDocuments(String index) throws IOException {
        client.create(_1 -> _1.index(index).id("1").document(createProduct("egg", 2, 1)).refresh(Refresh.True));
        client.create(_1 -> _1.index(index).id("2").document(createProduct("meat", 15, 2)).refresh(Refresh.True));
        client.create(_1 -> _1.index(index).id("3").document(createProduct("ham", 30, 3)).refresh(Refresh.True));
        client.create(_1 -> _1.index(index).id("4").document(createProduct("cheese", 25, 4)).refresh(Refresh.True));
        client.create(_1 -> _1.index(index).id("5").document(createProduct("pasta", 8, 5)).refresh(Refresh.True));
        client.create(_1 -> _1.index(index).id("6").document(createProduct("oil", 50, 6)).refresh(Refresh.True));
    }

    private ProductDetails createProduct(String name, int cost, int plusDays) {
        return new ProductDetails(name, cost, getDatePlusDays(plusDays));
    }

    private Date getDatePlusDays(int plusDays) {
        return java.sql.Date.from(LocalDateTime.of(2023, 2, 20, 0, 0, 0).plusDays(plusDays).toInstant(ZoneOffset.UTC));
    }

    @BeforeEach
    public void configureClient() {
        Node node = new Node(HttpHost.create("http://localhost:9200"));
        RestClient restClient = RestClient
                .builder(node)
                .build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        client = new OpenSearchClient(transport);
    }

    @AfterEach
    public void cleanup() throws Exception {
        final IndicesResponse response = client
                .cat()
                .indices(r -> r.expandWildcards(ExpandWildcard.All));

        for (IndicesRecord index : response.valueBody()) {
            if (index.index() != null && !".opendistro_security".equals(index.index())) {
                client.indices().delete(new DeleteIndexRequest.Builder().index(index.index()).build());
            }
        }
    }
}
