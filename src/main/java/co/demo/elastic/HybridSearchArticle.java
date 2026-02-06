package co.demo.elastic;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.ollama.OllamaEmbeddingModel;
import dev.langchain4j.rag.content.Content;
import dev.langchain4j.rag.content.retriever.elasticsearch.ElasticsearchContentRetriever;
import dev.langchain4j.rag.query.Query;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.elasticsearch.ElasticsearchConfigurationHybrid;
import dev.langchain4j.store.embedding.elasticsearch.ElasticsearchConfigurationKnn;
import dev.langchain4j.store.embedding.elasticsearch.ElasticsearchEmbeddingStore;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

public class HybridSearchArticle {

    public static void main(String[] args) throws IOException, InterruptedException {

        String elasticsearchServerUrl = System.getenv("server-url");
        String elasticsearchApiKey = System.getenv("api-key");

        String ollamaUrl = System.getenv("ollama-url");
        String ollamaModelName = System.getenv("model-name");

        String moviesUrl = "https://gist.githubusercontent.com/ssh-esh/103fb8220de3b0e045393760c2f36575/raw" +
                          "/f56342573d8e28e69baff307ebb1a46833fe1ce1/scifi_1000.csv";

        try (RestClient restClient = RestClient
            .builder(HttpHost.create(elasticsearchServerUrl))
            .setDefaultHeaders(new Header[]{
                new BasicHeader("Authorization", "ApiKey " + elasticsearchApiKey)
            })
            .build()) {

            EmbeddingModel embeddingModel = OllamaEmbeddingModel.builder()
                .baseUrl(ollamaUrl)
                .modelName(ollamaModelName)
                .build();

            EmbeddingStore<TextSegment> embeddingStore = ElasticsearchEmbeddingStore.builder()
                .restClient(restClient)
                .build();

            CsvMapper csvMapper = new CsvMapper();
            CsvSchema schema = CsvSchema.builder()
                .addColumn("movie_id") // same order as in the csv
                .addColumn("movie_name")
                .addColumn("year")
                .addColumn("genre")
                .addColumn("description")
                .addColumn("director")
                .setColumnSeparator(',')
                .build();

            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(moviesUrl))
                .build();

            HttpResponse<InputStream> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofInputStream());
            InputStream csvContentStream = response.body();

            MappingIterator<Movie> it = csvMapper
                .readerFor(Movie.class)
                .with(schema)
                .readValues(new InputStreamReader(csvContentStream));

            List<Embedding> embeddings = new ArrayList<>();
            List<TextSegment> embedded = new ArrayList<>();

            boolean hasNext = true;

            while (hasNext) {
                try {
                    Movie movie = it.nextValue();
                    String text = movie.toString();

                    Embedding embedding = embeddingModel.embed(text).content();
                    embeddings.add(embedding);
                    embedded.add(new TextSegment(text, new Metadata()));
                    hasNext = it.hasNextValue();
                } catch (JsonParseException | InvalidFormatException e) {
                    // ignore malformed data
                }
            }

            embeddingStore.addAll(embeddings, embedded);

            restClient.performRequest(new Request("GET", "/default/_refresh"));


            String query =
                "Find movies where the main character is stuck in a time loop and reliving the same day.";


            ElasticsearchContentRetriever contentRetrieverVector = ElasticsearchContentRetriever.builder()
                .restClient(restClient)
                .configuration(ElasticsearchConfigurationKnn.builder().build())
                .maxResults(3)
                .embeddingModel(embeddingModel)
                .build();

            List<Content> vectorSearchResult = contentRetrieverVector.retrieve(Query.from(query));

            System.out.println("Vector search results: " + vectorSearchResult);
            System.out.println(vectorSearchResult.getFirst().textSegment());
            System.out.println(vectorSearchResult.get(1).textSegment());
            System.out.println(vectorSearchResult.get(2).textSegment());

            ElasticsearchContentRetriever contentRetrieverHybrid = ElasticsearchContentRetriever.builder()
                .restClient(restClient)
                .configuration(ElasticsearchConfigurationHybrid.builder().build())
                .maxResults(3)
                .embeddingModel(embeddingModel)
                .build();

            List<Content> hybridSearchResult = contentRetrieverHybrid.retrieve(Query.from(query));

            System.out.println("Hybrid search results: " + hybridSearchResult);
            System.out.println(hybridSearchResult.getFirst().textSegment());
            System.out.println(hybridSearchResult.get(1).textSegment());
            System.out.println(hybridSearchResult.get(2).textSegment());
        }
    }
}
