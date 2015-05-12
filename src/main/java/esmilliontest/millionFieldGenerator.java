package esmilliontest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.math.random.RandomData;
import org.apache.commons.math.random.RandomDataImpl;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class millionFieldGenerator {
	private static RandomData rnd = new RandomDataImpl();

	public static void main(String[] args) throws IOException,
			InterruptedException {

		long num_of_fields = Long.parseLong(args[0]);
		long num_of_document_ids = Long.parseLong(args[1]);
		// Usage
		if (args.length != 2) {
			System.out
					.println("Usage: java -jar millionFieldGenerator <num_of_fields> <num_of_document_ids>");
			System.exit(0);
		}

		System.out.println("num_of_fields: " + num_of_fields);
		System.out.println("num_of_Document ID's: " + num_of_document_ids);

		// Node node = nodeBuilder().node();
		// Client client = node.client();

		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "elasticsearch").build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						"localhost", 9300));

		BulkRequestBuilder bulkRequest = client.prepareBulk();
		long count = 1;

		// Using Map to load the fields and random binary data into the es
		// database.
		Map<String, Object> jsonData = new HashMap<String, Object>();
		for (long i = 0; i < num_of_fields; i++) {
			jsonData.put("field" + (i + 1), rnd.nextInt(0, 1));
			// System.out.println("Field Number:" + (i + 1));
		}

		// Generates more documents in these database based on the Number of
		// document ID's
		for (int i = 0; i < num_of_document_ids; i++) {
			bulkRequest.add(client.prepareIndex("user_1", "profile_1",
					Integer.toString(i + 1)).setSource(jsonData));
		}

		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			System.out.println(bulkResponse.buildFailureMessage());
		}

		System.out.println("Waiting for client to finish: 5000 ms");
		Thread.sleep(4000);

		GetMappingsResponse res;
		try {
			res = client.admin().indices()
					.getMappings(new GetMappingsRequest().indices("user_1"))
					.get();

			// // Field Counts
			// int field_count = 1;
			// for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
			// System.out.println(c.key + " = " + c.value.source());
			// field_count++;
			// }
			// System.out.println("Total Field count:" + field_count);

		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Closing client");
		client.close();

	}
}
