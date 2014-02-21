package at.molindo.elasticsync.jest.internal;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.ClientConfig;

import java.util.List;

public final class ClientFactory {

	private ClientFactory() {
	}

	public static JestClient newJestClient(List<String> hosts) {
		// Configuration
		ClientConfig clientConfig = new ClientConfig.Builder(hosts).multiThreaded(true).build();

		// Construct a new Jest client according to configuration via factory
		JestClientFactory factory = new JestClientFactory();
		factory.setClientConfig(clientConfig);
		return factory.getObject();
	}

}
