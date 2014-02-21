package at.molindo.elasticsync.jest.internal;

import at.molindo.elasticsync.api.ElasticsearchIndex;
import at.molindo.elasticsync.api.ElasticsearchIndexFactory;
import at.molindo.elasticsync.api.IdAndVersionFactory;
import at.molindo.elasticsync.api.Index;
import at.molindo.elasticsync.jest.internal.version.IVersionHelper;

public class ElasticsearchJestIndexFactory implements ElasticsearchIndexFactory {

	@Override
	public ElasticsearchIndex createElasticsearchIndex(Index index, String query, IdAndVersionFactory factory) {

		if (!isVersionSupported(index.getVersion())) {
			throw new IllegalArgumentException("factory not suited for version " + index.getVersion());
		}

		return new ElasticsearchJestIndex(ClientFactory.newJestClient(index.getHosts()), index.getIndexName(), query,
				factory, getVersionHelper(index.getVersion()));
	}

	private IVersionHelper getVersionHelper(String version) {
		// TODO support differences in versions
		return IVersionHelper.DEFAULT;
	}

	@Override
	public String getVersion() {
		return "jest";
	}

	@Override
	public boolean isVersionSupported(String version) {
		return true;
	}

}
