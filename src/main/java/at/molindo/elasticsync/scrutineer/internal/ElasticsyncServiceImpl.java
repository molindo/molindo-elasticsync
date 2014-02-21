package at.molindo.elasticsync.scrutineer.internal;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.molindo.elasticsync.api.ElasticsearchIndex;
import at.molindo.elasticsync.api.ElasticsearchIndexFactory;
import at.molindo.elasticsync.api.ElasticsyncService;
import at.molindo.elasticsync.api.Index;
import at.molindo.elasticsync.jest.internal.ElasticsearchJestIndexFactory;

import com.google.common.base.Function;

public class ElasticsyncServiceImpl implements ElasticsyncService {

	private static final Logger log = LoggerFactory.getLogger(ElasticsyncServiceImpl.class);

	@Override
	public void verify(Index source, Index target, List<String> types, String query, boolean update) {

		ElasticsearchIndexFactory sourceFactory = getService(source.getVersion());
		ElasticsearchIndexFactory targetFactory = getService(target.getVersion());

		try (ElasticsearchIndex sourceIndex = sourceFactory.createElasticsearchIndex(source, query,
				StringIdAndVersionFactory.INSTANCE);
				ElasticsearchIndex targetIndex = targetFactory.createElasticsearchIndex(target, query,
						StringIdAndVersionFactory.INSTANCE);) {
			log.info("starting verification");
			try (IdAndVersionStreamVerifierListener verifierListener = newVerifierListener(sourceIndex, targetIndex,
					update);) {
				new Elasticsync().verify(sourceIndex, targetIndex, types, verifierListener);
			}
			log.info("verification finished");
		} catch (Exception e) {
			log.error("verification failed", e);
		}
	}

	private ElasticsearchIndexFactory getService(String version) {
		return new ElasticsearchJestIndexFactory();
	}

	private IdAndVersionStreamVerifierListener newVerifierListener(ElasticsearchIndex sourceIndex,
			ElasticsearchIndex targetIndex, boolean update) {
		if (update) {
			return new UpdatingVersionStreamVerifierListener(sourceIndex, targetIndex);
		} else {
			Function<Long, Object> formatter = PrintStreamOutputVersionStreamVerifierListener.DEFAULT_FORMATTER;
			return new PrintStreamOutputVersionStreamVerifierListener(System.err, formatter);
		}
	}

}
