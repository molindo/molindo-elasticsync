package at.molindo.elasticsync.launcher;

import java.io.IOException;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.molindo.elasticsync.api.ElasticsyncService;
import at.molindo.elasticsync.api.Index;
import at.molindo.elasticsync.scrutineer.internal.ElasticsyncServiceImpl;

import com.beust.jcommander.JCommander;

public class Launcher {

	private static final Logger log = LoggerFactory.getLogger(Launcher.class);

	public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {

		// parse arguments
		ElasticsyncCommandLineOptions opts = parseOptions(args);

		// run verifcation
		log.info("environment ready, starting verification");
		try {
			verify(opts);
			System.exit(0);
		} catch (Throwable t) {
			log.error("verification failed", t);
			System.exit(1);
		}
	}

	private static ElasticsyncCommandLineOptions parseOptions(String[] args) {
		ElasticsyncCommandLineOptions options = new ElasticsyncCommandLineOptions();
		JCommander jcmdr = new JCommander(options, args);

		if (options.help) {
			jcmdr.setProgramName(Launcher.class.getName());
			jcmdr.usage();
			System.exit(0);
		}

		return options;
	}

	private static void verify(ElasticsyncCommandLineOptions opts) {

		Index source = new Index(opts.sourceHosts, opts.indexName, opts.sourceVersion);
		Index target = new Index(opts.targetHosts, opts.indexName, opts.targetVersion);

		ElasticsyncService svc = new ElasticsyncServiceImpl();
		svc.verify(source, target, opts.types, opts.query, opts.update);
	}

}
