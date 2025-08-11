package edu.usfca.cs272;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs272.utils.WorkQueue;

/**
 * Class responsible for running this project based on the provided command-line
 * arguments. See the README for details.
 *
 * @author Aarav Gupta
 * @author CS 272 Software Development (University of San Francisco)
 * @version Spring 2025
 */
public class Driver {

	/**
	 * Logger to log
	 */
	private static final Logger log = LogManager.getLogger(Driver.class);

	/**
	 * Initializes the classes necessary based on the provided command-line
	 * arguments. This includes (but is not limited to) how to build or search an
	 * inverted index.
	 *
	 * @param args flag/value pairs used to start this program
	 */
	public static void main(String[] args) {
		// store initial start time
		Instant start = Instant.now();

		// Fill in and modify as needed
		System.out.println("Working Directory: " + Path.of(".").toAbsolutePath().normalize());
		System.out.println("Arguments: " + Arrays.toString(args));

		ArgumentParser parser = new ArgumentParser(args);

		InvertedIndex index;
		QueryProcessorInterface processor;

		ThreadSafeInvertedIndex safe = null;
		WorkQueue queue = null;

		if (parser.hasFlag("-threads") || parser.hasFlag("-html") || parser.hasFlag("-server")) {
			int threads = parser.getInteger("-threads", 5);
			if (threads < 1) {
				threads = 5;
			}
			log.info("Multithreading is being used.");
			queue = new WorkQueue(threads);
			safe = new ThreadSafeInvertedIndex();
			index = safe;
			processor = new MultiThreadingQueryProcessor(safe, queue);
		} else {
			log.info("No multithreading; using single-threaded mode.");
			index = new InvertedIndex();
			processor = new QueryProcessor(index);
		}

		if (parser.hasFlag("-text")) {
			Path inputPath = parser.getPath("-text");
			if (inputPath != null) {
				try {
					if (safe != null && queue != null) {
						log.info("Building index with multithreading...");
						MultiThreadingIndexBuilder.build(inputPath, safe, queue);
						log.info("Index built. Number of words: {}", safe.numWords());
					} else {
						log.info("Building index without multithreading...");
						InvertedIndexBuilder.buildPath(inputPath, index);
						log.info("Index built. Number of words: {}", index.numWords());
					}
				} catch (IOException e) {
					System.err.println("Error processing input files: " + e.getMessage());
				}
			} else {
				System.out.println("The path given is invalid to process the files.");
			}
		}

		if (parser.hasFlag("-html")) {
			String seed = parser.getString("-html");
			int total = parser.getInteger("-crawl", 1);

			if (seed == null || seed.isBlank()) {
				System.err.println("There was an error: No seed URL provided after -html flag.");
			} else if (safe != null && queue != null) {
				WebCrawler crawler = new WebCrawler(safe, queue, total);
				crawler.crawl(seed);
				queue.finish();
			} else {
				System.err.println("There was an error: Internal issue initializing WebCrawler.");
			}
		}

		boolean partial = parser.hasFlag("-partial");

		if (parser.hasFlag("-query")) {
			Path queryPath = parser.getPath("-query");

			if (queryPath != null) {
				try {
					processor.processQueryFile(queryPath, partial);
				} catch (IOException e) {
					System.err.println("There was an error whilst processing queries: " + e.getMessage());
				}
			} else {
				System.out.println("An invalid path for query file was found: " + queryPath);
			}
		}

		if (parser.hasFlag("-server")) {
			int port = parser.getInteger("-server", 8080);
			try {
				log.info("Launching server on port {}...", port);
				EngineServer.launch(port, safe, queue);
			} catch (Exception e) {
				System.out.println("Failed to start server: " + e.getMessage());
			}
		}
		if (parser.hasFlag("-counts")) {
			Path pathCounts = parser.getPath("-counts", Path.of("counts.json"));
			try {
				JsonWriter.writeObject(index.getCounts(), pathCounts);
			} catch (IOException e) {
				System.err
						.println("There was an error whilst writing the Count of Words to the file: " + e.getMessage());
			}
		}

		if (parser.hasFlag("-index")) {
			Path pathInvertedIndex = parser.getPath("-index", Path.of("index.json"));
			try {
				index.toJson(pathInvertedIndex);
			} catch (IOException e) {
				System.err
						.println("There was an error whist writing the InvertedIndexes to the file: " + e.getMessage());
			}
		}

		if (parser.hasFlag("-results")) {
			Path resultsPath = parser.getPath("-results", Path.of("results.json"));
			try {
				processor.writeResults(partial, resultsPath);
			} catch (IOException e) {
				System.err.println("There was an error whilst printing out to results file: " + e.getMessage());
			}
		}

		if (queue != null) {
			queue.shutdown();
			queue.join();
		}

		// calculate time elapsed and output
		long elapsed = Duration.between(start, Instant.now()).toMillis();
		double seconds = (double) elapsed / Duration.ofSeconds(1).toMillis();
		System.out.printf("Elapsed: %f seconds%n", seconds);
	}

	/** Prevent instantiating this class of static methods. */
	private Driver() {
	}
}