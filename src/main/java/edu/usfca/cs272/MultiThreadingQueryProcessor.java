package edu.usfca.cs272;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import edu.usfca.cs272.InvertedIndex.QueryResult;
import edu.usfca.cs272.utils.WorkQueue;

/**
 * A multithreaded extension of the {@link QueryProcessor} class that uses a
 * {@link WorkQueue} to concurrently process query lines from an input file.
 * 
 * @see QueryProcessor
 * @see WorkQueue
 * 
 * @author Aarav
 */
public class MultiThreadingQueryProcessor implements QueryProcessorInterface {

	/**
	 * Stores search results with normalized queries as keys, with a corresponding
	 * boolean value differentiating between exact and partial.
	 */
	private final Map<Boolean, Map<String, List<QueryResult>>> results;

	/** The inverted index to use. */
	private final ThreadSafeInvertedIndex index;
	/** The work queue used */
	private final WorkQueue queue;

	/**
	 * Initializes a multithreaded query processor with the given thread-safe index
	 * and number of threads.
	 *
	 * @param index the thread-safe inverted index to search
	 * @param queue the work queue
	 */
	public MultiThreadingQueryProcessor(ThreadSafeInvertedIndex index, WorkQueue queue) {
		this.index = index;
		this.queue = queue;
		this.results = new TreeMap<>();
		this.results.put(true, new TreeMap<>());
		this.results.put(false, new TreeMap<>());
	}

	@Override
	public void processQueryFile(Path queryPath, boolean partial) throws IOException {
		QueryProcessorInterface.super.processQueryFile(queryPath, partial);
		queue.finish();
	}

	@Override
	public void processQueryLine(String line, boolean partial) {
		queue.execute(() -> {
			TreeSet<String> uniqueStems = FileStemmer.uniqueStems(line);
			if (uniqueStems.isEmpty()) {
				return;
			}

			String queryKey = String.join(" ", uniqueStems);

			Map<String, List<QueryResult>> typeResults;

			synchronized (results) {
				typeResults = results.get(partial);

				if (typeResults.containsKey(queryKey)) {
					return;
				}
			}

			List<QueryResult> searchResults = index.search(uniqueStems, partial);

			synchronized (results) {
				typeResults.put(queryKey, searchResults);
			}
		});
	}

	@Override
	public Set<String> getQueries(boolean partial) {
		synchronized (results) {
			Map<String, List<QueryResult>> searchTypeResults = results.get(partial);
			return searchTypeResults == null ? Set.of() : Collections.unmodifiableSet(searchTypeResults.keySet());
		}
	}

	@Override
	public List<QueryResult> getQueryResults(String query, boolean partial) {
		TreeSet<String> uniqueStems = FileStemmer.uniqueStems(query);
		String queryKey = String.join(" ", uniqueStems);
		synchronized (results) {
			Map<String, List<QueryResult>> typeResults = results.get(partial);
			List<QueryResult> list = typeResults.get(queryKey);
			return list == null ? List.of() : Collections.unmodifiableList(list);
		}
	}

	@Override
	public Set<Boolean> getSearchTypes() {
		synchronized (results) {
			return Collections.unmodifiableSet(results.keySet());
		}
	}

	@Override
	public String toString() {
		synchronized (results) {
			return results.toString();
		}
	}

	@Override
	public void writeResults(boolean partial, Path resultsPath) throws IOException {
		Map<String, List<QueryResult>> typeResults;
		synchronized (results) {
			typeResults = results.get(partial);
			try (BufferedWriter writer = Files.newBufferedWriter(resultsPath, UTF_8)) {
				QueryProcessorInterface.writeResults(typeResults, writer, 0);
			}
		}
	}

	@Override
	public int numQueries(boolean partial) {
		synchronized (results) {
			return QueryProcessorInterface.super.numQueries(partial);
		}
	}

	@Override
	public int numResults(String query, boolean partial) {
		synchronized (results) {
			return QueryProcessorInterface.super.numResults(query, partial);
		}
	}

	/**
	 * Returns the WorkQueue instance
	 * 
	 * @return this.queue the current work queue
	 */
	public WorkQueue getQueue() {
		return this.queue;
	}
}