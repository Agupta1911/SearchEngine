package edu.usfca.cs272;

import static java.nio.charset.StandardCharsets.UTF_8;
import static opennlp.tools.stemmer.snowball.SnowballStemmer.ALGORITHM.ENGLISH;

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
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

/**
 * Handles query processing and searching. Cleans and stems query words, removes
 * duplicates, and performs exact search.
 * 
 * @author Aarav Gupta
 */
public class QueryProcessor implements QueryProcessorInterface {
	/**
	 * Stores search results with normalized queries as keys, with a corresponding
	 * boolean value differentiating between exact and partial.
	 */
	private final Map<Boolean, Map<String, List<QueryResult>>> results;

	/** The inverted index to use. */
	private final InvertedIndex index;

	/** The stemmer to use. */
	private final Stemmer stemmer;

	/**
	 * Initializes the processor with the given index.
	 *
	 * @param index the inverted index to use.
	 */
	public QueryProcessor(InvertedIndex index) {
		this.index = index;
		this.results = new TreeMap<>();
		this.results.put(true, new TreeMap<>());
		this.results.put(false, new TreeMap<>());
		this.stemmer = new SnowballStemmer(ENGLISH);
	}

	/** Processes one line for interface implementation */
	@Override
	public void processQueryLine(String line, boolean partial) {
		TreeSet<String> uniqueStems = FileStemmer.uniqueStems(line, stemmer);

		if (uniqueStems.isEmpty()) {
			return;
		}

		String queryKey = String.join(" ", uniqueStems);

		Map<String, List<QueryResult>> typeResults = results.get(partial);

		if (typeResults.containsKey(queryKey)) {
			return;
		}

		List<QueryResult> searchResults = index.search(uniqueStems, partial);
		typeResults.put(queryKey, searchResults);
	}

	/**
	 * Returns the set of queries for the specified search type.
	 *
	 * @param partial true for partial search, false for exact search
	 * @return the set of queries (unmodifiable), or empty if none
	 */
	@Override
	public Set<String> getQueries(boolean partial) {
		Map<String, List<QueryResult>> searchTypeResults = results.get(partial);
		return searchTypeResults == null ? Set.of() : Collections.unmodifiableSet(searchTypeResults.keySet());
	}

	/**
	 * Returns the unmodifiable result list for a given normalized query.
	 *
	 * @param query   the query string (must be normalized)
	 * @param partial true for partial search, false for exact search
	 * @return unmodifiable list of results or empty list if not found
	 */
	@Override
	public List<QueryResult> getQueryResults(String query, boolean partial) {
		TreeSet<String> uniqueStems = FileStemmer.uniqueStems(query, stemmer);
		String queryKey = String.join(" ", uniqueStems);
		Map<String, List<QueryResult>> typeResults = results.get(partial);
		List<QueryResult> list = typeResults.get(queryKey);
		return list == null ? List.of() : Collections.unmodifiableList(list);
	}

	/**
	 * Returns the set of search types present (true for partial, false for exact).
	 *
	 * @return the set of search types (unmodifiable)
	 */
	@Override
	public Set<Boolean> getSearchTypes() {
		return Collections.unmodifiableSet(results.keySet());
	}

	/**
	 * Returns a string representation of all search results.
	 *
	 * @return formatted string of all results
	 */
	@Override
	public String toString() {
		return results.toString();
	}

	/**
	 * Writes the query results for a specific search type (exact or partial) to the
	 * specified JSON file.
	 *
	 * @param partial     true if partial search results should be written, false
	 *                    for exact search
	 * @param resultsPath the path to the JSON file where the results should be
	 *                    written
	 * @throws IOException if an IO error occurs writing the file
	 */
	@Override
	public void writeResults(boolean partial, Path resultsPath) throws IOException {
		Map<String, List<QueryResult>> typeResults = results.get(partial);
		try (BufferedWriter writer = Files.newBufferedWriter(resultsPath, UTF_8)) {
			QueryProcessorInterface.writeResults(typeResults, writer, 0);
		}
	}

}