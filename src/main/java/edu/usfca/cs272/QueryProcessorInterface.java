package edu.usfca.cs272;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.usfca.cs272.InvertedIndex.QueryResult;

/**
 * Interface for processing queries and retrieving search results.
 * 
 * @author Aarav
 */
public interface QueryProcessorInterface {

	/**
	 * Processes a query file line by line and adds to results.
	 *
	 * @param queryPath the path to the query file
	 * @param partial   true for partial search, false for exact search
	 * @throws IOException if an error occurs reading the file
	 */
	default void processQueryFile(Path queryPath, boolean partial) throws IOException {
		try (BufferedReader reader = Files.newBufferedReader(queryPath)) {
			String line;
			while ((line = reader.readLine()) != null) {
				processQueryLine(line, partial);
			}
		}
	}

	/**
	 * Processes a single line from the query file.
	 *
	 * @param line    the query line
	 * @param partial true for partial search, false for exact search
	 */
	void processQueryLine(String line, boolean partial);

	/**
	 * Returns the set of queries for the specified search type.
	 *
	 * @param partial true for partial search, false for exact search
	 * @return the set of queries (unmodifiable), or empty if none
	 */
	Set<String> getQueries(boolean partial);

	/**
	 * Returns the unmodifiable result list for a given normalized query.
	 *
	 * @param query   the query string (must be normalized)
	 * @param partial true for partial search, false for exact search
	 * @return unmodifiable list of results or empty list if not found
	 */
	List<QueryResult> getQueryResults(String query, boolean partial);

	/**
	 * Returns the set of search types present (true for partial, false for exact).
	 *
	 * @return the set of search types (unmodifiable)
	 */
	Set<Boolean> getSearchTypes();

	/**
	 * Returns the number of distinct queries processed for search type.
	 * 
	 * @param partial true for partial search, false for exact search
	 *
	 * @return number of queries
	 */
	default int numQueries(boolean partial) {
		return getQueries(partial).size();
	};

	/**
	 * Returns the number of results for a given query and search type.
	 *
	 * @param query   the query string (must be normalized)
	 * @param partial true for partial search, false for exact search
	 * @return getQueryResults(query, partial).size();
	 */
	default int numResults(String query, boolean partial) {
		return getQueryResults(query, partial).size();
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
	void writeResults(boolean partial, Path resultsPath) throws IOException;

	/**
	 * Writes a single QueryResult object as a JSON object.
	 *
	 * @param result the QueryResult to write
	 * @param writer the writer to output the JSON
	 * @param indent the indent level for pretty formatting
	 * @throws IOException if an IO error occurs writing to the writer
	 */
	static void writeResults(QueryResult result, Writer writer, int indent) throws IOException {
		JsonWriter.writeIndent(writer, indent);
		writer.write("{\n");
		JsonWriter.writeQuote("count", writer, indent + 1);
		writer.write(": " + result.getMatches() + ",\n");
		JsonWriter.writeQuote("score", writer, indent + 1);
		writer.write(": " + String.format("%.8f", result.getScore()) + ",\n");
		JsonWriter.writeQuote("where", writer, indent + 1);
		writer.write(": ");
		JsonWriter.writeQuote(result.getFile(), writer, 0);
		writer.write("\n");
		JsonWriter.writeIndent("}", writer, indent);
	}

	/**
	 * Writes a list of QueryResult objects as a JSON array.
	 *
	 * @param results the list of QueryResult objects to write
	 * @param writer  the writer to output the JSON
	 * @param indent  the indent level for pretty formatting
	 * @throws IOException if an IO error occurs writing to the writer
	 */
	static void writeResults(List<QueryResult> results, Writer writer, int indent) throws IOException {
		writer.write("[");
		var iterator = results.iterator();
		if (iterator.hasNext()) {
			writer.write("\n");
			writeResults(iterator.next(), writer, indent + 1);
			while (iterator.hasNext()) {
				writer.write(",\n");
				writeResults(iterator.next(), writer, indent + 1);
			}
		}
		writer.write("\n");
		JsonWriter.writeIndent("]", writer, indent);
	}

	/**
	 * Writes a map from the corresponding query to lists of QueryResult objects as
	 * a JSON object.
	 *
	 * @param resultMap the map from query strings to corresponding collections of
	 *                  QueryResult objects
	 * @param writer    the writer to output the JSON
	 * @param indent    the indent level for pretty formatting
	 * @throws IOException if an IO error occurs writing to the writer
	 */
	static void writeResults(Map<String, List<QueryResult>> resultMap, Writer writer, int indent) throws IOException {
		writer.write("{");
		var sortedEntries = resultMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).iterator();
		if (sortedEntries.hasNext()) {
			writer.write("\n");
			var entry = sortedEntries.next();
			JsonWriter.writeQuote(entry.getKey(), writer, indent + 1);
			writer.write(": ");
			writeResults(entry.getValue(), writer, indent + 1);
			while (sortedEntries.hasNext()) {
				writer.write(",\n");
				entry = sortedEntries.next();
				JsonWriter.writeQuote(entry.getKey(), writer, indent + 1);
				writer.write(": ");
				writeResults(entry.getValue(), writer, indent + 1);
			}
		}
		writer.write("\n");
		JsonWriter.writeIndent("}", writer, indent);
	}
}
