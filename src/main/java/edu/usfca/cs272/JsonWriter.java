package edu.usfca.cs272;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

/**
 * Outputs several simple data structures in "pretty" JSON format where newlines
 * are used to separate elements and nested elements are indented using spaces.
 *
 * Warning: This class is not thread-safe. If multiple threads access this class
 * concurrently, access must be synchronized externally.
 *
 * @author CS 272 Software Development (University of San Francisco)
 * @version Spring 2025
 */
public class JsonWriter {
	/**
	 * Indents the writer by the specified number of times. Does nothing if the
	 * indentation level is 0 or less.
	 *
	 * @param writer the writer to use
	 * @param indent the number of times to indent
	 * @throws IOException if an IO error occurs
	 */
	public static void writeIndent(Writer writer, int indent) throws IOException {
		while (indent-- > 0) {
			writer.write("  ");
		}
	}

	/**
	 * Indents and then writes the String element.
	 *
	 * @param element the element to write
	 * @param writer  the writer to use
	 * @param indent  the number of times to indent
	 * @throws IOException if an IO error occurs
	 */
	public static void writeIndent(String element, Writer writer, int indent) throws IOException {
		writeIndent(writer, indent);
		writer.write(element);
	}

	/**
	 * Indents and then writes the text element surrounded by {@code " "} quotation
	 * marks.
	 *
	 * @param element the element to write
	 * @param writer  the writer to use
	 * @param indent  the number of times to indent
	 * @throws IOException if an IO error occurs
	 */
	public static void writeQuote(String element, Writer writer, int indent) throws IOException {
		writeIndent(writer, indent);
		writer.write('"');
		writer.write(element);
		writer.write('"');
	}

	/**
	 * Writes the elements as a pretty JSON array.
	 *
	 * @param elements the elements to write
	 * @param writer   the writer to use
	 * @param indent   the initial indent level; the first bracket is not indented,
	 *                 inner elements are indented by one, and the last bracket is
	 *                 indented at the initial indentation level
	 * @throws IOException if an IO error occurs
	 *
	 * @see Writer#write(String)
	 * @see #writeIndent(Writer, int)
	 * @see #writeIndent(String, Writer, int)
	 */
	public static void writeArray(Collection<? extends Number> elements, Writer writer, int indent) throws IOException {
		writer.write("[");
		var iterator = elements.iterator();
		if (iterator.hasNext()) {
			writer.write("\n");
			writeIndent(iterator.next().toString(), writer, indent + 1);
			while (iterator.hasNext()) {
				writer.write(",\n");
				writeIndent(iterator.next().toString(), writer, indent + 1);
			}
		}
		writer.write("\n");
		writeIndent("]", writer, indent);
	}

	/**
	 * Writes the elements as a pretty JSON array to file.
	 *
	 * @param elements the elements to write
	 * @param path     the file path to use
	 * @throws IOException if an IO error occurs
	 *
	 * @see Files#newBufferedReader(Path, java.nio.charset.Charset)
	 * @see StandardCharsets#UTF_8
	 * @see #writeArray(Collection, Writer, int)
	 */
	public static void writeArray(Collection<? extends Number> elements, Path path) throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(path, UTF_8)) {
			writeArray(elements, writer, 0);
		}
	}

	/**
	 * Returns the elements as a pretty JSON array.
	 *
	 * @param elements the elements to use
	 * @return a {@link String} containing the elements in pretty JSON format
	 *
	 * @see StringWriter
	 * @see #writeArray(Collection, Writer, int)
	 */
	public static String writeArray(Collection<? extends Number> elements) {
		try {
			StringWriter writer = new StringWriter();
			writeArray(elements, writer, 0);
			return writer.toString();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Writes the elements as a pretty JSON object.
	 *
	 * @param elements the elements to write
	 * @param writer   the writer to use
	 * @param indent   the initial indent level; the first bracket is not indented,
	 *                 inner elements are indented by one, and the last bracket is
	 *                 indented at the initial indentation level
	 * @throws IOException if an IO error occurs
	 *
	 * @see Writer#write(String)
	 * @see #writeIndent(Writer, int)
	 * @see #writeIndent(String, Writer, int)
	 */
	public static void writeObject(Map<String, ? extends Number> elements, Writer writer, int indent)
			throws IOException {
		var iterator = elements.entrySet().iterator();
		writer.write("{");
		if (iterator.hasNext()) {
			writer.write("\n");
			// Writes the key value pairs
			var pair = iterator.next();
			writeQuote(pair.getKey(), writer, indent + 1);
			writer.write(": " + pair.getValue());
			while (iterator.hasNext()) {
				writer.write(",\n");
				pair = iterator.next();
				writeQuote(pair.getKey(), writer, indent + 1);
				writer.write(": " + pair.getValue());
			}
		}
		writer.write("\n");
		writeIndent("}", writer, indent);
	}

	/**
	 * Writes the elements as a pretty JSON object to file.
	 *
	 * @param elements the elements to write
	 * @param path     the file path to use
	 * @throws IOException if an IO error occurs
	 *
	 * @see Files#newBufferedReader(Path, java.nio.charset.Charset)
	 * @see StandardCharsets#UTF_8
	 * @see #writeObject(Map, Writer, int)
	 */
	public static void writeObject(Map<String, ? extends Number> elements, Path path) throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(path, UTF_8)) {
			writeObject(elements, writer, 0);
		}
	}

	/**
	 * Returns the elements as a pretty JSON object.
	 *
	 * @param elements the elements to use
	 * @return a {@link String} containing the elements in pretty JSON format
	 *
	 * @see StringWriter
	 * @see #writeObject(Map, Writer, int)
	 */
	public static String writeObject(Map<String, ? extends Number> elements) {
		try {
			StringWriter writer = new StringWriter();
			writeObject(elements, writer, 0);
			return writer.toString();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Writes the elements as a pretty JSON object with nested arrays. The generic
	 * notation used allows this method to be used for any type of map with any type
	 * of nested collection of number objects.
	 *
	 * @param elements the elements to write
	 * @param writer   the writer to use
	 * @param indent   the initial indent level; the first bracket is not indented,
	 *                 inner elements are indented by one, and the last bracket is
	 *                 indented at the initial indentation level
	 * @throws IOException if an IO error occurs
	 *
	 * @see Writer#write(String)
	 * @see #writeIndent(Writer, int)
	 * @see #writeIndent(String, Writer, int)
	 * @see #writeArray(Collection)
	 */
	public static void writeObjectArrays(Map<String, ? extends Collection<? extends Number>> elements, Writer writer,
			int indent) throws IOException {
		var iterator = elements.entrySet().iterator();
		writer.write("{");
		if (iterator.hasNext()) {
			writer.write("\n");
			var pair = iterator.next();
			writeQuote(pair.getKey(), writer, indent + 1);
			writer.write(": ");
			writeArray(pair.getValue(), writer, indent + 1);
			while (iterator.hasNext()) {
				writer.write(",\n");
				pair = iterator.next();
				writeQuote(pair.getKey(), writer, indent + 1);
				writer.write(": ");
				writeArray(pair.getValue(), writer, indent + 1);
			}
		}
		writer.write("\n");
		writeIndent("}", writer, indent);
	}

	/**
	 * Writes the elements as a pretty JSON object with nested arrays to file.
	 *
	 * @param elements the elements to write
	 * @param path     the file path to use
	 * @throws IOException if an IO error occurs
	 *
	 * @see Files#newBufferedReader(Path, java.nio.charset.Charset)
	 * @see StandardCharsets#UTF_8
	 * @see #writeObjectArrays(Map, Writer, int)
	 */
	public static void writeObjectArrays(Map<String, ? extends Collection<? extends Number>> elements, Path path)
			throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(path, UTF_8)) {
			writeObjectArrays(elements, writer, 0);
		}
	}

	/**
	 * Returns the elements as a pretty JSON object with nested arrays.
	 *
	 * @param elements the elements to use
	 * @return a {@link String} containing the elements in pretty JSON format
	 *
	 * @see StringWriter
	 * @see #writeObjectArrays(Map, Writer, int)
	 */
	public static String writeObjectArrays(Map<String, ? extends Collection<? extends Number>> elements) {
		try {
			StringWriter writer = new StringWriter();
			writeObjectArrays(elements, writer, 0);
			return writer.toString();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Writes the elements as a pretty JSON array with nested objects. The generic
	 * notation used allows this method to be used for any type of collection with
	 * any type of nested map of String keys to number objects.
	 *
	 * @param elements the elements to write
	 * @param writer   the writer to use
	 * @param indent   the initial indent level; the first bracket is not indented,
	 *                 inner elements are indented by one, and the last bracket is
	 *                 indented at the initial indentation level
	 * @throws IOException if an IO error occurs
	 *
	 * @see Writer#write(String)
	 * @see #writeIndent(Writer, int)
	 * @see #writeIndent(String, Writer, int)
	 * @see #writeObject(Map)
	 */
	public static void writeArrayObjects(Collection<? extends Map<String, ? extends Number>> elements, Writer writer,
			int indent) throws IOException {
		var iterator = elements.iterator();
		writer.write("[");
		if (iterator.hasNext()) {
			writer.write("\n");
			writeIndent(writer, indent + 1);
			writeObject(iterator.next(), writer, indent + 1);
			while (iterator.hasNext()) {
				writer.write(",\n");
				writeIndent(writer, indent + 1); // The correct indent for each object
				// Write each object in the array of objects
				writeObject(iterator.next(), writer, indent + 1);
			}
		}
		writer.write("\n");
		writeIndent("]", writer, indent);
	}

	/**
	 * Writes the elements as a pretty JSON array with nested objects to file.
	 *
	 * @param elements the elements to write
	 * @param path     the file path to use
	 * @throws IOException if an IO error occurs
	 *
	 * @see Files#newBufferedReader(Path, java.nio.charset.Charset)
	 * @see StandardCharsets#UTF_8
	 * @see #writeArrayObjects(Collection)
	 */
	public static void writeArrayObjects(Collection<? extends Map<String, ? extends Number>> elements, Path path)
			throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(path, UTF_8)) {
			writeArrayObjects(elements, writer, 0);
		}
	}

	/**
	 * Returns the elements as a pretty JSON array with nested objects.
	 *
	 * @param elements the elements to use
	 * @return a {@link String} containing the elements in pretty JSON format
	 *
	 * @see StringWriter
	 * @see #writeArrayObjects(Collection)
	 */
	public static String writeArrayObjects(Collection<? extends Map<String, ? extends Number>> elements) {
		try {
			StringWriter writer = new StringWriter();
			writeArrayObjects(elements, writer, 0);
			return writer.toString();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Writes the Indexes as a pretty JSON array with nested objects to file.
	 *
	 * @param index  the elements to write
	 * @param writer the writer to use
	 * @param indent the indent to use
	 * @throws IOException if an IO error occurs
	 *
	 * @see Files#newBufferedReader(Path, java.nio.charset.Charset)
	 * @see StandardCharsets#UTF_8
	 * @see #writeArrayObjects(Collection)
	 */
	public static void writeInvertedIndexes(
			Map<String, ? extends Map<String, ? extends Collection<? extends Number>>> index, Writer writer, int indent)
			throws IOException {
		var iterator = index.entrySet().iterator();
		writer.write("{");
		if (iterator.hasNext()) {
			writer.write("\n");
			var value = iterator.next();
			writeQuote(value.getKey(), writer, indent + 1);
			writer.write(": ");
			writeObjectArrays(value.getValue(), writer, indent + 1);
			while (iterator.hasNext()) {
				writer.write(",\n");
				value = iterator.next();
				writeQuote(value.getKey(), writer, indent + 1);
				writer.write(": ");
				writeObjectArrays(value.getValue(), writer, indent + 1);
			}
		}
		writer.write("\n");
		writeIndent("}", writer, indent);
	}

	/**
	 * Writes the Indexes as a pretty JSON object with nested objects to file.
	 *
	 * @param index the elements to write
	 * @param path  the file path to use
	 * @throws IOException if an IO error occurs
	 */
	public static void writeInvertedIndexes(
			Map<String, ? extends Map<String, ? extends Collection<? extends Number>>> index, Path path)
			throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(path, UTF_8)) {
			writeInvertedIndexes(index, writer, 0);
		}
	}

	/**
	 * Returns the Indexes as a pretty JSON object with nested objects.
	 *
	 * @param index the elements to write
	 * @return a {@link String} containing the elements in pretty JSON format
	 */
	public static String writeInvertedIndex(
			Map<String, ? extends Map<String, ? extends Collection<? extends Number>>> index) {
		try (StringWriter writer = new StringWriter()) {
			writeInvertedIndexes(index, writer, 0);
			return writer.toString();
		} catch (IOException e) {
			return null;
		}
	}

	/** Prevent instantiating this class of static methods. */
	private JsonWriter() {
	}
}
