package edu.usfca.cs272;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;

import edu.usfca.cs272.InvertedIndex.QueryResult;
import edu.usfca.cs272.utils.HtmlFetcher;
import edu.usfca.cs272.utils.WorkQueue;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * A Jetty-based web server that accepts multi-word queries and performs partial
 * search on a multithreaded inverted index.
 * 
 * @author Aarav
 */
public class EngineServer {
	/** The logger to use */
	private static final Logger log = LogManager.getLogger();

	/**
	 * Launches the Jetty server with a servlet that can access the inverted index.
	 *
	 * @param port  the port number to bind the server to
	 * @param index the thread-safe inverted index
	 * @param queue the work queue for background tasks (unused in servlet, but for
	 *              future use)
	 * @throws Exception if Jetty fails to launch
	 */
	public static void launch(int port, ThreadSafeInvertedIndex index, WorkQueue queue) throws Exception {
		log.info("Launching Jetty server on port {}", port);
		Server server = new Server(port);

		ServletContextHandler handler = new ServletContextHandler();
		handler.addServlet(new ServletHolder(new SearchServlet(index)), "/search");
		handler.addServlet(new ServletHolder(new FileServlet()), "/file");

		server.setHandler(handler);
		server.start();
		log.info("Server started on port {}.", port);
		server.join();
	}

	/**
	 * A servlet that handles partial search queries from a web form.
	 */
	public static class SearchServlet extends HttpServlet {
		private static final long serialVersionUID = 202505;

		private final ThreadSafeInvertedIndex index;

		/**
		 * Creates a servlet instance that can perform search queries.
		 *
		 * @param index the thread-safe inverted index to query
		 */
		public SearchServlet(ThreadSafeInvertedIndex index) {
			this.index = index;
		}

		@Override
		protected void doGet(HttpServletRequest request, HttpServletResponse response)
				throws ServletException, IOException {
			String query = request.getParameter("query");
			log.info("Received query: {}", query);

			StringBuilder html = new StringBuilder();
			html.append("""
					<!DOCTYPE html>
					<html lang="en">
					<head>
						<meta charset="UTF-8">
						<title>Search Engine</title>
					</head>
					<body>
						<h1>Partial Search</h1>
						<form method="get" action="/search">
							<input type="text" name="query" size="50" required>
							<button type="submit">Search</button>
						</form>
						<hr>
						<div>
					""");

			if (query != null && !query.isBlank()) {
				Set<String> stems = FileStemmer.uniqueStems(query);
				log.info("Stemmed query words: {}", stems);
				List<QueryResult> results = index.partialSearch(stems);
				log.info("Found {} results", results.size());

				if (results.isEmpty()) {
					html.append("<p>No results found.</p>");
				} else {
					html.append("<ul>");
					for (QueryResult result : results) {
						String encoded = URLEncoder.encode(result.getFile(), StandardCharsets.UTF_8);
						html.append(String.format("<li><a href=\"/file?path=%s\">%s</a> - %.8f (%d matches)</li>",
								encoded, result.getFile(), result.getScore(), result.getMatches()));
					}
					html.append("</ul>");
				}
			}

			html.append("""
						</div>
					</body>
					</html>
					""");

			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			try (PrintWriter out = response.getWriter()) {
				out.print(html);
			}
		}
	}

	/**
	 * A servlet that handles file viewing requests.
	 */
	public static class FileServlet extends HttpServlet {
		private static final long serialVersionUID = 202505;

		@Override
		protected void doGet(HttpServletRequest request, HttpServletResponse response)
				throws ServletException, IOException {

			String path = request.getParameter("path");
			log.info("Received file request: {}", path);

			if (path == null || path.isBlank()) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No file path provided");
				return;
			}

			try {
				if (path.startsWith("http://") || path.startsWith("https://")) {
					String html = HtmlFetcher.fetch(new URI(path), 3);
					if (html == null) {
						response.sendError(HttpServletResponse.SC_BAD_GATEWAY, "Unable to fetch remote content");
						return;
					}
					response.setContentType("text/html");
					response.setStatus(HttpServletResponse.SC_OK);
					try (PrintWriter out = response.getWriter()) {
						out.print(html);
					}
					return;
				}

				Path filePath = Path.of(path);
				if (!Files.exists(filePath)) {
					response.sendError(HttpServletResponse.SC_NOT_FOUND, "File not found: " + path);
					return;
				}

				response.setContentType("text/plain");
				response.setStatus(HttpServletResponse.SC_OK);
				try (PrintWriter out = response.getWriter()) {
					Files.lines(filePath).forEach(out::println);
				}

			} catch (Exception e) {
				log.error("Error processing file request: {}", path, e);
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						"Error processing request: " + e.getMessage());
			}
		}
	}

}
