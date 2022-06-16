package com.example.solrimportexport;

import com.example.solrimportexport.config.CommandLineConfig;
import com.example.solrimportexport.config.ConfigFactory;
import com.example.solrimportexport.config.SolrField;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.cli.ParseException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SpringBootApplication
public class SolrImportExportApplication {
	private static Logger logger = LoggerFactory.getLogger(SolrImportExportApplication.class);
	private static CommandLineConfig config = null;
	private static ObjectMapper objectMapper = new ObjectMapper();
	private static long counter;
	private static long skipCount;
	private static Integer commitAfter;
	private static long lastCommit = 0;

	private static Set<SolrField> includeFieldsEquals;
	private static Set<SolrField> skipFieldsEquals;
	private static Set<SolrField> skipFieldsStartWith;
	private static Set<SolrField> skipFieldsEndWith;

	public static long incrementCounter(long counter) {
		SolrImportExportApplication.counter += counter;
		return SolrImportExportApplication.counter;
	}

	public static void main(String[] args) throws ParseException, IOException {
		SpringApplication.run(SolrImportExportApplication.class, args);
		config = ConfigFactory.getConfigFromArgs(args);

		includeFieldsEquals = config.getIncludeFieldSet()
				.stream()
				.filter(s -> s.getMatch() == SolrField.MatchType.EQUAL)
				.collect(Collectors.toSet());

		skipFieldsEquals = config.getSkipFieldSet()
				.stream()
				.filter(s -> s.getMatch() == SolrField.MatchType.EQUAL)
				.collect(Collectors.toSet());
		skipFieldsStartWith = config.getSkipFieldSet()
				.stream()
				.filter(s -> s.getMatch() == SolrField.MatchType.STARTS_WITH)
				.collect(Collectors.toSet());
		skipFieldsEndWith = config.getSkipFieldSet()
				.stream()
				.filter(s -> s.getMatch() == SolrField.MatchType.ENDS_WITH)
				.collect(Collectors.toSet());
		skipCount = config.getSkipCount();
		commitAfter = config.getCommitAfter();

		logger.info("Found solr.indexer.config: " + config);

		if (config.getUniqueKey() == null) {
			readUniqueKeyFromSolrSchema();
		}

		HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

		if (config.hasCredentials()) {
			CredentialsProvider provider = new BasicCredentialsProvider();
			UsernamePasswordCredentials credentials
					= new UsernamePasswordCredentials(config.getUser(), config.getPassword());
			provider.setCredentials(AuthScope.ANY, credentials);
			httpClientBuilder = httpClientBuilder.addInterceptorFirst(new PreemptiveAuthInterceptor())
					.setDefaultCredentialsProvider(provider);
		}

		HttpClient httpClient = httpClientBuilder.build();

		try (HttpSolrClient client = new HttpSolrClient.Builder().withBaseSolrUrl(config.getSolrUrl())
				.withHttpClient(httpClient)
				.build()) {

			try {
				switch (config.getActionType()) {
					case EXPORT:
					case BACKUP:

						readAllDocuments(client, new File(config.getFileName()));
						break;

					case RESTORE:
					case IMPORT:

						writeAllDocuments(client, new File(config.getFileName()));
						break;

					default:
						throw new RuntimeException("unsupported sitemap type");
				}

				logger.info("Build complete.");

			} catch (SolrServerException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private static void readUniqueKeyFromSolrSchema() throws IOException, JsonParseException, JsonMappingException, MalformedURLException {
		String sUrl = config.getSolrUrl() + "/schema/uniquekey?wt=json";
		Map<String, Object> uniqueKey = null;
		try {
			uniqueKey = objectMapper.readValue(readUrl(sUrl), new TypeReference<Map<String, Object>>() {});
			if (uniqueKey.containsKey("uniqueKey")) {
				config.setUniqueKey((String) uniqueKey.get("uniqueKey"));
			} else {
				config.setUniqueKey("id");
				logger.warn("unable to find valid uniqueKey defaulting to \"id\".");
			}
		} catch (IOException e) {
			config.setUniqueKey("id");
			logger.warn("unable to find valid uniqueKey defaulting to \"id\".");
		}
	}

	/**
	 * @param sUrl
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	private static String readUrl(String sUrl) throws MalformedURLException, IOException {
		StringBuilder sbJson = new StringBuilder();
		URL url = new URL(sUrl);
		String userInfo = url.getUserInfo();
		URLConnection openConnection = url.openConnection();
		if (userInfo != null && !userInfo.isEmpty()) {
			String authStr = Base64.getEncoder()
					.encodeToString(userInfo.getBytes());
			openConnection.setRequestProperty("Authorization", "Basic " + authStr);
		}

		BufferedReader in = new BufferedReader(new InputStreamReader(openConnection.getInputStream()));
		String inputLine;
		while ((inputLine = in.readLine()) != null)
			sbJson.append(inputLine);
		in.close();
		return sbJson.toString();
	}

	/**
	 * @param j
	 * @return
	 */
	private static SolrInputDocument json2SolrInputDocument(String j) {
		SolrInputDocument s = new SolrInputDocument();
		try {
			Map<String, Object> map = objectMapper.readValue(j, new TypeReference<Map<String, Object>>() {});
			for (Map.Entry<String, Object> e : map.entrySet()) {
				if (!e.getKey()
						.equals("_version_"))
					s.addField(e.getKey(), e.getValue());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return s;
	}

	/**
	 * @param client
	 * @param outputFile
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SolrServerException
	 */
	private static void writeAllDocuments(HttpSolrClient client, File outputFile) throws FileNotFoundException, IOException, SolrServerException {
		AtomicInteger counter = new AtomicInteger(10000);
		if (!config.getDryRun() && config.getDeleteAll()) {
			logger.info("delete all!");
			client.deleteByQuery("*:*");
		}
		logger.info("Reading " + config.getFileName());

		try (BufferedReader pw = new BufferedReader(new FileReader(outputFile))) {
			pw.lines()
					.collect(StreamUtils.batchCollector(config.getBlockSize(), l ->
					{
						List<SolrInputDocument> collect = l.stream()
								.map(SolrImportExportApplication::json2SolrInputDocument)
								.map(d ->
								{
									skipFieldsEquals.forEach(f -> d.removeField(f.getText()));
									if (!skipFieldsStartWith.isEmpty()) {
										d.getFieldNames()
												.removeIf(name -> skipFieldsStartWith.stream()
														.anyMatch(skipField -> name
																.startsWith(skipField
																		.getText())));
									}
									if (!skipFieldsEndWith.isEmpty()) {
										d.getFieldNames()
												.removeIf(name -> skipFieldsEndWith.stream()
														.anyMatch(skipField -> name
																.endsWith(skipField
																		.getText())));
									}
									return d;
								})
								.collect(Collectors.toList());
						if (!insertBatch(client, collect)) {
							int retry = 5;
							while (--retry > 0 && !insertBatch(client, collect))// randomly when imported 10M documents, solr failed
								// on Timeout exactly 10 minutes..
								;
						}
					}));
		}

		commit(client);

	}

	private static boolean insertBatch(HttpSolrClient client, List<SolrInputDocument> collect) {
		try {

			if (!config.getDryRun()) {
				logger.info("adding " + collect.size() + " documents (" + incrementCounter(collect.size()) + ")");
				if (counter >= skipCount) {
					client.add(collect);
					if (commitAfter != null && counter - lastCommit > commitAfter) {
						commit(client);
						lastCommit = counter;
					}
				} else {
					logger.info("Skipping as current number of counter :" + counter + " is smaller than skipCount: " + skipCount);
				}
			}
		} catch (SolrServerException | IOException e) {
			logger.error("Problem while saving", e);
			return false;
		}
		return true;
	}

	private static void commit(HttpSolrClient client) throws SolrServerException, IOException {
		if (!config.getDryRun()) {
			client.commit();
			logger.info("Committed");
		}
	}

	/**
	 * @param client
	 * @param outputFile
	 * @throws SolrServerException
	 * @throws IOException
	 */
	private static void readAllDocuments(HttpSolrClient client, File outputFile) throws SolrServerException, IOException {

		SolrQuery solrQuery = new SolrQuery();
		solrQuery.setTimeAllowed(-1);
		solrQuery.setQuery("*:*");
		solrQuery.setFields("*");
		if (config.getFilterQuery() != null) {
			solrQuery.addFilterQuery(config.getFilterQuery());
		}
		if (!includeFieldsEquals.isEmpty()) {
			solrQuery.setFields(includeFieldsEquals.stream()
					.map(f -> f.getText())
					.collect(Collectors.joining(" ")));
		}
		solrQuery.setRows(0);

		solrQuery.addSort(config.getUniqueKey(), SolrQuery.ORDER.asc); // Pay attention to this line

		String cursorMark = CursorMarkParams.CURSOR_MARK_START;

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

		DateFormat df = new SimpleDateFormat(config.getDateTimeFormat());
		objectMapper.setDateFormat(df);
		objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

		QueryRequest req = new QueryRequest(solrQuery);

		QueryResponse r = req.process(client);

		long nDocuments = r.getResults()
				.getNumFound();
		logger.info("Found " + nDocuments + " documents");

		if (!config.getDryRun()) {
			logger.info("Creating " + config.getFileName());

			try (PrintWriter pw = new PrintWriter(outputFile)) {
				solrQuery.setRows(config.getBlockSize());
				boolean done = false;
				boolean disableCursors = config.getDisableCursors();
				if (disableCursors) {
					logger.warn("WARNING: you have disabled Solr Cursors, using standard pagination");
				}
				int page = 0;
				QueryResponse rsp;
				while (!done) {
					if (!disableCursors) {
						solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
					} else {
						solrQuery.setStart(page * config.getBlockSize());
					}
					rsp = client.query(solrQuery);
					String nextCursorMark = rsp.getNextCursorMark();
					if (nextCursorMark == null && !disableCursors) {
						disableCursors = true;
						logger.warn("WARNING: you're dealing with a old version of Solr which does not support cursors, using standard pagination");
					}

					SolrDocumentList results = rsp.getResults();
					for (SolrDocument d : results) {
						skipFieldsEquals.forEach(f -> d.removeFields(f.getText()));
						if (skipFieldsStartWith.size() > 0 || skipFieldsEndWith.size() > 0) {
							Map<String, Object> collect = d.entrySet()
									.stream()
									.filter(e -> !skipFieldsStartWith.stream()
											.filter(f -> e.getKey()
													.startsWith(f
															.getText()))
											.findFirst()
											.isPresent())
									.filter(e -> !skipFieldsEndWith.stream()
											.filter(f -> e.getKey()
													.endsWith(f
															.getText()))
											.findFirst()
											.isPresent())
									.collect(Collectors
											.toMap(e -> e.getKey(), e -> e.getValue()));
							pw.write(objectMapper.writeValueAsString(collect));
						} else {
							pw.write(objectMapper.writeValueAsString(d));
						}
						pw.write("\n");
					}
					if (!disableCursors && cursorMark.equals(nextCursorMark)) {
						done = true;
					} else {
						logger.info("reading " + results.size() + " documents (" + incrementCounter(results
								.size()) + ")");
						done = (results.size() == 0);
						page++;
					}

					cursorMark = nextCursorMark;
				}

			}
		}

	}

}
