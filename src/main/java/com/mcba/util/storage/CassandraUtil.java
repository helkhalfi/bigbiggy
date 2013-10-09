package com.mcba.util.storage;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraUtil {

	private static final Logger logger = LoggerFactory.getLogger(CassandraUtil.class);
	private static Cluster cluster;
	private static Session session;
	private static PreparedStatement statementInsertTweets;
	private static PreparedStatement statementInsertGeoCity;
	private static boolean isConnected = false;
	
	public static synchronized void connect(String... nodes) {
		if(isConnected) {
			return;
		}
		Builder builder = null;
		for(String node : nodes) {
			builder = Cluster.builder().addContactPoint(node);
		}
		cluster = builder.build();
		Metadata metadata = cluster.getMetadata();
		logger.info(String.format("Connected to cluster: %s\n",
				metadata.getClusterName()));
		for ( Host host : metadata.getAllHosts() ) {
			logger.info(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack()));
		}
		session = cluster.connect();
		createTwitterSchema();
		isConnected = true;
	}

	private static void ensureOpen() {
		if(cluster == null || session == null) {
			throw new IllegalAccessError("no cluster/session has been opened, please call connect(String... nodes) method");
		}
	}
	
	public static Session getSession() {
		ensureOpen();
		return session;
	}

	public void shutdown() {
		ensureOpen();
		cluster.shutdown();
	}


	public static void createTwitterSchema(){
		ensureOpen();
		String keySpace = "twitter";
		try {
			session.execute("DROP KEYSPACE " + keySpace);
		}catch(Exception ex) {
			logger.info("Cannot delete keyspace [" + keySpace +"], maybe the keyspace doesn't exist");
		}
		session.execute("CREATE KEYSPACE twitter WITH replication " +
				"= {'class':'SimpleStrategy', 'replication_factor':3};");

		session.execute(
				"CREATE TABLE twitter.words (" +
						"id uuid PRIMARY KEY,"  +
						"status text);"
				);
		session.execute(
				"CREATE TABLE twitter.geoCity (" +
						"id uuid PRIMARY KEY," +
						"latitude double," +
						"longitude double," +
						"city text," +
						"country text);"
				);
		//Create Statement
		statementInsertTweets = getSession().prepare("INSERT INTO twitter.words (id, status) VALUES (?, ?);");
		statementInsertGeoCity = getSession().prepare("INSERT INTO twitter.geoCity (id, latitude, longitude, city, country) VALUES (?, ?, ?, ?, ?);");
	}


	public static UUID insertDataTweetStatus(UUID id, String statusJSON) {
		ensureOpen();
		BoundStatement boundStatement = new BoundStatement(statementInsertTweets);
		getSession().execute(boundStatement.bind(
				id,
				statusJSON));
		return id;
	}

    public static UUID insertDataTweetGeo(UUID id, double latitude, double longitude, String city, String country) {
		ensureOpen();
        BoundStatement boundStatement = new BoundStatement(statementInsertGeoCity);
        getSession().execute(boundStatement.bind(
                id,
                latitude,
                longitude,
                city));
        return id;
    }

	public static void getAllData() {
		ensureOpen();
		ResultSet results = session.execute("SELECT * FROM twitter.words;");

		System.out.println(String.format("%-30s\t%-20s\t%-20s\n", "word", "json",
				"-------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(row.getString("word") +"|"+row.getString("who"));
		}
		System.out.println();
	}


	public void createSchema() {
		ensureOpen();

		session.execute("CREATE KEYSPACE simplex WITH replication " +
				"= {'class':'SimpleStrategy', 'replication_factor':3};");

		session.execute(
				"CREATE TABLE simplex.songs (" +
						"id uuid PRIMARY KEY," +
						"title text," +
						"album text," +
						"artist text," +
						"tags set<text>," +
						"data blob" +
				");");
		session.execute(
				"CREATE TABLE simplex.playlists (" +
						"id uuid," +
						"title text," +
						"album text, " +
						"artist text," +
						"song_id uuid," +
						"PRIMARY KEY (id, title, album, artist)" +
				");");

	}



	public void loadData() {
		ensureOpen();

		session.execute(
				"INSERT INTO simplex.songs (id, title, album, artist, tags) " +
						"VALUES (" +
						"756716f7-2e54-4715-9f00-91dcbea6cf50," +
						"'La Petite Tonkinoise'," +
						"'Bye Bye Blackbird'," +
						"'Jos�phine Baker'," +
						"{'jazz', '2013'})" +
				";");
		session.execute(
				"INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
						"VALUES (" +
						"2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
						"756716f7-2e54-4715-9f00-91dcbea6cf50," +
						"'La Petite Tonkinoise'," +
						"'Bye Bye Blackbird'," +
						"'Jos�phine Baker'" +
				");");


	}

	public void querySchema() {

		ensureOpen();


		//		ResultSet results = session.execute("SELECT * FROM simplex.playlists " +
		//				"WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

		ResultSet results = session.execute("SELECT * FROM simplex.playlists;");

		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
				"-------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
					row.getString("album"),  row.getString("artist")));
		}
		System.out.println();

	}


	public void loadDataUsingBoundStatements() {
		ensureOpen();
		PreparedStatement statement = getSession().prepare(
				"INSERT INTO simplex.songs " +
						"(id, title, album, artist, tags) " +
				"VALUES (?, ?, ?, ?, ?);");
		BoundStatement boundStatement = new BoundStatement(statement);
		Set<String> tags = new HashSet<String>();
		tags.add("jazz");
		tags.add("2013");
		getSession().execute(boundStatement.bind(
				UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
				"La Petite Tonkinoise'",
				"Bye Bye Blackbird'",
				"Jos�phine Baker",
				tags ) );
		statement = getSession().prepare(
				"INSERT INTO simplex.playlists " +
						"(id, song_id, title, album, artist) " +
				"VALUES (?, ?, ?, ?, ?);");
		boundStatement = new BoundStatement(statement);
		getSession().execute(boundStatement.bind(
				UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
				UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
				"La Petite Tonkinoise",
				"Bye Bye Blackbird",
				"Jos�phine Baker") );
	}


	public static void main(String[] args) {
		CassandraUtil client = new CassandraUtil();
		client.connect("127.0.0.1");
		client.createTwitterSchema();
		//		client.createSchema();
		//		client.loadDataUsingBoundStatements();
		//		client.querySchema();
		//		client.updateSchema();
		//		client.dropSchema();
		//		client.close();

	}



}
