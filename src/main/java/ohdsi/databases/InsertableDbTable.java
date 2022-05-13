package ohdsi.databases;

import ohdsi.utilities.files.Row;

import java.util.ArrayList;
import java.util.List;

public class InsertableDbTable implements AutoCloseable {

	public static final int batchSize = 1000;
	private List<Row> batch;
	private final ConnectionWrapper connectionWrapper;
	private final String tableName;
	private boolean firstRow;

	public InsertableDbTable(ConnectionWrapper connectionWrapper, String tableName) {
		batch = new ArrayList<>(batchSize);
		firstRow = true;
		this.tableName = tableName;
		this.connectionWrapper = connectionWrapper;
	}

	public void write(Row row) {
		if (firstRow) {
			createTable(row);
			firstRow = false;
		}
		batch.add(row);
		if (batch.size() == batchSize) {
			connectionWrapper.insertIntoTable(tableName, batch, true);
			batch.clear();
		}
	}

	private void createTable(Row row) {
		connectionWrapper.dropTableIfExists(tableName);
		List<String> fields = row.getFieldNames();
		List<String> types = new ArrayList<String>(fields.size());
		for (int i = 0; i < fields.size(); i++) {
			try {
				Integer.parseInt(row.get(i));
				types.add("INT");
			} catch (NumberFormatException e) {
				types.add("VARCHAR(512)");
			}
		}
		connectionWrapper.createTable(tableName, fields, types, null);
	}

	public void close() {
		connectionWrapper.insertIntoTable(tableName, batch, true);
		batch.clear();
	}
}
