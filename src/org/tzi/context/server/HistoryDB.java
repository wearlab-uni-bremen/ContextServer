/*
   Copyright 2007-2013 Hendrik Iben, University Bremen

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package org.tzi.context.server;

import static org.tzi.context.server.DefaultDBProperties.KEY_BLOBTHRESHOLD;


import static org.tzi.context.server.DefaultDBProperties.KEY_CREATEFIELDSEP;
import static org.tzi.context.server.DefaultDBProperties.KEY_CREATEFIELDSPEC;
import static org.tzi.context.server.DefaultDBProperties.KEY_CREATEPRIMARYFIELDINTEGERSPEC;
import static org.tzi.context.server.DefaultDBProperties.KEY_CREATETABLEPATTERN;
import static org.tzi.context.server.DefaultDBProperties.KEY_CREATETABLEPRIMARYKEYINTEGERPATTERN;
import static org.tzi.context.server.DefaultDBProperties.KEY_CTXPREFIX;
import static org.tzi.context.server.DefaultDBProperties.KEY_DBCONNECTION;
import static org.tzi.context.server.DefaultDBProperties.KEY_INSERT;
import static org.tzi.context.server.DefaultDBProperties.KEY_INSERTFIELDSEP;
import static org.tzi.context.server.DefaultDBProperties.KEY_INSERTFIELDSPEC;
import static org.tzi.context.server.DefaultDBProperties.KEY_INSERTPLACEHOLDERSEP;
import static org.tzi.context.server.DefaultDBProperties.KEY_INSERTPLACEHOLDERSPEC;
import static org.tzi.context.server.DefaultDBProperties.KEY_JAVATYPEBOOL;
import static org.tzi.context.server.DefaultDBProperties.KEY_JAVATYPENUMTAGS;
import static org.tzi.context.server.DefaultDBProperties.KEY_JAVATYPEPROPERTY;
import static org.tzi.context.server.DefaultDBProperties.KEY_JAVATYPESOURCE;
import static org.tzi.context.server.DefaultDBProperties.KEY_JAVATYPETIMESTAMP;
import static org.tzi.context.server.DefaultDBProperties.KEY_NEEDSTABLESEARCH;
import static org.tzi.context.server.DefaultDBProperties.KEY_NUMTAGS;
import static org.tzi.context.server.DefaultDBProperties.KEY_PASS;
import static org.tzi.context.server.DefaultDBProperties.KEY_PERSISTENT;
import static org.tzi.context.server.DefaultDBProperties.KEY_PROPERTY;
import static org.tzi.context.server.DefaultDBProperties.KEY_PRPPREFIX;
import static org.tzi.context.server.DefaultDBProperties.KEY_SELECTFIELDSEP;
import static org.tzi.context.server.DefaultDBProperties.KEY_SELECTFIELDSPEC;
import static org.tzi.context.server.DefaultDBProperties.KEY_SELECTFROMLATESTTIMESTAMPPATTERN;
import static org.tzi.context.server.DefaultDBProperties.KEY_SELECTMAXTIMESTAMPPATTERN;
import static org.tzi.context.server.DefaultDBProperties.KEY_SELECTMINTIMESTAMPPATTERN;
import static org.tzi.context.server.DefaultDBProperties.KEY_SELECTPATTERN;
import static org.tzi.context.server.DefaultDBProperties.KEY_SELECTPATTERNID;
import static org.tzi.context.server.DefaultDBProperties.KEY_SELECTRANGEFROM;
import static org.tzi.context.server.DefaultDBProperties.KEY_SELECTRANGEFROMTO;
import static org.tzi.context.server.DefaultDBProperties.KEY_SETUP;
import static org.tzi.context.server.DefaultDBProperties.KEY_SOURCE;
import static org.tzi.context.server.DefaultDBProperties.KEY_SQLTYPEBOOL;
import static org.tzi.context.server.DefaultDBProperties.KEY_SQLTYPENUMTAGS;
import static org.tzi.context.server.DefaultDBProperties.KEY_SQLTYPEPROPERTY;
import static org.tzi.context.server.DefaultDBProperties.KEY_SQLTYPESOURCE;
import static org.tzi.context.server.DefaultDBProperties.KEY_SQLTYPETIMESTAMP;
import static org.tzi.context.server.DefaultDBProperties.KEY_SRCPREFIX;
import static org.tzi.context.server.DefaultDBProperties.KEY_SUPPORTSLIMIT;
import static org.tzi.context.server.DefaultDBProperties.KEY_TABLESEARCHCATALOG;
import static org.tzi.context.server.DefaultDBProperties.KEY_TABLESEARCHSCHEMAPATTERN;
import static org.tzi.context.server.DefaultDBProperties.KEY_TABLESEARCHTABLENAMEPATTERN;
import static org.tzi.context.server.DefaultDBProperties.KEY_TABLESEARCHTYPES;
import static org.tzi.context.server.DefaultDBProperties.KEY_TAGS;
import static org.tzi.context.server.DefaultDBProperties.KEY_TIMESTAMP;
import static org.tzi.context.server.DefaultDBProperties.KEY_TYPEBLOB;
import static org.tzi.context.server.DefaultDBProperties.KEY_TYPEBOOL;
import static org.tzi.context.server.DefaultDBProperties.KEY_TYPENUMTAGS;
import static org.tzi.context.server.DefaultDBProperties.KEY_TYPEPROPERTY;
import static org.tzi.context.server.DefaultDBProperties.KEY_TYPESOURCE;
import static org.tzi.context.server.DefaultDBProperties.KEY_TYPETAGS;
import static org.tzi.context.server.DefaultDBProperties.KEY_TYPETIMESTAMP;
import static org.tzi.context.server.DefaultDBProperties.KEY_TYPEVALUE;
import static org.tzi.context.server.DefaultDBProperties.KEY_UPDATE;
import static org.tzi.context.server.DefaultDBProperties.KEY_USER;
import static org.tzi.context.server.DefaultDBProperties.KEY_VALUE;
import static org.tzi.context.server.DefaultDBProperties.KEY_VALUEBOOLFALSE;
import static org.tzi.context.server.DefaultDBProperties.KEY_VALUEBOOLTRUE;

import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.Collator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.tzi.context.abstractenv.ContextAbstraction;
import org.tzi.context.abstractenv.PropertyAbstraction;
import org.tzi.context.abstractenv.SourceAbstraction;
import org.tzi.context.common.ContextElement;
import org.tzi.context.common.Util;

/**
 * A history implementation that uses a SQL database for storage.<br>
 * This was engineered towards MySQL but is flexible enough for other dialects
 * (tested with MySQL, SQLite and DerbyDB).<br>
 * Non standard settings can be provided via Properties (see also {@link DefaultDBProperties}).<br>
 * Large values (size configurable) are stored as BLOB types.
 * @author hendrik
 *
 */
public class HistoryDB extends HistoryDBAbstraction {
	private static final String CONTEXT_TABLE = "contexts";
	private static final String ID_FIELD = "id";
	private static final String NAME_FIELD = "name";
	private static final String ACTIVE_FIELD = "active";

	private static final String BLOB_TABLE = "blobtable";
	private static final String BLOB_FIELD = "blobdata";
	
	private boolean needsTableSearch;
	private boolean limitSupported;
	
	private Class<?> timestampClass;
	private Class<?> sourceClass;
	private Class<?> propertyClass;
	private Class<?> numtagsClass;
	private Class<?> booleanClass;
	
	private int timestampSQLtype;
	private int sourceSQLtype;
	private int propertySQLtype;
	private int numtagsSQLtype;
	private int booleanSQLtype;
	
	private String [] searchTypes = null;
	
	private String booleanTrue;
	private String booleanFalse;
	
	private Connection historyDBConnection = null;
	
	private String [] ctxFields;
	private String [] ctxTypes;
	
	private int idxTimestamp = 0;
	private int idxSource = 1;
	private int idxProperty = 2;
	@SuppressWarnings("unused")
	private int idxValue = 3;
	@SuppressWarnings("unused")
	private int idxNumTags = 4;
	@SuppressWarnings("unused")
	private int idxTags = 5;
	@SuppressWarnings("unused")
	private int idxPersistent = 6;

	private String blobType;
	private int blobThreshold;
	
	private String ctxS;
	private String srcS;
	private String prpS;
	
	// caching of table names to avoid 
	// unecessary database queries
	private Set<String> knownTables;
	
	// statements for accessing the blob table
	private PreparedStatement blobSelectStatement = null;
	private PreparedStatement blobInsertStatement = null;

	// lookup keys for storing specific database queries 
	private static final String dInsert = "_db_insert";
	private static final String dLatest = "_db_latest";
	private static final String dSelectFrom = "_db_selectFrom";
	private static final String dSelectFromTo = "_db_selectFromTo";
	
	private Charset asciiCharset = Charset.forName("ASCII");
	
	/**
	 * retrieve BLOB from database<br>
	 * SQL errors are silently ignored
	 * @param blobId assigned id
	 * @return BLOB data (ASCII string) or <em>null</em>
	 */
	private String getFromBlob(int blobId) {
		if(!knownTables.contains(BLOB_TABLE))
			return null;
		
		if(blobSelectStatement==null) {
			String bssS = createSelectIdStatement(BLOB_TABLE, BLOB_FIELD);
			try {
				blobSelectStatement = historyDBConnection.prepareStatement(bssS);
			} catch (SQLException e) {
				e.printStackTrace();
				return null;
			}
		}
		
		try {
			blobSelectStatement.setInt(1, blobId);
			blobSelectStatement.execute();
			
			ResultSet rs = blobSelectStatement.getResultSet();
			
			String result = null;
			if(rs.next()) {
				Blob b = rs.getBlob(1);
				long len = b.length();

				if(len < 0)
					len = 0;
				
				if(len > Integer.MAX_VALUE)
					len = Integer.MAX_VALUE;
				
				byte [] data = b.getBytes(1, (int)len);
				result = new String(data, asciiCharset);
			}
			
			rs.close();
			
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * store a BLOB in the database
	 * @param s blob data (ASCII string)
	 * @return assigned BLOB id or -1 on error
	 */
	private int putBlob(String s) {
		byte [] data = s.getBytes(asciiCharset);
		
		if(!createTableWithPrimIndex(BLOB_TABLE, ID_FIELD, new String [] { BLOB_FIELD }, new String [] { blobType }))
			return -1;
		
		if(blobInsertStatement == null) {
			String bisS = createInsertStatement(BLOB_TABLE, BLOB_FIELD);
			try {
				blobInsertStatement = historyDBConnection.prepareStatement(bisS, Statement.RETURN_GENERATED_KEYS);
			} catch (SQLException e) {
				e.printStackTrace();
				return -1;
			}
		}
		
		try {
			Blob b = historyDBConnection.createBlob();
			b.setBytes(1, data);
			blobInsertStatement.setBlob(1, b);
			blobInsertStatement.execute();

			ResultSet rs = blobInsertStatement.getGeneratedKeys();
			
			int id = -1;
			if(rs.next()) {
				id = rs.getInt(1);
			} else {
				System.err.println("DB-Error: Not key returned for blob creation!");
			}
			
			rs.close();
			blobInsertStatement.close();
			
			return id;
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	/**
	 * Initialize the history<br>
	 * No database access is done at this point but
	 * the configuration is checked. Invalid configurations
	 * result in a RuntimeException.
	 * @param props configuration properties
	 */
	public HistoryDB(Properties props) {
		super(props);
		
		String err = null;
		if( (err = checkTypes(props)) != null)
			throw new RuntimeException(err);
		if( (err = checkEssential(props)) != null)
			throw new RuntimeException(err);
		
		blobType = getProperty(KEY_TYPEBLOB);
		
		if(blobType == null) {
			throw new RuntimeException("Missing type for blob!");
		}
		
		try {
			blobThreshold = Integer.parseInt(getProperty(KEY_BLOBTHRESHOLD));
			if(blobThreshold<0)
				throw new NumberFormatException();
		} catch(NumberFormatException nfe) {
			throw new RuntimeException("Invalid blob-threshold: " + getProperty(KEY_BLOBTHRESHOLD));
		}
		
		
		timestampClass = getTypeClass(props.getProperty(KEY_JAVATYPETIMESTAMP));
		sourceClass = getTypeClass(props.getProperty(KEY_JAVATYPESOURCE));
		propertyClass = getTypeClass(props.getProperty(KEY_JAVATYPEPROPERTY));
		numtagsClass = getTypeClass(props.getProperty(KEY_JAVATYPENUMTAGS));
		booleanClass = getTypeClass(props.getProperty(KEY_JAVATYPEBOOL));
		
		booleanTrue = props.getProperty(KEY_VALUEBOOLTRUE);
		booleanFalse = props.getProperty(KEY_VALUEBOOLFALSE);

		timestampSQLtype = getSQLType(props.getProperty(KEY_SQLTYPETIMESTAMP));
		sourceSQLtype = getSQLType(props.getProperty(KEY_SQLTYPESOURCE));
		propertySQLtype = getSQLType(props.getProperty(KEY_SQLTYPEPROPERTY));
		numtagsSQLtype = getSQLType(props.getProperty(KEY_SQLTYPENUMTAGS));
		booleanSQLtype = getSQLType(props.getProperty(KEY_SQLTYPEBOOL));
		
		needsTableSearch = Boolean.parseBoolean(props.getProperty(KEY_NEEDSTABLESEARCH, "true"));
		limitSupported = Boolean.parseBoolean(props.getProperty(KEY_SUPPORTSLIMIT, "false"));
		
		String types = props.getProperty(KEY_TABLESEARCHTYPES);
		
		if(types!=null) {
			String [] items = types.split("\\s");
			ArrayList<String> il = new ArrayList<String>();
			for(String i : items) {
				i = i.trim();
				if(i.length()>0) {
					il.add(i);
				}
			}
			searchTypes = il.toArray(searchTypes = new String [il.size()]);
		}
		
		ctxFields = new String [] {
				  getProperty(KEY_TIMESTAMP)	
				, getProperty(KEY_SOURCE)	
				, getProperty(KEY_PROPERTY)	
				, getProperty(KEY_VALUE)	
				, getProperty(KEY_NUMTAGS)	
				, getProperty(KEY_TAGS)	
				, getProperty(KEY_PERSISTENT)	
			};
		
		ctxTypes = new String [] {
				  getProperty(KEY_TYPETIMESTAMP)
				, getProperty(KEY_TYPESOURCE)
				, getProperty(KEY_TYPEPROPERTY)
				, getProperty(KEY_TYPEVALUE)
				, getProperty(KEY_TYPENUMTAGS)
				, getProperty(KEY_TYPETAGS)
				, getProperty(KEY_TYPEBOOL)
			};
		
		ctxS = getProperty(KEY_CTXPREFIX);
		srcS = getProperty(KEY_SRCPREFIX);
		prpS = getProperty(KEY_PRPPREFIX);
		
		// create a table-set that ignores case differences
		Collator c = Collator.getInstance(Locale.US);
		c.setStrength(Collator.PRIMARY);
		knownTables = new TreeSet<String>(c);
	}
	
	public boolean hasConnection() {
		return historyDBConnection != null;
	}
	
	public Connection getConnection() {
		return historyDBConnection;
	}
	
	public boolean connectToDatabase() {
		String setupClass = getProperty(KEY_SETUP);
		
		if(setupClass!=null) {
			try {
				Class<?> sc = Class.forName(setupClass);
				Class<?> [] interfaces = sc.getInterfaces();
				boolean isCheckingClass = false;
				for(Class<?> ic : interfaces) {
					if(ic == HistoryDBCheck.class) {
						isCheckingClass = true;
						break;
					}
				}
				if(!isCheckingClass) {
					System.err.println("History-Database-Error; Setup-Class: " + setupClass + " is not a valid checking class!");
					return false;
				}
				HistoryDBCheck check = (HistoryDBCheck)sc.newInstance();
				if(!check.checkDatabase(this)) {
					System.err.println("History-Database-Error; Setup-Class: " + setupClass + " reported an error!");
					return false;
				}
				
			} catch (ClassNotFoundException e) {
				System.err.println("History-Database-Error; Setup-Class: " + setupClass + " not found!");
				return false;
			} catch (InstantiationException e) {
				System.err.println("History-Database-Error; Setup-Class: " + setupClass + " could not be instantiated!");
				return false;
			} catch (IllegalAccessException e) {
				System.err.println("History-Database-Error; Setup-Class: " + setupClass + " could not be accessed!");
				return false;
			}
		}
		
		try {
			historyDBConnection = DriverManager.getConnection(getProperty(KEY_DBCONNECTION), getProperty(KEY_USER), getProperty(KEY_PASS));
			return true;
		} catch(Exception e) {
			System.err.println("History-Database-Error: " + e.getMessage());
			return false;
		}
	}
	public static <A> boolean oneOf(A a, A [] list) {
		for(A lm : list)
			if(lm.equals(a))
				return true;
		
		return false;
	}
	
	private static final String [] javaTypes = { "long", "integer", "short", "byte", "boolean", "string" };
	private  static final String [] sqlTypes = { "bigint", "integer", "smallint", "tinyint", "bit", "boolean", "varchar" };
	
	private static Class<?> getTypeClass(String str) {
		str = str.trim().toLowerCase();
		
		if(str.equals("long"))
			return Long.class;
		if(str.equals("integer"))
			return Integer.class;
		if(str.equals("short"))
			return Short.class;
		if(str.equals("byte"))
			return Byte.class;
		if(str.equals("string"))
			return String.class;
		if(str.equals("boolean"))
			return Boolean.class;
		
		System.err.println("No class for " + str);
		
		return null;
	}
	
	private static int getSQLType(String str) {
		str = str.trim().toLowerCase();
		
		if(str.equals("bigint"))
			return Types.BIGINT;
		if(str.equals("integer"))
			return Types.INTEGER;
		if(str.equals("smallint"))
			return Types.SMALLINT;
		if(str.equals("tinyint"))
			return Types.TINYINT;
		if(str.equals("bit"))
			return Types.BIT;
		if(str.equals("boolean"))
			return Types.BOOLEAN;
		if(str.equals("varchar"))
			return Types.VARCHAR;

		return -1;
	}
	
	private static String checkTypes(Properties p) {
		for(String typeKey : new String [] {
				  KEY_JAVATYPETIMESTAMP 
				, KEY_JAVATYPESOURCE 
				, KEY_JAVATYPEPROPERTY 
				, KEY_JAVATYPENUMTAGS
				, KEY_JAVATYPEBOOL 
		} ) {
			String val = p.getProperty(typeKey);
			if(val == null)
				return String.format("Missing Java-Type for %s", typeKey);
			val = val.toLowerCase().trim();
			if(!oneOf(val, javaTypes))
				return String.format("Invalid Java-Type '%s' for %s", val, typeKey);
		}
		
		for(String typeKey : new String [] {
				  KEY_SQLTYPETIMESTAMP
				, KEY_SQLTYPESOURCE
				, KEY_SQLTYPEPROPERTY
				, KEY_SQLTYPENUMTAGS
				, KEY_SQLTYPEBOOL
		}) {
			String val = p.getProperty(typeKey);
			if(val == null)
				return String.format("Missing SQL-Type for %s", typeKey);
			val = val.toLowerCase().trim();
			if(!oneOf(val, sqlTypes))
				return String.format("Invalid SQL-Type '%s' for %s", val, typeKey);
		}
		
		return null;
	}
	
	public static boolean hasValue(Properties p, String key) {
		String v = p.getProperty(key);
		if(v == null || v.trim().length()==0)
			return false;
		
		return true;
	}
	
	public static String checkEssential(Properties p) {
		if(!hasValue(p, KEY_DBCONNECTION))
			return String.format("No database connection given (key: %s)", KEY_DBCONNECTION);
		
		return null;
	}
	
	private String createTableCreationStatement(String tableName, String [] fields, String [] types) {
		tableName = tableName.trim();
		
		if(tableName.length()==0)
			throw new RuntimeException("Table name is empty!");
		
		if(fields.length == 0)
			throw new RuntimeException("No fields given!");
		
		if(fields.length != types.length)
			throw new RuntimeException("fields and types have different item count!");
		
		StringBuilder sb = new StringBuilder();
		String fieldSpec = getProperty(KEY_CREATEFIELDSPEC);
		String fieldSep = getProperty(KEY_CREATEFIELDSEP);
		
		for(int i=0; i<fields.length; i++) {
			if(i>0)
				sb.append(fieldSep);
			sb.append(String.format(fieldSpec, fields[i], types[i]));
		}

		String createSpec = getProperty(KEY_CREATETABLEPATTERN);
		
		String s = String.format(createSpec, tableName, sb.toString());
		
		return s;
	}
	
	public String createTableWithIntegerPrimaryIdCreationStatement(String tableName, String id, String [] fields, String [] types) {
		tableName = tableName.trim();
		
		if(tableName.length()==0)
			throw new RuntimeException("Table name is empty!");
		
		id = id.trim();

		if(id.length()==0)
			throw new RuntimeException("Id field-name is empty!");
		
		if(fields.length != types.length)
			throw new RuntimeException("fields and types have different item count!");
		
		StringBuilder sb = new StringBuilder();
		String fieldSpec = getProperty(KEY_CREATEFIELDSPEC);
		String fieldSep = getProperty(KEY_CREATEFIELDSEP);
		
		for(int i=0; i<fields.length; i++) {
			if(i>0)
				sb.append(fieldSep);
			sb.append(String.format(fieldSpec, fields[i], types[i]));
		}

		String primSpec = getProperty(KEY_CREATEPRIMARYFIELDINTEGERSPEC);
		String prim = String.format(primSpec, id);
		
		String createSpec = getProperty(KEY_CREATETABLEPRIMARYKEYINTEGERPATTERN);
		String s = String.format(createSpec, tableName, prim, sb.toString());
		
		return s;
	}
	
	private String createSelectStatement(String table, String...fields) {
		if(fields.length==0)
			throw new RuntimeException("Fields is empty!");
		
		table= table.trim();

		if(table.length()==0)
			throw new RuntimeException("Table name is empty!");
		
		StringBuilder sb = new StringBuilder();
		String fieldSpec = getProperty(KEY_SELECTFIELDSPEC);
		String fieldSep = getProperty(KEY_SELECTFIELDSEP);
		
		for(int i=0; i<fields.length; i++) {
			if(i>0)
				sb.append(fieldSep);
			sb.append(String.format(fieldSpec, fields[i]));
		}
		
		return String.format(getProperty(KEY_SELECTPATTERN), sb.toString(), table);
	}
	
	private String createSelectIdStatement(String table, String...fields) {
		if(fields.length==0)
			throw new RuntimeException("Fields is empty!");
		
		table= table.trim();

		if(table.length()==0)
			throw new RuntimeException("Table name is empty!");
		
		StringBuilder sb = new StringBuilder();
		String fieldSpec = getProperty(KEY_SELECTFIELDSPEC);
		String fieldSep = getProperty(KEY_SELECTFIELDSEP);
		
		for(int i=0; i<fields.length; i++) {
			if(i>0)
				sb.append(fieldSep);
			sb.append(String.format(fieldSpec, fields[i]));
		}
		
		return String.format(getProperty(KEY_SELECTPATTERNID), sb.toString(), table, ID_FIELD);
	}
	
	private String createSelectMaxTimestampStatement(String field, String table, String src, String prp) {
		field = field.trim();
		
		if(field.length()==0)
			throw new RuntimeException("Field name is empty!");
		
		table= table.trim();

		if(table.length()==0)
			throw new RuntimeException("Table name is empty!");
		
		src = src.trim();
		
		if(src.length()==0)
			throw new RuntimeException("Src name is empty!");

		prp = prp.trim();
		
		if(prp.length()==0)
			throw new RuntimeException("Prp name is empty!");
		
		return String.format(getProperty(KEY_SELECTMAXTIMESTAMPPATTERN), field, table, src, prp);		
	}
	
	private String createSelectMinTimestampStatement(String field, String table, String src, String prp) {
		field = field.trim();
		
		if(field.length()==0)
			throw new RuntimeException("Field name is empty!");
		
		table= table.trim();

		if(table.length()==0)
			throw new RuntimeException("Table name is empty!");
		
		src = src.trim();
		
		if(src.length()==0)
			throw new RuntimeException("Src name is empty!");

		prp = prp.trim();
		
		if(prp.length()==0)
			throw new RuntimeException("Prp name is empty!");
		
		return String.format(getProperty(KEY_SELECTMINTIMESTAMPPATTERN), field, table, src, prp);		
	}
	
	private String createSelectFromLatestTimestampStatement(String [] fields, String timestamp, String table, String src, String prp) {
		if(fields.length==0)
			throw new RuntimeException("Fields is empty!");
		
		table= table.trim();

		if(table.length()==0)
			throw new RuntimeException("Table name is empty!");
		
		timestamp = timestamp.trim();

		if(timestamp.length()==0)
			throw new RuntimeException("Timestamp name is empty!");
		
		StringBuilder sb = new StringBuilder();
		String fieldSpec = getProperty(KEY_SELECTFIELDSPEC);
		String fieldSep = getProperty(KEY_SELECTFIELDSEP);
		
		for(int i=0; i<fields.length; i++) {
			if(i>0)
				sb.append(fieldSep);
			sb.append(String.format(fieldSpec, fields[i]));
		}

		return String.format(getProperty(KEY_SELECTFROMLATESTTIMESTAMPPATTERN), sb.toString(), timestamp, table, src, prp);
	}
	
	private String createSelectRangeStatement(String [] fields, String timestamp, String table, String src, String prp, boolean fromTo) {
		if(fields.length==0)
			throw new RuntimeException("Fields is empty!");
		
		table= table.trim();

		if(table.length()==0)
			throw new RuntimeException("Table name is empty!");
		
		timestamp = timestamp.trim();

		if(timestamp.length()==0)
			throw new RuntimeException("Timestamp name is empty!");
		StringBuilder sb = new StringBuilder();
		String fieldSpec = getProperty(KEY_SELECTFIELDSPEC);
		String fieldSep = getProperty(KEY_SELECTFIELDSEP);
		
		for(int i=0; i<fields.length; i++) {
			if(i>0)
				sb.append(fieldSep);
			sb.append(String.format(fieldSpec, fields[i]));
		}

		return String.format(getProperty(fromTo ? KEY_SELECTRANGEFROMTO : KEY_SELECTRANGEFROM), sb.toString(), table, src, prp, timestamp);
	}
	
	private String createUpdateStatement(String table, String field, String condField) {
		field = field.trim();
		
		if(field.length()==0)
			throw new RuntimeException("Field name is empty!");
		
		table= table.trim();

		if(table.length()==0)
			throw new RuntimeException("Table name is empty!");

		condField = condField.trim();

		if(condField.length()==0)
			throw new RuntimeException("Condition-field name is empty!");
		
		return String.format(getProperty(KEY_UPDATE), table, field, condField);
	}
	
	private String createInsertStatement(String table, String...fields) {
		table= table.trim();

		if(table.length()==0)
			throw new RuntimeException("Table name is empty!");

		if(fields.length == 0)
			throw new RuntimeException("No fields given!");
		
		StringBuilder sbf = new StringBuilder();
		StringBuilder sbp = new StringBuilder();
		String fieldSpec = getProperty(KEY_INSERTFIELDSPEC);
		String fieldSep = getProperty(KEY_INSERTFIELDSEP);
		String pSpec = getProperty(KEY_INSERTPLACEHOLDERSPEC);
		String pSep = getProperty(KEY_INSERTPLACEHOLDERSEP);
		
		for(int i=0; i<fields.length; i++) {
			if(i>0) {
				sbf.append(fieldSep);
				sbp.append(pSep);
			}
			sbf.append(String.format(fieldSpec, fields[i]));
			sbp.append(pSpec);
		}
		
		return String.format(getProperty(KEY_INSERT), table, sbf.toString(), sbp.toString());
	}

	public boolean needsTableSearch() {
		return needsTableSearch;
	}
	
	public boolean supportsLimit() {
		return limitSupported;
	}
	
	public Class<?> getTimestampClass() {
		return timestampClass;
	}
	
	public Class<?> getPropertyClass() {
		return propertyClass;
	}
	
	public Class<?> getSourceClass() {
		return sourceClass;
	}
	
	public Class<?> getNumTagsClass() {
		return numtagsClass;
	}
	
	public Class<?> getPersistentClass() {
		return booleanClass;
	}
	
	private Object getType(ResultSet rs, int col, Class<?> c) throws SQLException {
		if(c == Long.class) {
			return new Long(rs.getLong(col));
		}
		if(c == Integer.class) {
			return new Integer(rs.getInt(col));
		}
		if(c == Short.class) {
			return new Short(rs.getShort(col));
		}
		if(c == Byte.class) {
			return new Byte(rs.getByte(col));
		}
		if(c == Boolean.class) {
			return new Boolean(rs.getBoolean(col));
		}
		
		return null;
	}
	
	public static long toLong(Object o) {
		if(o == null)
			return 0;
		
		if(o instanceof Long) {
			return (Long)o;
		}
		if(o instanceof Integer) {
			return (Integer)o;
		}
		if(o instanceof Short) {
			return (Short)o;
		}
		if(o instanceof Byte) {
			return (Byte)o;
		}
		if(o instanceof Boolean) {
			return ((Boolean)o)?1:0;
		}
		if(o instanceof String) {
			return Long.parseLong((String)o);
		}
		
		return 0;
	}
	
	public static int toInt(Object o) {
		if(o == null)
			return 0;
		
		if(o instanceof Long) {
			return ((Long)o).intValue();
		}
		if(o instanceof Integer) {
			return (Integer)o;
		}
		if(o instanceof Short) {
			return (Short)o;
		}
		if(o instanceof Byte) {
			return (Byte)o;
		}
		if(o instanceof Boolean) {
			return ((Boolean)o)?1:0;
		}
		if(o instanceof String) {
			return Integer.parseInt((String)o);
		}
		
		return 0;
	}
	
	public boolean toBoolean(Object o) {
		if(o == null)
			return false;
		
		if(o instanceof Long) {
			return ((Long)o)==0;
		}
		if(o instanceof Integer) {
			return ((Integer)o)==0;
		}
		if(o instanceof Short) {
			return ((Short)o)==0;
		}
		if(o instanceof Byte) {
			return ((Byte)o)==0;
		}
		if(o instanceof Boolean) {
			return (Boolean)o;
		}
		if(o instanceof String) {
			String s = (String)o;
			return booleanTrue.equals(s) || ((!booleanFalse.equals(s)) && Boolean.parseBoolean(s));
		}
		
		return false;
	}
	
	public Object fromLong(long l, Class<?> c) {
		if(c == Long.class) {
			return new Long(l);
		}
		if(c == Integer.class) {
			return new Integer((int)l);
		}
		if(c == Short.class) {
			return Short.valueOf((short)l);
		}
		if(c == Byte.class) {
			return Byte.valueOf((byte)l);
		}
		if(c == Boolean.class) {
			return (l != 0) ? Boolean.TRUE : Boolean.FALSE;
		}
		if(c == String.class) {
			return Long.toString(l);
		}
		
		return null;
	}
	
	public Object fromInt(int i, Class<?> c) {
		if(c == Long.class) {
			return new Long(i);
		}
		if(c == Integer.class) {
			return new Integer(i);
		}
		if(c == Short.class) {
			return Short.valueOf((short)i);
		}
		if(c == Byte.class) {
			return Byte.valueOf((byte)i);
		}
		if(c == Boolean.class) {
			return (i != 0) ? Boolean.TRUE : Boolean.FALSE;
		}
		if(c == String.class) {
			return Integer.toString(i);
		}
		
		return null;
	}

	
	public Object fromBoolean(boolean b, Class<?> c) {
		if(c == Long.class) {
			return Long.valueOf(b ? 1 : 0);
		}
		if(c == Integer.class) {
			return Integer.valueOf(b ? 1 : 0);
		}
		if(c == Short.class) {
			return Short.valueOf((short)(b ? 1 : 0));
		}
		if(c == Byte.class) {
			return Byte.valueOf((byte)(b ? 1 : 0));
		}
		if(c == Boolean.class) {
			return b ? Boolean.TRUE : Boolean.FALSE;
		}
		if(c == String.class) {
			return b ? booleanTrue : booleanFalse;
		}
		
		return null;
	}
	
	public long getTimestamp(ResultSet rs, int col) throws SQLException {
		return toLong(getType(rs, col, timestampClass));
	}

	public int getSource(ResultSet rs, int col) throws SQLException {
		return toInt(getType(rs, col, sourceClass));
	}
	
	public int getProperty(ResultSet rs, int col) throws SQLException {
		return toInt(getType(rs, col, propertyClass));
	}
	
	public int getNumTags(ResultSet rs, int col) throws SQLException {
		return toInt(getType(rs, col, numtagsClass));
	}
	
	public boolean getBoolean(ResultSet rs, int col) throws SQLException {
		return toBoolean(getType(rs, col, booleanClass));
	}
	
	public void setTimestamp(PreparedStatement stmt, int i, long ts) throws SQLException {
		stmt.setObject(i, fromLong(ts, timestampClass), timestampSQLtype);
	}
	
	public void setSource(PreparedStatement stmt, int i, int src) throws SQLException {
		stmt.setObject(i, fromInt(src, sourceClass), sourceSQLtype);
	}
	
	public void setProperty(PreparedStatement stmt, int i, int prp) throws SQLException {
		stmt.setObject(i, fromInt(prp, propertyClass), propertySQLtype);
	}
	
	public void setNumTags(PreparedStatement stmt, int i, int numTags) throws SQLException {
		stmt.setObject(i, fromInt(numTags, numtagsClass), numtagsSQLtype);
	}
	
	public void setBoolean(PreparedStatement stmt, int i, boolean state) throws SQLException {
		stmt.setObject(i, fromBoolean(state, booleanClass), booleanSQLtype);
	}
	
	public ResultSet performTableSearch(String tablePattern) throws SQLException {
		return historyDBConnection.getMetaData().getTables(getProperty(KEY_TABLESEARCHCATALOG), getProperty(KEY_TABLESEARCHSCHEMAPATTERN), String.format(getProperty(KEY_TABLESEARCHTABLENAMEPATTERN), tablePattern), searchTypes);
	}
	
	public boolean hasTable(String tablePattern) throws SQLException {
		ResultSet rs = historyDBConnection.getMetaData().getTables(getProperty(KEY_TABLESEARCHCATALOG), getProperty(KEY_TABLESEARCHSCHEMAPATTERN), String.format(getProperty(KEY_TABLESEARCHTABLENAMEPATTERN), tablePattern), searchTypes);
		boolean hasTable = rs.next();
		rs.close();
		// turns out case sensitivity is not even consistent inside a single driver...
		if(!hasTable) {
			rs = historyDBConnection.getMetaData().getTables(getProperty(KEY_TABLESEARCHCATALOG), getProperty(KEY_TABLESEARCHSCHEMAPATTERN), String.format(getProperty(KEY_TABLESEARCHTABLENAMEPATTERN), tablePattern.toUpperCase()), searchTypes);
			hasTable = rs.next();
		}
		if(!hasTable) {
			rs = historyDBConnection.getMetaData().getTables(getProperty(KEY_TABLESEARCHCATALOG), getProperty(KEY_TABLESEARCHSCHEMAPATTERN), String.format(getProperty(KEY_TABLESEARCHTABLENAMEPATTERN), tablePattern.toLowerCase()), searchTypes);
			hasTable = rs.next();
		}
		return hasTable;
	}
	
	public boolean needsTableCreation(String tablePattern) throws SQLException {
		return !(needsTableSearch && hasTable(tablePattern));
	}
	
	private boolean createTable(String tableName, String [] fields, String [] types) {
		if(knownTables.contains(tableName))
			return true;
		
		try {
			if(needsTableCreation(tableName)) {
				Statement stmt = historyDBConnection.createStatement();
				stmt.execute(createTableCreationStatement(tableName, fields, types));
				stmt.close();
			}
			knownTables.add(tableName);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	private boolean createTableWithPrimIndex(String tableName, String idField, String [] fields, String [] types) {
		if(knownTables.contains(tableName))
			return true;
		
		try {
			if(needsTableCreation(tableName)) {
				Statement stmt = historyDBConnection.createStatement();
				stmt.execute(createTableWithIntegerPrimaryIdCreationStatement(tableName, idField, fields, types));
				stmt.close();
			}
			knownTables.add(tableName);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean createHistoryTable(String tableName) {
		return createTable(tableName, ctxFields, ctxTypes);
	}
	
	public Integer addContext(String contextName) {
		ContextAbstraction ca;
		Integer ctxId = null;
		if( (ca = getCAByName(contextName)) != null ) {
			if(!activeItem(ca)) {
				String pss = createUpdateStatement(CONTEXT_TABLE, ACTIVE_FIELD, ID_FIELD);
				try {
					PreparedStatement pstmt = historyDBConnection.prepareStatement(pss);
					setBoolean(pstmt, 1, true);
					pstmt.setInt(2, ca.getId());
					pstmt.execute();
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				setActiveItem(ca, true);
			}
			return ca.getId();
		}
		
		try {
			String stmtS = createInsertStatement(CONTEXT_TABLE, NAME_FIELD);

			PreparedStatement pstmt = historyDBConnection.prepareStatement(stmtS, Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(1, contextName);

			pstmt.executeUpdate();
			ResultSet rs = pstmt.getGeneratedKeys();

			if(rs.next()) {
				ctxId = rs.getInt(1);

				ca = dbenv.createDBContext(ctxId, contextName);
				setActiveItem(ca, true);
			} else {
				System.err.println("DB-Error: Not key returned for context creation!");
			}

			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return ctxId;
	}
	
	public Integer addSource(String contextName, String sourceName) {
		Integer ctxId = addContext(contextName);
		
		if(ctxId==null)
			return null;
		
		ContextAbstraction ca = getCA(ctxId);
		
		assert(ca!=null);
		
		SourceAbstraction sa = ca.getSourceByName(sourceName);
		
		Integer srcId = sa == null ? null : sa.getId();
		String sourceTable = ctxS + ctxId + srcS;
		
		if(sa!=null) {
			if(!activeItem(sa)) {
				String pss = createUpdateStatement(sourceTable, ACTIVE_FIELD, ID_FIELD);
				try {
					PreparedStatement pstmt = historyDBConnection.prepareStatement(pss);
					setBoolean(pstmt, 1, true);
					pstmt.setInt(2, srcId);
					pstmt.execute();
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				setActiveItem(sa, true);
			}
			return srcId;
		} else {
			if(!createTableWithPrimIndex(sourceTable, ID_FIELD, new String [] { NAME_FIELD, ACTIVE_FIELD }, new String [] { getProperty(DefaultDBProperties.KEY_TYPENAME), getProperty(DefaultDBProperties.KEY_TYPEBOOLDEFTRUE) })) {
				return null;
			}
			try {
				String pss = createInsertStatement(sourceTable, NAME_FIELD);
				
				PreparedStatement pstmt = historyDBConnection.prepareStatement(pss, Statement.RETURN_GENERATED_KEYS);
				pstmt.setString(1, sourceName);
				pstmt.execute();
				
				ResultSet rs = pstmt.getGeneratedKeys();
				
				if(rs.next()) {
					srcId = rs.getInt(1);
					
					sa = dbenv.createDBSource(ca, srcId, sourceName);
					setActiveItem(sa, true);
				} else {
					System.err.println("DB-Error: Not key returned for source creation!");
				}
				
				rs.close();
				pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return srcId;
	}
	
	public Integer addProperty(String contextName, String sourceName, String propertyName) {
		Integer ctxId = addContext(contextName);
		
		if(ctxId==null)
			return null;
		
		ContextAbstraction ca = getCA(ctxId);
		
		assert(ca != null);

		Integer srcId = addSource(contextName, sourceName);
		
		if(srcId==null)
			return null;
		
		SourceAbstraction sa = ca.getSourceById(srcId);
		
		assert(sa!=null);
		
		PropertyAbstraction pa = sa.getPropertyByName(propertyName);
		
		String propertyTable = ctxS + ctxId + srcS + srcId + prpS;
		Integer prpId = pa == null ? null : pa.getId();
		
		if(pa!=null) {
			if(!activeItem(pa)) {
				String pss = createUpdateStatement(propertyTable, ACTIVE_FIELD, ID_FIELD);
				try {
					PreparedStatement pstmt = historyDBConnection.prepareStatement(pss);
					setBoolean(pstmt, 1, true);
					pstmt.setInt(2, pa.getId());
					pstmt.execute();
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				setActiveItem(pa, true);
			}
		} else {
			if(!createTableWithPrimIndex(propertyTable, ID_FIELD, new String [] { NAME_FIELD, ACTIVE_FIELD }, new String [] { getProperty(DefaultDBProperties.KEY_TYPENAME), getProperty(DefaultDBProperties.KEY_TYPEBOOLDEFTRUE) })) {
				return null;
			}
			
			try {
				String pss = createInsertStatement(propertyTable, NAME_FIELD);
				
				PreparedStatement pstmt = historyDBConnection.prepareStatement(pss, Statement.RETURN_GENERATED_KEYS);
				pstmt.setString(1, propertyName);
				pstmt.execute();
				
				ResultSet rs = pstmt.getGeneratedKeys();
				
				if(rs.next()) {
					prpId = rs.getInt(1);
					
					pa = dbenv.createDBProperty(sa, prpId, propertyName);
					setActiveItem(pa, true);
				} else {
					System.err.println("DB-Error: Not key returned for property creation!");
				}
				
				rs.close();
				pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return prpId;
	}
	
	public void removeProperty(String contextName, String sourceName, String propertyName) {
		ContextAbstraction ca = dbenv.getContextByName(contextName);
		
		if(ca == null)
			return;
		
		SourceAbstraction sa = ca.getSourceByName(sourceName);
		
		if(sa == null)
			return;
		
		PropertyAbstraction pa = sa.getPropertyByName(propertyName);
		
		if(pa==null)
			return;
		
		removeProperty(ca.getId(), sa.getId(), pa.getId());
	}
	
	private void removeProperty(Integer ctxId, Integer srcId, Integer prpId) {
		PropertyAbstraction pa = getPA(ctxId, srcId, prpId);
		
		if(pa==null)
			return;
		
		setActiveItem(pa, false);
		
		String propertyTable = ctxS + ctxId + srcS + srcId + prpS;
		String pss = createUpdateStatement(propertyTable, ACTIVE_FIELD, ID_FIELD);
		try {
			PreparedStatement pstmt = historyDBConnection.prepareStatement(pss);
			setBoolean(pstmt, 1, false);
			pstmt.setInt(2, prpId);
			pstmt.execute();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void removeSource(String contextName, String sourceName) {
		SourceAbstraction sa = getSAByName(contextName, sourceName);
		
		if(sa == null)
			return;

		removeSource(sa.getContext().getId(), sa.getId());
	}
	
	private void removeSource(Integer ctxId, Integer srcId) {
		SourceAbstraction sa = getSA(ctxId, srcId);
		assert(sa!=null);
		
		for(PropertyAbstraction pa : sa.getProperties()) {
			removeProperty(ctxId, srcId, pa.getId());
		}
		
		setActiveItem(sa, false);
		
		String sourceTable = ctxS + ctxId + srcS;
		String pss = createUpdateStatement(sourceTable, ACTIVE_FIELD, ID_FIELD);
		try {
			PreparedStatement pstmt = historyDBConnection.prepareStatement(pss);
			setBoolean(pstmt, 1, true);
			pstmt.setInt(2, srcId);
			pstmt.execute();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void removeContext(String contextName) {
		Integer ctxId = getContextId(contextName);
		if(ctxId == null)
			return;
		
		removeContext(ctxId);
	}
	
	private void removeContext(Integer ctxId) {
		ContextAbstraction ca = getCA(ctxId);
		
		assert(ca!=null);
		
		for(SourceAbstraction sa : ca.getSources()) {
			removeSource(ctxId, sa.getId());
		}
		
		setActiveItem(ca, false);
		
		String pss = createUpdateStatement(CONTEXT_TABLE, ACTIVE_FIELD, ID_FIELD);
		try {
			PreparedStatement pstmt = historyDBConnection.prepareStatement(pss);
			setBoolean(pstmt, 1, false);
			pstmt.setInt(2, ctxId);
			pstmt.execute();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void propertyChange(String contextName, String sourceName, String propertyName, long timestamp, String value, Set<String> tags, boolean persistent) {
		PropertyAbstraction pa = getPAByName(contextName, sourceName, propertyName);
		
		if(pa==null)
			return;
		
		StringBuilder sb = new StringBuilder();
		for(String s : tags) {
			if(sb.length()>0)
				sb.append(" ");
			sb.append(Util.urlencode(s));
		}

		propertyChange(pa.getSource().getContext().getId(), pa.getSource().getId(), pa.getId(), timestamp, value, tags.size(), sb.toString(), persistent);
	}
	
	private boolean propertyChange(Integer ctxId, Integer srcId, Integer prpId, long timestamp, String value, int numTags, String tags, boolean persistent) {
		PropertyAbstraction pa = getPA(ctxId, srcId, prpId);
		
		if(pa==null)
			return false;
		
		PreparedStatement pstmt = (PreparedStatement)pa.getData(dInsert);
		
		String insTable = ctxS + ctxId;
		if(pstmt == null) {
			String insStr = createInsertStatement(insTable, ctxFields);
			
			try {
				pstmt = historyDBConnection.prepareStatement(insStr);
				pa.setData(dInsert, pstmt);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		}
		
		if(!createHistoryTable(insTable)) {
			return false;
		}

		// check if value needs to be stored as BLOB
		boolean createBlob = false;
		
		if( 
			   (value.length() > 0)
			&& (
					   (value.length() > blobThreshold)
					|| (value.length() == blobThreshold && value.charAt(0) == '#' ) 
				) 
			) {
			createBlob = true;
		}
		
		// if no BLOB required, check if the value needs to be
		// modified to do not look like a blob id
		if(!createBlob && value.length()>0 && value.charAt(0) == '#') {
			value = '#' + value;
		}
		
		// if blob needed, save id to value
		if(createBlob) {
			int blobId = putBlob(value);
			if(blobId==-1)
				return false;
			
			value = "#" + blobId;
		}
		
		try {
			setTimestamp(pstmt, 1, timestamp);
			setSource(pstmt, 2, srcId);
			setProperty(pstmt, 3, prpId);
			pstmt.setString(4, value);
			setNumTags(pstmt, 5, numTags);
			pstmt.setString(6, tags);
			setBoolean(pstmt, 7, persistent);
			pstmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

	/**
	 * Connect to the database and make active items available<br>
	 * see also {@link HistoryDBAbstraction#initialize()}
	 */
	public boolean initialize() {
		createTableWithPrimIndex(CONTEXT_TABLE, ID_FIELD, new String [] { NAME_FIELD, ACTIVE_FIELD },  new String [] { getProperty(DefaultDBProperties.KEY_TYPENAME), getProperty(DefaultDBProperties.KEY_TYPEBOOLDEFTRUE) });
		
		try {
			Statement stmt = historyDBConnection.createStatement();
			stmt.execute(createSelectStatement(CONTEXT_TABLE, ID_FIELD, NAME_FIELD, ACTIVE_FIELD));
			ResultSet rs = stmt.getResultSet();
			
			while(rs.next()) {
				int id = rs.getInt(1);
				String name = rs.getString(2);
				boolean active = getBoolean(rs, 3);
				ContextAbstraction ca = dbenv.createDBContext(id, name);
				setActiveItem(ca, active);
				if(hasTable(ctxS + id)) {
					knownTables.add(ctxS + id);
				}
			}
			
			rs.close();
			stmt.close();
			
			for(ContextAbstraction ca : getCAs()) {
				String srcTable = ctxS + ca.getId() + srcS;
				if(hasTable(srcTable)) {
					knownTables.add(srcTable);
					stmt = historyDBConnection.createStatement();
					stmt.execute(createSelectStatement(srcTable, ID_FIELD, NAME_FIELD, ACTIVE_FIELD));
					rs = stmt.getResultSet();
					while(rs.next()) {
						int sid = rs.getInt(1);
						String sname = rs.getString(2);
						boolean sactive = getBoolean(rs, 3);
						SourceAbstraction sa = dbenv.createDBSource(ca, sid, sname);
						setActiveItem(sa, sactive);
					}
					rs.close();
					stmt.close();
					
					for(SourceAbstraction sa : ca.getSources()) {
						String prpTable = ctxS + ca.getId() + srcS + sa.getId() + prpS;
						if(hasTable(prpTable)) {
							knownTables.add(prpTable);
							
							stmt = historyDBConnection.createStatement();
							stmt.execute(createSelectStatement(prpTable, ID_FIELD, NAME_FIELD, ACTIVE_FIELD));
							rs = stmt.getResultSet();
							while(rs.next()) {
								int pid = rs.getInt(1);
								String pname = rs.getString(2);
								boolean pactive = getBoolean(rs, 3);
								
								PropertyAbstraction pa = dbenv.createDBProperty(sa, pid, pname);
								setActiveItem(pa, pactive);
							}
							rs.close();
							stmt.close();

						}
					}
				}
			}
			
			if(hasTable(BLOB_TABLE))
				knownTables.add(BLOB_TABLE);

			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * Reconstruct a ContextElement from a ResultSet
	 * @param source name of the source
	 * @param property name of the property
	 * @param rs result set containing data
	 * @return reconstructed ContextElement (or <em>null</em> if rs is <em>null</em>)
	 * @throws SQLException on SQL error
	 */
	private ContextElement ceFromRS(String source, String property, ResultSet rs) throws SQLException {
		if(rs==null)
			return null;
		
		long ts = getTimestamp(rs, 1);
//		int src = getSource(rs, 2);
//	    int prp = getProperty(rs, 3);
		String value = rs.getString(4);
		int numTags = getNumTags(rs, 5);
		String tags = rs.getString(6);
		boolean persistent = getBoolean(rs, 7);
		
		String [] taglist = DefaultDBProperties.whitespace.split(tags);
		ArrayList<String> al = new ArrayList<String>(numTags);
		
		for(String s : taglist) {
			s = s.trim();
			if(s.length()>0)
				al.add(Util.urldecode(s));
		}
		
		// check if the value was stored as a BLOB and
		// the id is given as '#<id>'
		boolean fromBlob = false;
		if(value.length()>1 && value.charAt(0) == '#' && value.charAt(1) != '#') {
			StringBuilder sb = new StringBuilder();
			int i=1;
			while(i < value.length() && Character.isDigit(value.charAt(i))) {
				sb.append(value.charAt(i));
				i++;
			}

			if(i==value.length()) {
				if(sb.length()>0) {
					int id = Integer.parseInt(sb.toString());

					String blobContent = getFromBlob(id);
					if(blobContent!=null) {
						value = blobContent;
						fromBlob = true;
					}
				}
			}
			
		}
		
		if(!fromBlob) {
			// check if a '#' was added in front and remove
			// this is needed to differentiate between the value
			// '#123' and the blob-id '#123' (value is stored as '##123')
			if(value.length()>1 && value.charAt(0) == '#') {
				value = value.substring(1);
			}
		}
		
		return new ContextElement(source, property, value, ts, persistent, al.toArray(new String [al.size()]));
	}
	
	private ContextElement getLastState(Integer ctxId, String source, Integer srcId, String property, Integer prpId) {
		String ctxTable = ctxS + ctxId;
		
		try {
			if(!hasTable(ctxTable))
				return null;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
		PropertyAbstraction pa = getPA(ctxId, srcId, prpId);
		
		if(pa==null)
			return null;
		
		String ltS = createSelectFromLatestTimestampStatement(ctxFields, ctxFields[idxTimestamp], ctxTable, ctxFields[idxSource], ctxFields[idxProperty]);
		
		PreparedStatement pstmt = (PreparedStatement)pa.getData(dLatest);
		
		if(pstmt==null) {
			try {
				pstmt = historyDBConnection.prepareStatement(ltS);
				pa.setData(dLatest, pstmt);
			} catch (SQLException e) {
				e.printStackTrace();
				return null;
			}
		}
		
		try {
			pstmt.setInt(1, srcId);
			pstmt.setInt(2, prpId);
			pstmt.execute();
			ResultSet rs = pstmt.getResultSet();
			
			if(rs.next()) {
				ContextElement ce = ceFromRS(source, property, rs);
				
				rs.close();
				
				return ce;
			} else {
				return null;
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public ContextElement getLastState(String context, String source, String property) {
		PropertyAbstraction pa = getPAByName(context, source, property);
		
		if(pa==null)
			return null;
		
		return getLastState(pa.getSource().getContext().getId(), source, pa.getSource().getId(), property, pa.getId());
	}
	
	private List<ContextElement> getHistory(Integer ctxId, String source, Integer srcId, String property, Integer prpId, int limit, long from, long to, Set<String> withTags, List<ContextElement> l) {
		if(l==null)
			l = new ArrayList<ContextElement>(limit);
		
		String ctxTable = ctxS + ctxId;
		
		try {
			if(!hasTable(ctxTable))
				return l;
		} catch (SQLException e) {
			e.printStackTrace();
			return l;
		}
		
		boolean fromTo = to != -1;
		
		PropertyAbstraction pa = getPA(ctxId, srcId, prpId);
		
		if(pa==null)
			return null;
		
		String pkey = fromTo ? dSelectFromTo : dSelectFrom;
		
		PreparedStatement pstmt = (PreparedStatement)pa.getData(pkey);
		
		if(pstmt == null) {
			try {
				String ltS = createSelectRangeStatement(ctxFields, ctxFields[idxTimestamp], ctxTable, ctxFields[idxSource], ctxFields[idxProperty], fromTo);
				pstmt = historyDBConnection.prepareStatement(ltS);
				pa.setData(pkey, pstmt);
			} catch (SQLException e) {
				e.printStackTrace();
				return null;
			}
		}
		
		try {
			setSource(pstmt, 1, srcId);
			setProperty(pstmt, 2, prpId);
			setTimestamp(pstmt, 3, from);
			if(fromTo) {
				setTimestamp(pstmt, 4, to);
			}
			if(supportsLimit()) {
				pstmt.setInt(fromTo ? 5 : 4, limit);
			}
			pstmt.execute();
			ResultSet rs = pstmt.getResultSet();
			int count = 0;
			
			while(rs.next() && count < limit) {
				ContextElement ce = ceFromRS(source, property, rs);
				boolean canAdd = true;
				if(withTags != null) {
					canAdd = (withTags.size() == 0 && ce.getTypeTags().size() == 0);
					for(String tag : withTags) {
						if(ce.getTypeTags().contains(tag)) {
							canAdd = true;
							break;
						}
					}
				}
				if(canAdd) {
					l.add(ce);
					count++;
				}
			}

			rs.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
			return l;
		}
		
		return l;
	}
	
	private long getPrpTimestamp(Integer ctxId, Integer srcId, Integer prpId, boolean getMax) {
		String ctxTable = ctxS + ctxId;
		
		try {
			if(!hasTable(ctxTable))
				return -1;
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
		
		String psl = 
			getMax ? 
					  createSelectMaxTimestampStatement(ctxFields[idxTimestamp], ctxTable, getProperty(KEY_SOURCE),getProperty(KEY_PROPERTY))
					: createSelectMinTimestampStatement(ctxFields[idxTimestamp], ctxTable, getProperty(KEY_SOURCE),getProperty(KEY_PROPERTY));
		try {
			PreparedStatement pstmt = historyDBConnection.prepareStatement(psl);
			pstmt.setInt(1, srcId);
			pstmt.setInt(2, prpId);
			
			pstmt.execute();
			
			ResultSet rs = pstmt.getResultSet();
			
			long ts = -1; 
			if(rs.next()) {
				ts = getTimestamp(rs, 1);
			}
			rs.close();
			pstmt.close();
			
			return ts;
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public long getPrpTimestamp(String context, String source, String property, boolean getMax) {
		ContextAbstraction ca = getCAByName(context); 
		
		if(ca == null)
			return -1;
		
		SourceAbstraction sa = ca.getSourceByName(source);
		
		if(sa == null)
			return -1;
		
		PropertyAbstraction pa = sa.getPropertyByName(property);
		
		if(pa==null)
			return -1;
		
		return getPrpTimestamp(ca.getId(), sa.getId(), pa.getId(), getMax);
	}
	
	public List<ContextElement> getHistory(String context, String source, String property, int limit, long from, long to, Set<String> withTags, List<ContextElement> l) {
		ContextAbstraction ca = getCAByName(context);
		
		if(ca == null)
			return null;

		SourceAbstraction sa = ca.getSourceByName(source);
		
		if(sa == null)
			return null;
		
		PropertyAbstraction pa = sa.getPropertyByName(property);
		
		if(pa == null)
			return null;
		
		return getHistory(ca.getId(), source, sa.getId(), property, pa.getId(), limit, from, to, withTags, l);
	}
	
	public static <A, B, C> Map<B, C> getCreateMap(Map<A, Map<B, C>> m, A key) {
		Map<B, C> mbc = m.get(key);
		if(mbc == null) {
			m.put(key, mbc = new TreeMap<B, C>());
		}
		
		return mbc;
	}
	
	public void printDBState() {
		for(ContextAbstraction ca : getCAs()) {
			System.out.format("Context %d %s%s\n", ca.getId(), ca.getName(), activeItem(ca) ? "" : " (inactive)");
			for(SourceAbstraction sa : ca.getSources()) {
				System.out.format("  Source %d %s%s\n", sa.getId(), sa.getName(), activeItem(sa) ? "" : " (inactive)");
				for(PropertyAbstraction pa : sa.getProperties()) {
					System.out.format("    Property %d %s%s\n", pa.getId(), pa.getName(), activeItem(pa) ? "" : " (inactive)");
				}
			}
		}
		System.out.println("Known tables: " + knownTables);
	}
}
