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

import java.lang.reflect.Field;

import java.lang.reflect.Modifier;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Default values for all history database related properties<br>
 * Upon instantiation, an object of this class contains all properties
 * initialized to their default values (some may be <em>null</em>).
 * @author hendrik
 *
 */
public class DefaultDBProperties extends Properties {
	private static final long serialVersionUID = 1L;

	/** Prefix for all property keys */
	public static final String prefix = "org.tzi.context.history.";
	
	/** Database jar inclusion (external) */
	public static final String KEY_JAR = prefix + "jar";
	/** Database extra-jar inclusion (external) */
	public static final String KEY_EXTRA_JAR = prefix + "extra.jar";

	/** Database library-path (external) */
	public static final String KEY_LIB = prefix + "lib";
	/** JDBC driver class */
	public static final String KEY_DRIVER = prefix + "driver";
	/** HistoryDBAbstraction class */
	public static final String KEY_CLASS = prefix + "class";
	/** History setup class (implements HistoryDBAbstraction.HistoryDBCheck) */
	public static final String KEY_SETUP = prefix + "setup";
	/** JDBC connection string */
	public static final String KEY_DBCONNECTION = prefix + "dbconnection";
	/** JDBC user */
	public static final String KEY_USER = prefix + "user";
	/** JDBC password */
	public static final String KEY_PASS = prefix + "pass";
	/** JDBC catalog for table search */
	public static final String KEY_TABLESEARCHCATALOG = prefix + "tablesearch.catalog";
	/** JDBC schema pattern for table search */
	public static final String KEY_TABLESEARCHSCHEMAPATTERN = prefix + "tablesearch.schemaPattern";
	/** JDBC table pattern for table search */
	public static final String KEY_TABLESEARCHTABLENAMEPATTERN = prefix + "tablesearch.tableNamePattern";
	/** JDBC types for table search */
	public static final String KEY_TABLESEARCHTYPES = prefix + "tablesearch.types";
	/** table name prefix for contexts */
	public static final String KEY_CTXPREFIX = prefix + "ctxPrefix";
	/** table name prefix for sources */
	public static final String KEY_SRCPREFIX = prefix + "srcPrefix";
	/** table name prefix for properties */
	public static final String KEY_PRPPREFIX = prefix + "prpPrefix";
	/** field name for timestamp */
	public static final String KEY_TIMESTAMP = prefix + "timestamp";
	/** field name for source */
	public static final String KEY_SOURCE = prefix + "source";
	/** field name for property */
	public static final String KEY_PROPERTY = prefix + "property";
	/** field name for value */
	public static final String KEY_VALUE = prefix + "value";
	/** maximum length of value field */
	public static final String KEY_VALUEMAXLEN = prefix + "valueMaxLen";
	/** field name for number of tags */
	public static final String KEY_NUMTAGS = prefix + "numTags";
	/** field name for tags */
	public static final String KEY_TAGS = prefix + "tags";
	/** field name for persistent */
	public static final String KEY_PERSISTENT = prefix + "persistent";
	
	// creation types are used literally when creating tables
	
	/** SQL type for timestamp (creation) */
	public static final String KEY_TYPETIMESTAMP = prefix + "typeTimestamp";
	/** SQL type for source (creation) */
	public static final String KEY_TYPESOURCE = prefix + "typeSource";
	/** SQL type for property (creation) */
	public static final String KEY_TYPEPROPERTY = prefix + "typeProperty";
	/** SQL type for value (creation) */
	public static final String KEY_TYPEVALUE = prefix + "typeValue";
	/** SQL type for number of tags (creation) */
	public static final String KEY_TYPENUMTAGS = prefix + "typeNumTags";
	/** SQL type for tags (creation) */
	public static final String KEY_TYPETAGS = prefix + "typeTags";
	/** SQL type for boolean (creation) */
	public static final String KEY_TYPEBOOL = prefix + "typeBool";

	/** SQL type for BLOB (creation) */
	public static final String KEY_TYPEBLOB = prefix + "typeBlob";
	/** size threshold for BLOB storage (instead of value) */
	public static final String KEY_BLOBTHRESHOLD = prefix + "blobThreshold";
	
	/** SQL type for names (creation) */
	public static final String KEY_TYPENAME = prefix + "typeName";

	// Java types are used for conversion
	
	/** Java type for timestamp */
	public static final String KEY_JAVATYPETIMESTAMP = prefix + "javaTypeTimestamp";
	/** Java type for source */
	public static final String KEY_JAVATYPESOURCE = prefix + "javaTypeSource";
	/** Java type for property */
	public static final String KEY_JAVATYPEPROPERTY = prefix + "javaTypeProperty";
	/** Java type for number of tags */
	public static final String KEY_JAVATYPENUMTAGS = prefix + "javaTypeNumTags";
	/** Java type for boolean */
	public static final String KEY_JAVATYPEBOOL = prefix + "javaTypeBool";
	
	// access types are used for setting values via prepared statements
	
	/** SQL type for timestamp (access) */
	public static final String KEY_SQLTYPETIMESTAMP = prefix + "sqlTypeTimestamp";
	/** SQL type for source (access) */
	public static final String KEY_SQLTYPESOURCE = prefix + "sqlTypeSource";
	/** SQL type for property (access) */
	public static final String KEY_SQLTYPEPROPERTY = prefix + "sqlTypeProperty";
	/** SQL type for number of tags (access) */
	public static final String KEY_SQLTYPENUMTAGS = prefix + "sqlTypeNumTags";
	/** SQL type for boolean (access) */
	public static final String KEY_SQLTYPEBOOL = prefix + "sqlTypeBool";
	
	/** SQL type for boolean with default TRUE value */
	public static final String KEY_TYPEBOOLDEFTRUE = prefix + "typeBoolDefTrue";
	/** SQL value for boolean TRUE */
	public static final String KEY_VALUEBOOLTRUE = prefix + "valueBoolTrue";
	/** SQL value for boolean FALSE */
	public static final String KEY_VALUEBOOLFALSE = prefix + "valueBoolFalse";
	/** SQL pattern for field creation */
	public static final String KEY_CREATEFIELDSPEC = prefix + "createFieldSpec";
	/** SQL separator used in field creation */
	public static final String KEY_CREATEFIELDSEP = prefix + "createFieldSep";
	/** SQL pattern for table creation */
	public static final String KEY_CREATETABLEPATTERN = prefix + "createTablePattern";
	/** SQL pattern for creating an integer field as primary id */
	public static final String KEY_CREATEPRIMARYFIELDINTEGERSPEC = prefix + "createPrimaryFieldIntegerSpec";
	/** SQL pattern for creating a table with a primary integer field */
	public static final String KEY_CREATETABLEPRIMARYKEYINTEGERPATTERN = prefix + "createTablePrimaryKeyIntegerPattern";
	/** Java boolean if the backend supports the LIMIT syntax on search results */
	public static final String KEY_SUPPORTSLIMIT = prefix + "supportsLimit"; 
	/** Java boolean if the backend needs a table search to avoid inserting existing tables */
	public static final String KEY_NEEDSTABLESEARCH = prefix + "needsTableSearch";
	/** SQL pattern to perform a search for maximum timestamp */
	public static final String KEY_SELECTMAXTIMESTAMPPATTERN = prefix + "selectMaxTimestampPattern";
	/** SQL pattern to perform a search for minimum timestamp */
	public static final String KEY_SELECTMINTIMESTAMPPATTERN = prefix + "selectMinTimestampPattern";
	
	/** SQL pattern to select fields in a query */
	public static final String KEY_SELECTFIELDSPEC = prefix + "selectFieldSpec";
	/** SQL field separator for a query */
	public static final String KEY_SELECTFIELDSEP = prefix + "selectFieldSep";
	/** SQL pattern for SELECT */
	public static final String KEY_SELECTPATTERN = prefix + "selectPattern";
	/** SQL pattern for SELECT with an id */
	public static final String KEY_SELECTPATTERNID = prefix + "selectPatternId";
	/** SQL pattern for SELECT with the latest timestamp */
	public static final String KEY_SELECTFROMLATESTTIMESTAMPPATTERN = prefix + "selectFromLatestTimestampPattern";
	/** SQL pattern for UPDATE<br>
	 * formatted with three arguments: table-name, field-name for change and field-name to check<br>
	 * (e.g. source-table, active-indicator and source-id)
	 */
	public static final String KEY_UPDATE = prefix + "update";
	/** SQL pattern for fields in INSERT<br>
	 * Formatted with the name of the field as argument
	 */
	public static final String KEY_INSERTFIELDSPEC = prefix + "insertFieldSpec";
	/** SQL separator for fields in INSERT<br>
	 * inserted between each formatted field name
	 */
	public static final String KEY_INSERTFIELDSEP = prefix + "insertFieldSep";
	/** SQL placeholder in INSERT (prepared statement) */
	public static final String KEY_INSERTPLACEHOLDERSPEC = prefix + "insertPlaceholderSpec";
	/** SQL separator for placeholders in INSERT (prepared statement) */
	public static final String KEY_INSERTPLACEHOLDERSEP = prefix + "insertPlaceholderSep";
	/** SQL pattern for selecting values from a specific time and later */
	public static final String KEY_SELECTRANGEFROM = prefix + "selectRangeFrom";
	/** SQL pattern for selecting a timestamp range */
	public static final String KEY_SELECTRANGEFROMTO = prefix + "selectRangeFromTo";
	/** SQL pattern for INSERT<br>
	 * Formatted with three arguments: table-name, list of fields and list of values<br>
	 * see also: {@link #KEY_INSERTFIELDSPEC}, {@link #KEY_INSERTFIELDSEP}
	 */
	public static final String KEY_INSERT = prefix + "insert";
	
	// DEFAULT VALUES (tailored to MySQL)
	
	public static final String VALUE_CLASS_DEFAULT = "org.tzi.context.server.HistoryDB";
	
	public static final String VALUE_TABLESEARCHTABLENAMEPATTERN_DEFAULT = "%s";
	
	public static final String VALUE_CTXPREFIX_DEFAULT = "ctx";
	public static final String VALUE_SRCPREFIX_DEFAULT = "src";
	public static final String VALUE_PRPPREFIX_DEFAULT = "prp";
	public static final String VALUE_TIMESTAMP_DEFAULT = "timestamp";
	public static final String VALUE_SOURCE_DEFAULT = "source";
	public static final String VALUE_PROPERTY_DEFAULT = "property";
	public static final String VALUE_VALUE_DEFAULT = "value";
	public static final String VALUE_VALUEMAXLEN_DEFAULT = "255";
	public static final String VALUE_NUMTAGS_DEFAULT = "numtags";
	public static final String VALUE_TAGS_DEFAULT = "tags";
	public static final String VALUE_PERSISTENT_DEFAULT = "persistent";
	
	public static final String VALUE_TYPETIMESTAMP_DEFAULT = "BIGINT";
	public static final String VALUE_TYPESOURCE_DEFAULT = "INTEGER";
	public static final String VALUE_TYPEPROPERTY_DEFAULT = "INTEGER";
	public static final String VALUE_TYPEVALUE_DEFAULT = "VARCHAR(255)";
	public static final String VALUE_TYPENUMTAGS_DEFAULT = "INTEGER";
	public static final String VALUE_TYPETAGS_DEFAULT = "VARCHAR(80)";
	public static final String VALUE_TYPEBOOL_DEFAULT = "BOOL";

	public static final String VALUE_TYPEBLOB_DEFAULT = "BLOB";
	public static final String VALUE_BLOBTHRESHOLD_DEFAULT = "200";
	
	public static final String VALUE_TYPENAME_DEFAULT = "VARCHAR(80)";

	public static final String VALUE_JAVATYPETIMESTAMP_DEFAULT = "long";
	public static final String VALUE_JAVATYPESOURCE_DEFAULT = "integer";
	public static final String VALUE_JAVATYPEPROPERTY_DEFAULT = "integer";
	public static final String VALUE_JAVATYPENUMTAGS_DEFAULT = "integer";
	public static final String VALUE_JAVATYPEBOOL_DEFAULT = "boolean";

	public static final String VALUE_SQLTYPETIMESTAMP_DEFAULT = "bigint";
	public static final String VALUE_SQLTYPESOURCE_DEFAULT = "integer";
	public static final String VALUE_SQLTYPEPROPERTY_DEFAULT = "integer";
	public static final String VALUE_SQLTYPENUMTAGS_DEFAULT = "integer";
	public static final String VALUE_SQLTYPEBOOL_DEFAULT = "boolean";

	public static final String VALUE_TYPEBOOLDEFTRUE_DEFAULT = "BOOL DEFAULT 1";
	public static final String VALUE_VALUEBOOLTRUE_DEFAULT = "1";
	public static final String VALUE_VALUEBOOLFALSE_DEFAULT = "0";
	
	public static final String VALUE_CREATEFIELDSPEC_DEFAULT = "%1$s %2$s";
	public static final String VALUE_CREATEFIELDSEP_DEFAULT = ", ";

	public static final String VALUE_SELECTFIELDSPEC_DEFAULT = "%s";
	public static final String VALUE_SELECTFIELDSEP_DEFAULT = ", ";

	public static final String VALUE_INSERTFIELDSPEC_DEFAULT = "%s";
	public static final String VALUE_INSERTFIELDSEP_DEFAULT = ", ";
	public static final String VALUE_INSERTPLACEHOLDERSPEC_DEFAULT = "?";
	public static final String VALUE_INSERTPLACEHOLDERSEP_DEFAULT = ", ";
	
	public static final String VALUE_CREATETABLEPATTERN_DEFAULT = "CREATE TABLE IF NOT EXISTS %1$s (%2$s)";
	public static final String VALUE_CREATEPRIMARYFIELDINTEGERSPEC_DEFAULT = "%1$s INTEGER PRIMARY KEY AUTO_INCREMENT";
	public static final String VALUE_CREATETABLEPRIMARYKEYINTEGERPATTERN_DEFAULT = "CREATE TABLE IF NOT EXISTS %1$s (%2$s, %3$s)";
	
	public static final String VALUE_SUPPORTSLIMIT_DEFAULT = "true";
	public static final String VALUE_NEEDSTABLESEARCH_DEFAULT = "false";
	
	public static final String VALUE_SELECTMAXTIMESTAMPPATTERN_DEFAULT = "SELECT MAX(%1$s) FROM %2$s WHERE %3$s=? AND %4$s=?";
	public static final String VALUE_SELECTMINTIMESTAMPPATTERN_DEFAULT = "SELECT MIN(%1$s) FROM %2$s WHERE %3$s=? AND %4$s=?";
	
	public static final String VALUE_SELECTPATTERN_DEFAULT = "SELECT %1$s FROM %2$s";
	public static final String VALUE_SELECTPATTERNID_DEFAULT = "SELECT %1$s FROM %2$s WHERE %3$s=?";
	public static final String VALUE_SELECTFROMLATESTTIMESTAMPPATTERN_DEFAULT = "SELECT %1$s FROM %3$s WHERE %4$s=? AND %5$s=? ORDER BY %2$s DESC";
	
	public static final String VALUE_UPDATE_DEFAULT = "UPDATE %1$s SET %2$s=? WHERE %3$s=?";
	public static final String VALUE_SELECTRANGEFROM_DEFAULT = "SELECT %1$s FROM %2$s WHERE %3$s=? AND %4$s=? AND %5$s>=? ORDER BY %5$s DESC LIMIT ?";
	public static final String VALUE_SELECTRANGEFROMTO_DEFAULT = "SELECT %1$s FROM %2$s WHERE %3$s=? AND %4$s=? AND %5$s>=? AND %5$s<=? ORDER BY %5$s DESC LIMIT ?";
	
	public static final String VALUE_INSERT_DEFAULT = "INSERT INTO %1$s (%2$s) VALUES (%3$s)";
	
	public static final Pattern whitespace = Pattern.compile("\\s+");
	
	public DefaultDBProperties() {
		super();
		Field [] myFields = getClass().getDeclaredFields();
		for(Field f : myFields) {
			if((f.getType() == String.class) && ((f.getModifiers() & Modifier.STATIC ) == Modifier.STATIC) && f.getName().startsWith("KEY_")) {
				try {
					String defName = "VALUE_" + f.getName().substring(4) + "_DEFAULT";
					String keyString = (String)f.get(null);

					String defValue = null;
					for(Field v : myFields) {
						if( (v.getType() == String.class) && ((v.getModifiers() & Modifier.STATIC) == Modifier.STATIC)  && defName.equals(v.getName())) {
							defValue = (String)v.get(null);
							break;
						}
					}
					
					if(defValue != null) {
						put(keyString, defValue);
					}
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
