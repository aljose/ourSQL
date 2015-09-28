package SystemCatalog;

import Shared.Structures.Metadata;
import StoredDataManager.Main.StoredDataManager;
import java.util.ArrayList;

/**
 *
 * @author Kevin
 */
/**
 * Clase encargada de construir el System Catalog que almacena la información
 * que controla urSQL y las bases de datos existentes.
 *
 */
public class CreateMetadata {

    ArrayList<ArrayList<ArrayList<String>>> metadata = new ArrayList<ArrayList<ArrayList<String>>>();

    public void buildSystemCatalog() {
        //Definición del nombre del catálogo del sistema.   
        String catalogName = "system";

        //Definición del nombre de las tablas que serán ingresadas en el System Catalog.
        String schemaName = "Schema";
        String tableName = "Table";
        String columnName = "Column";
        String queryLogName = "QueryLog";
        String foreignKeyName = "ForeignKey";

        //Definición de las columnas a ingresar en tabla Schema.
        ArrayList<String> schemaColumns = new ArrayList<String>();
        String schema = "SchemaName";
        schemaColumns.add(schema);
        ArrayList<ArrayList<String>> schemaTable = new ArrayList<ArrayList<String>>();
        schemaTable.add(schemaColumns);

        //Definición de las columnas a ingresar en tabla Table.
        ArrayList<String> tableColumns = new ArrayList<String>();
        String tableFirstCol = "SchemaName";
        String tableSecondCol = "TableName";
        String tableThirdCol = "PrimaryKey";
        String tableFourthCol = "ForeignKey";
        tableColumns.add(tableFirstCol);
        tableColumns.add(tableSecondCol);
        tableColumns.add(tableThirdCol);
        tableColumns.add(tableFourthCol);
        ArrayList<ArrayList<String>> tablesTable = new ArrayList<ArrayList<String>>();
        tablesTable.add(tableColumns);

        //Definición de las columnas a ingresar en la tabla Column.
        ArrayList<String> columnColumns = new ArrayList<String>();
        String columnFirstCol = "Schema";
        String columnSecondCol = "Table";
        String columnThirdCol = "Column";
        String columnFourthCol = "Type";
        String columnFifthCol = "Constraint";
        String columnSixthCol = "PrimaryKey'";
        String columnSeventhCol = "ForeignKey";
        columnColumns.add(columnFirstCol);
        columnColumns.add(columnSecondCol);
        columnColumns.add(columnThirdCol);
        columnColumns.add(columnFourthCol);
        columnColumns.add(columnFifthCol);
        columnColumns.add(columnSixthCol);
        columnColumns.add(columnSeventhCol);
        ArrayList<ArrayList<String>> columnsTable = new ArrayList<ArrayList<String>>();
        columnsTable.add(columnColumns);

//Definición de las columnas a ingresar en la tabla Query Log.
        ArrayList<String> queryColumns = new ArrayList<String>();
        String queryFirstCol = "Schema";
        String querySecondCol = "Table";
        String queryThirdCol = "Query";
        queryColumns.add(queryFirstCol);
        queryColumns.add(querySecondCol);
        queryColumns.add(queryThirdCol);
        ArrayList<ArrayList<String>> queryTable = new ArrayList<ArrayList<String>>();
        queryTable.add(queryColumns);

        //Definición de las columnas a ingresar en la tabla Foreign Key.
        ArrayList<String> foreignKeyColumns = new ArrayList<String>();
        String foreignFirstCol = "Schema";
        String foreignSecondCol = "Column";
        String foreignThirdCol = "OriginTable";
        String foreignFourthCol = "ColumnReferenced";
        String foreignFifthCol = "TableReferenced";
        foreignKeyColumns.add(foreignFirstCol);
        foreignKeyColumns.add(foreignSecondCol);
        foreignKeyColumns.add(foreignThirdCol);
        foreignKeyColumns.add(foreignFourthCol);
        foreignKeyColumns.add(foreignFifthCol);
        ArrayList<ArrayList<String>> foreignTable = new ArrayList<ArrayList<String>>();
        foreignTable.add(foreignKeyColumns);

        Metadata met;
        StoredDataManager storedDataManager = new StoredDataManager();
        met = storedDataManager.deserealizateMetadata();
        if (met == null) {
            metadata.add(schemaTable);
            metadata.add(tablesTable);
            metadata.add(columnsTable);
            metadata.add(queryTable);
            metadata.add(foreignTable);
            met = new Metadata(metadata);
            storedDataManager.serializeMetadata(met);
        }
    }
}
