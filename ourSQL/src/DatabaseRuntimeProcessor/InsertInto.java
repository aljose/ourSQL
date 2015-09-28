package DatabaseRuntimeProcessor;

/**
 * Clase creada con el objetivo de implementar el comando DML insert into. Se
 * recibe como parámetros el nombre de la tabla, un arraylist de las columnas a
 * modificar, un arraylist con los valores a ingresar en la nueva fila y el
 * esquema al que pertenece dicha tabla.
 *
 * @author Kevin
 */
import Shared.Structures.Row;
import Shared.Structures.Field;
import Shared.Structures.Metadata;
import java.util.ArrayList;
import StoredDataManager.Main.StoredDataManager;
import SystemCatalog.Constants;

public class InsertInto {

    private StoredDataManager insertManager;
    Metadata metadata = new Metadata();

    public void executeInsertion(String tableName, ArrayList<String> columns, ArrayList<String> values, String schemaName) {
        boolean doesExist;
        insertManager = new StoredDataManager();
        Row rowToInsert = new Row();
        doesExist = verifyExistence(schemaName, tableName, columns.get(0));
        
        if (doesExist) {
            System.out.println("existe la tabla");
            rowToInsert = buildRow(columns, values, tableName, schemaName);
            rowToInsert.setTableName(tableName);
            insertManager.initStoredDataManager(schemaName);
            System.out.println("datos" + rowToInsert.getTableName() + "+" + rowToInsert.getColumns().get(0).getContent());
            insertManager.insertIntoTable(rowToInsert);
            ArrayList<Row> arrayRow = insertManager.getAllTuplesFromTable(tableName);
            for (int i = 0; i < arrayRow.size(); i++) {
                ArrayList<Field> fieldList = arrayRow.get(i).getColumns();
                for (int u = 0; u < fieldList.size(); u++) {
                    System.out.print(fieldList.get(u).getContent() + "   ");
                }
                System.out.println();
            }
        } else {
            System.out.println("Operation can not be completed. Table does not exist.");
        }
    }

    private boolean verifyExistence(String schemaName, String tableName, String primaryKey) {
        metadata = insertManager.deserealizateMetadata();
        ArrayList<ArrayList<ArrayList<String>>> met = metadata.getMetadata();
        ArrayList<ArrayList<String>> metadataTables = met.get(Constants.TABLES);
        int length = metadataTables.size();
        ArrayList<String> row;
        String schemaCol;
        String tableCol;
        boolean result = false;
        boolean verifyPrimary;

        for (int i = 1; i < length; i++) {
            row = metadataTables.get(i);
            schemaCol = row.get(Constants.TABLE_SCHNAME);
            tableCol = row.get(Constants.TABLE_TABNAME);
            if (schemaCol.equals(schemaName) && tableCol.equals(tableName)) {
                verifyPrimary = verifyPK(primaryKey, row);
                if (verifyPrimary) {
                    result = true;
                    break;
                } else {
                    result = false;
                    System.out.println("Error. La llave primaria no corresponde con la primera columna de lo ingrsado.");
                    break;
                }

            } else {
                result = false;
            }
        }
        return result;
    }

    //private boolean elementsInOrder
    private boolean verifyPK(String primaryKey, ArrayList<String> row) {
        String currPK = row.get(Constants.TABLE_PK);
        return currPK.equals(primaryKey);
    }

    private Row buildRow(ArrayList<String> columns, ArrayList<String> values, String tableName, String schemaName) {
        Row newTuple = new Row();
        ArrayList<Field> temp = new ArrayList<Field>();
        Field element = new Field();
        for (int i = 0; i < columns.size(); i++) {
            if (i == 0) {
                element.setContent(values.get(i));
                element.setSchemaName(schemaName);
                element.setTableName(tableName);
                element.setPrimaryKey(true);
                temp.add(element);
            } else {
                element.setContent(values.get(i));
                element.setSchemaName(schemaName);
                element.setTableName(tableName);
                element.setPrimaryKey(false);
                temp.add(element);
            }
        }
        newTuple.setColumns(temp);
        newTuple.setTableName(tableName);
        return newTuple;
    }
}
