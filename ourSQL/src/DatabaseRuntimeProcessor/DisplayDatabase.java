package DatabaseRuntimeProcessor;

import Shared.Structures.Metadata;
import StoredDataManager.Main.StoredDataManager;
import SystemCatalog.Constants;
import java.util.ArrayList;

/**
 * Clase encargada de hacer el comando DatabaseRuntimeProcessor.DisplayDatabase.
 *
 * @author Kevin Umaña
 */
public class DisplayDatabase {

private StoredDataManager dataManager;

    // FetchMetadata metadata = new FetchMetadata();
    /**
     *
     * @param schemaName
     * @return String that represents the elements contained in the specified
     * schema.
     */
    public String displayDatabase(String schemaName) {
        boolean doesExist;
        ArrayList<String> tablesName = new ArrayList<String>();
        doesExist = verifyExistence(schemaName);
        String resultToPrint1 = "";
        String resultToPrint2 = "";
        if (doesExist) {
            tablesName = gatherMatchingTables(schemaName);
            resultToPrint1 = printTables(tablesName, schemaName);
            resultToPrint2 = describeTables(schemaName, tablesName);
        } else {
            System.out.println("Error. There is no schema with such name. Please try again.");
        }
        resultToPrint1 = resultToPrint1.concat(resultToPrint2);
        return resultToPrint1;
    }

    /**/
    private boolean verifyExistence(String schemaName) {
        Metadata metadata = new Metadata();
        metadata = dataManager.deserealizateMetadata();
        ArrayList<ArrayList<ArrayList<String>>> met = metadata.getMetadata();
        ArrayList<String> row;
        String element;
        boolean result = false;
        for (int i = 0; i < met.get(Constants.SCHEMA).size(); i++) {
            row = met.get(Constants.SCHEMA).get(i);
            element = row.get(i);
            if (element.equals(schemaName)) {
                result = true;
                break;
            } else {
                result = false;
            }
        }
        return result;
    }

    @SuppressWarnings("empty-statement")
    private ArrayList<String> gatherMatchingTables(String schemaName) {
        Metadata metadata = new Metadata();
        metadata = dataManager.deserealizateMetadata();
        ArrayList<ArrayList<ArrayList<String>>> met = metadata.getMetadata();
        ArrayList<ArrayList<String>> tableSet;
        tableSet = met.get(Constants.TABLES);
        int i = 1;
        ArrayList<String> tables = new ArrayList<String>();
        while (i < tableSet.size()) {
            ArrayList<String> row = tableSet.get(i);
            String element = row.get(Constants.TABLE_SCHNAME);
            if (element.equals(schemaName)) {
                tables.add(element);
            } else {
                ;
            }
        }
        i++;
        return tables;
    }

    private String printTables(ArrayList<String> tablesToDisplay, String schemaName) {
        //StringBuilder en caso de que no se imprima en consola.
        StringBuilder tempResult = new StringBuilder();
        tempResult.append("+------------------------------------------------------------------------------+\n");
        tempResult.append("| Lista de tablas del esquema: ");
        tempResult.append(schemaName);
        tempResult.append(" |\n");
        tempResult.append("+------------------------------------------------------------------------------+\n");
        System.out.println("+------------------------------------------------------------------------------+\n");
        System.out.println("| Lista de tablas del esquema: " + schemaName + " |\n");
        System.out.println("+------------------------------------------------------------------------------+\n");
        for (int i = 0; i < tablesToDisplay.size(); i++) {
            if (i + 1 == tablesToDisplay.size()) {
                System.out.println("| " + tablesToDisplay.get(i) + " |\n");
                System.out.println("+------------------------------------------------------------------------------+\n");
                tempResult.append("| ");
                tempResult.append(tablesToDisplay.get(i));
                tempResult.append(" |\n");
                tempResult.append("+------------------------------------------------------------------------------+\n");
            } else {
                System.out.println("| " + tablesToDisplay.get(i) + " |\n");
                tempResult.append("| ");
                tempResult.append(tablesToDisplay.get(i));
                tempResult.append(" |\n");
            }
        }
        String result = tempResult.toString();
        return result;
    }

    private String describeTables(String schemaName, ArrayList<String> tablesName) {
        Metadata metadata = new Metadata();
        metadata = dataManager.deserealizateMetadata();
        ArrayList<ArrayList<ArrayList<String>>> met = metadata.getMetadata();
        ArrayList<ArrayList<String>> columnTable;
        columnTable = met.get(Constants.COLUMNS);
        ArrayList<String> row;
        StringBuilder result = new StringBuilder();
        result.append("Lista de columnas para la base de datos: ");
        result.append(schemaName);
        result.append("\n");
        for (int k = 0; k < tablesName.size(); k++) {
            result.append("La tabla: ");
            result.append(tablesName.get(k));
            result.append(" posee las siguientes columnas: \n");
            result.append("Nombre               Tipo               IsNull               isPK               isFK                \n");
            for (int i = 0; i < columnTable.size(); i++) {
                row = columnTable.get(i);
                String currSchema = row.get(Constants.COLUMNS_SCHNAME);
                String currTable = row.get(Constants.COLUMNS_TABNAME);
                if (currSchema.equals(schemaName) && currTable.equals(tablesName.get(k))) {
                    result.append(row.get(Constants.COLUMNS_COL));
                    result.append("                ");
                    result.append(row.get(Constants.COLUMNS_TYPE));
                    result.append("                ");
                    result.append(row.get(Constants.COLUMNS_CONSTRAINT));
                    result.append("                ");
                    result.append(row.get(Constants.COLUMNS_PK));
                    result.append("                ");
                    result.append(row.get(Constants.COLUMNS_FK));
                    result.append("                 \n");
                }
            }
        }
        /*
         for (int j = 0; j < columnTable.getRows().get(i).getColumns().size(); j++) { //Cantidad elementos de una fila
         Field element = columnTable.getRows().get(i).getColumns().get(j); //Elemento
         if (element.getSchemaName().equals(schemaName)
         && element.getTableName().equals(tablesName.get(k))) {
         result.append(element.getContent());
         result.append(" ");
         result.append(element.getType());
         result.append(" ");
         result.append(element.getIsNull());
         result.append(" ");
         result.append(element.isPrimaryKey());
         result.append(" \n");
         }
         }

         }

         }*/
        return result.toString();
    }
}
