package DatabaseRuntimeProcessor;

import Shared.Structures.Metadata;
import StoredDataManager.Main.StoredDataManager;
import SystemCatalog.Constants;
import java.util.ArrayList;

/*
 *  Alter Table!
 *  Altera la metadata de una tabla para anadir un constraint (llave foranea)
 * 
 * Something different!
 */
/**
 *
 * @author Nicolas Jimenez
 */
public class AlterTable {

    public void alterTable(String schemaName, String tableName, String columnName, String tableReferenced,
            String referencedColumn) {

        StoredDataManager storer = new StoredDataManager();
        Metadata meta = storer.deserealizateMetadata();
        ArrayList<String> foreignTable = new ArrayList<>();

        foreignTable.add(tableName);
        foreignTable.add(schemaName);
        foreignTable.add(columnName);
        foreignTable.add(referencedColumn);
        foreignTable.add(tableReferenced);

        meta.getMetadata().get(Constants.FOREIGNKEY).add(foreignTable);

        ArrayList<String> columnTable = new ArrayList<>();

        ArrayList<ArrayList<String>> filas = meta.getMetadata().get(Constants.COLUMNS);

        for (int i = 0; i < filas.size(); i++) {

            ArrayList<String> fila = filas.get(i);
            if (fila.get(Constants.COLUMNS_COL).equals(columnName)
                    && fila.get(Constants.COLUMNS_TABNAME).equals(tableName)
                    && fila.get(Constants.COLUMNS_SCHNAME).equals(schemaName)) {

                meta.getMetadata().get(Constants.COLUMNS).get(i).set(Constants.COLUMNS_FK, "true");
            }
        }

        ArrayList<ArrayList<ArrayList<String>>> metadata = meta.getMetadata();// variable donder se guarda al final
        meta.setMetadata(metadata);
        storer.serializeMetadata(meta);
    }
}
