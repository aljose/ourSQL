package DatabaseRuntimeProcessor;
/*
 * DatabaseRuntimeProcessor.DropDatabase
 * Elimina un esquema de urSQL
 * con todo lo que tenga asociado como tablas y columnas
 *
 */

import Shared.Structures.Field;
import Shared.Structures.Metadata;
import Shared.Structures.Row;
import Shared.Structures.Table;
import StoredDataManager.Main.StoredDataManager;
import SystemCatalog.Constants;
import java.util.ArrayList;
import SystemCatalog.FetchMetadata;
import SystemCatalog.WriteMetadata;

/**
 *
 * @author Nicolas Jimenez
 */
public class DropDatabase {

    public void dropDatabase(String dataBase) {

//        if (!verifyExist(dataBase)) {
//            System.out.println("No se puede eliminar la base de datos ya que no existe la base de datos");
//            return;
//        }
       // deleteMetadata(dataBase);
        deleteSchema(dataBase);
    }

    /**
     * Verifica que la base de datos no exista.
     *
     * @param dataBaseSchemas
     * @param dataBase
     * @return
     */
    private boolean verifyExist(String dataBase) {

        StoredDataManager storer = new StoredDataManager();
        Metadata meta = storer.deserealizateMetadata();

//        ArrayList<ArrayList<String>> metadata = meta.getMetadata().get(Constants.SCHEMA);

//        for (ArrayList<String> fila : metadata) {
//
//            for (String campo : fila) {
//
//                if (campo.equals(dataBase)) {
//                    return true;
//                }
//            }
//        }
        return false;
    }

    /**
     * Elimina todo lo que este asociado al esquema en la metadata, incluyendo
     * tablas y columnas.
     *
     * @param databaseName
     */
    private void deleteMetadata(String databaseName) {

        StoredDataManager storer = new StoredDataManager();
        Metadata meta = storer.deserealizateMetadata();

        ArrayList<ArrayList<String>> tablaEsquema = meta.getMetadata().get(Constants.SCHEMA);

        for (int i = 0; i < tablaEsquema.size(); i++) {

            ArrayList<String> fila = tablaEsquema.get(i);

            if (fila.get(Constants.SCHEMA_SCHNAME).equals(databaseName)) {

                meta.getMetadata().get(Constants.SCHEMA).get(Constants.SCHEMA_SCHNAME).remove(i);
            }
        }
        ArrayList<ArrayList<String>> tablaTabla = meta.getMetadata().get(Constants.TABLES);

        for (int i = 0; i < tablaTabla.size(); i++) {

            ArrayList<String> fila = tablaTabla.get(i);

            if (fila.get(Constants.TABLE_SCHNAME).equals(databaseName)) {

                meta.getMetadata().get(Constants.TABLES).get(Constants.TABLE_SCHNAME).remove(i);
            }
        }
        ArrayList<ArrayList<String>> tablaCol = meta.getMetadata().get(Constants.COLUMNS);

        for (int i = 0; i < tablaCol.size(); i++) {

            ArrayList<String> fila = tablaCol.get(i);

            if (fila.get(Constants.COLUMNS_SCHNAME).equals(databaseName)) {

                meta.getMetadata().get(Constants.COLUMNS).get(Constants.COLUMNS_SCHNAME).remove(i);
            }
        }

        ArrayList<ArrayList<String>> tablaFor = meta.getMetadata().get(Constants.FOREIGNKEY);

        for (int i = 0; i < tablaFor.size(); i++) {

            ArrayList<String> fila = tablaFor.get(i);

            if (fila.get(Constants.FK_SCHNAME).equals(databaseName)) {

                meta.getMetadata().get(Constants.FOREIGNKEY).get(Constants.FK_SCHNAME).remove(i);
            }
        }
        ArrayList<ArrayList<ArrayList<String>>> metadata = meta.getMetadata();// variable donder se guarda al final
        meta.setMetadata(metadata);
        storer.serializeMetadata(meta);

    }

    /**
     * Elimina un esquema de disco.
     *
     * @param dataBase
     */
    private void deleteSchema(String dataBase) {

        StoredDataManager temp = new StoredDataManager();
        temp.dropDatabase(dataBase);

    }

}
