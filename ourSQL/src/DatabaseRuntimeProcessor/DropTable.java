package DatabaseRuntimeProcessor;

import Shared.Structures.Field;
import Shared.Structures.Metadata;
import Shared.Structures.Row;
import Shared.Structures.Table;
import StoredDataManager.Main.StoredDataManager;
import SystemCatalog.Constants;
import java.util.ArrayList;



 
 

 /*
 *@author Nicolas Jimenez
 *
*/
public class DropTable {

 
/* 
* @param nombreEsquema
 *@param nombreTabla
 */
 public void dropTable(String nombreEsquema, String nombreTabla) {

 if (!verifyIntegrity(nombreEsquema, nombreTabla)) {
 return;
 }

 deleteFromMetadata(nombreEsquema, nombreTabla);
 deleteFromDisc(nombreEsquema, nombreTabla);
 }

 
/* 
 *@param nombreEsquema
 *@param nombreTabla
 *@return
 */
 public boolean verifyIntegrity(String nombreEsquema, String nombreTabla) {

 StoredDataManager storer =  StoredDataManager.getInstance();
 Metadata meta = storer.deserealizateMetadata();

 ArrayList<ArrayList<String>> tablaTabla = meta.getMetadata().get(Constants.TABLES);

 for (int i = 0; i< tablaTabla.size(); i++) {

 ArrayList<String> fila = tablaTabla.get(i);

 if (fila.get(Constants.TABLE_FK).equals(true)) {

 return false;
 }
 }
 return true;
 }

 
 
 /*@param databaseName
 *@param nombreEsquema
 *@param nombreTabla
 */
 public void deleteFromMetadata(String databaseName, String nombreTabla) {

 StoredDataManager storer = StoredDataManager.getInstance();
 Metadata meta = storer.deserealizateMetadata();

 ArrayList<ArrayList<String>> tablaTabla = meta.getMetadata().get(Constants.TABLES);

 for (int i = 0; i< tablaTabla.size(); i++) {

 ArrayList<String> fila = tablaTabla.get(i);

 if (fila.get(Constants.TABLE_SCHNAME).equals(databaseName)) {

 meta.getMetadata().get(Constants.TABLES).remove(i);
 }
 }
 ArrayList<ArrayList<String>> tablaCol = meta.getMetadata().get(Constants.COLUMNS);

 for (int i = 0; i <tablaCol.size(); i++) {

 ArrayList<String> fila = tablaCol.get(i);

 if (fila.get(Constants.COLUMNS_SCHNAME).equals(databaseName)
 && fila.get(Constants.COLUMNS_TABNAME).equals(nombreTabla)) {

 meta.getMetadata().get(Constants.COLUMNS).remove(i);
 }
 }

 ArrayList<ArrayList<String>> tablaFor = meta.getMetadata().get(Constants.FOREIGNKEY);

 for (int i = 0; i< tablaFor.size(); i++) {

 ArrayList<String> fila = tablaFor.get(i);

 if (fila.get(Constants.FK_SCHNAME).equals(databaseName)) {

 meta.getMetadata().get(Constants.FOREIGNKEY).remove(i);
 }
 }
 ArrayList<ArrayList<ArrayList<String>>> metadata = meta.getMetadata();// variable donder se guarda al final
 meta.setMetadata(metadata);
 storer.serializeMetadata(meta);
 }

 public void deleteFromDisc(String databaseName, String tableName) {

 StoredDataManager temp =  StoredDataManager.getInstance();
 temp.initStoredDataManager(databaseName);
 System.out.println("Drop table " + temp.dropTable(tableName));
 }

}