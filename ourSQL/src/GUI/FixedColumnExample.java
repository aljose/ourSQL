/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GUI;

/**
 *
 * @author JoséAlberto
 */
// Example from http://www.crionics.com/products/opensource/faq/swing_ex/SwingExamples.html
/* (swing1.1beta3) */

import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.AbstractTableModel;

/**
 * @version 1.0 12/05/98
 */

public class FixedColumnExample {
  Object[][] data;

  Object[] column;

  JTable fixedTable, table;

  public FixedColumnExample() {

    data = new Object[][] { { "1", "11", "A", "", "", "", "", "" },
        { "2", "22", "", "B", "", "", "", "" },
        { "3", "33", "", "", "C", "", "", "" },
        { "4", "44", "", "", "", "D", "", "" },
        { "5", "55", "", "", "", "", "E", "" },
        { "6", "66", "", "", "", "", "", "F" } };
    column = new Object[] { "fixed 1", "fixed 2", "a", "b", "c", "d", "e",
        "f" };

    AbstractTableModel fixedModel = new AbstractTableModel() {
      public int getColumnCount() {
        return 2;
      }

      public int getRowCount() {
        return data.length;
      }

      public String getColumnName(int col) {
        return (String) column[col];
      }

      public Object getValueAt(int row, int col) {
        return data[row][col];
      }
    };
    AbstractTableModel model = new AbstractTableModel() {
      public int getColumnCount() {
        return column.length - 2;
      }

      public int getRowCount() {
        return data.length;
      }

      public String getColumnName(int col) {
        return (String) column[col + 2];
      }

      public Object getValueAt(int row, int col) {
        return data[row][col + 2];
      }

      public void setValueAt(Object obj, int row, int col) {
        data[row][col + 2] = obj;
      }

      public boolean CellEditable(int row, int col) {
        return true;
      }
    };

    fixedTable = new JTable(fixedModel);
    table = new JTable(model);
            
    fixedTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    fixedTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    table.getTableHeader().setReorderingAllowed(false);
    fixedTable.getTableHeader().setReorderingAllowed(false);
  }

}