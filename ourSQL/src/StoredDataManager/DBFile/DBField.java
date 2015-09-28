package StoredDataManager.DBFile;

/**
 * Created by manzumbado on 25/09/15.
 */

/**
 * Clase que representa un campo escrito en disco
 */

public class DBField {



    private int mFieldLength;
    private String mValue;

    public DBField(){
    }

    public DBField(String value, int length){
        this.mValue=value;
        this.mFieldLength=length;
    }

    public int getFieldLength() {
        return mFieldLength;
    }

    public String getValue() {
        return mValue;
    }

    public void setValue(String mValue) {
        this.mValue = mValue;
    }

    public void setFieldLength(int mFieldLength) {

        this.mFieldLength = mFieldLength;
    }

    public String toString(){
        String length =  Integer.toString(this.getFieldLength());
        String value = this.getValue();
        return "Length: "+ length+ " Value: "+value;
    }
}