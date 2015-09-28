package Shared.Structures;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Clase encargada de encapsular la metadata.
 *
 * @author Kevin
 */
public class Metadata implements Serializable {

    private ArrayList<ArrayList<ArrayList<String>>> metadata;

    public Metadata(ArrayList<ArrayList<ArrayList<String>>> metadata) {
        this.metadata = metadata;
    }

    public Metadata() {
        this.metadata = null;
    }

    /**
     * @return the metadata
     */
    public ArrayList<ArrayList<ArrayList<String>>> getMetadata() {
        return metadata;
    }

    /**
     * @param metadata the metadata to set
     */
    public void setMetadata(ArrayList<ArrayList<ArrayList<String>>> metadata) {
        this.metadata = metadata;
    }

}
