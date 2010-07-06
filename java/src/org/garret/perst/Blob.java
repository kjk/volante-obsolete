package org.garret.perst;

/**
 * Interface to store/fetch large binary objects
 */
public interface Blob extends IPersistent, IResource { 
    /**
     * Get input stream. InputStream.availabe method can be used to get BLOB size
     * @return input stream with BLOB data
     */
    java.io.InputStream getInputStream();    

    /**
     * Get output stream to append data to the BLOB.
     * @return output srteam 
     */
    java.io.OutputStream getOutputStream();
};