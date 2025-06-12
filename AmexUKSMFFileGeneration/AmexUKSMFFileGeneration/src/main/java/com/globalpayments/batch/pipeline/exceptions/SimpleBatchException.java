package com.globalpayments.batch.pipeline.exceptions;


public class SimpleBatchException extends Exception {

    private static final long serialVersionUID = -513118499829128767L;

    private final String message;

    public SimpleBatchException(String msg){
        super();
        this.message = msg;
    }
    @Override
    public String toString(){
        return this.message;
    }

}