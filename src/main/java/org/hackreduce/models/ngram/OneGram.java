package org.hackreduce.models.ngram;

import org.apache.hadoop.io.Text;


public class OneGram extends Gram {

    private String gram1;

    public OneGram(String inputString) {
        super(inputString);
        setGram1(getLineElements()[0]);
    }

    public OneGram(Text inputText) {
        this(inputText.toString());
    }


    public String getGram1() {
        return gram1;
    }
    public void setGram1(String gram1) {
        this.gram1 = gram1;
    }

    @Override
    public String getGrams() {
        return getGram1();
    }
}
