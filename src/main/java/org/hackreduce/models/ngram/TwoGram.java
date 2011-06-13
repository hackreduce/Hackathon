package org.hackreduce.models.ngram;

import org.apache.hadoop.io.Text;


public class TwoGram extends Gram {

    private String gram1;
    private String gram2;

    public TwoGram(String inputString) {
        super(inputString);
        setGram1(getLineElements()[0]);
        setGram2(getLineElements()[1]);
    }

    public TwoGram(Text inputText) {
        this(inputText.toString());
    }


    public String getGram1() {
        return gram1;
    }
    public void setGram1(String gram1) {
        this.gram1 = gram1;
    }
    public String getGram2() {
        return gram2;
    }
    public void setGram2(String gram2) {
        this.gram2 = gram2;
    }

    @Override
    public String getGrams() {
        return getGram1() + " " + getGram2();
    }
}
