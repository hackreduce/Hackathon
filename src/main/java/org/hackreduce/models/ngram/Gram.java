package org.hackreduce.models.ngram;


/**
 * Represents a line in the Ngram dataset (http://ngrams.googlelabs.com/datasets).
 *
 * File format: Each of the numbered files below is zipped tab-separated data.
 * (Yes, we know the files have .csv extensions.) Each line has the following format:
 * ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
 *
 */
public abstract class Gram {

    private String[] lineElements;
    private int year;
    private int matchCount;
    private int pageCount;
    private int volumeCount;

    public Gram(String inputString) {
        setLineElements(inputString.split("\t"));
        int length = getLineElements().length;

        // In order for this to be a valid line, there should be at least 5 elements: a 1gram word plus the year,
        // match_count, page_count and volume_count.
        if (length < 5) {
            throw new IllegalArgumentException("Input string given must have at least " +
                    "5 tab-separated values to be a valid ngram line");
        }

        // The last 4 elements in the array should always be the same whether it's a 1gram, 2gram, etc.
        int i = (length - 4);
        setYear(Integer.parseInt(getLineElements()[i++]));
        setMatchCount(Integer.parseInt(getLineElements()[i++]));
        setPageCount(Integer.parseInt(getLineElements()[i++]));
        setVolumeCount(Integer.parseInt(getLineElements()[i++]));
    }


    public String[] getLineElements() {
        return lineElements;
    }
    public void setLineElements(String[] lineElements) {
        this.lineElements = lineElements;
    }
    public int getYear() {
        return year;
    }
    public void setYear(int year) {
        this.year = year;
    }
    public int getMatchCount() {
        return matchCount;
    }
    public void setMatchCount(int matchCount) {
        this.matchCount = matchCount;
    }
    public int getPageCount() {
        return pageCount;
    }
    public void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }
    public int getVolumeCount() {
        return volumeCount;
    }
    public void setVolumeCount(int volumeCount) {
        this.volumeCount = volumeCount;
    }
    public abstract String getGrams();

}
