package org.hackreduce.models;

import org.apache.hadoop.io.Text;


/**
 * Model created in order to access a record from the million song dataset (TSV format) in Java. Getters
 * and setters are present for every field.
 *
 * The TSV format of the dataset was converted by Echo Nest for HackReduce, so it might not be present for
 * download on the site.
 *
 * @see http://labrosa.ee.columbia.edu/millionsong/pages/getting-dataset
 *
 * @author greg
 *
 */
public class MSD {

    public static String LIST_SEPARATOR = ",";
    
    private String trackId;
    private int analysisSampleRate;
    private int artist7DigitalId;
    private double artistFamiliarity;
    private double artistHotness;
    private String artistId;
    private double artistLatitude;
    private String artistLocation;
    private double artistLongitude;
    private String artistMbid;
    private String[] artistMbtags;
    private int artistMbTagsCount;
    private String artistName;
    private int artistPlaymeid;
    private String[] artistTerms;
    private double[] artistTermsFreq;
    private double[] artistTermsWeight;
    private String audioMd5;
    private double[] barsConfidence;
    private double[] barsStart;
    private double[] beatsConfidence;
    private double[] beatsStart;
    private double danceability;
    private double duration;
    private double endOfFadeIn;
    private double energy;
    private int key;
    private double keyConfidence;
    private double loudness;
    private int mode;
    private double modeConfidence;
    private String release;
    private int release7digitalid;
    private double[] sectionsConfidence;
    private double[] sectionsStart;
    private double[] segmentsConfidence;
    private double[] segmentsLoudnessMax;
    private double[] segmentsLoudnessMaxTime;
    private double[] segmentsLoudnessStart;
    private double[] segmentsPitches;
    private double[] segmentsStart;
    private double[] segmentsTimbre;
    private String[] similarArtists;
    private double songHotness;
    private String songId;
    private double startOfFadeOut;
    private double[] tatumsConfidence;
    private double[] tatumsStart;
    private double tempo;
    private int timeSignature;
    private double timeSignatureConfidence;
    private String title;
    private int track7digitalid;
    private int year;


    public MSD(String inputString) {
        String[] fields = inputString.split("\t");

        if (fields.length != 54)
            throw new IllegalArgumentException("Input string given did not match the expected MSD tsv format");

        setTrackId(fields[0]);
        setAnalysisSampleRate(parseInt(fields[1]));
        setArtist7DigitalId(parseInt(fields[2]));
        setArtistFamiliarity(parseDouble(fields[3]));
        setArtistHotness(parseDouble(fields[4]));
        setArtistId(fields[5]);
        setArtistLatitude(parseDouble(fields[6]));
        setArtistLocation(fields[7]);
        setArtistLongitude(parseDouble(fields[8]));
        setArtistMbid(fields[9]);
        setArtistMbtags(fields[10].split(LIST_SEPARATOR));
        setArtistMbTagsCount(parseInt(fields[11]));
        setArtistName(fields[12]);
        setArtistPlaymeid(parseInt(fields[13]));
        setArtistTerms(fields[14].split(LIST_SEPARATOR));
        setArtistTermsFreq(parseDoubleArray(fields[15]));
        setArtistTermsWeight(parseDoubleArray(fields[16]));
        setAudioMd5(fields[17]);
        setBarsConfidence(parseDoubleArray(fields[18]));
        setBarsStart(parseDoubleArray(fields[19]));
        setBeatsConfidence(parseDoubleArray(fields[20]));
        setBeatsStart(parseDoubleArray(fields[21]));
        setDanceability(parseDouble(fields[22]));
        setDuration(parseDouble(fields[23]));
        setEndOfFadeIn(parseDouble(fields[24]));
        setEnergy(parseDouble(fields[25]));
        setKey(parseInt(fields[26]));
        setKeyConfidence(parseDouble(fields[27]));
        setLoudness(parseDouble(fields[28]));
        setMode(parseInt(fields[29]));
        setModeConfidence(parseDouble(fields[30]));
        setRelease(fields[31]);
        setRelease7digitalid(parseInt(fields[32]));
        setSectionsConfidence(parseDoubleArray(fields[33]));
        setSectionsStart(parseDoubleArray(fields[34]));
        setSegmentsConfidence(parseDoubleArray(fields[35]));
        setSegmentsLoudnessMax(parseDoubleArray(fields[36]));
        setSegmentsLoudnessMaxTime(parseDoubleArray(fields[37]));
        setSegmentsLoudnessStart(parseDoubleArray(fields[38]));
        setSegmentsPitches(parseDoubleArray(fields[39]));
        setSegmentsStart(parseDoubleArray(fields[40]));
        setSegmentsTimbre(parseDoubleArray(fields[41]));
        setSimilarArtists(fields[42].split(LIST_SEPARATOR));
        setSongHotness(parseDouble(fields[43]));
        setSongId(fields[44]);
        setStartOfFadeOut(parseDouble(fields[45]));
        setTatumsConfidence(parseDoubleArray(fields[46]));
        setTatumsStart(parseDoubleArray(fields[47]));
        setTempo(parseDouble(fields[48]));
        setTimeSignature(parseInt(fields[49]));
        setTimeSignatureConfidence(parseDouble(fields[50]));
        setTitle(fields[51]);
        setTrack7digitalid(parseInt(fields[52]));
        setYear(parseInt(fields[53]));
    }

    public MSD(Text inputText) {
        this(inputText.toString());
    }


    public String getTrackId() {
        return trackId;
    }

    public void setTrackId(String trackId) {
        this.trackId = trackId;
    }

    public int getAnalysisSampleRate() {
        return analysisSampleRate;
    }

    public void setAnalysisSampleRate(int analysisSampleRate) {
        this.analysisSampleRate = analysisSampleRate;
    }

    public int getArtist7DigitalId() {
        return artist7DigitalId;
    }

    public void setArtist7DigitalId(int artist7DigitalId) {
        this.artist7DigitalId = artist7DigitalId;
    }

    public double getArtistFamiliarity() {
        return artistFamiliarity;
    }

    public void setArtistFamiliarity(double artistFamiliarity) {
        this.artistFamiliarity = artistFamiliarity;
    }

    public double getArtistHotness() {
        return artistHotness;
    }

    public void setArtistHotness(double artistHotness) {
        this.artistHotness = artistHotness;
    }

    public String getArtistId() {
        return artistId;
    }

    public void setArtistId(String artistId) {
        this.artistId = artistId;
    }

    public double getArtistLatitude() {
        return artistLatitude;
    }

    public void setArtistLatitude(double artistLatitude) {
        this.artistLatitude = artistLatitude;
    }

    public String getArtistLocation() {
        return artistLocation;
    }

    public void setArtistLocation(String artistLocation) {
        this.artistLocation = artistLocation;
    }

    public double getArtistLongitude() {
        return artistLongitude;
    }

    public void setArtistLongitude(double artistLongitude) {
        this.artistLongitude = artistLongitude;
    }

    public String getArtistMbid() {
        return artistMbid;
    }

    public void setArtistMbid(String artistMbid) {
        this.artistMbid = artistMbid;
    }

    public String[] getArtistMbtags() {
        return artistMbtags;
    }

    public void setArtistMbtags(String[] artistMbtags) {
        this.artistMbtags = artistMbtags;
    }

    public int getArtistMbTagsCount() {
        return artistMbTagsCount;
    }

    public void setArtistMbTagsCount(int artistMbTagsCount) {
        this.artistMbTagsCount = artistMbTagsCount;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public int getArtistPlaymeid() {
        return artistPlaymeid;
    }

    public void setArtistPlaymeid(int artistPlaymeid) {
        this.artistPlaymeid = artistPlaymeid;
    }

    public String[] getArtistTerms() {
        return artistTerms;
    }

    public void setArtistTerms(String[] artistTerms) {
        this.artistTerms = artistTerms;
    }

    public double[] getArtistTermsFreq() {
        return artistTermsFreq;
    }

    public void setArtistTermsFreq(double[] artistTermsFreq) {
        this.artistTermsFreq = artistTermsFreq;
    }

    public double[] getArtistTermsWeight() {
        return artistTermsWeight;
    }

    public void setArtistTermsWeight(double[] artistTermsWeight) {
        this.artistTermsWeight = artistTermsWeight;
    }

    public String getAudioMd5() {
        return audioMd5;
    }

    public void setAudioMd5(String audioMd5) {
        this.audioMd5 = audioMd5;
    }

    public double[] getBarsConfidence() {
        return barsConfidence;
    }

    public void setBarsConfidence(double[] barsConfidence) {
        this.barsConfidence = barsConfidence;
    }

    public double[] getBarsStart() {
        return barsStart;
    }

    public void setBarsStart(double[] barsStart) {
        this.barsStart = barsStart;
    }

    public double[] getBeatsConfidence() {
        return beatsConfidence;
    }

    public void setBeatsConfidence(double[] beatsConfidence) {
        this.beatsConfidence = beatsConfidence;
    }

    public double[] getBeatsStart() {
        return beatsStart;
    }

    public void setBeatsStart(double[] beatsStart) {
        this.beatsStart = beatsStart;
    }

    public double getDanceability() {
        return danceability;
    }

    public void setDanceability(double danceability) {
        this.danceability = danceability;
    }

    public double getDuration() {
        return duration;
    }

    public void setDuration(double duration) {
        this.duration = duration;
    }

    public double getEndOfFadeIn() {
        return endOfFadeIn;
    }

    public void setEndOfFadeIn(double endOfFadeIn) {
        this.endOfFadeIn = endOfFadeIn;
    }

    public double getEnergy() {
        return energy;
    }

    public void setEnergy(double energy) {
        this.energy = energy;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public double getKeyConfidence() {
        return keyConfidence;
    }

    public void setKeyConfidence(double keyConfidence) {
        this.keyConfidence = keyConfidence;
    }

    public double getLoudness() {
        return loudness;
    }

    public void setLoudness(double loudness) {
        this.loudness = loudness;
    }

    public int getMode() {
        return mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public double getModeConfidence() {
        return modeConfidence;
    }

    public void setModeConfidence(double modeConfidence) {
        this.modeConfidence = modeConfidence;
    }

    public String getRelease() {
        return release;
    }

    public void setRelease(String release) {
        this.release = release;
    }

    public int getRelease7digitalid() {
        return release7digitalid;
    }

    public void setRelease7digitalid(int release7digitalid) {
        this.release7digitalid = release7digitalid;
    }

    public double[] getSectionsConfidence() {
        return sectionsConfidence;
    }

    public void setSectionsConfidence(double[] sectionsConfidence) {
        this.sectionsConfidence = sectionsConfidence;
    }

    public double[] getSectionsStart() {
        return sectionsStart;
    }

    public void setSectionsStart(double[] sectionsStart) {
        this.sectionsStart = sectionsStart;
    }

    public double[] getSegmentsConfidence() {
        return segmentsConfidence;
    }

    public void setSegmentsConfidence(double[] segmentsConfidence) {
        this.segmentsConfidence = segmentsConfidence;
    }

    public double[] getSegmentsLoudnessMax() {
        return segmentsLoudnessMax;
    }

    public void setSegmentsLoudnessMax(double[] segmentsLoudnessMax) {
        this.segmentsLoudnessMax = segmentsLoudnessMax;
    }

    public double[] getSegmentsLoudnessMaxTime() {
        return segmentsLoudnessMaxTime;
    }

    public void setSegmentsLoudnessMaxTime(double[] segmentsLoudnessMaxTime) {
        this.segmentsLoudnessMaxTime = segmentsLoudnessMaxTime;
    }

    public double[] getSegmentsLoudnessStart() {
        return segmentsLoudnessStart;
    }

    public void setSegmentsLoudnessStart(double[] segmentsLoudnessStart) {
        this.segmentsLoudnessStart = segmentsLoudnessStart;
    }

    public double[] getSegmentsPitches() {
        return segmentsPitches;
    }

    public void setSegmentsPitches(double[] segmentsPitches) {
        this.segmentsPitches = segmentsPitches;
    }

    public double[] getSegmentsStart() {
        return segmentsStart;
    }

    public void setSegmentsStart(double[] segmentsStart) {
        this.segmentsStart = segmentsStart;
    }

    public double[] getSegmentsTimbre() {
        return segmentsTimbre;
    }

    public void setSegmentsTimbre(double[] segmentsTimbre) {
        this.segmentsTimbre = segmentsTimbre;
    }

    public String[] getSimilarArtists() {
        return similarArtists;
    }

    public void setSimilarArtists(String[] similarArtists) {
        this.similarArtists = similarArtists;
    }

    public double getSongHotness() {
        return songHotness;
    }

    public void setSongHotness(double songHotness) {
        this.songHotness = songHotness;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public double getStartOfFadeOut() {
        return startOfFadeOut;
    }

    public void setStartOfFadeOut(double startOfFadeOut) {
        this.startOfFadeOut = startOfFadeOut;
    }

    public double[] getTatumsConfidence() {
        return tatumsConfidence;
    }

    public void setTatumsConfidence(double[] tatumsConfidence) {
        this.tatumsConfidence = tatumsConfidence;
    }

    public double[] getTatumsStart() {
        return tatumsStart;
    }

    public void setTatumsStart(double[] tatumsStart) {
        this.tatumsStart = tatumsStart;
    }

    public double getTempo() {
        return tempo;
    }

    public void setTempo(double tempo) {
        this.tempo = tempo;
    }

    public int getTimeSignature() {
        return timeSignature;
    }

    public void setTimeSignature(int timeSignature) {
        this.timeSignature = timeSignature;
    }

    public double getTimeSignatureConfidence() {
        return timeSignatureConfidence;
    }

    public void setTimeSignatureConfidence(double timeSignatureConfidence) {
        this.timeSignatureConfidence = timeSignatureConfidence;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getTrack7digitalid() {
        return track7digitalid;
    }

    public void setTrack7digitalid(int track7digitalid) {
        this.track7digitalid = track7digitalid;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    /**
     * Given a string containing comma separated values, split them up, parse them as double numbers
     * and return the array that it represents.
     *
     * @param inputString
     * @return 
     */
    public static double[] parseDoubleArray(String inputString) {
        String[] items = inputString.split(LIST_SEPARATOR);
        double[] result = new double[items.length];
        int index = 0;

        for (String item : items) {
            if (item == null) continue;
            try {
                result[index++] = parseDouble(item);
            } catch (NumberFormatException e) {
                
            }
        }

        return result;
    }

    /**
     * Parses the given string in order to extract the double number contained within. Defaults
     * to returning {@link Double#MIN_VALUE} if none could be found, or an exception occurred while parsing.
     *
     * @param inputString
     * @return the value as a double, or {@link Double#MIN_VALUE} if none could be parsed.
     */
    public static double parseDouble(String inputString) {
        try {
            return Double.parseDouble(inputString);
        } catch (NumberFormatException e) {
            return Double.MIN_VALUE;
        }
    }

    /**
     * Parses the given string in order to extract the integer number contained within. Defaults
     * to returning {@link Integer#MIN_VALUE} if none could be found, or an exception occurred while parsing.
     *
     * @param inputString
     * @return the value as an int, or {@link Integer#MIN_VALUE} if none could be parsed.
     */
    public static int parseInt(String inputString) {
        try {
            return Integer.parseInt(inputString);
        } catch (NumberFormatException e) {
            return Integer.MIN_VALUE;
        }
    }
}
