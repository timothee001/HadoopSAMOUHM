package relativefrequency;

class Pair implements Comparable<Pair> {
    double relativeFrequency;
    private String word;
    private String neighbor;

    Pair(double relativeFrequency, String word, String neighbor) {
        this.relativeFrequency = relativeFrequency;
        this.word = word;
        this.neighbor = neighbor;
    }
    
    public void setWord(String word){
    	this.word=word;
    }
    
    public void setNeighbor(String neighbor){
    	this.neighbor=neighbor;
    }
    
    public String getWord(){
    	return this.word;
    }
    
    public String getNeighbor(){
    	return this.neighbor;
    }


    @Override
    public int compareTo(Pair pair) {
        if (this.relativeFrequency > pair.relativeFrequency) {
            return 1;
        } else {
            return -1;
        }
    }
}