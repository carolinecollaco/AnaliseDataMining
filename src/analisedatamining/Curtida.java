package analisedatamining;

class Curtida {
    public final String person;
    public final int rating;
    public final String bandUri;

    public Curtida(String person, int rating, String bandUri) {
        this.person = person;
        this.rating = rating;
        this.bandUri = bandUri;
    }

    @Override
    public String toString() {
        String asString = "";
        asString += this.person;
        asString += " "+ this.rating;
        asString += " "+  this.bandUri;
        return asString;
    }
    
    
}
