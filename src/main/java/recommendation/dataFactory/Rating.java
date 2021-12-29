package recommendation.dataFactory;

public class Rating {
    int movieId;
    int userId;
    float score;
    long timestamp;

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String toString(){
        return "\n" + "movieId: " + movieId + " userId: " + userId + " score: " + score + " timestamp: " + timestamp;
    }

}
