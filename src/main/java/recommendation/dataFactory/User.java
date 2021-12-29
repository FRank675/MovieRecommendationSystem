package recommendation.dataFactory;
import recommendation.calcuate.Embedding;


import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class User {
    int userId;
    double averageRating = 0;
    double highestRating = 0;
    double lowestRating = 5.0;
    int ratingCount = 0;

    List<Rating> ratings;

    Embedding emb;

    Map<String, String> userFeatures;

    List<Integer> historyMovie  = new LinkedList<>();;
    public User(){
        this.ratings = new ArrayList<>();
        this.emb = null;
        this.userFeatures = null;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public List<Rating> getRatings() {
        return ratings;
    }

    public void setRatings(List<Rating> ratings) {
        this.ratings = ratings;
    }

    public void addRating(Rating rating) {
        this.ratings.add(rating);
        this.averageRating = (this.averageRating * ratingCount + rating.getScore()) / (ratingCount + 1);
        if (rating.getScore() > highestRating){
            highestRating = rating.getScore();
        }

        if (rating.getScore() < lowestRating){
            lowestRating = rating.getScore();
        }

        ratingCount++;
    }

    public Embedding getEmb() {
        return emb;
    }

    public void setEmb(Embedding emb) {
        this.emb = emb;
    }

    public Map<String, String> getUserFeatures() {
        return userFeatures;
    }

    public void setUserFeatures(Map<String, String> userFeatures) {
        this.userFeatures = userFeatures;
    }

    public String toString(){
        return "userId: " + userId + "\naverageRating: " + averageRating + "\nhighestRating: " + highestRating
                + "\nlowestRating: " + lowestRating + "\nratings: " + ratings.toString();
    }
    public  void addHistory(int movieId){
        historyMovie.add(movieId);
    }

    public  List<Integer> getHistory(){
        return historyMovie;
    }
}
