package recommendation.mainFun;
import recommendation.dataFactory.Factory;
import recommendation.dataFactory.Movie;
import recommendation.dataFactory.Redis;
import recommendation.dataFactory.User;
import recommendation.deal.Utility;

import java.util.*;


public class UserRecommend {

    private static boolean DATA_SOURCE_REDIS =false;
    private static boolean IS_LOAD_USER_FEATURE_FROM_REDIS = false;
    public static List<Movie> getRecList(int userId, int size, String model){
        User user = Factory.getInstance().getUserById(userId);
        if (null == user){
            return new ArrayList<>();
        }
        List<Movie> candidates = recallAndScore.multipleRetrievalCandidates(userId);

        if (DATA_SOURCE_REDIS){
            String userEmbKey = "uEmb:" + userId;
            String userEmb = Redis.getInstance().get(userEmbKey);
            if (null != userEmb){
                user.setEmb(Utility.parseEmbStr(userEmb));
            }
        }

        if (IS_LOAD_USER_FEATURE_FROM_REDIS){
            String userFeaturesKey = "uf:" + userId;
            Map<String, String> userFeatures = Redis.getInstance().hgetAll(userFeaturesKey);
            if (null != userFeatures){
                user.setUserFeatures(userFeatures);
            }
        }

        List<Movie> rankedList = ranker(user, candidates, model);
        List<Movie> finalList = new LinkedList<Movie>();
        finalList = rankedList;
        if (rankedList.size() > size){
            finalList =  rankedList.subList(0, size);
        }
        for(Movie a : finalList){
            if((int) a.getScores()==-1){
                Random ran = new Random();
                a.setScores(ran.nextDouble()*100);
            }
        }
        finalList.sort(Comparator.comparing(Movie::getScores).reversed());

        return finalList;
    }

    public static List<Movie> ranker(User user, List<Movie> candidates, String model){
        HashMap<Movie, Double> candidateScoreMap = new HashMap<>();

        switch (model){
            case "embedding":
                for (Movie candidate : candidates){
                    double similarity = calculateEmbSimilarScore(user, candidate);
                    candidate.setScores(similarity);
                    candidateScoreMap.put(candidate, similarity);
                }
                break;
            default:
                for (int i = 0 ; i < candidates.size(); i++){
                    candidateScoreMap.put(candidates.get(i), (double) (candidates.size() - i));
                }
        }

        List<Movie> rankedList = new ArrayList<>();
        candidateScoreMap.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(m -> rankedList.add(m.getKey()));

        return rankedList;
    }

    public static double calculateEmbSimilarScore(User user, Movie candidate){
        if (null == user || null == candidate || null == user.getEmb()){
            return -1;
        }
        return user.getEmb().calculateSimilarity(candidate.getEmb());
    }

}
