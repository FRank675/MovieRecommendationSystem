package recommendation.calcuate;

import java.util.ArrayList;

public class Embedding {
    //使用一个vector来存储embedding相关数据
    ArrayList<Float> embVector;

    public Embedding(){
        this.embVector = new ArrayList<>();
    }

    public Embedding(ArrayList<Float> embVector){
        this.embVector = embVector;
    }

    public void addDim(Float element){
        this.embVector.add(element);
    }

    public ArrayList<Float> getEmbVector() {
        return embVector;
    }

    public void setEmbVector(ArrayList<Float> embVector) {
        this.embVector = embVector;
    }

    /**
     * 计算两个向量的余弦相似度
     * 夹角越小，余弦值越大，相似度越高
     */
    public double calculateSimilarity(Embedding otherEmb){
        if (null == embVector || null == otherEmb || null == otherEmb.getEmbVector()
                || embVector.size() != otherEmb.getEmbVector().size()){

            return -1;
        }
        double dotProduct = 0;
        double denominator1 = 0;
        double denominator2 = 0;

        for (int i = 0; i < embVector.size(); i++){
            dotProduct += embVector.get(i) * otherEmb.getEmbVector().get(i);
            denominator1 += embVector.get(i) * embVector.get(i);
            denominator2 += otherEmb.getEmbVector().get(i) * otherEmb.getEmbVector().get(i);
        }
        double d = dotProduct / (Math.sqrt(denominator1) * Math.sqrt(denominator2));
        double a  = d*100;
        return a;
    }
}
