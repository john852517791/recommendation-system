package com.wzy.business.service;

import com.wzy.business.model.domain.Product;
import com.wzy.business.model.recom.Recommendation;
import com.wzy.business.utils.Constant;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class ProductService {

    @Resource
    private MongoClient mongoClient;

    @Resource
    private ObjectMapper objectMapper;

    private MongoCollection<Document> productCollection;
    private MongoCollection<Document> averageProductsScoreCollection;

    private MongoCollection<Document> getProductCollection(){
        if(null == productCollection)
            productCollection = mongoClient.getDatabase(Constant.MONGODB_DATABASE).getCollection(Constant.MONGODB_PRODUCT_COLLECTION);
        return productCollection;
    }
//  获取数据库连接对象
    private MongoCollection<Document> getAverageProductsScoreCollection(){
        if(null == averageProductsScoreCollection)
            averageProductsScoreCollection = mongoClient.getDatabase(Constant.MONGODB_DATABASE).getCollection(Constant.MONGODB_AVERAGE_PRODUCTS_SCORE_COLLECTION);
        return averageProductsScoreCollection;
    }

//    获取商品推荐列表
    public List<Product> getRecommendProducts(List<Recommendation> recommendations){
        List<Integer> ids = new ArrayList<>();
        for (Recommendation rec: recommendations) {
            ids.add(rec.getProductId());
        }
        return getProducts(ids);
    }

//    查询数据库获取商品信息
    private List<Product> getProducts(List<Integer> productIds){
        FindIterable<Document> documents = getProductCollection().find(Filters.in("productId", productIds));
        List<Product> products = new ArrayList<>();
        for (Document document: documents) {
            products.add(documentToProduct(document));
        }
        return products;
    }

//    将json格式序列化为product实体对象
    private Product documentToProduct(Document document){
        Product product = null;
        try{
            product = objectMapper.readValue(JSON.serialize(document), Product.class);
            Document score = getAverageProductsScoreCollection().find(Filters.eq("productId", product.getProductId())).first();
            if(null == score || score.isEmpty())
                product.setScore(0D);
            else
                product.setScore((Double) score.get("avg",0D));
        }catch (IOException e) {
            e.printStackTrace();
        }
        return product;
    }

//    根据商品ID查询商品信息
    public Product findByProductId(int productId) {
        Document document = getProductCollection().find(new Document("productId", productId)).first();
        if(document == null || document.isEmpty())
            return null;
        return documentToProduct(document);
    }

//    根据搜索栏输入的关键字对商品进行模糊查询
    public List<Product> findByProductName(String name) {
//        FindIterable<Document> documents = getProductCollection().find(new Document("name", name));
        FindIterable<Document> documents = getProductCollection().find(Filters.regex("name", name));
        List<Product> products = new ArrayList<>();
        for (Document document: documents) {
            products.add(documentToProduct(document));
        }
        return products;
    }

}
