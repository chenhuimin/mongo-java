package com.laanto.it.mongodb;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
//import org.bson.Document;
//
//import com.mongodb.Block;
//import com.mongodb.MongoClient;
//import com.mongodb.MongoClientURI;
//import com.mongodb.client.FindIterable;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;

/**
 * Created by user on 2016/2/2.
 */
public class MongoDBJDBC implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4577061894027656445L;

	public static Date getSearchDate(int addDays) {
		Calendar currentCal = Calendar.getInstance();
		currentCal.set(Calendar.HOUR_OF_DAY, 0);
		currentCal.set(Calendar.MINUTE, 0);
		currentCal.set(Calendar.SECOND, 0);
		currentCal.set(Calendar.MILLISECOND, 0);
		currentCal.add(Calendar.DAY_OF_MONTH, addDays);
		return currentCal.getTime();

	}

	public static MongoCollection<Document> getMongoCollection() {
		// 连接到 mongodb 服务
		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://172.16.91.36:22117"));
		// 连接到数据库
		MongoDatabase db = mongoClient.getDatabase("user-behavior");
		// System.out.println("Connect to database successfully");
		return db.getCollection("behavior");
	}

	public static void query(MongoCollection<Document> collection, Document query) {
		FindIterable<Document> iterable = collection.find(query).projection(fields(include("productId", "uv", "pv"), excludeId()))
				.sort(orderBy(descending("uv", "pv"))).limit(10);
		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document.toString());
			}
		});
		long count = collection.count(query);
		System.out.println("count=" + count);
	}

	public static void update(MongoCollection<Document> collection, Document query) {
		// remove a field using $unset with mongo-java driver
		collection.updateMany(query, new Document("$unset", new Document("userId", "").append("userName", "")));
		long count = collection.count(query);
		System.out.println("count=" + count);
	}

	public static void main(String[] args) throws Exception {

		MongoCollection collection = getMongoCollection();
		Document query = new Document("appName", "star-product").append("userId", "-1");
		query(collection, query);

		//
		// FindIterable<Document> iterable = collection.find().limit(10);
		// iterable.forEach(new Block<Document>() {
		// @Override
		// public void apply(final Document document) {
		// System.out.println(document.toString());
		// // System.out.println("id=" + document.getObjectId("_id") + ",appName=" + document.getString("appName") + ",createTime="
		// // + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss").format(document.getDate("createTime")));
		// }
		// });
		//
		// FindIterable<Document> iterable = collection.find(new BasicDBObject("_id", new ObjectId("56ea1418149b3f00136fff35")));
		//
		// iterable.forEach(new Block<Document>() {
		// @Override
		// public void apply(final Document document) {
		// System.out.println("appName=" + document.getString("appName") + ",createTime="
		// + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss").format(document.getDate("createTime")));
		// }
		// });

		// QueryBuilder query = QueryBuilder.start("createTime").greaterThanEquals(startDate).lessThan(endDate);
		//
		// DBCursor cursor = collection.find(query.get()).sort(new BasicDBObject("createTime", 1));
		// while (cursor.hasNext()) {
		// System.out.println(cursor.next());
		// }

		// document.putAll(query.get());
		// DBCursor cursor = collection.find(query.get()).sort(new BasicDBObject("createTime", -1)).limit(10);
		// while (cursor.hasNext()) {
		// System.out.println(cursor.next());
		//
		// int count = collection.find(query.get()).count();
		// System.out.println(count);
		// System.out.println(TimeZone.getDefault().getDisplayName());

		// DBCursor cursor = collection.find(document).sort(new BasicDBObject("createTime", 1));

		// Document doc = new Document("name", "MongoDB")
		// .append("type", "database")
		// .append("count", 1)
		// .append("info", new Document("x", 203).append("y", 102));
		// collection.insertOne(doc);

		// List<Document> documents = new ArrayList<Document>();
		// for (int i = 0; i < 100; i++) {
		// documents.add(new Document("i", i));
		// }
		// collection.insertMany(documents);
		//
		// Document myDoc = collection.find().first();
		// System.out.println(myDoc.toJson());

		// MongoCursor<Document> cursor = collection.find().iterator();
		// try {
		// while (cursor.hasNext()) {
		// System.out.println(cursor.next().toJson());
		// }
		// } finally {
		// cursor.close();
		// }

		// Document myDoc = collection.find(eq("i", 71)).first();
		// System.out.println(myDoc.toJson());

		// Block<Document> printBlock = new Block<Document>() {
		// public void apply(Document document) {
		// System.out.println(document.toJson());
		// }
		// };
		// collection.find(gt("i", 50)).forEach(printBlock);

		// Document myDoc = collection.find(exists("i")).sort(descending("i")).first();
		// System.out.println(myDoc.toJson());

		// Document myDoc = collection.find().projection(exclude("_id","name", "type")).first();
		// System.out.println(myDoc.toJson());

		// UpdateResult updateResult = collection.updateOne(eq("i", 10), new Document("$set", new Document("i", 110)));
		// System.out.println(updateResult.toString());
		// UpdateResult updateResult = collection.updateMany(lt("i", 100),
		// new Document("$inc", new Document("i", 100)));
		// System.out.println(updateResult.getModifiedCount());

		// DeleteResult deleteResult = collection.deleteOne(eq("i", 110));
		// System.out.println(deleteResult.getDeletedCount());

		// DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		// Calendar currentCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		// System.out.println("currentCal=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(currentCal.getTime()));
		// currentCal.set(Calendar.HOUR_OF_DAY, 0);
		// currentCal.set(Calendar.MINUTE, 0);
		// currentCal.set(Calendar.SECOND, 0);
		// currentCal.set(Calendar.MILLISECOND, 0);

		// Date endDate = formatter.parse(formatter.format(currentCal.getTime()));
		// currentCal.add(Calendar.DAY_OF_MONTH, -1);
		// Date startDate = formatter.parse(formatter.format(currentCal.getTime()));

		// System.out.println("startDate=" + formatter.parse("2016-03-14").getTime());
		// System.out.println("endDate=" + formatter.parse("2016-03-15").getTime());
		// long startDate = currentCal.getTime().getTime();
		// String test = String.format("{\"createTime\":{\"$gte\":%d,\"$lt\":%d}}", startDate, endDate);
		// java.util.Date utilDate = new java.util.Date();
		// java.sql.Timestamp sqlDate = new java.sql.Timestamp(utilDate.getTime());

		// System.out.println("startDate=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(startDate));
		// System.out.println("endDate=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(endDate));

	}

}
