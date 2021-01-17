package go_mongoDB_tool

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (conn *MongoConnection) CreateCollection(ctx context.Context, name string, opts ...*options.CreateCollectionOptions) error {
	db, err := conn.GetMongoDB()
	if err != nil {
		return err
	}
	defer conn.PutMongoDB(db)
	err = db.CreateCollection(ctx, name, opts...)
	if err != nil {
		return err
	}
	return nil
}

func (conn *MongoConnection) DropCollection(ctx context.Context, name string) error {
	db, err := conn.GetMongoDB()
	if err != nil {
		return err
	}
	defer conn.PutMongoDB(db)
	err = db.Collection(name).Drop(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (conn *MongoConnection) Aggregate(ctx context.Context, name string, pipeline interface{}, opts ...*options.AggregateOptions) ([]map[string]interface{}, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return nil, err
	}
	defer conn.PutMongoDB(db)
	cur, err := db.Collection(name).Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	results := make([]map[string]interface{}, 0)
	for cur.Next(ctx) {
		var result map[string]interface{}
		if err = cur.Decode(&result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (conn *MongoConnection) InsertOne(ctx context.Context, name string, document interface{}, opts ...*options.InsertOneOptions) (string, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return "", err
	}
	defer conn.PutMongoDB(db)
	insertResult, err := db.Collection(name).InsertOne(ctx, document, opts...)
	if err != nil {
		return "", err
	}
	return insertResult.InsertedID.(primitive.ObjectID).String(), nil
}

func (conn *MongoConnection) InsertMulti(ctx context.Context, name string, documents []interface{}, opts ...*options.InsertManyOptions) ([]interface{}, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return nil, err
	}
	defer conn.PutMongoDB(db)
	insertManyResult, err := db.Collection(name).InsertMany(ctx, documents, opts...)
	if err != nil {
		return nil, err
	}
	return insertManyResult.InsertedIDs, nil
}

func (conn *MongoConnection) FindOne(ctx context.Context, name string, filter interface{}, opts ...*options.FindOneOptions) (primitive.M, error) {
	var result primitive.M
	db, err := conn.GetMongoDB()
	if err != nil {
		return nil, err
	}
	defer conn.PutMongoDB(db)
	if err = db.Collection(name).FindOne(ctx, filter, opts...).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func (conn *MongoConnection) FindMulti(ctx context.Context, name string, filter interface{}, opts ...*options.FindOptions) ([]primitive.M, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return nil, err
	}
	defer conn.PutMongoDB(db)
	cur, err := db.Collection(name).Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	results := make([]primitive.M, 0)
	for cur.Next(ctx) {
		var result primitive.M
		if err = cur.Decode(&result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (conn *MongoConnection) FindOneAndReplace(ctx context.Context, name string, filter interface{},
	replacement interface{}, opts ...*options.FindOneAndReplaceOptions) (primitive.M, error) {
	var result primitive.M
	db, err := conn.GetMongoDB()
	if err != nil {
		return nil, err
	}
	defer conn.PutMongoDB(db)
	if err = db.Collection(name).FindOneAndReplace(ctx, filter, replacement, opts...).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func (conn *MongoConnection) FindOneAndUpdate(ctx context.Context, name string, filter interface{},
	update interface{}, opts ...*options.FindOneAndUpdateOptions) (primitive.M, error) {
	var result primitive.M
	db, err := conn.GetMongoDB()
	if err != nil {
		return nil, err
	}
	defer conn.PutMongoDB(db)
	if err = db.Collection(name).FindOneAndUpdate(ctx, filter, update, opts...).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func (conn *MongoConnection) UpdateOne(ctx context.Context, name string, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (int64, int64, int64, interface{}, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return 0, 0, 0, nil, err
	}
	defer conn.PutMongoDB(db)
	updateOneResult, err := db.Collection(name).UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	return updateOneResult.MatchedCount, updateOneResult.ModifiedCount, updateOneResult.UpsertedCount, updateOneResult.UpsertedID, nil
}

func (conn *MongoConnection) UpdateMulti(ctx context.Context, name string, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (int64, int64, int64, interface{}, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return 0, 0, 0, nil, err
	}
	defer conn.PutMongoDB(db)
	updateMultiResult, err := db.Collection(name).UpdateMany(ctx, filter, update, opts...)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	return updateMultiResult.MatchedCount, updateMultiResult.ModifiedCount, updateMultiResult.UpsertedCount, updateMultiResult.UpsertedID, nil
}

func (conn *MongoConnection) DeleteOne(ctx context.Context, name string, filter interface{}, opts ...*options.DeleteOptions) (int64, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return 0, err
	}
	defer conn.PutMongoDB(db)
	deleteResult, err := db.Collection(name).DeleteOne(ctx, filter, opts...)
	if err != nil {
		return 0, err
	}
	return deleteResult.DeletedCount, nil
}

func (conn *MongoConnection) DeleteMulti(ctx context.Context, name string, filter interface{}, opts ...*options.DeleteOptions) (int64, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return 0, err
	}
	defer conn.PutMongoDB(db)
	deleteResult, err := db.Collection(name).DeleteMany(ctx, filter, opts...)
	if err != nil {
		return 0, err
	}
	return deleteResult.DeletedCount, nil
}

func (conn *MongoConnection) CreateOneIndex(ctx context.Context, name string, model mongo.IndexModel) (string, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return "", err
	}
	defer conn.PutMongoDB(db)
	opts := options.CreateIndexes().SetMaxTime(5 * time.Second)
	indexName, err := db.Collection(name).Indexes().CreateOne(ctx, model, opts)
	if err != nil {
		return "", err
	}
	return indexName, nil
}

func (conn *MongoConnection) CreateMultiIndexes(ctx context.Context, name string, models []mongo.IndexModel) ([]string, error) {
	db, err := conn.GetMongoDB()
	if err != nil {
		return nil, err
	}
	defer conn.PutMongoDB(db)
	opts := options.CreateIndexes().SetMaxTime(5 * time.Second)
	indexNames, err := db.Collection(name).Indexes().CreateMany(ctx, models, opts)
	if err != nil {
		return nil, err
	}
	return indexNames, nil
}