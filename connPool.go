package go_mongoDB_tool

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"sync"
	"time"
)

type Config struct {
	Addr    string `default:"mongodb://localhost:27017"`
	MaxPool int    `default:"100"`
	DBName  string `default:""`
	// TODO: add credentials
}

type MongoConnection struct {
	mu         sync.Mutex
	pool       chan *mongo.Database
	poolLength int
	Errors     chan error
	config     *Config
}

func NewMongoConnection(dbConfig *Config) *MongoConnection {
	return &MongoConnection{
		pool:       make(chan *mongo.Database, dbConfig.MaxPool),
		poolLength: 0,
		Errors:     make(chan error),
		config:     dbConfig,
	}
}

func (conn *MongoConnection) GetMongoDB() (*mongo.Database, error) {
	defer func() {
		if rec := recover(); rec != nil {
			conn.Errors <- errors.New(fmt.Sprintf("%v", rec))
		}
	}()
	if conn.config.MaxPool < 1 {
		conn.config.MaxPool = 1
	}
	conn.mu.Lock()
	if len(conn.pool) == 0 && conn.poolLength < conn.config.MaxPool {
		db, err := conn.NewMongoDB()
		if err == nil {
			conn.poolLength++
		}
		conn.mu.Unlock()
		return db, err
	}
	conn.mu.Unlock()
	for db := range conn.pool {
		if err := db.Client().Ping(context.TODO(), nil); err == nil {
			return db, nil
		}
	}
	return conn.NewMongoDB() // TODO: check quantity of connections
}

func (conn *MongoConnection) PutMongoDB(db *mongo.Database) error {
	if db == nil {
		return errors.New("nil database cannot be putted")
	}
	if err := db.Client().Ping(context.TODO(), nil); err != nil {
		return err
	}
	go func() {
		conn.pool <- db
	}()
	return nil
}

func (conn *MongoConnection) CloseMongoDB() (err error) {
	close(conn.pool)
	for db := range conn.pool {
		err = db.Client().Disconnect(context.TODO())
		if err != nil {
			return err
		}
	}
	return
}

func (conn *MongoConnection) NewMongoDB() (*mongo.Database, error) {
	// Create client
	mongoOptions := options.Client().ApplyURI(conn.config.Addr)
	mongoOptions = mongoOptions.SetConnectTimeout(time.Minute)
	mongoOptions.SetMaxPoolSize(uint64(conn.config.MaxPool))
	mongoOptions.SetReadPreference(readpref.Secondary(readpref.WithMaxStaleness(500*time.Second)))
	client, err := mongo.NewClient(mongoOptions)
	if err != nil {
		return nil, err
	}
	// Create connect
	err = client.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	// Check the connection
	err = client.Ping(context.TODO(), readpref.Secondary(readpref.WithMaxStaleness(500*time.Second)))
	if err != nil {
		return nil, err
	}
	return client.Database(conn.config.DBName), nil
}

func (conn *MongoConnection) DBAsync(wg *sync.WaitGroup, f func(db *mongo.Database, errChan chan error) error) (err error) {
	defer func() {
		if err != nil {
			conn.Errors <- err
		}
	}()
	if wg != nil {
		defer wg.Done()
	}
	db, err := conn.GetMongoDB()
	if err != nil {
		return
	}
	if db == nil {
		err = errors.New("cannot get db")
		return
	}
	defer func() {
		conn.PutMongoDB(db)
	}()
	err = f(db, conn.Errors)
	return
}
