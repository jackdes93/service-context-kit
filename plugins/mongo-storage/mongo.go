package mongostorage

import (
	"context"
	"flag"
	"fmt"
	"image/color"
	"time"

	sctx "github.com/jackdes93/service-context-kit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"golang.org/x/tools/go/analysis/passes/defers"
)

type MongoOpt struct {
	uri             string
	dbName          string
	maxConn         int
	maxConnIdleTime int
}

type mongoStore struct {
	id     string
	prefix string
	logger sctx.Logger
	client *mongo.Database
	*MongoOpt
	*options.Credential
}

func NewMongoDB(id, prefix string) *mongoStore {
	return &mongoStore{
		id:         id,
		prefix:     prefix,
		MongoOpt:   new(MongoOpt),
		Credential: new(options.Credential),
	}
}

func (m *mongoStore) ID() string {
	return m.id
}

func (m *mongoStore) InitFlags() {
	prefix := m.prefix
	if m.prefix != "" {
		prefix += "_"
	}

	flag.StringVar(&m.uri, fmt.Sprintf("%s_uri", m.prefix), "", "uri connection string")
	flag.StringVar(&m.dbName, fmt.Sprintf("%s_db_name", m.prefix), "", "name of database")
	flag.StringVar(&m.Username, fmt.Sprintf("%s_user_name", m.prefix), "", "user name use for connect to database")
	flag.StringVar(&m.Password, fmt.Sprintf("%s_pwd", m.prefix), "", "password of username use for connect to database")
	flag.IntVar(&m.maxConn, fmt.Sprintf("%s_max_con", m.maxConn), 50, "number of max connection to database")
	flag.IntVar(&m.maxConnIdleTime, fmt.Sprintf("%s_idle_time", m.maxConnIdleTime), 1000, "number of max connection idle time")
}

func (m *mongoStore) isDisabled() bool {
	return m.uri == ""
}

func (m *mongoStore) Activate(_ sctx.ServiceContext) error {
	m.logger = sctx.GlobalLogger().GetLogger(m.id)
	clientOpts := options.Client().ApplyURI(m.uri)
	clientOpts.SetMaxConnecting(uint64(m.maxConn))
	clientOpts.SetMaxConnIdleTime(time.Duration(m.maxConnIdleTime * int(time.Second)))
	authMongo := options.Credential{
		AuthSource: m.dbName,
		Username:   m.Username,
		Password:   m.Password,
	}

	clientOpts.SetAuth(authMongo)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second.Seconds()*5))
	var err error
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		m.logger.Errorf("error connecting database %v", err)
	}

	defer cancel()

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		m.logger.Errorf("error ping to datbase have error %v", err)
		return err
	}

	m.client = client.Database(m.dbName)
	m.logger.Infof("connect success to database %v at uri: %v\n", m.dbName, m.uri)
	return nil
}

func (m *mongoStore) Stop() error {
	return nil
}

/* Functions process in mongo database */
func (m *mongoStore) CreateIndex(collectionName string, keys bson.D, unique bool) bool {
	mod := mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetUnique(unique),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	collection := m.client.Collection(collectionName)
	_, err := collection.Indexes().CreateOne(ctx, mod)
	if err != nil {
		m.logger.Errorf("create index of collection %s fail, error: %v", collectionName, err)
		return false
	}
	return true
}

func (m *mongoStore) InsertOne(ctx context.Context, colName string, data interface{}) error {
	_, err := m.client.Collection(colName).InsertOne(ctx, data)
	return err
}

func (m *mongoStore) InsertMany(ctx context.Context, colName string, data []interface{}) error {
	_, err := m.client.Collection(colName).InsertMany(ctx, data)
	return err
}
