const { MongoClient } = require("mongodb");

let db;

const connect = async () => {
  const client = new MongoClient(process.env.MONGOURL);

  try {
    await client.connect(process.env.MONGOURL);
    console.log("Connected to MongoDB!");
    db = client.db("RQ_Analytics");
  } catch (err) {
    console.error("connection error : ", err);
    process.exit(1);
  }
};

const getDb = () => {
  if (!db) {
    throw new Error("Database not initialized");
  }
  return db;
};

module.exports = { connect, getDb };
