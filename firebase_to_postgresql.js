const { initializeApp } = require("firebase/app");
const { getDatabase, ref, get, child } = require("firebase/database"); 
const { Client } = require("pg");

const firebaseConfig = {
    //
};

const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

const client = new Client("//");

init();

async function init() {
  try {
    await client.connect();
    let pgRes = await client.query("SELECT $1::text as message", [
      "Connected to PostgreSQL!",
    ]);
    console.log(pgRes.rows[0].message);

    pgRes = await client.query(`CREATE TABLE IF NOT EXISTS answer (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        question TEXT NOT NULL,
        answer TEXT NOT NULL,
        email TEXT NOT NULL,
        timestamp BIGINT NOT NULL,
        tags VARCHAR(150)[]
    )`);
    console.log("PostgreSQL table 'answer' created or already exists.");

    console.log("\nFetching data from Firebase Realtime Database...");
    let data = await getDataFromRealtimeDB("data");
    
    if (data) { 
      let arr = Object.keys(data);
      console.log(`Found ${arr.length} items to insert into PostgreSQL.`);

      for (const key of arr) {
        try {
          let {
            question,
            answer,
            timestamp,
            email = "bindurana46@gmail.com",
            tags,
          } = data[key];

          if (data[key].subject) {
            tags = [data[key].class, data[key].subject, data[key].chapter];
          }

          if (!Array.isArray(tags)) {
            tags = []; 
          }

          let insertRes = await client.query(
            "INSERT INTO answer (question, answer, timestamp, email, tags) VALUES ($1, $2, $3, $4, $5)",
            [question, answer, timestamp, email, tags]
          );
        } catch (err) {
          console.error(`Error inserting data for key ${key}:`, err);
        }
      }
      console.log("All available data from Realtime Database processed for insertion.");
    } else {
      console.log("No data fetched from Realtime Database to insert.");
    }

  } catch (error) {
    console.error("An error occurred during initialization or database operations:", error);
  } finally {
    if (client && client._connected) { 
      await client.end();
      console.log("Disconnected from PostgreSQL.");
    } else {
        console.log("PostgreSQL client was already disconnected or not connected.");
    }
  }
}

async function getDataFromRealtimeDB(path) {
  try {
    const dbRef = ref(db);
    
    const snapshot = await get(child(dbRef, path));

    if (snapshot.exists()) {
      console.log(`Data found at path '${path}':`);
      return snapshot.val(); 
    } else {
      console.log(`No data available at path '${path}'.`);
      return null;
    }
  } catch (error) {
    console.error(`Error getting data from Realtime Database path '${path}':`, error);
    return null; 
  }
}