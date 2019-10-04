require("dotenv").config();
const initCassandra = require("./cassandra");
const initCollection = require("./collection");
const uuidV4 = require("uuid/v4");
const faker = require('faker/locale/id_ID');

const start = async () => {

    const collection = initCollection();
    const cassandra = initCassandra();

    const dummy = {
        id: uuidV4(),
        firstname: faker.name.findName(),
        birthday: new Date(),
        nationality: "Indonesia"
    };
    //await collection("table1").insertOne(dummy);
    //let results = await collection("table1").find().limit(1).toArray();
    // let results = await cassandra.execute(`SELECT * FROM test1.table1`);
    let results = await collection("table1").findOne({
        firstname: "Indra Aang",
        nationality: "Canada"
    
    });
   
    console.log(results);
    console.log("query berhasil!");
}

start();
