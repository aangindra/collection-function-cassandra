require("dotenv").config();
const initCassandra = require("./cassandra");
const initCollection = require("./collection");
const uuidV4 = require("uuid/v4");

const start = async () => {

    const collection = initCollection();

    const dummy = {
        id: uuidV4(),
        firstname: "Farel Yandi",
        birthday: new Date(),
        nationality: "England"
    };
    // await collection("table1").insertOne(dummy);
    let results = await collection("table1").find({},{
        // projection:{
        //     // id: 0,
        //     // firstname:1
           
        // }
    })
    .count()
    console.log(results);

    console.log("oke........");
}

start();