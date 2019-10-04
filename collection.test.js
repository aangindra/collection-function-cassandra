const initCollection = require('./collection');
const collection = initCollection();
const initCassandra = require("./cassandra");
const cassandra = initCassandra();
const uuidV4 = require("uuid/v4");

expect.extend({
  toContainObject(received, argument) {
    const pass = this.equals(received,
      expect.arrayContaining([
        expect.objectContaining(argument)
      ])
    );

    if (pass) {
      return {
        message: () => (`expected ${this.utils.printReceived(received)} not to contain object ${this.utils.printExpected(argument)}`),
        pass: true
      }
    } else {
      return {
        message: () => (`expected ${this.utils.printReceived(received)} to contain object ${this.utils.printExpected(argument)}`),
        pass: false
      }
    }
  }
});

const newData = {
  id: uuidV4(),
  firstname: "Indra Aang",
  birthday: "09-08-2000",
  nationality: "Canada"
};

test('Fungsi insertOne menghasilkan success', async () => {

  await collection("table1").insertOne(newData);

  let results = await cassandra.execute(`SELECT * FROM test1.table1`);
  results = results.rows;

  for (result of results) {
    expect(result.firstname).toEqual(newData.firstname);
  }

});

test("Fungsi findOne menghasilkan object data", async () => {

  let data = await collection("table1").findOne({
    firstname: "Indra Aang",
    nationality: "Canada"
  }, {
      projection: {
        firstname: 1
      }
    });
  expect(data.firstname).toEqual(newData.firstname);

});

test("Fungsi count menghasilkan nilai lebih besar dari 0", async () => {
  await expect(collection("table1").find({}, {}).count()).resolves.toBeGreaterThan(0);
});
// test('testing collection find toArray', async () => {
//   await expect(collection("table1").find().toArray()).resolves.toContainObject(newData);
// });
// test('testing collection', async () => {
//   //const dbData = await collection("table1").find().toArray();
//   // for(const foo of dbData){

//   //   expect(foo).toEqual(newData);
//   // }

//   // await expect(newData).toIncludePartial({ firstname: "Aang" });

//   expect(dbData).toEqual(
//     expect.arrayContaining([
//       expect.objectContaining(newData)
//     ])
//   );


// });

// test('Fungsi insertOne menghasilkan success', async () => {
//   //await cassandra.execute(`INSERT INTO test1.table1 JSON '${JSON.stringify(newData)}'`);
//   await expect(collection("table1").insertOne(newData)).resolves.toEqual("success");
//   let results = await cassandra.execute(`SELECT * FROM test1.table1`);
//   results = results.rows;
//   //await cassandra.execute(`INSERT INTO test1.table1 JSON '${JSON.stringify(newData)}'`);
//   for(result of results){
//     expect(result.firstname).toEqual(newData.firstname);
//   }
//   // expect(results).toEqual(
//   //   expect.arrayContaining([
//   //     expect.objectContaining(newData)
//   //   ])
//   // );
//   // for (let result of results) {
//   //   expect(result).toEqual(newData);
//   // }
// });