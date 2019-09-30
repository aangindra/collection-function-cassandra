const initCassandra = require("./cassandra");

const initCollection = () => {
  const cassandra = initCassandra();


  return (tableName) => {
    const limit = query => (number) => {
      query.limit = number;
      const theRealSort = sort(query);
      const theRealSkip = skip(query);
      const theRealToArray = toArray(query);
      return {
        sort: theRealSort,
        skip: theRealSkip,
        toArray: theRealToArray
      };
    };

    const skip = query => (number) => {
      query.skip = number;
      const theRealSort = sort(query);
      const theRealLimit = limit(query);
      const theRealToArray = toArray(query);
      return {
        sort: theRealSort,
        limit: theRealLimit,
        toArray: theRealToArray
      }
    };

    const sort = query => (sortQuery) => {
      query.sort = sortQuery;
      const theRealLimit = limit(query);
      const theRealSkip = skip(query);
      const theRealToArray = toArray(query);
      return {
        limit: theRealLimit,
        skip: theRealSkip,
        toArray: theRealToArray
      }
    }
    const count = query => async () => {
      let cql = `SELECT COUNT(*) FROM test1.${tableName}`;
      let results = await cassandra.execute(cql);
      return parseInt(results.rows[0].count);
    }
    const toArray = query => async () => {
      let selectedProjection;
      if (query && query.projection !== undefined) {

        if (Object.keys(query.projection).length) {
          let objects = query.projection;
          let results = [];

          results = Object.keys(objects).filter(e => {
            if (objects[e] === 1) {
              return true;
            } else {
              return false;
            }
          });
          selectedProjection = String(results);
        } else {
          selectedProjection = "*"
        }
      } else {
        console.log("not projection")
        selectedProjection = "*";
      }
      let cql = `SELECT ${selectedProjection} FROM test1.${tableName}`;
      let results = await cassandra.execute(cql);
      return results.rows;
    }

    const insertOne = async (data) => {
      await cassandra.execute(`INSERT INTO test1.${tableName} JSON '${JSON.stringify(data)}'`);
      return "success";
    }
    const find = (filter, option) => {
      let query = { where: null, projection: option ? option.projection : null, skip: null, limit: null, sort: null, };
      const theRealLimit = limit(query);
      const theRealSort = sort(query);
      const theRealSkip = skip(query);
      const theRealToArray = toArray(query);
      const theRealCount = count(query);

      return {
        limit: theRealLimit,
        sort: theRealSort,
        skip: theRealSkip,
        toArray: theRealToArray,
        count: theRealCount
      }
    }
    return { find, insertOne }
  }
  //return { find,insertOne }
};

module.exports = initCollection;